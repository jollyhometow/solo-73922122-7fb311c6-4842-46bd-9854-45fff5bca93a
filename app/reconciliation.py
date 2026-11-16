"""船方清单对账服务。

正确性设计
==========
1. **受理即持久化**：先落 PENDING 核对单与范围（事务一），再做匹配计算并把
   结论、依据、对应在同一事务写入（事务二）。任一步崩溃都留下可恢复的
   PENDING 单，启动恢复或下次 GET 时幂等重算——结论与完整依据原子生效。

2. **范围快照 + 主动失效双保险**：
   - 受理计算时把范围内已有净重明细的 (partition, offset) 位置集合存为快照；
   - 消费入仓 / 补正入仓在**写明细的同一事务**内调用
     :func:`expire_jobs_for_position`，凡范围覆盖该位点且仍有效的核对单
     （含在途 PENDING）立即置 EXPIRED；
   - 读取 / 发布时再次比对当前位置集合与快照，兜住任何边缘时序。
   范围外的位点不命中任何范围，天然不影响核对单。

3. **不加写锁、不阻塞消费**：匹配计算只读净重表并写自己的核对单表；
   入仓路径只额外执行一条按索引的批量 UPDATE，行锁仅落在核对单表，
   净重明细表的消费/补正写路径完全不被对账 API 阻塞。

4. **发布守卫**：`SELECT ... FOR UPDATE` 串行化并发发布（PostgreSQL 行锁），
   发布前强制快照复核；过期单一律拒绝发布，也不会提供当前通过结论。
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.config import get_settings
from app.matching import find_two_matchings
from app.models import (
    ACTIVE_RECON_STATUSES,
    NetWeightRecord,
    ReconciliationBasis,
    ReconciliationJob,
    ReconciliationMatchLine,
    ReconciliationRange,
    ReconciliationStatus,
)

logger = logging.getLogger(__name__)


class ReconciliationError(Exception):
    """对账请求被拒绝（输入非法或状态机不允许的操作）。"""

    def __init__(self, code: str, detail: str, status_code: int = 422) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.status_code = status_code


# --------------------------------------------------------------------------
# 入仓路径调用的失效钩子（与净重明细写入同一事务）
# --------------------------------------------------------------------------

def expire_jobs_for_position(
    session: Session, *, topic: str, partition: int, offset: int
) -> int:
    """把范围覆盖给定源位点的有效核对单标记为 EXPIRED。

    由净重明细的新增（消费）与补正入仓在**同一事务**内调用，
    因此“入仓已提交”与“核对单已失效”原子可见，不存在漏判窗口。
    返回被置失效的单据数。
    """
    covered_job_ids = select(ReconciliationRange.job_id).where(
        ReconciliationRange.source_partition == partition,
        ReconciliationRange.offset_start <= offset,
        ReconciliationRange.offset_end > offset,
    )
    result = session.execute(
        update(ReconciliationJob)
        .where(ReconciliationJob.id.in_(covered_job_ids))
        .where(ReconciliationJob.topic == topic)
        .where(ReconciliationJob.status.in_(ACTIVE_RECON_STATUSES))
        .values(
            status=ReconciliationStatus.EXPIRED,
            passed=None,
            expired_at=func.now(),
            expire_reason=(
                f"范围内源位点 partition={partition} offset={offset} "
                "发生新增/补正入仓，结论作废，须重新核对"
            ),
        )
    )
    if result.rowcount:
        logger.warning(
            "expired %s reconciliation job(s) by in-range insert at %s-%s:%s",
            result.rowcount, topic, partition, offset,
        )
    return result.rowcount or 0


# --------------------------------------------------------------------------
# 输入校验
# --------------------------------------------------------------------------

def validate_reconcile_request(payload) -> None:
    """对受理请求做业务级校验，非法时抛 ReconciliationError(422)。"""
    settings = get_settings()

    manifest = payload.manifest
    ranges = payload.offset_ranges

    if len(manifest) > settings.recon_max_manifest_rows:
        raise ReconciliationError(
            "TOO_MANY_ROWS",
            f"清单至多 {settings.recon_max_manifest_rows} 行，实际 {len(manifest)} 行",
        )
    line_nos = [line.line_no for line in manifest]
    if len(set(line_nos)) != len(line_nos):
        dupes = sorted({x for x in line_nos if line_nos.count(x) > 1})
        raise ReconciliationError(
            "DUPLICATE_LINE_NO", f"清单行号必须唯一，重复行号: {dupes}"
        )
    for line in manifest:
        if line.net_weight_min > line.net_weight_max:
            raise ReconciliationError(
                "INVALID_WEIGHT_RANGE",
                f"行 {line.line_no} 的净重下限 {line.net_weight_min} "
                f"大于上限 {line.net_weight_max}",
            )

    total_positions = 0
    by_partition: dict[int, list[tuple[int, int]]] = {}
    for rng in ranges:
        if rng.partition >= settings.topic_partitions:
            raise ReconciliationError(
                "UNKNOWN_PARTITION",
                f"主题 {settings.topic} 只有 {settings.topic_partitions} 个分区，"
                f"分区 {rng.partition} 不存在",
            )
        if rng.offset_start >= rng.offset_end:
            raise ReconciliationError(
                "INVALID_OFFSET_RANGE",
                f"分区 {rng.partition} 范围必须左闭右开且 start < end: "
                f"[{rng.offset_start}, {rng.offset_end})",
            )
        span = rng.offset_end - rng.offset_start
        if span > settings.recon_max_range_span:
            raise ReconciliationError(
                "RANGE_TOO_WIDE",
                f"分区 {rng.partition} 单段范围跨度 {span} 超过上限 "
                f"{settings.recon_max_range_span}",
            )
        total_positions += span
        by_partition.setdefault(rng.partition, []).append(
            (rng.offset_start, rng.offset_end)
        )

    if total_positions > settings.recon_max_source_positions:
        raise ReconciliationError(
            "TOO_MANY_POSITIONS",
            f"各范围合计源位置至多 {settings.recon_max_source_positions} 个，"
            f"实际 {total_positions} 个",
        )

    for partition, intervals in by_partition.items():
        intervals.sort()
        for (s1, e1), (s2, e2) in zip(intervals, intervals[1:]):
            if s2 < e1:  # 左闭右开区间重叠（端点相接 [a,b)+[b,c) 合法）
                raise ReconciliationError(
                    "OVERLAPPING_RANGES",
                    f"分区 {partition} 的范围 [{s1},{e1}) 与 [{s2},{e2}) 重叠",
                )


# --------------------------------------------------------------------------
# 受理与结论计算
# --------------------------------------------------------------------------

def create_reconciliation(session: Session, payload, *, topic: str) -> ReconciliationJob:
    """受理一份对账请求：先持久化 PENDING 单，再计算并生效结论。"""
    validate_reconcile_request(payload)

    manifest_json = json.dumps(
        [line.model_dump() for line in payload.manifest], ensure_ascii=False
    )
    total_positions = sum(
        rng.offset_end - rng.offset_start for rng in payload.offset_ranges
    )

    # 事务一：受理落库（PENDING + 范围）。崩溃后该单可被恢复重算。
    job = ReconciliationJob(
        topic=topic,
        manifest_payload=manifest_json,
        manifest_row_count=len(payload.manifest),
        source_position_count=total_positions,
        status=ReconciliationStatus.PENDING,
    )
    session.add(job)
    session.flush()  # 取 job.id
    for rng in payload.offset_ranges:
        session.add(ReconciliationRange(
            job_id=job.id,
            source_partition=rng.partition,
            offset_start=rng.offset_start,
            offset_end=rng.offset_end,
        ))
    session.commit()
    logger.info("reconciliation job %s accepted (rows=%s, positions=%s)",
                job.id, job.manifest_row_count, total_positions)

    # 事务二：计算结论（若受理后范围内已变化，钩子已把单据置 EXPIRED）
    job = session.get(ReconciliationJob, job.id)
    finalize_pending_job(session, job)
    return session.get(ReconciliationJob, job.id)


def _load_basis_records(
    session: Session, job: ReconciliationJob
) -> list[NetWeightRecord]:
    """读取当前范围内的全部净重明细（只追加事实表）。"""
    ranges = session.execute(
        select(ReconciliationRange).where(ReconciliationRange.job_id == job.id)
    ).scalars().all()

    records: list[NetWeightRecord] = []
    for rng in ranges:
        rows = session.execute(
            select(NetWeightRecord)
            .where(
                NetWeightRecord.topic == job.topic,
                NetWeightRecord.source_partition == rng.source_partition,
                NetWeightRecord.source_offset >= rng.offset_start,
                NetWeightRecord.source_offset < rng.offset_end,
            )
            .order_by(NetWeightRecord.source_partition, NetWeightRecord.source_offset)
        ).scalars().all()
        records.extend(rows)
    records.sort(key=lambda r: (r.source_partition, r.source_offset, r.id))
    return records


def finalize_pending_job(session: Session, job: ReconciliationJob) -> None:
    """对 PENDING 单计算结论，并把依据与对应在同一事务写入。

    幂等：非 PENDING 状态直接返回。任何异常都回滚，单据保持 PENDING，
    等待启动恢复或下次访问重试。
    """
    if job.status != ReconciliationStatus.PENDING:
        return

    try:
        manifest = [dict(x) for x in json.loads(job.manifest_payload)]
        records = _load_basis_records(session, job)

        # 1) 保存受理快照（范围内当前净重明细的位置集合）与完整依据
        snapshot = [[r.source_partition, r.source_offset] for r in records]
        job.basis_snapshot = json.dumps(snapshot)
        for r in records:
            session.add(ReconciliationBasis(
                job_id=job.id,
                net_weight_id=r.id,
                container_no=r.container_no,
                net_weight=r.net_weight,
                topic=r.topic,
                source_partition=r.source_partition,
                source_offset=r.source_offset,
                from_correction=r.from_correction,
            ))

        # 2) 构造二分图候选边：箱号相同且净重落入闭区间
        record_by_id = {r.id: r for r in records}
        lines = [str(x["line_no"]) for x in manifest]
        adjacency: dict[str, list[int]] = {}
        for line in manifest:
            candidates = [
                r.id for r in records
                if r.container_no == line["container_no"]
                and line["net_weight_min"] <= r.net_weight <= line["net_weight_max"]
            ]
            adjacency[str(line["line_no"])] = candidates

        matchings = find_two_matchings(lines, adjacency)

        # 3) 结论 + 对应持久化（与依据同一事务，原子生效）
        if not matchings:
            job.status = ReconciliationStatus.REJECTED
            job.passed = False
            job.result_detail = (
                "无完整对应：至少一条清单行无法在范围内找到箱号相同且净重"
                "落在闭区间内的可用明细（每条明细最多使用一次）"
            )
        else:
            for matching_no, matching in enumerate(matchings, start=1):
                for line in manifest:
                    line_no = str(line["line_no"])
                    rec = record_by_id[matching[line_no]]
                    session.add(ReconciliationMatchLine(
                        job_id=job.id,
                        matching_no=matching_no,
                        line_no=line_no,
                        container_no=line["container_no"],
                        net_weight_min=line["net_weight_min"],
                        net_weight_max=line["net_weight_max"],
                        net_weight_id=rec.id,
                        matched_container_no=rec.container_no,
                        matched_net_weight=rec.net_weight,
                        source_partition=rec.source_partition,
                        source_offset=rec.source_offset,
                    ))
            if len(matchings) == 1:
                job.status = ReconciliationStatus.PASSED
                job.passed = True
                job.result_detail = "存在唯一一份完整对应，核对通过"
            else:
                job.status = ReconciliationStatus.AMBIGUOUS
                job.passed = False
                job.result_detail = (
                    "存在多份不同完整对应，返回两份对应；需人工裁定后重新核对"
                )

        job.finalized_at = datetime.now(timezone.utc)
        session.commit()
        logger.info("reconciliation job %s finalized -> %s", job.id, job.status)
    except Exception:
        session.rollback()
        logger.exception("finalize reconciliation job %s failed; kept PENDING", job.id)
        raise


def recover_pending_jobs(session: Session) -> int:
    """启动恢复：重算所有遗留 PENDING 单（崩溃/重启后的接续）。"""
    pending = session.execute(
        select(ReconciliationJob).where(
            ReconciliationJob.status == ReconciliationStatus.PENDING
        )
    ).scalars().all()
    recovered = 0
    for job in pending:
        finalize_pending_job(session, job)
        recovered += 1
    if recovered:
        logger.info("recovered %s pending reconciliation job(s) on startup", recovered)
    return recovered


# --------------------------------------------------------------------------
# 读取（含快照复核）与发布
# --------------------------------------------------------------------------

def _snapshot_positions(job: ReconciliationJob) -> set[tuple[int, int]]:
    if not job.basis_snapshot:
        return set()
    return {tuple(x) for x in json.loads(job.basis_snapshot)}


def refresh_job(session: Session, job_id: int, *, for_update: bool = False):
    """读取前的一致性刷新：恢复 PENDING、按快照复核失效。"""
    job = session.get(ReconciliationJob, job_id, with_for_update=for_update)
    if job is None:
        return None

    if job.status == ReconciliationStatus.PENDING:
        # 崩溃遗留或受理后尚未算完：幂等重算
        finalize_pending_job(session, job)
        job = session.get(ReconciliationJob, job_id, with_for_update=for_update)
        return job

    # 已被入仓事务主动置失效的单据：保留具体失效原因，不再覆盖
    if job.status == ReconciliationStatus.EXPIRED:
        return job

    # 快照复核：当前范围内位置集合必须与受理时完全一致
    current = {(r.source_partition, r.source_offset)
               for r in _load_basis_records(session, job)}
    if current != _snapshot_positions(job):
        job.status = ReconciliationStatus.EXPIRED
        job.passed = None
        job.expired_at = datetime.now(timezone.utc)
        job.expire_reason = (
            "读取时快照复核发现范围内净重明细位置集合已变化，结论作废，须重新核对"
        )
        session.commit()
        logger.warning("reconciliation job %s expired on snapshot re-check", job_id)
        job = session.get(ReconciliationJob, job_id, with_for_update=for_update)
    return job


def publish_reconciliation(session: Session, job_id: int) -> ReconciliationJob:
    """发布核对单结论。过期单/不通过单不得发布；并发发布由行锁串行化。"""
    job = refresh_job(session, job_id, for_update=True)
    if job is None:
        raise ReconciliationError("NOT_FOUND", f"核对单 {job_id} 不存在", 404)

    if job.status == ReconciliationStatus.EXPIRED:
        raise ReconciliationError(
            "EXPIRED",
            f"核对单 {job_id} 已过期（{job.expire_reason or '范围内数据已变化'}），"
            "不能发布，请重新核对",
            409,
        )
    if job.status == ReconciliationStatus.PENDING:
        raise ReconciliationError(
            "NOT_FINALIZED", f"核对单 {job_id} 结论尚未生效，请稍后重试", 409
        )
    if job.status != ReconciliationStatus.PASSED:
        # REJECTED（无解）与 AMBIGUOUS（多解）都不是当前通过结论，禁止发布
        raise ReconciliationError(
            "NOT_PASSABLE",
            f"核对单 {job_id} 结论为 {job.status.value}，"
            f"不构成唯一通过结论（{job.result_detail or ''}），不能发布",
            409,
        )

    if job.published_at is not None:
        # 幂等：重复发布直接返回当前单据
        return job

    job.published_at = datetime.now(timezone.utc)
    session.commit()
    logger.info("reconciliation job %s published (%s)", job_id, job.status)
    return session.get(ReconciliationJob, job_id)


# --------------------------------------------------------------------------
# 查询
# --------------------------------------------------------------------------

def list_jobs(
    session: Session,
    *,
    status: ReconciliationStatus | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[ReconciliationJob]:
    stmt = select(ReconciliationJob).order_by(ReconciliationJob.id.desc())
    if status is not None:
        stmt = stmt.where(ReconciliationJob.status == status)
    stmt = stmt.limit(limit).offset(offset)
    return list(session.execute(stmt).scalars().all())


def get_job(session: Session, job_id: int) -> ReconciliationJob | None:
    return session.get(ReconciliationJob, job_id)


def load_ranges(session: Session, job_id: int) -> list[ReconciliationRange]:
    return list(session.execute(
        select(ReconciliationRange)
        .where(ReconciliationRange.job_id == job_id)
        .order_by(ReconciliationRange.source_partition, ReconciliationRange.offset_start)
    ).scalars().all())


def load_basis(session: Session, job_id: int) -> list[ReconciliationBasis]:
    return list(session.execute(
        select(ReconciliationBasis)
        .where(ReconciliationBasis.job_id == job_id)
        .order_by(
            ReconciliationBasis.source_partition,
            ReconciliationBasis.source_offset,
            ReconciliationBasis.id,
        )
    ).scalars().all())


def load_match_lines(
    session: Session, job_id: int
) -> dict[int, list[ReconciliationMatchLine]]:
    rows = list(session.execute(
        select(ReconciliationMatchLine)
        .where(ReconciliationMatchLine.job_id == job_id)
        .order_by(ReconciliationMatchLine.matching_no, ReconciliationMatchLine.id)
    ).scalars().all())
    grouped: dict[int, list[ReconciliationMatchLine]] = {}
    for row in rows:
        grouped.setdefault(row.matching_no, []).append(row)
    return grouped
