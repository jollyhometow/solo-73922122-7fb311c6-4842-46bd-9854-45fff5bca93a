"""对账服务与 API 端到端测试。

覆盖：
1. 受理输入校验（行数/位置数/行号唯一/区间合法/分区存在/区间重叠）；
2. 通过（唯一对应）、不通过（无解）、多解（两份对应）三种结论及依据；
3. 每条明细最多用一次、明细允许剩余；
4. 范围内新增入仓 / 补正入仓使在途与已完成核对单过期，范围外变化不影响；
5. 过期单不能发布、不提供当前通过结论；发布幂等与并发状态机；
6. 崩溃恢复：PENDING 单在启动恢复/GET 时幂等重算；
7. 快照复核兜底失效。
"""
from __future__ import annotations

import time

import pytest
from sqlalchemy import select

from app.models import (
    NetWeightRecord,
    ReconciliationBasis,
    ReconciliationMatchLine,
    ReconciliationStatus,
)
from app.reconciliation import (
    ReconciliationError,
    create_reconciliation,
    expire_jobs_for_position,
    get_job,
    publish_reconciliation,
    recover_pending_jobs,
    refresh_job,
)
from app.schemas import ManifestLineIn, OffsetRangeIn, ReconcileRequest
from app.services import apply_correction, get_quarantine_by_position, process_consumed_message

TOPIC = "weighbridge-events"


# --------------------------------------------------------------------------
# 辅助
# --------------------------------------------------------------------------

def _net(session, partition, offset, container, net, *, from_correction=False, qid=None):
    record = NetWeightRecord(
        container_no=container,
        gross_weight=net + 1000,
        tare_weight=1000,
        net_weight=net,
        topic=TOPIC,
        source_partition=partition,
        source_offset=offset,
        from_correction=from_correction,
        quarantine_id=qid,
    )
    session.add(record)
    session.flush()
    expire_jobs_for_position(
        session, topic=TOPIC, partition=partition, offset=offset
    )
    session.commit()
    return record


def _req(lines, ranges):
    return ReconcileRequest(
        manifest=[ManifestLineIn(**x) for x in lines],
        offset_ranges=[OffsetRangeIn(**x) for x in ranges],
    )


def _wait_until(predicate, timeout=10.0, interval=0.05):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"条件在 {timeout}s 内未满足，最后值={last!r}")


# --------------------------------------------------------------------------
# 输入校验
# --------------------------------------------------------------------------

def test_reject_too_many_manifest_rows(db_session):
    lines = [
        {"line_no": f"L{i}", "container_no": "MSCU1234567",
         "net_weight_min": 1, "net_weight_max": 2}
        for i in range(81)
    ]
    with pytest.raises(ReconciliationError) as exc:
        create_reconciliation(
            db_session,
            _req(lines, [{"partition": 0, "offset_start": 0, "offset_end": 10}]),
            topic=TOPIC,
        )
    assert exc.value.code == "TOO_MANY_ROWS" and exc.value.status_code == 422


def test_reject_more_than_160_source_positions(db_session):
    # 两段各 81，合计 162 > 160
    ranges = [
        {"partition": 0, "offset_start": 0, "offset_end": 81},
        {"partition": 1, "offset_start": 0, "offset_end": 81},
    ]
    with pytest.raises(ReconciliationError) as exc:
        create_reconciliation(
            db_session,
            _req([{"line_no": "L1", "container_no": "A123",
                   "net_weight_min": 1, "net_weight_max": 2}], ranges),
            topic=TOPIC,
        )
    assert exc.value.code == "TOO_MANY_POSITIONS"


def test_reject_duplicate_line_no(db_session):
    lines = [
        {"line_no": "L1", "container_no": "A123", "net_weight_min": 1, "net_weight_max": 2},
        {"line_no": "L1", "container_no": "B123", "net_weight_min": 1, "net_weight_max": 2},
    ]
    with pytest.raises(ReconciliationError) as exc:
        create_reconciliation(
            db_session,
            _req(lines, [{"partition": 0, "offset_start": 0, "offset_end": 5}]),
            topic=TOPIC,
        )
    assert exc.value.code == "DUPLICATE_LINE_NO"


def test_reject_inverted_weight_bounds(db_session):
    with pytest.raises(ReconciliationError) as exc:
        create_reconciliation(
            db_session,
            _req([{"line_no": "L1", "container_no": "A123",
                   "net_weight_min": 9, "net_weight_max": 1}],
                 [{"partition": 0, "offset_start": 0, "offset_end": 5}]),
            topic=TOPIC,
        )
    assert exc.value.code == "INVALID_WEIGHT_RANGE"


def test_reject_unknown_partition_and_overlapping_ranges(db_session):
    with pytest.raises(ReconciliationError) as exc:
        create_reconciliation(
            db_session,
            _req([{"line_no": "L1", "container_no": "A123",
                   "net_weight_min": 1, "net_weight_max": 2}],
                 [{"partition": 7, "offset_start": 0, "offset_end": 5}]),
            topic=TOPIC,
        )
    assert exc.value.code == "UNKNOWN_PARTITION"

    with pytest.raises(ReconciliationError) as exc:
        create_reconciliation(
            db_session,
            _req([{"line_no": "L1", "container_no": "A123",
                   "net_weight_min": 1, "net_weight_max": 2}],
                 [{"partition": 0, "offset_start": 0, "offset_end": 5},
                  {"partition": 0, "offset_start": 4, "offset_end": 9}]),
            topic=TOPIC,
        )
    assert exc.value.code == "OVERLAPPING_RANGES"

    # 端点相接的左闭右开区间合法
    job = create_reconciliation(
        db_session,
        _req([{"line_no": "L1", "container_no": "A123",
               "net_weight_min": 1, "net_weight_max": 2}],
             [{"partition": 0, "offset_start": 0, "offset_end": 5},
              {"partition": 0, "offset_start": 5, "offset_end": 9}]),
        topic=TOPIC,
    )
    assert job.source_position_count == 9


# --------------------------------------------------------------------------
# 三种结论
# --------------------------------------------------------------------------

def test_pass_with_unique_matching_and_basis(db_session):
    r1 = _net(db_session, 0, 0, "MSCU1234567", 16000)
    r2 = _net(db_session, 0, 1, "MSCU7654321", 14700)
    # 一条不会被使用的多余明细（允许剩余）
    _net(db_session, 0, 2, "MSCU0000000", 500)

    job = create_reconciliation(
        db_session,
        _req(
            [
                {"line_no": "L1", "container_no": "MSCU1234567",
                 "net_weight_min": 15000, "net_weight_max": 17000},
                {"line_no": "L2", "container_no": "MSCU7654321",
                 "net_weight_min": 14000, "net_weight_max": 15000},
            ],
            [{"partition": 0, "offset_start": 0, "offset_end": 3}],
        ),
        topic=TOPIC,
    )
    assert job.status == ReconciliationStatus.PASSED and job.passed is True
    assert job.finalized_at is not None

    basis = db_session.scalars(
        select(ReconciliationBasis).where(ReconciliationBasis.job_id == job.id)
    ).all()
    assert {b.net_weight_id for b in basis} == {r1.id, r2.id, _id_of(db_session, 2)}

    matches = db_session.scalars(
        select(ReconciliationMatchLine).where(ReconciliationMatchLine.job_id == job.id)
    ).all()
    assert {m.matching_no for m in matches} == {1}
    by_line = {m.line_no: m for m in matches}
    assert by_line["L1"].net_weight_id == r1.id
    assert by_line["L2"].net_weight_id == r2.id
    # 闭区间边界命中也算
    assert by_line["L2"].matched_net_weight == 14700


def _id_of(session, offset):
    return session.execute(
        select(NetWeightRecord.id).where(
            NetWeightRecord.source_partition == 0,
            NetWeightRecord.source_offset == offset,
        )
    ).scalar_one()


def test_rejected_when_no_complete_matching(db_session):
    _net(db_session, 0, 0, "MSCU1234567", 16000)
    job = create_reconciliation(
        db_session,
        _req(
            [
                {"line_no": "L1", "container_no": "MSCU1234567",
                 "net_weight_min": 15000, "net_weight_max": 17000},
                {"line_no": "L2", "container_no": "MSCU1234567",
                 "net_weight_min": 15000, "net_weight_max": 17000},
            ],
            [{"partition": 0, "offset_start": 0, "offset_end": 1}],
        ),
        topic=TOPIC,
    )
    assert job.status == ReconciliationStatus.REJECTED and job.passed is False
    assert db_session.scalar(
        select(ReconciliationMatchLine).where(
            ReconciliationMatchLine.job_id == job.id
        )
    ) is None
    # 依据仍然保存
    assert db_session.scalars(
        select(ReconciliationBasis).where(ReconciliationBasis.job_id == job.id)
    ).all()


def test_ambiguous_returns_two_distinct_matchings(db_session):
    _net(db_session, 0, 0, "MSCU1234567", 16000)
    _net(db_session, 0, 1, "MSCU1234567", 16500)

    job = create_reconciliation(
        db_session,
        _req(
            [
                {"line_no": "L1", "container_no": "MSCU1234567",
                 "net_weight_min": 16000, "net_weight_max": 17000},
                {"line_no": "L2", "container_no": "MSCU1234567",
                 "net_weight_min": 16000, "net_weight_max": 17000},
            ],
            [{"partition": 0, "offset_start": 0, "offset_end": 2}],
        ),
        topic=TOPIC,
    )
    assert job.status == ReconciliationStatus.AMBIGUOUS and job.passed is False
    groups: dict[int, dict[str, int]] = {}
    for m in db_session.scalars(
        select(ReconciliationMatchLine).where(ReconciliationMatchLine.job_id == job.id)
    ).all():
        groups.setdefault(m.matching_no, {})[m.line_no] = m.net_weight_id
    assert set(groups) == {1, 2}
    assert groups[1] != groups[2]
    for g in groups.values():
        assert len(set(g.values())) == 2  # 每份对应内明细不重复


def test_closed_interval_boundaries_and_container_mismatch(db_session):
    rec = _net(db_session, 1, 4, "MSCU7654321", 100)
    # 净重正好等于上下限 -> 命中
    job = create_reconciliation(
        db_session,
        _req([{"line_no": "L1", "container_no": "MSCU7654321",
               "net_weight_min": 100, "net_weight_max": 100}],
             [{"partition": 1, "offset_start": 4, "offset_end": 5}]),
        topic=TOPIC,
    )
    assert job.status == ReconciliationStatus.PASSED

    # 箱号不同即使净重合适也不匹配
    job2 = create_reconciliation(
        db_session,
        _req([{"line_no": "L1", "container_no": "OTHER000000",
               "net_weight_min": 90, "net_weight_max": 110}],
             [{"partition": 1, "offset_start": 4, "offset_end": 5}]),
        topic=TOPIC,
    )
    assert job2.status == ReconciliationStatus.REJECTED
    assert rec.id  # 占位引用，保留语义说明


# --------------------------------------------------------------------------
# 失效：范围内变化过期、范围外变化不影响
# --------------------------------------------------------------------------

def test_in_range_new_insert_expires_active_job(db_session):
    _net(db_session, 0, 0, "MSCU1234567", 16000)
    job = create_reconciliation(
        db_session,
        _req([{"line_no": "L1", "container_no": "MSCU1234567",
               "net_weight_min": 15000, "net_weight_max": 17000}],
             [{"partition": 0, "offset_start": 0, "offset_end": 5}]),
        topic=TOPIC,
    )
    assert job.status == ReconciliationStatus.PASSED

    # 范围内新增入仓（offset=3 在 [0,5) 内）
    _net(db_session, 0, 3, "MSCU1111111", 100)
    db_session.expire_all()
    refreshed = get_job(db_session, job.id)
    assert refreshed.status == ReconciliationStatus.EXPIRED
    assert refreshed.passed is None and refreshed.expired_at is not None
    assert "partition=0 offset=3" in (refreshed.expire_reason or "")


def test_out_of_range_change_does_not_expire(db_session):
    _net(db_session, 0, 0, "MSCU1234567", 16000)
    job = create_reconciliation(
        db_session,
        _req([{"line_no": "L1", "container_no": "MSCU1234567",
               "net_weight_min": 15000, "net_weight_max": 17000}],
             [{"partition": 0, "offset_start": 0, "offset_end": 5}]),
        topic=TOPIC,
    )
    # 分区 1 的变化与分区 0 offset=9（范围外）都不影响
    _net(db_session, 1, 0, "MSCU2222222", 100)
    _net(db_session, 0, 9, "MSCU3333333", 200)
    refreshed = refresh_job(db_session, job.id)
    assert refreshed.status == ReconciliationStatus.PASSED


def test_correction_insert_expires_in_range_job(db_session):
    # 先造一条隔离记录并补正入仓（位点在核对范围内）
    bad = '{"container_no": "BADU0000001", "gross_weight": 3000, "tare_weight": 4200}'
    process_consumed_message(
        db_session, topic=TOPIC, partition=0, offset=2, raw_text=bad
    )
    _net(db_session, 0, 0, "MSCU1234567", 16000)
    job = create_reconciliation(
        db_session,
        _req([{"line_no": "L1", "container_no": "MSCU1234567",
               "net_weight_min": 15000, "net_weight_max": 17000}],
             [{"partition": 0, "offset_start": 0, "offset_end": 5}]),
        topic=TOPIC,
    )
    assert job.status == ReconciliationStatus.PASSED

    q = get_quarantine_by_position(db_session, topic=TOPIC, partition=0, offset=2)
    apply_correction(db_session, q.id, {
        "container_no": "MSCU9999999", "gross_weight": 5000, "tare_weight": 4200
    })
    db_session.expire_all()
    refreshed = get_job(db_session, job.id)
    assert refreshed.status == ReconciliationStatus.EXPIRED
    assert "补正入仓" in (refreshed.expire_reason or "")


def test_pending_job_also_expired_by_in_range_insert(db_session):
    _net(db_session, 0, 0, "MSCU1234567", 16000)
    job = create_reconciliation(
        db_session,
        _req([{"line_no": "L1", "container_no": "MSCU1234567",
               "net_weight_min": 15000, "net_weight_max": 17000}],
             [{"partition": 0, "offset_start": 0, "offset_end": 5}]),
        topic=TOPIC,
    )
    # 手工构造一个在途 PENDING 单（模拟受理后、结论前崩溃）
    job.status = ReconciliationStatus.PENDING
    job.finalized_at = None
    db_session.commit()

    _net(db_session, 0, 4, "MSCU1111111", 100)
    db_session.expire_all()
    refreshed = get_job(db_session, job.id)
    assert refreshed.status == ReconciliationStatus.EXPIRED


def test_finalize_skips_job_expired_between_accept_and_finalize(db_session, monkeypatch):
    """受理事务与结论事务之间发生范围内入仓：单据已 EXPIRED，结论绝不能再生效。"""
    from app import reconciliation as recon_mod

    _net(db_session, 0, 0, "MSCU1234567", 16000)
    payload = _req(
        [{"line_no": "L1", "container_no": "MSCU1234567",
          "net_weight_min": 15000, "net_weight_max": 17000}],
        [{"partition": 0, "offset_start": 0, "offset_end": 5}],
    )
    # 在受理落库之后、结论计算之前插入范围内明细并触发失效
    real_finalize = recon_mod.finalize_pending_job

    def finalize_with_concurrent_insert(session, job):
        _net(session, 0, 4, "MSCU1111111", 100)
        return real_finalize(session, job)

    monkeypatch.setattr(recon_mod, "finalize_pending_job", finalize_with_concurrent_insert)
    job = recon_mod.create_reconciliation(db_session, payload, topic=TOPIC)
    assert job.status == ReconciliationStatus.EXPIRED
    assert job.passed is None
    # 没有任何结论性对应写入
    assert db_session.scalars(
        select(ReconciliationMatchLine).where(ReconciliationMatchLine.job_id == job.id)
    ).all() == []


# --------------------------------------------------------------------------
# 发布守卫
# --------------------------------------------------------------------------

def test_publish_happy_path_and_idempotent(db_session):
    _net(db_session, 0, 0, "MSCU1234567", 16000)
    job = create_reconciliation(
        db_session,
        _req([{"line_no": "L1", "container_no": "MSCU1234567",
               "net_weight_min": 15000, "net_weight_max": 17000}],
             [{"partition": 0, "offset_start": 0, "offset_end": 5}]),
        topic=TOPIC,
    )
    published = publish_reconciliation(db_session, job.id)
    assert published.published_at is not None and published.status == ReconciliationStatus.PASSED
    first_ts = published.published_at
    again = publish_reconciliation(db_session, job.id)
    assert again.published_at == first_ts  # 幂等，不重复发布


def test_expired_job_cannot_publish(db_session):
    _net(db_session, 0, 0, "MSCU1234567", 16000)
    job = create_reconciliation(
        db_session,
        _req([{"line_no": "L1", "container_no": "MSCU1234567",
               "net_weight_min": 15000, "net_weight_max": 17000}],
             [{"partition": 0, "offset_start": 0, "offset_end": 5}]),
        topic=TOPIC,
    )
    publish_reconciliation(db_session, job.id)
    _net(db_session, 0, 3, "MSCU1111111", 100)  # 发布后范围内变化
    with pytest.raises(ReconciliationError) as exc:
        publish_reconciliation(db_session, job.id)
    assert exc.value.code == "EXPIRED" and exc.value.status_code == 409


def test_rejected_and_ambiguous_job_cannot_publish(db_session):
    _net(db_session, 0, 0, "MSCU1234567", 16000)
    rejected = create_reconciliation(
        db_session,
        _req([
            {"line_no": "L1", "container_no": "MSCU1234567",
             "net_weight_min": 15000, "net_weight_max": 17000},
            {"line_no": "L2", "container_no": "MSCU1234567",
             "net_weight_min": 15000, "net_weight_max": 17000},
        ], [{"partition": 0, "offset_start": 0, "offset_end": 1}]),
        topic=TOPIC,
    )
    assert rejected.status == ReconciliationStatus.REJECTED
    with pytest.raises(ReconciliationError) as exc:
        publish_reconciliation(db_session, rejected.id)
    assert exc.value.code == "NOT_PASSABLE"

    _net(db_session, 0, 1, "MSCU1234567", 16500)
    ambiguous = create_reconciliation(
        db_session,
        _req([
            {"line_no": "L1", "container_no": "MSCU1234567",
             "net_weight_min": 16000, "net_weight_max": 17000},
            {"line_no": "L2", "container_no": "MSCU1234567",
             "net_weight_min": 16000, "net_weight_max": 17000},
        ], [{"partition": 0, "offset_start": 0, "offset_end": 2}]),
        topic=TOPIC,
    )
    assert ambiguous.status == ReconciliationStatus.AMBIGUOUS
    with pytest.raises(ReconciliationError) as exc:
        publish_reconciliation(db_session, ambiguous.id)
    assert exc.value.code == "NOT_PASSABLE"


# --------------------------------------------------------------------------
# 崩溃恢复与快照兜底
# --------------------------------------------------------------------------

def test_recover_pending_jobs_after_crash(db_session):
    _net(db_session, 0, 0, "MSCU1234567", 16000)
    job = create_reconciliation(
        db_session,
        _req([{"line_no": "L1", "container_no": "MSCU1234567",
               "net_weight_min": 15000, "net_weight_max": 17000}],
             [{"partition": 0, "offset_start": 0, "offset_end": 5}]),
        topic=TOPIC,
    )
    # 模拟结论事务前崩溃：回退为无依据/无对应的 PENDING 单
    job.status = ReconciliationStatus.PENDING
    job.finalized_at = None
    job.basis_snapshot = None
    job.result_detail = None
    db_session.query(ReconciliationBasis).filter(
        ReconciliationBasis.job_id == job.id
    ).delete()
    db_session.query(ReconciliationMatchLine).filter(
        ReconciliationMatchLine.job_id == job.id
    ).delete()
    db_session.commit()

    n = recover_pending_jobs(db_session)
    assert n == 1
    db_session.expire_all()
    recovered = get_job(db_session, job.id)
    assert recovered.status == ReconciliationStatus.PASSED
    assert recovered.basis_snapshot is not None
    assert db_session.scalars(
        select(ReconciliationMatchLine).where(
            ReconciliationMatchLine.job_id == job.id
        )
    ).all()


def test_snapshot_recheck_expires_job_without_hook(db_session):
    """绕过主动失效钩子直接插明细，验证读取时快照复核仍能兜底。"""
    _net(db_session, 0, 0, "MSCU1234567", 16000)
    job = create_reconciliation(
        db_session,
        _req([{"line_no": "L1", "container_no": "MSCU1234567",
               "net_weight_min": 15000, "net_weight_max": 17000}],
             [{"partition": 0, "offset_start": 0, "offset_end": 5}]),
        topic=TOPIC,
    )
    # 直接写表，不调用失效钩子（模拟任何边缘时序）
    db_session.add(NetWeightRecord(
        container_no="MSCU1111111", gross_weight=2000, tare_weight=1900,
        net_weight=100, topic=TOPIC, source_partition=0, source_offset=3,
        from_correction=False,
    ))
    db_session.commit()

    refreshed = refresh_job(db_session, job.id)
    assert refreshed.status == ReconciliationStatus.EXPIRED


def test_sequential_publish_state_machine(db_session):
    """模拟 PostgreSQL FOR UPDATE 串行化后的第二请求：先发布提交，再以新事务进入。"""
    from app.database import get_database

    _net(db_session, 0, 0, "MSCU1234567", 16000)
    payload = _req(
        [{"line_no": "L1", "container_no": "MSCU1234567",
          "net_weight_min": 15000, "net_weight_max": 17000}],
        [{"partition": 0, "offset_start": 0, "offset_end": 5}],
    )
    job = create_reconciliation(db_session, payload, topic=TOPIC)

    # 请求 A：独立事务发布并提交
    session_a = get_database().session()
    published_a = publish_reconciliation(session_a, job.id)
    first_ts = published_a.published_at
    assert first_ts is not None
    session_a.close()

    # 请求 B：行锁已释放，新事务进入，幂等返回且发布时间不被改写
    session_b = get_database().session()
    published_b = publish_reconciliation(session_b, job.id)
    assert published_b.published_at is not None
    # SQLite 不保留 tzinfo，按 naive 时间分量比较；PG 下二者为同一时刻
    assert published_b.published_at.replace(tzinfo=None) == first_ts.replace(tzinfo=None)
    session_b.close()


def test_limits_boundary_80_rows_and_160_positions_accepted(db_session):
    # 恰好 80 行、合计恰好 160 个位置 -> 受理成功（上限边界）
    lines = [
        {"line_no": f"L{i:02d}", "container_no": f"CONT{i:02d}",
         "net_weight_min": 1, "net_weight_max": 2}
        for i in range(80)
    ]
    ranges = [
        {"partition": 0, "offset_start": 0, "offset_end": 80},
        {"partition": 1, "offset_start": 0, "offset_end": 80},
    ]
    job = create_reconciliation(db_session, _req(lines, ranges), topic=TOPIC)
    assert job.manifest_row_count == 80 and job.source_position_count == 160
    # 范围内没有任何明细 -> 无解
    assert job.status == ReconciliationStatus.REJECTED


def test_failed_correction_does_not_expire_job(db_session):
    """非法补正不产生净重明细，核对单必须保持有效（只有真正入仓才失效）。"""
    _net(db_session, 0, 0, "MSCU7654321", 15000)
    process_consumed_message(
        db_session, topic=TOPIC, partition=0, offset=2,
        raw_text='{"container_no": "BADU0000001", "gross_weight": 3000, "tare_weight": 4200}',
    )
    job = create_reconciliation(
        db_session,
        _req([{"line_no": "L1", "container_no": "MSCU7654321",
               "net_weight_min": 14000, "net_weight_max": 16000}],
             [{"partition": 0, "offset_start": 0, "offset_end": 5}]),
        topic=TOPIC,
    )
    q = get_quarantine_by_position(db_session, topic=TOPIC, partition=0, offset=2)
    _, net, reason = apply_correction(
        db_session, q.id,
        {"container_no": "X", "gross_weight": 100, "tare_weight": 200},
    )
    assert net is None and reason is not None  # 非法补正被拒
    db_session.expire_all()
    assert get_job(db_session, job.id).status == ReconciliationStatus.PASSED


# --------------------------------------------------------------------------
# HTTP 端到端（含消费者真实链路驱动失效）
# --------------------------------------------------------------------------

def _recon_body(lines, ranges):
    return {"manifest": lines, "offset_ranges": ranges}


def test_api_reconcile_pass_then_expire_via_consumer(client):
    # 先投两条合法消息入仓
    for cn in ("MSCU1234567", "MSCU7654321"):
        r = client.post("/messages", json={
            "message": {"container_no": cn, "gross_weight": 20000, "tare_weight": 4000}
        })
        assert r.status_code == 201
    _wait_until(lambda: len(client.get("/net-weights").json()) >= 2)
    nets = client.get("/net-weights").json()

    body = _recon_body(
        [
            {"line_no": "L1", "container_no": "MSCU1234567",
             "net_weight_min": 15000, "net_weight_max": 17000},
            {"line_no": "L2", "container_no": "MSCU7654321",
             "net_weight_min": 15000, "net_weight_max": 17000},
        ],
        [{"partition": 0, "offset_start": 0, "offset_end": 20},
         {"partition": 1, "offset_start": 0, "offset_end": 20}],
    )
    r = client.post("/reconciliations", json=body)
    assert r.status_code == 201, r.text
    job = r.json()
    assert job["status"] == "PASSED" and job["passed"] is True
    assert len(job["matchings"]) == 1 and len(job["matchings"][0]) == 2
    assert len(job["basis_records"]) == 2
    job_id = job["id"]

    # 列表可查
    listing = client.get("/reconciliations").json()
    assert any(x["id"] == job_id for x in listing)

    # 发布
    r = client.post(f"/reconciliations/{job_id}/publish")
    assert r.status_code == 200 and r.json()["published"] is True
    assert r.json()["reconciliation"]["published_at"] is not None

    # 再投一条消息，消费者入仓后核对单必须过期（分区可能是 0 或 1，均在范围内）
    client.post("/messages", json={
        "message": {"container_no": "MSCU5555555", "gross_weight": 9000, "tare_weight": 1000}
    })

    def expired():
        detail = client.get(f"/reconciliations/{job_id}").json()
        return detail if detail["status"] == "EXPIRED" else None

    expired_job = _wait_until(expired)
    assert expired_job["passed"] is None and expired_job["expire_reason"]

    # 过期单不能发布
    r = client.post(f"/reconciliations/{job_id}/publish")
    assert r.status_code == 409 and r.json()["detail"]["code"] == "EXPIRED"

    # 重新核对 -> 新单通过（明细允许剩余，新箱号没有对应行不影响）
    r2 = client.post("/reconciliations", json=body)
    assert r2.status_code == 201 and r2.json()["status"] == "PASSED"
    assert r2.json()["id"] != job_id

    # 明细数确实增加了（依据快照与新现状一致）
    assert len(client.get("/net-weights").json()) == len(nets) + 1


def test_api_validation_errors(client):
    # 空清单
    r = client.post("/reconciliations", json={
        "manifest": [],
        "offset_ranges": [{"partition": 0, "offset_start": 0, "offset_end": 1}],
    })
    assert r.status_code == 422

    # 业务校验：行号重复
    r = client.post("/reconciliations", json=_recon_body(
        [
            {"line_no": "L1", "container_no": "A123", "net_weight_min": 1, "net_weight_max": 2},
            {"line_no": "L1", "container_no": "B123", "net_weight_min": 1, "net_weight_max": 2},
        ],
        [{"partition": 0, "offset_start": 0, "offset_end": 1}],
    ))
    assert r.status_code == 422 and r.json()["detail"]["code"] == "DUPLICATE_LINE_NO"


def test_api_get_unknown_job_404(client):
    r = client.get("/reconciliations/999999")
    assert r.status_code == 404


@pytest.mark.parametrize("bad_value", [True, 1.5, "100"])
def test_api_rejects_non_integer_weights(client, bad_value):
    # 严格整数：布尔/浮点/字符串数字一律 422
    r = client.post("/reconciliations", json=_recon_body(
        [{"line_no": "L1", "container_no": "A123",
          "net_weight_min": bad_value, "net_weight_max": 2}],
        [{"partition": 0, "offset_start": 0, "offset_end": 1}],
    ))
    assert r.status_code == 422


def test_api_rejects_boolean_partition(client):
    r = client.post("/reconciliations", json=_recon_body(
        [{"line_no": "L1", "container_no": "A123",
          "net_weight_min": 1, "net_weight_max": 2}],
        [{"partition": True, "offset_start": 0, "offset_end": 1}],
    ))
    assert r.status_code == 422


def test_api_ambiguous_flow(client):
    # 同箱号两条净重都落在区间内 -> 多解
    for net in (16000, 16500):
        client.post("/messages", json={
            "message": {"container_no": "MSCU1234567",
                        "gross_weight": net + 1000, "tare_weight": 1000}
        })
    _wait_until(lambda: len(client.get("/net-weights").json()) >= 2)

    r = client.post("/reconciliations", json=_recon_body(
        [
            {"line_no": "L1", "container_no": "MSCU1234567",
             "net_weight_min": 16000, "net_weight_max": 17000},
            {"line_no": "L2", "container_no": "MSCU1234567",
             "net_weight_min": 16000, "net_weight_max": 17000},
        ],
        [{"partition": 0, "offset_start": 0, "offset_end": 20},
         {"partition": 1, "offset_start": 0, "offset_end": 20}],
    ))
    assert r.status_code == 201
    job = r.json()
    assert job["status"] == "AMBIGUOUS" and job["passed"] is False
    assert len(job["matchings"]) == 2
    m1 = {x["line_no"]: x["net_weight_id"] for x in job["matchings"][0]}
    m2 = {x["line_no"]: x["net_weight_id"] for x in job["matchings"][1]}
    assert m1 != m2
    # 多解单不能发布
    pub = client.post(f"/reconciliations/{job['id']}/publish")
    assert pub.status_code == 409 and pub.json()["detail"]["code"] == "NOT_PASSABLE"
