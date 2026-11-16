"""FastAPI 入口：演示消息生产、隔离查询/补正、净重明细查询、船方清单对账。

启动时：
1. 初始化数据库表；
2. 确保 Topic 存在（Kafka 后端）；
3. 启动后台消费工作线程（可通过 CONSUMER_ENABLED=false 关闭）；
4. 恢复崩溃/重启遗留的在途（PENDING）核对单。
"""
from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query, status
from sqlalchemy.orm import Session

from app import __version__
from app.config import get_settings
from app.consumer import ConsumerWorker
from app.database import get_database, get_session
from app.messaging import get_broker
from app.models import QuarantineRecord, QuarantineStatus, ReconciliationJob, ReconciliationStatus
from app.reconciliation import (
    ReconciliationError,
    create_reconciliation,
    list_jobs,
    load_basis,
    load_match_lines,
    load_ranges,
    publish_reconciliation,
    recover_pending_jobs,
    refresh_job,
)
from app.schemas import (
    BasisRecordOut,
    CorrectionRequest,
    CorrectionResult,
    HealthOut,
    ManifestLineIn,
    MatchLineOut,
    NetWeightOut,
    OffsetRangeOut,
    ProducedMessage,
    ProduceMessageRequest,
    PublishResult,
    QuarantineOut,
    ReconcileRequest,
    ReconciliationOut,
    ReconciliationSummary,
)
from app.services import (
    CorrectionError,
    apply_correction,
    get_quarantine_by_position,
    list_net_records,
    list_quarantine,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("weighbridge")

settings = get_settings()
_worker: ConsumerWorker | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _worker
    db = get_database()
    db.init_schema()

    broker = get_broker()
    if settings.consumer_enabled:
        # Kafka 后端确保 Topic 就绪；内存后端在工厂里已建
        from app.messaging import KafkaBroker

        if isinstance(broker, KafkaBroker):
            try:
                broker.create_topic(settings.topic, settings.topic_partitions)
            except Exception:
                logger.exception("topic 初始化失败（若已存在可忽略）")
        _worker = ConsumerWorker(broker=broker)
        _worker.start()

    # 恢复崩溃/重启遗留的在途核对单：结论与依据会被幂等重算后一起生效
    try:
        with db.session() as session:
            recover_pending_jobs(session)
    except Exception:
        logger.exception("启动恢复在途核对单失败（将在下次访问时重试）")

    logger.info("weighbridge API ready (instance=%s, version=%s)",
                settings.instance_id, __version__)
    try:
        yield
    finally:
        if _worker is not None:
            _worker.stop()
        db.dispose()


app = FastAPI(
    title="港口地磅称量流 - 净重明细与清单对账服务",
    description=(
        "## 功能\n"
        "- 接收含箱号、毛重、皮重（整数千克）的称量消息；\n"
        "- 字段无效或毛重 < 皮重的消息进入隔离区，可按源分区/offset 查看原文并补正；\n"
        "- 合法消息生成净重明细，每条源消息保证仅入仓一次；\n"
        "- 双实例同组消费两分区，持久化成功后才提交消费位点，失败可接续、不丢不重；\n"
        "- **船方清单对账**：提交至多 80 行清单与至多 160 个源位置范围，\n"
        "  按范围内已入仓明细做二分图匹配，返回唯一对应（通过）、无解（不通过）\n"
        "  或两份不同完整对应（多解）；范围内任何新增/补正入仓都会使核对单过期。\n\n"
        "在本页 **Try it out** 即可发送演示消息、提交补正、发起对账并发布核对单。"
    ),
    version=__version__,
    lifespan=lifespan,
)


# --------------------------------------------------------------------------
# 健康检查与演示生产
# --------------------------------------------------------------------------

@app.get("/health", response_model=HealthOut, tags=["运维"])
def health() -> HealthOut:
    partitions = _worker.assigned_partitions if _worker else []
    return HealthOut(
        status="ok",
        instance_id=settings.instance_id,
        consumer_enabled=settings.consumer_enabled,
        assigned_partitions=partitions,
    )


@app.post(
    "/messages",
    response_model=ProducedMessage,
    status_code=status.HTTP_201_CREATED,
    tags=["演示消息"],
    summary="发送一条称量消息（可构造非法消息验证隔离）",
)
def produce_message(req: ProduceMessageRequest) -> ProducedMessage:
    if req.raw_text is not None:
        text = req.raw_text
    elif req.message is not None:
        text = json.dumps(req.message, ensure_ascii=False)
    else:
        raise HTTPException(status_code=422, detail="message 与 raw_text 至少提供一个")

    key = req.key
    if key is None and isinstance(req.message, dict):
        cn = req.message.get("container_no")
        if isinstance(cn, str):
            key = cn

    try:
        delivered = get_broker().producer().produce(
            settings.topic,
            value=text.encode("utf-8"),
            key=key.encode("utf-8") if key else None,
            partition=req.partition,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"消息投递失败: {exc}") from exc

    return ProducedMessage(
        topic=delivered.topic,
        partition=delivered.partition,
        offset=delivered.offset,
        key=key,
        delivered_text=text,
    )


@app.post(
    "/demo/seed",
    response_model=list[ProducedMessage],
    tags=["演示消息"],
    summary="一键投递一组演示消息（合法/非法/毛重小于皮重）",
)
def seed_demo() -> list[ProducedMessage]:
    demo_messages = [
        {"container_no": "MSCU1234567", "gross_weight": 20000, "tare_weight": 4000},
        {"container_no": "MSCU7654321", "gross_weight": 18500, "tare_weight": 3800},
        {"container_no": "BADU0000001", "gross_weight": 3000, "tare_weight": 4200},  # 毛<皮
        {"container_no": "BAD", "gross_weight": 1000, "tare_weight": 200},          # 箱号非法
        {"container_no": "BADU0000002", "gross_weight": "1000", "tare_weight": 200},  # 类型非法
        {"container_no": "MSCU1111111"},                                              # 缺字段
    ]
    producer = get_broker().producer()
    out: list[ProducedMessage] = []
    for msg in demo_messages:
        text = json.dumps(msg, ensure_ascii=False)
        delivered = producer.produce(
            settings.topic, value=text.encode("utf-8"),
            key=msg.get("container_no", "").encode("utf-8"), partition=None,
        )
        out.append(ProducedMessage(
            topic=delivered.topic, partition=delivered.partition,
            offset=delivered.offset, key=msg.get("container_no"), delivered_text=text,
        ))
    producer.flush()
    return out


# --------------------------------------------------------------------------
# 净重明细查询
# --------------------------------------------------------------------------

@app.get(
    "/net-weights",
    response_model=list[NetWeightOut],
    tags=["净重明细"],
    summary="查询净重明细（可按箱号过滤）",
)
def get_net_weights(
    container_no: str | None = Query(default=None, description="精确箱号过滤"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
) -> list[NetWeightOut]:
    rows = list_net_records(session, container_no=container_no, limit=limit, offset=offset)
    return [NetWeightOut.model_validate(r) for r in rows]


# --------------------------------------------------------------------------
# 隔离区：查询原文、补正
# --------------------------------------------------------------------------

@app.get(
    "/quarantine",
    response_model=list[QuarantineOut],
    tags=["隔离区"],
    summary="查询隔离记录（可按源分区、状态过滤）",
)
def get_quarantine_list(
    partition: int | None = Query(default=None, ge=0, description="源分区过滤"),
    status_filter: QuarantineStatus | None = Query(
        default=None, alias="status", description="OPEN / CORRECTED"
    ),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
) -> list[QuarantineOut]:
    rows = list_quarantine(
        session, partition=partition, status=status_filter, limit=limit, offset=offset
    )
    return [QuarantineOut.model_validate(r) for r in rows]


@app.get(
    "/quarantine/by-position/{partition}/{offset}",
    response_model=QuarantineOut,
    tags=["隔离区"],
    summary="按源分区与 offset 查看隔离原文",
)
def get_quarantine_position(
    partition: int,
    offset: int,
    session: Session = Depends(get_session),
) -> QuarantineOut:
    row = get_quarantine_by_position(
        session, topic=settings.topic, partition=partition, offset=offset
    )
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=f"分区 {partition} offset {offset} 不存在隔离记录",
        )
    return QuarantineOut.model_validate(row)


@app.post(
    "/quarantine/{quarantine_id}/correction",
    response_model=CorrectionResult,
    tags=["隔离区"],
    summary="提交补正（每条隔离记录仅接受一次合法补正）",
)
def correct_quarantine(
    quarantine_id: int,
    req: CorrectionRequest,
    session: Session = Depends(get_session),
) -> CorrectionResult:
    try:
        q, net, reason = apply_correction(
            session,
            quarantine_id,
            {
                "container_no": req.container_no,
                "gross_weight": req.gross_weight,
                "tare_weight": req.tare_weight,
            },
        )
    except CorrectionError as exc:
        raise HTTPException(status_code=exc.status_code, detail={
            "code": exc.code, "message": exc.detail,
        }) from exc

    return CorrectionResult(
        accepted=reason is None,
        quarantine_id=q.id,
        status=q.status,
        reason=reason,
        net_weight=NetWeightOut.model_validate(net) if net else None,
    )


@app.get(
    "/quarantine/{quarantine_id}",
    response_model=QuarantineOut,
    tags=["隔离区"],
    summary="按主键查询单条隔离记录",
)
def get_quarantine_one(
    quarantine_id: int,
    session: Session = Depends(get_session),
) -> QuarantineOut:
    row = session.get(QuarantineRecord, quarantine_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"隔离记录 {quarantine_id} 不存在")
    return QuarantineOut.model_validate(row)


# --------------------------------------------------------------------------
# 船方清单对账
# --------------------------------------------------------------------------

def _serialize_job(session: Session, job: ReconciliationJob) -> ReconciliationOut:
    """组装核对单详情：清单、范围、完整依据、对应（结论与依据一起返回）。"""
    manifest = [ManifestLineIn(**line) for line in json.loads(job.manifest_payload)]
    ranges = [OffsetRangeOut.model_validate(r) for r in load_ranges(session, job.id)]
    basis = [
        BasisRecordOut(
            net_weight_id=b.net_weight_id,
            container_no=b.container_no,
            net_weight=b.net_weight,
            source_partition=b.source_partition,
            source_offset=b.source_offset,
            from_correction=b.from_correction,
        )
        for b in load_basis(session, job.id)
    ]
    matchings = [
        [MatchLineOut.model_validate(line) for line in lines]
        for _, lines in sorted(load_match_lines(session, job.id).items())
    ]
    return ReconciliationOut(
        id=job.id,
        topic=job.topic,
        status=job.status,
        passed=job.passed,
        manifest_row_count=job.manifest_row_count,
        source_position_count=job.source_position_count,
        manifest=manifest,
        offset_ranges=ranges,
        basis_records=basis,
        matchings=matchings,
        result_detail=job.result_detail,
        expire_reason=job.expire_reason,
        accepted_at=job.accepted_at,
        finalized_at=job.finalized_at,
        published_at=job.published_at,
        expired_at=job.expired_at,
        created_at=job.created_at,
        updated_at=job.updated_at,
    )


def _raise_recon_error(exc: ReconciliationError) -> None:
    raise HTTPException(status_code=exc.status_code, detail={
        "code": exc.code, "message": exc.detail,
    }) from exc


@app.post(
    "/reconciliations",
    response_model=ReconciliationOut,
    status_code=status.HTTP_201_CREATED,
    tags=["清单对账"],
    summary="受理船方清单对账（至多 80 行、合计至多 160 个源位置）",
)
def submit_reconciliation(
    req: ReconcileRequest,
    session: Session = Depends(get_session),
) -> ReconciliationOut:
    try:
        job = create_reconciliation(session, req, topic=settings.topic)
    except ReconciliationError as exc:
        _raise_recon_error(exc)
    return _serialize_job(session, job)


@app.get(
    "/reconciliations",
    response_model=list[ReconciliationSummary],
    tags=["清单对账"],
    summary="查询核对单列表（可按状态过滤；过期单不会提供当前通过结论）",
)
def list_reconciliations(
    status_filter: ReconciliationStatus | None = Query(
        default=None, alias="status", description="PENDING/PASSED/AMBIGUOUS/REJECTED/EXPIRED"
    ),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
) -> list[ReconciliationSummary]:
    rows = list_jobs(session, status=status_filter, limit=limit, offset=offset)
    return [ReconciliationSummary.model_validate(r) for r in rows]


@app.get(
    "/reconciliations/{job_id}",
    response_model=ReconciliationOut,
    tags=["清单对账"],
    summary="查询核对单详情（结论、完整依据与对应；读取时做失效复核）",
)
def get_reconciliation(
    job_id: int,
    session: Session = Depends(get_session),
) -> ReconciliationOut:
    job = refresh_job(session, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"核对单 {job_id} 不存在")
    return _serialize_job(session, job)


@app.post(
    "/reconciliations/{job_id}/publish",
    response_model=PublishResult,
    tags=["清单对账"],
    summary="发布核对单结论（过期单/不通过单拒绝发布；并发发布行锁串行化）",
)
def publish_reconciliation_endpoint(
    job_id: int,
    session: Session = Depends(get_session),
) -> PublishResult:
    try:
        job = publish_reconciliation(session, job_id)
    except ReconciliationError as exc:
        _raise_recon_error(exc)
    detail = (
        "已发布（重复发布幂等返回）" if job.published_at else "发布成功"
    )
    return PublishResult(
        id=job.id,
        status=job.status,
        published=True,
        pass_result=job.passed,
        detail=detail,
        reconciliation=_serialize_job(session, job),
    )
