"""ORM 模型：净重明细、隔离记录。

幂等键 (topic, source_partition, source_offset) 在两张表上各自唯一，
与消费位点的手工提交配合，实现“每条源消息仅入仓一次”。
"""
from __future__ import annotations

import enum
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base

# SQLite 仅允许 INTEGER PRIMARY KEY 自增，生产 PostgreSQL 仍使用 BIGINT
PKBigInt = BigInteger().with_variant(Integer(), "sqlite")


class QuarantineStatus(str, enum.Enum):
    OPEN = "OPEN"          # 仍在隔离，等待合法补正
    CORRECTED = "CORRECTED"  # 已接受唯一一次合法补正并生成净重明细


class ReconciliationStatus(str, enum.Enum):
    """核对单状态机。

    PENDING    已受理落库、结论尚未生效（崩溃重启后会被恢复重算）；
    PASSED     唯一完整对应，结论与依据已一起生效；
    AMBIGUOUS  存在多份完整对应，结论与两份对应已一起生效；
    REJECTED   无完整对应（不通过），结论与依据已一起生效；
    EXPIRED    范围内发生新增/补正入仓，结论作废，必须重新核对。
    """

    PENDING = "PENDING"
    PASSED = "PASSED"
    REJECTED = "REJECTED"
    AMBIGUOUS = "AMBIGUOUS"
    EXPIRED = "EXPIRED"


# 结论态：已生效且尚未过期
ACTIVE_RECON_STATUSES = (
    ReconciliationStatus.PASSED,
    ReconciliationStatus.AMBIGUOUS,
    ReconciliationStatus.REJECTED,
    ReconciliationStatus.PENDING,
)


class NetWeightRecord(Base):
    """净重明细（入仓事实表，只追加）。"""

    __tablename__ = "net_weight_records"
    __table_args__ = (
        UniqueConstraint(
            "topic",
            "source_partition",
            "source_offset",
            name="uq_net_weight_source_position",
        ),
        CheckConstraint("gross_weight >= 0", name="ck_net_gross_non_negative"),
        CheckConstraint("tare_weight >= 0", name="ck_net_tare_non_negative"),
        CheckConstraint("gross_weight >= tare_weight", name="ck_net_gross_ge_tare"),
        CheckConstraint("net_weight >= 0", name="ck_net_weight_non_negative"),
        Index("ix_net_weight_container_no", "container_no"),
        Index("ix_net_weight_created_at", "created_at"),
    )

    id: Mapped[int] = mapped_column(PKBigInt, primary_key=True)
    container_no: Mapped[str] = mapped_column(String(32), nullable=False)
    gross_weight: Mapped[int] = mapped_column(BigInteger, nullable=False)
    tare_weight: Mapped[int] = mapped_column(BigInteger, nullable=False)
    net_weight: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # 来源定位
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    source_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    source_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)

    # 来源标记：消费者正常处理 / 隔离补正
    from_correction: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    # 补正成功时关联的隔离记录 id；正常路径为空
    quarantine_id: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class QuarantineRecord(Base):
    """隔离记录：无效消息的原文、错误原因与补正状态。"""

    __tablename__ = "quarantine_records"
    __table_args__ = (
        UniqueConstraint(
            "topic",
            "source_partition",
            "source_offset",
            name="uq_quarantine_source_position",
        ),
        Index("ix_quarantine_status", "status"),
        Index("ix_quarantine_partition_offset", "source_partition", "source_offset"),
        Index("ix_quarantine_container_no", "container_no"),
    )

    id: Mapped[int] = mapped_column(PKBigInt, primary_key=True)

    # 原文原样保留（UTF-8 解码失败时保留可打印转义），同文不同位置分别保留
    raw_payload: Mapped[str] = mapped_column(String, nullable=False)
    # 解析出的箱号（无法解析时为空串），便于检索
    container_no: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    reason_code: Mapped[str] = mapped_column(String(64), nullable=False)
    reason_detail: Mapped[str] = mapped_column(String(512), nullable=False)

    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    source_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    source_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)

    status: Mapped[QuarantineStatus] = mapped_column(
        Enum(QuarantineStatus, name="quarantine_status"),
        nullable=False,
        default=QuarantineStatus.OPEN,
    )
    # 最近一次失败补正的原因（成功后清空思路：保留最后错误，便于审计）
    last_correction_error: Mapped[str | None] = mapped_column(
        String(512), nullable=True
    )
    correction_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    corrected_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


# --------------------------------------------------------------------------
# 船方清单对账（核对单）
# --------------------------------------------------------------------------

class ReconciliationJob(Base):
    """核对单：一次对账受理的完整凭证与结论。

    生命周期：受理即落 PENDING（含范围），结论计算后同事务翻转为
    PASSED / REJECTED / AMBIGUOUS 并保存依据与对应；范围内任何
    新增/补正入仓都会把未过期的单据翻成 EXPIRED。
    """

    __tablename__ = "reconciliation_jobs"
    __table_args__ = (
        Index("ix_recon_job_status", "status"),
        Index("ix_recon_job_created_at", "created_at"),
        CheckConstraint("manifest_row_count >= 0", name="ck_recon_row_count_non_negative"),
        CheckConstraint("source_position_count >= 0", name="ck_recon_position_count_non_negative"),
    )

    id: Mapped[int] = mapped_column(PKBigInt, primary_key=True)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)

    # 原始清单（JSON 文本），原样保存受理输入
    manifest_payload: Mapped[str] = mapped_column(Text, nullable=False)
    manifest_row_count: Mapped[int] = mapped_column(Integer, nullable=False)
    source_position_count: Mapped[int] = mapped_column(Integer, nullable=False)

    status: Mapped[ReconciliationStatus] = mapped_column(
        Enum(ReconciliationStatus, name="reconciliation_status"),
        nullable=False,
        default=ReconciliationStatus.PENDING,
    )
    # 结论口径的冗余标记，便于检索：PENDING/EXPIRED 为 NULL
    passed: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    # 受理时范围内净重明细位置快照（JSON: [[partition, offset], ...]）。
    # 读取/发布时与当前范围内位置集合比对，作为失效判定的最终兜底。
    basis_snapshot: Mapped[str | None] = mapped_column(Text, nullable=True)
    # 结论说明（不通过原因/多解说明等）
    result_detail: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    # 失效原因与触发位点（置 EXPIRED 时写入）
    expire_reason: Mapped[str | None] = mapped_column(String(512), nullable=True)

    accepted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    finalized_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    expired_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class ReconciliationRange(Base):
    """核对单声明的左闭右开 offset 范围，按分区保存。"""

    __tablename__ = "reconciliation_ranges"
    __table_args__ = (
        # 同一核对单同一分区可有多段不重叠范围，联合行本身去重
        UniqueConstraint(
            "job_id",
            "source_partition",
            "offset_start",
            "offset_end",
            name="uq_recon_range_job_partition_span",
        ),
        Index("ix_recon_range_partition_offsets", "source_partition", "offset_start", "offset_end"),
        CheckConstraint("offset_end > offset_start", name="ck_recon_range_offsets_ordered"),
    )

    id: Mapped[int] = mapped_column(PKBigInt, primary_key=True)
    job_id: Mapped[int] = mapped_column(
        PKBigInt, ForeignKey("reconciliation_jobs.id"), nullable=False
    )
    source_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    offset_start: Mapped[int] = mapped_column(BigInteger, nullable=False)
    offset_end: Mapped[int] = mapped_column(BigInteger, nullable=False)


class ReconciliationBasis(Base):
    """核对依据：受理时范围内已入仓的每条净重明细快照（结论一起生效）。"""

    __tablename__ = "reconciliation_basis"
    __table_args__ = (
        Index("ix_recon_basis_job", "job_id"),
        Index("ix_recon_basis_position", "source_partition", "source_offset"),
    )

    id: Mapped[int] = mapped_column(PKBigInt, primary_key=True)
    job_id: Mapped[int] = mapped_column(
        PKBigInt, ForeignKey("reconciliation_jobs.id"), nullable=False
    )
    net_weight_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    container_no: Mapped[str] = mapped_column(String(32), nullable=False)
    net_weight: Mapped[int] = mapped_column(BigInteger, nullable=False)
    topic: Mapped[str] = mapped_column(String(255), nullable=False)
    source_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    source_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    from_correction: Mapped[bool] = mapped_column(Boolean, nullable=False)


class ReconciliationMatchLine(Base):
    """对应明细：一份核对单的一份完整对应，每行清单到一条净重明细的指派。

    matching_no：1 = 第一份完整对应；2 = 多解时的第二份不同完整对应。
    """

    __tablename__ = "reconciliation_match_lines"
    __table_args__ = (
        UniqueConstraint(
            "job_id", "matching_no", "line_no", name="uq_recon_match_line"
        ),
        Index("ix_recon_match_job", "job_id"),
        CheckConstraint("matching_no IN (1, 2)", name="ck_recon_matching_no"),
    )

    id: Mapped[int] = mapped_column(PKBigInt, primary_key=True)
    job_id: Mapped[int] = mapped_column(
        PKBigInt, ForeignKey("reconciliation_jobs.id"), nullable=False
    )
    matching_no: Mapped[int] = mapped_column(Integer, nullable=False)
    line_no: Mapped[str] = mapped_column(String(64), nullable=False)
    container_no: Mapped[str] = mapped_column(String(32), nullable=False)
    net_weight_min: Mapped[int] = mapped_column(BigInteger, nullable=False)
    net_weight_max: Mapped[int] = mapped_column(BigInteger, nullable=False)
    net_weight_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    matched_container_no: Mapped[str] = mapped_column(String(32), nullable=False)
    matched_net_weight: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    source_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
