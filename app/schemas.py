"""HTTP 接口的 Pydantic 模型。

刻意保持宽松：演示消息接口接受任意 JSON 对象（甚至畸形 JSON 原文），
以便把“坏消息”真正送进流水线验证隔离能力。
"""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, StrictInt, field_validator

from app.models import QuarantineStatus, ReconciliationStatus


class ProduceMessageRequest(BaseModel):
    """演示生产请求：消息内容由调用方自由构造。"""

    message: dict | list | str | int | float | bool | None = Field(
        default=None,
        description="消息体，将以 JSON 序列化后投递；典型为 "
        '{"container_no": "ABCD1234567", "gross_weight": 20000, "tare_weight": 4000}',
    )
    raw_text: str | None = Field(
        default=None,
        description="直接投递的原始文本（与 message 二选一），可用于投递畸形 JSON",
    )
    partition: int | None = Field(default=None, ge=0, description="可选，指定目标分区")
    key: str | None = Field(default=None, description="可选，消息 key（默认取箱号）")

    @field_validator("message", "raw_text")
    @classmethod
    def _not_both_empty(cls, v, info):  # pragma: no cover - 声明性校验
        return v


class ProducedMessage(BaseModel):
    topic: str
    partition: int
    offset: int
    key: str | None = None
    delivered_text: str


class CorrectionRequest(BaseModel):
    """对隔离记录的补正：只接受三个业务字段的合法值。"""

    container_no: str = Field(..., examples=["ABCD1234567"])
    gross_weight: int = Field(..., ge=0, examples=[20000])
    tare_weight: int = Field(..., ge=0, examples=[4000])


class NetWeightOut(BaseModel):
    id: int
    container_no: str
    gross_weight: int
    tare_weight: int
    net_weight: int
    topic: str
    source_partition: int
    source_offset: int
    from_correction: bool
    quarantine_id: int | None
    created_at: datetime

    model_config = {"from_attributes": True}


class QuarantineOut(BaseModel):
    id: int
    raw_payload: str
    container_no: str
    reason_code: str
    reason_detail: str
    topic: str
    source_partition: int
    source_offset: int
    status: QuarantineStatus
    last_correction_error: str | None
    correction_attempts: int
    corrected_at: datetime | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class CorrectionResult(BaseModel):
    accepted: bool
    quarantine_id: int
    status: QuarantineStatus
    reason: str | None = None
    net_weight: NetWeightOut | None = None


class HealthOut(BaseModel):
    status: str
    instance_id: str
    consumer_enabled: bool
    assigned_partitions: list[int] = Field(default_factory=list)


# --------------------------------------------------------------------------
# 船方清单对账
# --------------------------------------------------------------------------

class OffsetRangeIn(BaseModel):
    """某分区上的左闭右开 offset 范围 [offset_start, offset_end)。"""

    partition: StrictInt = Field(..., ge=0, examples=[0])
    offset_start: StrictInt = Field(..., ge=0, examples=[0])
    offset_end: StrictInt = Field(..., ge=1, examples=[10])


class ManifestLineIn(BaseModel):
    """清单行：唯一行号、箱号、整数净重闭区间 [net_weight_min, net_weight_max]。"""

    line_no: str = Field(..., min_length=1, max_length=64, examples=["L1"])
    container_no: str = Field(..., min_length=1, max_length=32, examples=["MSCU1234567"])
    # 严格整数：布尔/浮点/字符串数字一律 422，避免区间被误写
    net_weight_min: StrictInt = Field(..., ge=0, examples=[15000])
    net_weight_max: StrictInt = Field(..., ge=0, examples=[17000])


class ReconcileRequest(BaseModel):
    """对账受理请求：清单 + 本主题各分区的左闭右开 offset 范围。"""

    manifest: list[ManifestLineIn] = Field(
        ..., min_length=1, description="船方清单，至多 80 行，行号唯一"
    )
    offset_ranges: list[OffsetRangeIn] = Field(
        ..., min_length=1, description="左闭右开 offset 范围，合计源位置至多 160 个"
    )


class MatchLineOut(BaseModel):
    """一份完整对应中的单行指派。"""

    line_no: str
    container_no: str
    net_weight_min: int
    net_weight_max: int
    net_weight_id: int
    matched_container_no: str
    matched_net_weight: int
    source_partition: int
    source_offset: int

    model_config = {"from_attributes": True}


class BasisRecordOut(BaseModel):
    """核对依据：受理快照中的一条净重明细。"""

    net_weight_id: int
    container_no: str
    net_weight: int
    source_partition: int
    source_offset: int
    from_correction: bool

    model_config = {"from_attributes": True}


class OffsetRangeOut(BaseModel):
    # ORM 字段名为 source_partition，对外统一为 partition
    partition: int = Field(validation_alias="source_partition")
    offset_start: int
    offset_end: int

    model_config = {"from_attributes": True, "populate_by_name": True}


class ReconciliationOut(BaseModel):
    """核对单详情：结论、完整依据、对应（通过/多解时）。"""

    id: int
    topic: str
    status: ReconciliationStatus
    passed: bool | None
    manifest_row_count: int
    source_position_count: int
    manifest: list[ManifestLineIn]
    offset_ranges: list[OffsetRangeOut]
    basis_records: list[BasisRecordOut]
    matchings: list[list[MatchLineOut]]
    result_detail: str | None = None
    expire_reason: str | None = None
    accepted_at: datetime
    finalized_at: datetime | None = None
    published_at: datetime | None = None
    expired_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class ReconciliationSummary(BaseModel):
    """核对单列表项。"""

    id: int
    topic: str
    status: ReconciliationStatus
    passed: bool | None
    manifest_row_count: int
    source_position_count: int
    result_detail: str | None = None
    expire_reason: str | None = None
    accepted_at: datetime
    finalized_at: datetime | None = None
    published_at: datetime | None = None
    expired_at: datetime | None = None

    model_config = {"from_attributes": True}


class PublishResult(BaseModel):
    """发布结果。"""

    id: int
    status: ReconciliationStatus
    published: bool
    pass_result: bool | None = None
    detail: str
    reconciliation: ReconciliationOut
