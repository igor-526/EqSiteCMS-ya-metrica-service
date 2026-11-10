"""Safe task history wire contract; no storage or integration internals."""

from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from core.entities.metrica_task_list import MetricaTaskStatus, MetricaTaskType


class MetricaTaskListInDto(BaseModel):
    equestrian_id: UUID
    limit: int = Field(default=20, strict=True, ge=1, le=100)
    offset: int = Field(default=0, strict=True, ge=0)


class MetricaTaskOutDto(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra="forbid")

    id: str = Field(strict=True)
    type: MetricaTaskType
    status: MetricaTaskStatus
    is_active: bool = Field(strict=True)
    created_at: AwareDatetime


class MetricaTaskListOutDto(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[MetricaTaskOutDto]
    total: int = Field(strict=True, ge=0)
