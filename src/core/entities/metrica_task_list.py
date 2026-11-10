"""Safe, infrastructure-independent projection of tenant operation history."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class MetricaTaskType(StrEnum):
    PARSING = "parsing"
    MERGE = "merge"
    DELETE = "delete"


class MetricaTaskStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class MetricaTaskListItem:
    id: str
    type: MetricaTaskType
    status: MetricaTaskStatus
    is_active: bool
    created_at: datetime


@dataclass(frozen=True)
class MetricaTaskListPage:
    items: list[MetricaTaskListItem]
    total: int
