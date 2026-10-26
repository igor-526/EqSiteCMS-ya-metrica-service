from datetime import UTC, date, datetime
from enum import StrEnum
from uuid import UUID

from pydantic import Field, model_validator

from .base import Entity, TimestampMixin


class TaskStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ParseKind(StrEnum):
    PAGES = "pages"
    SEARCH_QUERIES = "search_queries"


class DateMode(StrEnum):
    EXPLICIT = "explicit"
    AUTO = "auto"


class ParseErrorCode(StrEnum):
    CREDENTIALS_INVALID = "credentials_invalid"
    CREDENTIALS_UNAVAILABLE = "credentials_unavailable"
    BINDING_CHANGED = "binding_changed"
    YANDEX_SERVICE_UNAVAILABLE = "yandex_service_unavailable"
    PROVIDER_QUERY_ERROR = "provider_query_error"
    PROVIDER_RATE_LIMITED = "provider_rate_limited"
    PROVIDER_UNAVAILABLE = "provider_unavailable"
    PROVIDER_RESPONSE_INVALID = "provider_response_invalid"
    SEO_UNAVAILABLE = "seo_unavailable"
    SEO_REJECTED = "seo_rejected"
    STORAGE_ERROR = "storage_error"
    LEASE_EXPIRED = "lease_expired"
    UNEXPECTED = "unexpected"


class ParseRun(Entity):
    equestrian_id: UUID
    date_mode: DateMode
    clamped: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ParseTask(Entity, TimestampMixin):
    run_id: UUID
    equestrian_id: UUID
    counter_id: str = Field(min_length=1, max_length=31)
    yandex_account_id: UUID
    kind: ParseKind
    date_from: date
    date_to: date
    sequence: int = Field(ge=0)
    status: TaskStatus = TaskStatus.QUEUED
    attempts: int = Field(default=0, ge=0)
    max_attempts: int = Field(gt=0)
    next_attempt_at: datetime | None = None
    next_offset: int = Field(default=1, ge=1)
    locked_by: str | None = Field(default=None, max_length=128)
    locked_until: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    pages_fetched: int = Field(default=0, ge=0)
    rows_fetched: int = Field(default=0, ge=0)
    rows_inserted: int = Field(default=0, ge=0)
    rows_updated: int = Field(default=0, ge=0)
    rows_skipped: int = Field(default=0, ge=0)
    rows_deleted: int = Field(default=0, ge=0)
    sampled: bool = False
    error_code: ParseErrorCode | None = None
    error_status: int | None = None

    @model_validator(mode="after")
    def validate_window(self) -> ParseTask:
        if not 0 <= (self.date_to - self.date_from).days <= 4:
            raise ValueError("Parse window must contain 1 to 5 days")
        return self
