"""Strict v1 maintenance wire contract; no transport or persistence dependencies."""

from datetime import datetime, timedelta
from typing import Annotated, Literal, Self
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

CLEANUP_REQUESTED = "events.yandex.metrica.cleanup.requested"
MERGE_COMMAND = "commands.seo.pages.merge"
MERGE_FINALIZED = "events.seo.pages.merge.finalized"
CLEANUP_COMPLETED = "events.metrica.cleanup.completed"
MERGE_COMPLETED = "events.metrica.pages.merge.completed"
MAX_BODY_BYTES = 256 * 1024
Bigint = Annotated[int, Field(strict=True, ge=0, le=2**63 - 1)]
PositiveBigint = Annotated[int, Field(strict=True, gt=0, le=2**63 - 1)]


def maintenance_message_id(subject: str, operation_id: UUID, kind: str | int) -> UUID:
    return uuid5(NAMESPACE_URL, f"eqcms:maintenance:v1:{subject}:{operation_id}:{kind}")


class Envelope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1]
    message_id: UUID
    operation_id: UUID
    equestrian_id: UUID
    occurred_at: AwareDatetime

    @field_validator("occurred_at", mode="before")
    @classmethod
    def rfc3339_timestamp(cls, value: object) -> object:
        if not isinstance(value, datetime) and (not isinstance(value, str) or "T" not in value):
            raise ValueError("Invalid RFC3339 timestamp")
        return value

    @field_validator("schema_version", mode="before")
    @classmethod
    def strict_version(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("Invalid version")
        return value

    @model_validator(mode="after")
    def utc_and_version(self) -> Self:
        if self.occurred_at.utcoffset() != timedelta(0) or type(self.schema_version) is not int:
            raise ValueError("Invalid UTC timestamp or version")
        return self


class CleanupRequested(Envelope):
    cleanup_seq: PositiveBigint
    binding_generation: Bigint
    reason: Literal["counter_unlinked", "account_deleted"]


class PageMapping(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    source_page_id: UUID
    target_page_id: UUID

    @model_validator(mode="after")
    def different_pages(self) -> Self:
        if self.source_page_id == self.target_page_id:
            raise ValueError("Source equals target")
        return self


class MergeMappings(Envelope):
    normalization_revision: PositiveBigint
    kind: Literal["mappings"]
    part_index: Bigint
    mappings: list[PageMapping] = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def unique_sources(self) -> Self:
        if len({pair.source_page_id for pair in self.mappings}) != len(self.mappings):
            raise ValueError("Duplicate source")
        return self


class MergeSeal(Envelope):
    normalization_revision: PositiveBigint
    kind: Literal["seal"]
    part_count: Bigint


class MergeFinalized(Envelope):
    normalization_revision: PositiveBigint
    result_message_id: UUID


class CleanupCompleted(Envelope):
    cleanup_seq: PositiveBigint
    status: Literal["completed"]


class MergeCompleted(Envelope):
    normalization_revision: PositiveBigint
    status: Literal["completed", "superseded_by_delete"]
    part_count: Bigint | None
    cleanup_operation_id: UUID | None = None
    cleanup_seq: PositiveBigint | None = None

    @model_validator(mode="after")
    def result_correlation(self) -> Self:
        if self.status == "completed":
            valid = self.part_count is not None and self.cleanup_operation_id is None and self.cleanup_seq is None
        else:
            valid = self.cleanup_operation_id is not None and self.cleanup_seq is not None
        if not valid:
            raise ValueError("Invalid result correlation")
        return self


def identity_kind(command: Envelope) -> str | int:
    if isinstance(command, MergeMappings):
        return command.part_index
    if isinstance(command, MergeSeal):
        return "seal"
    if isinstance(command, MergeFinalized):
        return f"finalized:{command.result_message_id}"
    if isinstance(command, CleanupRequested):
        return "requested"
    if isinstance(command, MergeCompleted):
        return command.status
    return "completed"
