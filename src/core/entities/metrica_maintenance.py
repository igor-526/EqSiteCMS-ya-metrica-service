"""Infrastructure-independent maintenance arbitration contracts."""

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from uuid import UUID


class WorkKind(StrEnum):
    CLEANUP = "cleanup"
    MERGE = "merge"
    PARSE = "parse"


@dataclass(frozen=True)
class MaintenanceLease:
    equestrian_id: UUID
    operation_id: UUID
    owner: UUID
    revision: int
    kind: WorkKind
    status: str


class MaintenanceLeaseLostError(RuntimeError):
    """Roll back the entire transaction, including effects before the CAS."""


class MaintenanceConflictError(ValueError):
    """Invalid immutable identity, payload or state; never include payload in logs."""


@dataclass(frozen=True)
class BindingLifecycle:
    """Authoritative non-secret snapshot fetched BEFORE entering the transaction."""

    binding_generation: int
    cleanup_requested_seq: int
    cleanup_completed_seq: int
    account_id: UUID | None = None
    counter_id: str | None = None

    def __post_init__(self) -> None:
        if not 0 <= self.cleanup_completed_seq <= self.cleanup_requested_seq or self.binding_generation < 0:
            raise ValueError("Invalid binding lifecycle")
        if (self.account_id is None) != (self.counter_id is None):
            raise ValueError("Incomplete binding identity")


class IntentAdmission(StrEnum):
    READY = "ready"
    ADMITTED = "admitted"
    DEFERRED = "deferred"
    DUPLICATE = "duplicate"
    ACTIVE_RUN = "active_run"
    CANCELLED = "cancelled"
    MISSING_BINDING = "missing_binding"


@dataclass(frozen=True)
class PendingParseIntent:
    job_id: UUID
    equestrian_id: UUID
    date_mode: str
    date_from: date | None = None
    date_to: date | None = None

    def __post_init__(self) -> None:
        if self.date_mode == "auto" and self.date_from is None and self.date_to is None:
            return
        if self.date_mode == "explicit" and self.date_from is not None and self.date_to is not None:
            if self.date_from <= self.date_to:
                return
        raise ValueError("Invalid intent dates")

    @property
    def payload_hash(self) -> str:
        # Lifecycle is observation, not command identity: redelivery may see a newer binding.
        payload = [str(self.equestrian_id), self.date_mode, str(self.date_from), str(self.date_to)]
        return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class CleanupBatch:
    """Committed batch outcome, NOT cleanup completion or permission to parse."""

    table: str
    deleted_rows: int
    exhausted: bool


@dataclass(frozen=True)
class MergeBatch:
    """One mapping's bounded date batch; not an operation completion."""

    part_index: int
    mapping_index: int
    moved_rows: int
    part_completed: bool


@dataclass(frozen=True)
class MaintenanceResultClaim:
    message_id: UUID
    subject: str
    payload: dict
    owner: UUID
    revision: int
    attempts: int


class MergeStorageError(RuntimeError):
    """Safe storage diagnostic; the failed effect transaction must roll back."""


class MergeOverflowError(MergeStorageError):
    """Aggregate cannot fit the storage type; operator intervention required."""
