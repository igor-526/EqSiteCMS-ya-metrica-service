"""Caller owns a short transaction; never hold these locks across peer I/O."""

from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import Protocol
from uuid import UUID

from core.entities.metrica_maintenance import (
    BindingLifecycle,
    CleanupBatch,
    IntentAdmission,
    MaintenanceLease,
    MaintenanceResultClaim,
    MergeBatch,
    PendingParseIntent,
    WorkKind,
)
from core.entities.metrica_parse import ParseRun, ParseTask


class MaintenanceArbitrationProtocol(Protocol):
    async def lock_scope(self, equestrian_ids: Iterable[UUID], account_ids: Iterable[UUID] = ()) -> None:
        """All sorted tenant locks first, then all sorted account locks."""
        ...

    async def selected_kind(self, equestrian_id: UUID) -> WorkKind:
        """Priority, not readiness: blocked/delayed operations still bar lower kinds."""
        ...

    async def claim(self, equestrian_id: UUID, *, owner: UUID, lease_seconds: int) -> MaintenanceLease | None: ...

    async def assert_owned(self, lease: MaintenanceLease) -> None:
        """Lock tenant, reject expired/stale/preempted lease; rollback on failure."""
        ...

    async def checkpoint(self, lease: MaintenanceLease, *, checkpoint: dict, lease_seconds: int) -> None:
        """CAS and renew in the SAME transaction as the bounded batch effect."""
        ...


class MaintenanceStorageProtocol(MaintenanceArbitrationProtocol, Protocol):
    async def save_operation(
        self,
        tenant: UUID,
        operation: UUID,
        *,
        kind: WorkKind,
        cleanup_seq: int | None = None,
        binding_generation: int | None = None,
        normalization_revision: int | None = None,
    ) -> bool: ...

    async def save_inbox(
        self,
        tenant: UUID,
        operation: UUID,
        *,
        consumer: str,
        message_id: UUID,
        payload: dict,
    ) -> bool: ...

    async def save_chunk(
        self,
        tenant: UUID,
        operation: UUID,
        *,
        part_index: int,
        mappings: list[dict[str, str]],
    ) -> bool: ...

    async def seal(self, tenant: UUID, operation: UUID, *, part_count: int) -> bool: ...

    async def finish_work(
        self,
        lease: MaintenanceLease,
        *,
        status: str,
        result_message_id: UUID | None = None,
    ) -> None:
        """Caller must persist result outbox in the SAME transaction. Errors require rollback."""
        ...

    async def supersede(self, tenant: UUID, operation: UUID, *, cleanup_operation: UUID) -> bool: ...

    async def finalize(
        self,
        tenant: UUID,
        operation: UUID,
        *,
        normalization_revision: int,
        result_message_id: UUID,
    ) -> bool: ...

    async def record_superseded_result(
        self,
        tenant: UUID,
        operation: UUID,
        *,
        result_message_id: UUID,
    ) -> bool: ...

    async def checkpoint_chunk(
        self,
        lease: MaintenanceLease,
        *,
        part_index: int,
        checkpoint: dict,
        completed: bool,
        lease_seconds: int,
    ) -> None: ...


class PendingParseIntentProtocol(Protocol):
    async def enqueue_intent(self, intent: PendingParseIntent, *, lifecycle: BindingLifecycle) -> IntentAdmission: ...

    async def prepare_admission(self, intent: PendingParseIntent, *, lifecycle: BindingLifecycle) -> IntentAdmission:
        """Transaction-local claim before auto-window resolution; not a worker lease."""
        ...

    async def consume_intent(
        self,
        intent: PendingParseIntent,
        *,
        lifecycle: BindingLifecycle,
        run: ParseRun,
        tasks: Sequence[ParseTask],
    ) -> IntentAdmission: ...

    async def ready_intents(self, *, limit: int) -> list[PendingParseIntent]: ...

    async def defer_intent(self, intent: PendingParseIntent, *, next_attempt_at: datetime) -> None: ...

    async def cancel_before_cleanup(self, tenant: UUID, *, cleanup_seq: int) -> None: ...

    async def admitted_run(self, intent: PendingParseIntent) -> UUID | None: ...


class CleanupBatchProtocol(Protocol):
    async def delete_batch(self, lease: MaintenanceLease, *, batch_size: int, lease_seconds: int) -> CleanupBatch:
        """One bounded effect + checkpoint + lease renewal; caller owns transaction."""
        ...


class CleanupFinalizerProtocol(Protocol):
    async def finalize_cleanup(self, lease: MaintenanceLease) -> bool:
        """Recheck fences and BOTH stats tables, atomically complete + persist result."""
        ...


class MaintenanceResultOutboxProtocol(Protocol):
    async def claim_result(self, *, owner: UUID, lease_seconds: int) -> MaintenanceResultClaim | None: ...

    async def mark_published(self, claim: MaintenanceResultClaim) -> None: ...

    async def retry_result(self, claim: MaintenanceResultClaim) -> None: ...


class MaintenanceResultPublisherProtocol(Protocol):
    async def publish_result(self, claim: MaintenanceResultClaim) -> bool:
        """Return PubAck.duplicate; raise if no valid PubAck. Never mark storage here."""
        ...


class CleanupWorkerProtocol(Protocol):
    async def claim_cleanup(self, tenant: UUID, *, owner: UUID, lease_seconds: int) -> MaintenanceLease | None:
        """Cancel old queued work, terminalize expired attempts, wait for live running."""
        ...

    async def yield_cleanup(self, lease: MaintenanceLease) -> None:
        """Fenced release after one committed batch; next tick must claim again."""
        ...

    async def fail_cleanup(self, lease: MaintenanceLease, *, max_attempts: int) -> str:
        """Fresh transaction after rollback: fenced retry or durable failed_blocked."""
        ...


class MergeBatchProtocol(Protocol):
    async def move_batch(self, lease: MaintenanceLease, *, batch_size: int, lease_seconds: int) -> MergeBatch | None:
        """One page mapping/date batch + chunk checkpoint + lease CAS, caller commits.

        None means no unfinished chunks, NOT permission to finalize or release gate.
        No parse upsert, no query-stat writes. Any error requires full rollback.
        """
        ...


class MergeWorkerRepositoryProtocol(MergeBatchProtocol, Protocol):
    async def claim_merge(self, tenant: UUID, *, owner: UUID, lease_seconds: int) -> MaintenanceLease | None: ...

    async def finalize_merge(self, lease: MaintenanceLease) -> bool:
        """Recheck complete sealed collection and persist result + awaiting_seo atomically."""
        ...

    async def yield_merge(self, lease: MaintenanceLease) -> None: ...

    async def fail_merge(self, lease: MaintenanceLease, *, overflow: bool, max_attempts: int) -> str:
        """Fresh transaction after rollback: fenced retry or durable failed_blocked."""
        ...
