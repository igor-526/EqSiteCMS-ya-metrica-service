"""Short fenced orchestration transactions around the bounded page move."""

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.exc import DBAPIError

from core.entities.metrica_maintenance import (
    MaintenanceLease,
    MaintenanceLeaseLostError,
    MergeBatch,
    MergeOverflowError,
    MergeStorageError,
    WorkKind,
)
from core.schemas.messaging.maintenance import MERGE_COMPLETED, MergeCompleted, maintenance_message_id
from models import metrica_maintenance_chunks as chunks
from models import metrica_maintenance_operations as operations
from repositories.metrica_merge_repository import MergeBatchRepository
from repositories.metrica_parse_task_repository import ParseTaskRepository
from repositories.metrica_result_outbox_repository import MaintenanceResultOutboxRepository


class MergeWorkerRepository(MergeBatchRepository, MaintenanceResultOutboxRepository):
    async def claim_merge(self, tenant: UUID, *, owner: UUID, lease_seconds: int) -> MaintenanceLease | None:
        self._duration(lease_seconds)
        if await self.selected_kind(tenant) != WorkKind.MERGE:
            return None
        # Expiry does not itself prove a drain. Reconcile attempts under the same
        # tenant -> account locks, but never cancel queued work merely for merge.
        await ParseTaskRepository(self.session).reap_expired_leases(equestrian_id=tenant)
        return await self.claim(tenant, owner=owner, lease_seconds=lease_seconds)

    async def move_batch(self, lease: MaintenanceLease, *, batch_size: int, lease_seconds: int) -> MergeBatch | None:
        try:
            return await super().move_batch(lease, batch_size=batch_size, lease_seconds=lease_seconds)
        except DBAPIError as error:
            if getattr(error.orig, "sqlstate", None) == "22003":
                raise MergeOverflowError("merge_aggregate_overflow") from None
            raise MergeStorageError("merge_storage_failed") from None

    async def finalize_merge(self, lease: MaintenanceLease) -> bool:
        await self.assert_owned(lease)
        row = await self._operation(lease.equestrian_id, lease.operation_id)
        finished = await self.session.scalar(
            select(func.count())
            .select_from(chunks)
            .where(
                chunks.c.equestrian_id == lease.equestrian_id,
                chunks.c.operation_id == lease.operation_id,
                chunks.c.status == "completed",
            )
        )
        if row["part_count"] is None or finished != row["part_count"]:
            return False
        result = MergeCompleted(
            schema_version=1,
            message_id=maintenance_message_id(MERGE_COMPLETED, lease.operation_id, "completed"),
            operation_id=lease.operation_id,
            equestrian_id=lease.equestrian_id,
            occurred_at=datetime.now(UTC),
            normalization_revision=row["normalization_revision"],
            status="completed",
            part_count=row["part_count"],
        )
        await self.save_result(result)
        await self.finish_work(lease, status="awaiting_seo", result_message_id=result.message_id)
        return True

    async def _release(self, lease: MaintenanceLease, **values) -> None:
        changed = await self.session.scalar(
            update(operations)
            .where(*self._owned(lease))
            .values(
                **values,
                lease_owner=None,
                lease_expires_at=None,
                fencing_revision=operations.c.fencing_revision + 1,
                updated_at=func.clock_timestamp(),
            )
            .returning(operations.c.id)
        )
        if changed is None:
            raise MaintenanceLeaseLostError("Merge lease expired before release")

    async def yield_merge(self, lease: MaintenanceLease) -> None:
        await self.assert_owned(lease)
        await self._release(lease, attempts=0, error_code=None)

    async def fail_merge(self, lease: MaintenanceLease, *, overflow: bool, max_attempts: int) -> str:
        await self.assert_owned(lease)
        row = await self._operation(lease.equestrian_id, lease.operation_id)
        attempts = row["attempts"] + 1
        blocked = overflow or attempts >= max_attempts
        await self._release(
            lease,
            status="failed_blocked" if blocked else "running",
            attempts=attempts,
            error_code="merge_aggregate_overflow" if overflow else "merge_storage_failed",
            next_attempt_at=func.clock_timestamp() + self._duration(min(60, 2 ** min(attempts - 1, 6))),
        )
        return "failed_blocked" if blocked else "retry"
