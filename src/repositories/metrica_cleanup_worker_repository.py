"""Cleanup orchestration storage; short tenant transactions, no scheduling or I/O."""

from uuid import UUID

from sqlalchemy import func, select, update

from core.entities.metrica_maintenance import MaintenanceLease, MaintenanceLeaseLostError, WorkKind
from models import metrica_maintenance_operations as operations
from repositories.metrica_cleanup_repository import CleanupBatchRepository
from repositories.metrica_parse_task_repository import ParseTaskRepository
from repositories.metrica_pending_parse_repository import PendingParseIntentRepository


class CleanupWorkerRepository(CleanupBatchRepository):
    async def claim_cleanup(self, tenant: UUID, *, owner: UUID, lease_seconds: int) -> MaintenanceLease | None:
        self._duration(lease_seconds)
        await self.lock_scope([tenant])
        row = (
            (
                await self.session.execute(
                    select(operations)
                    .where(
                        operations.c.equestrian_id == tenant,
                        operations.c.kind == WorkKind.CLEANUP,
                        operations.c.status != "completed",
                    )
                    .order_by(operations.c.cleanup_seq)
                    .limit(1)
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None or row["status"] not in {"pending", "draining", "deleting"}:
            return None
        tasks = ParseTaskRepository(self.session)
        # Old queued work never drains the entire run. New observed-seq intents survive.
        await tasks.cancel_queued_before_cleanup(tenant, cleanup_seq=row["cleanup_seq"])
        await PendingParseIntentRepository(self.session).cancel_before_cleanup(tenant, cleanup_seq=row["cleanup_seq"])
        # Expiry alone is not a drain: terminalize under tenant -> account locks.
        # Local cleanup ledger is sufficient; no peer request while holding locks.
        await tasks.reap_expired_leases(equestrian_id=tenant)
        if await self._running_parse(tenant):
            await self.session.execute(
                update(operations)
                .where(
                    operations.c.equestrian_id == tenant,
                    operations.c.id == row["id"],
                    operations.c.status.in_(["pending", "draining"]),
                )
                .values(status="draining", updated_at=func.clock_timestamp())
            )
            return None
        # claim rechecks prefix, next_attempt_at, priority and live lease under same lock.
        return await self.claim(tenant, owner=owner, lease_seconds=lease_seconds)

    async def yield_cleanup(self, lease: MaintenanceLease) -> None:
        await self.assert_owned(lease)
        changed = await self.session.scalar(
            update(operations)
            .where(*self._owned(lease))
            .values(
                lease_owner=None,
                lease_expires_at=None,
                fencing_revision=operations.c.fencing_revision + 1,
                updated_at=func.clock_timestamp(),
            )
            .returning(operations.c.id)
        )
        if changed is None:
            raise MaintenanceLeaseLostError("Cleanup lease expired before yield")

    async def fail_cleanup(self, lease: MaintenanceLease, *, max_attempts: int) -> str:
        if lease.kind != WorkKind.CLEANUP or lease.status != "deleting" or max_attempts <= 0:
            raise ValueError("Invalid cleanup failure transition")
        await self.assert_owned(lease)
        row = await self._operation(lease.equestrian_id, lease.operation_id)
        attempts = row["attempts"] + 1
        blocked = attempts >= max_attempts
        changed = await self.session.scalar(
            update(operations)
            .where(*self._owned(lease))
            .values(
                status="failed_blocked" if blocked else "deleting",
                attempts=attempts,
                error_code="cleanup_storage_failed",
                next_attempt_at=func.clock_timestamp() + self._duration(min(60, 2 ** min(attempts - 1, 6))),
                lease_owner=None,
                lease_expires_at=None,
                fencing_revision=operations.c.fencing_revision + 1,
                updated_at=func.clock_timestamp(),
            )
            .returning(operations.c.id)
        )
        if changed is None:
            raise MaintenanceLeaseLostError("Cleanup lease expired before failure transition")
        return "failed_blocked" if blocked else "retry"
