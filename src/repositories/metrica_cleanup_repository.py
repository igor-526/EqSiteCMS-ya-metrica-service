"""Bounded cleanup effects; no commits, broker calls or terminal transitions."""

from datetime import UTC, datetime

from sqlalchemy import delete, exists, func, select

from core.entities.metrica_maintenance import (
    CleanupBatch,
    MaintenanceConflictError,
    MaintenanceLease,
    WorkKind,
)
from core.schemas.messaging.maintenance import CLEANUP_COMPLETED, CleanupCompleted, maintenance_message_id
from models import metrica_page_stats, metrica_search_query_stats
from repositories.metrica_result_outbox_repository import MaintenanceResultOutboxRepository


class CleanupBatchRepository(MaintenanceResultOutboxRepository):
    async def delete_batch(self, lease: MaintenanceLease, *, batch_size: int, lease_seconds: int) -> CleanupBatch:
        if lease.kind != WorkKind.CLEANUP or lease.status != "deleting":
            raise ValueError("Cleanup requires a deleting lease")
        if isinstance(batch_size, bool) or not 1 <= batch_size <= 10000:
            raise ValueError("Invalid cleanup batch size")
        self._duration(lease_seconds)
        # Locks/fences before the effect; checkpoint CAS rechecks expiry after it.
        await self.assert_owned(lease)
        row = await self._operation(lease.equestrian_id, lease.operation_id)
        checkpoint = row["checkpoint"]
        stage = checkpoint.get("table", "pages")
        if stage not in {"pages", "search_queries", "exhausted"}:
            raise MaintenanceConflictError("Invalid cleanup checkpoint")
        tables = {"pages": metrica_page_stats, "search_queries": metrica_search_query_stats}
        deleted = 0
        next_stage = stage
        if stage != "exhausted":
            table = tables[stage]
            # The first remaining PK is the keyset cursor: committed keys are gone.
            # Do not use OFFSET or a saved UUID lower bound (which could skip rows).
            keys = (
                select(table.c.id)
                .where(table.c.equestrian_id == lease.equestrian_id)
                .order_by(table.c.id)
                .limit(batch_size)
            )
            effect = (
                delete(table)
                .where(table.c.equestrian_id == lease.equestrian_id, table.c.id.in_(keys))
                .returning(table.c.id)
                .cte("deleted_cleanup_rows")
            )
            # Aggregate on the server: never materialize RETURNING rows in Python.
            deleted = (await self.session.execute(select(func.count()).select_from(effect))).scalar_one()
            if deleted < batch_size:
                next_stage = "search_queries" if stage == "pages" else "exhausted"
        if next_stage == "exhausted":
            # Exhaustion is only a candidate for result finalization. Recheck BOTH
            # tables under the same tenant lock; never trust a checkpoint alone.
            for name, table in tables.items():
                if await self.session.scalar(select(exists().where(table.c.equestrian_id == lease.equestrian_id))):
                    next_stage = name
                    break
        await self.checkpoint(
            lease,
            checkpoint={"table": next_stage},
            lease_seconds=lease_seconds,
        )
        return CleanupBatch(stage, deleted, next_stage == "exhausted")

    async def finalize_cleanup(self, lease: MaintenanceLease) -> bool:
        if lease.kind != WorkKind.CLEANUP or lease.status != "deleting":
            raise ValueError("Cleanup requires a deleting lease")
        await self.assert_owned(lease)
        for table in (metrica_page_stats, metrica_search_query_stats):
            if await self.session.scalar(select(exists().where(table.c.equestrian_id == lease.equestrian_id))):
                return False
        row = await self._operation(lease.equestrian_id, lease.operation_id)
        result = CleanupCompleted(
            schema_version=1,
            message_id=maintenance_message_id(CLEANUP_COMPLETED, lease.operation_id, "completed"),
            operation_id=lease.operation_id,
            equestrian_id=lease.equestrian_id,
            occurred_at=datetime.now(UTC),
            cleanup_seq=row["cleanup_seq"],
            status="completed",
        )
        await self.save_result(result)
        # finish_work repeats ownership and expiry CAS AFTER all checks/effects.
        # Both terminal state/prefix and outbox roll back on any failure.
        await self.finish_work(lease, status="completed", result_message_id=result.message_id)
        return True
