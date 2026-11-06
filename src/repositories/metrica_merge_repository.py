"""Bounded historical page moves; caller owns the effect/checkpoint transaction."""

from uuid import UUID

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert

from core.entities.metrica_maintenance import (
    MaintenanceConflictError,
    MaintenanceLease,
    MergeBatch,
    WorkKind,
)
from core.entities.metrica_metrics import BASE_METRICS
from models import metrica_maintenance_chunks as chunks
from models import metrica_page_stats as pages
from repositories.metrica_maintenance_storage_repository import MaintenanceStorageRepository


class MergeBatchRepository(MaintenanceStorageRepository):
    async def move_batch(self, lease: MaintenanceLease, *, batch_size: int, lease_seconds: int) -> MergeBatch | None:
        if lease.kind != WorkKind.MERGE or lease.status != "running":
            raise ValueError("Merge requires a running lease")
        if type(batch_size) is not int or not 1 <= batch_size <= 1000:
            raise ValueError("Invalid merge batch size")
        self._duration(lease_seconds)
        # Tenant lock serializes target collisions and excludes parse/cleanup.
        # The final checkpoint CAS rechecks DB-clock expiry after all effects.
        await self.assert_owned(lease)
        chunk = (
            (
                await self.session.execute(
                    select(chunks)
                    .where(
                        chunks.c.equestrian_id == lease.equestrian_id,
                        chunks.c.operation_id == lease.operation_id,
                        chunks.c.status != "completed",
                    )
                    .order_by(chunks.c.part_index)
                    .limit(1)
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        if chunk is None:
            return None
        # Only ONE persisted part (<=500 mappings) is loaded, never the operation.
        mappings = self._mappings(chunk["mappings"])
        checkpoint = chunk["checkpoint"]
        index = checkpoint.get("mapping_index", 0)
        if set(checkpoint) - {"mapping_index"} or type(index) is not int or not 0 <= index < len(mappings):
            raise MaintenanceConflictError("Invalid merge checkpoint")
        pair = mappings[index]
        source, target = UUID(pair["source_page_id"]), UUID(pair["target_page_id"])
        rows = (
            (
                await self.session.execute(
                    select(pages)
                    .where(pages.c.equestrian_id == lease.equestrian_id, pages.c.page_id == source)
                    .order_by(pages.c.stat_date)
                    .limit(batch_size)
                    .with_for_update()
                )
            )
            .mappings()
            .all()
        )
        # Deleted source dates are the durable keyset. No OFFSET or lower-bound
        # watermark that could hide rows after restart. Empty mappings create no rows.
        if rows:
            statement = insert(pages).values(
                [
                    {
                        "equestrian_id": lease.equestrian_id,
                        "stat_date": row["stat_date"],
                        "page_id": target,
                        "parse_task_id": None,
                        **{name: row[name] for name in BASE_METRICS},
                    }
                    for row in rows
                ]
            )
            await self.session.execute(
                statement.on_conflict_do_update(
                    index_elements=["equestrian_id", "stat_date", "page_id"],
                    set_={
                        **{name: pages.c[name] + statement.excluded[name] for name in BASE_METRICS},
                        "parse_task_id": None,
                        "updated_at": func.now(),
                    },
                )
            )
            await self.session.execute(
                delete(pages).where(
                    pages.c.equestrian_id == lease.equestrian_id,
                    pages.c.page_id == source,
                    pages.c.id.in_([row["id"] for row in rows]),
                )
            )
        next_index = index + 1 if len(rows) < batch_size else index
        completed = next_index == len(mappings)
        await self.checkpoint_chunk(
            lease,
            part_index=chunk["part_index"],
            checkpoint={"mapping_index": next_index},
            completed=completed,
            lease_seconds=lease_seconds,
        )
        return MergeBatch(chunk["part_index"], index, len(rows), completed)
