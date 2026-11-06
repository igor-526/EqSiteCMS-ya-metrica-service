"""Admission-time preemption, inbox and correlated outbox share one transaction."""

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select

from core.entities.metrica_maintenance import WorkKind
from core.schemas.messaging.maintenance import MERGE_COMPLETED, MergeCompleted, maintenance_message_id
from models import metrica_maintenance_operations as operations
from repositories.metrica_result_outbox_repository import MaintenanceResultOutboxRepository


class MaintenanceDeliveryRepository(MaintenanceResultOutboxRepository):
    async def save_operation(
        self,
        tenant: UUID,
        operation: UUID,
        *,
        kind: WorkKind,
        cleanup_seq: int | None = None,
        binding_generation: int | None = None,
        normalization_revision: int | None = None,
    ) -> bool:
        created = await super().save_operation(
            tenant,
            operation,
            kind=kind,
            cleanup_seq=cleanup_seq,
            binding_generation=binding_generation,
            normalization_revision=normalization_revision,
        )
        # The tenant lock also serializes this with an in-flight atomic merge batch.
        cleanup = (
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
        if cleanup is None:
            return created
        candidates = select(operations).where(
            operations.c.equestrian_id == tenant,
            operations.c.kind == WorkKind.MERGE,
            operations.c.status.in_(["collecting", "waiting", "running", "failed_blocked"]),
            operations.c.cleanup_operation_id.is_(None),
        )
        if kind == WorkKind.MERGE:
            candidates = candidates.where(operations.c.id == operation)
        # Only ledger metadata, never mappings/stats. No heavy worker work in NATS.
        for merge in (await self.session.execute(candidates)).mappings():
            if not await self.supersede(tenant, merge["id"], cleanup_operation=cleanup["id"]):
                continue
            result = MergeCompleted(
                schema_version=1,
                message_id=maintenance_message_id(MERGE_COMPLETED, merge["id"], "superseded_by_delete"),
                operation_id=merge["id"],
                equestrian_id=tenant,
                occurred_at=datetime.now(UTC),
                normalization_revision=merge["normalization_revision"],
                status="superseded_by_delete",
                part_count=merge["part_count"],
                cleanup_operation_id=cleanup["id"],
                cleanup_seq=cleanup["cleanup_seq"],
            )
            await self.save_result(result)
            await self.record_superseded_result(tenant, merge["id"], result_message_id=result.message_id)
        return created
