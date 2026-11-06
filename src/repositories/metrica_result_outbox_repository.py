"""Durable result delivery. Caller owns transactions; never publish while locked."""

from uuid import UUID

from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert

from core.entities.metrica_maintenance import (
    MaintenanceConflictError,
    MaintenanceLeaseLostError,
    MaintenanceResultClaim,
)
from core.schemas.messaging.maintenance import (
    CLEANUP_COMPLETED,
    MERGE_COMPLETED,
    CleanupCompleted,
    MergeCompleted,
    identity_kind,
    maintenance_message_id,
)
from models import metrica_maintenance_outbox as outbox
from repositories.metrica_maintenance_storage_repository import MaintenanceStorageRepository, payload_hash


class MaintenanceResultOutboxRepository(MaintenanceStorageRepository):
    async def save_result(self, result: CleanupCompleted | MergeCompleted) -> None:
        subject = CLEANUP_COMPLETED if isinstance(result, CleanupCompleted) else MERGE_COMPLETED
        if result.message_id != maintenance_message_id(subject, result.operation_id, identity_kind(result)):
            raise MaintenanceConflictError("Result message identity conflict")
        await self._operation(result.equestrian_id, result.operation_id)
        payload = result.model_dump(mode="json")
        values = dict(
            message_id=result.message_id,
            equestrian_id=result.equestrian_id,
            operation_id=result.operation_id,
            subject=subject,
            payload=payload,
            payload_hash=payload_hash(payload),
        )
        created = await self.session.scalar(
            insert(outbox).values(**values).on_conflict_do_nothing().returning(outbox.c.message_id)
        )
        if created is None:
            row = (
                (await self.session.execute(select(outbox).where(outbox.c.message_id == result.message_id)))
                .mappings()
                .one()
            )
            if any(row[key] != value for key, value in values.items()):
                raise MaintenanceConflictError("Result identity or payload conflict")

    async def claim_result(self, *, owner: UUID, lease_seconds: int) -> MaintenanceResultClaim | None:
        duration = self._duration(lease_seconds)
        row = (
            (
                await self.session.execute(
                    select(outbox)
                    .where(
                        outbox.c.status == "pending",
                        outbox.c.next_attempt_at <= func.clock_timestamp(),
                        or_(outbox.c.lease_expires_at.is_(None), outbox.c.lease_expires_at <= func.clock_timestamp()),
                    )
                    .order_by(outbox.c.next_attempt_at, outbox.c.message_id)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        revision, attempts = row["fencing_revision"] + 1, row["attempts"] + 1
        await self.session.execute(
            update(outbox)
            .where(outbox.c.message_id == row["message_id"])
            .values(
                lease_owner=owner,
                lease_expires_at=func.clock_timestamp() + duration,
                fencing_revision=revision,
                attempts=attempts,
                updated_at=func.clock_timestamp(),
            )
        )
        return MaintenanceResultClaim(row["message_id"], row["subject"], row["payload"], owner, revision, attempts)

    def _result_owned(self, claim: MaintenanceResultClaim) -> tuple:
        return (
            outbox.c.message_id == claim.message_id,
            outbox.c.status == "pending",
            outbox.c.lease_owner == claim.owner,
            outbox.c.fencing_revision == claim.revision,
            outbox.c.lease_expires_at > func.clock_timestamp(),
        )

    async def _mark(self, claim: MaintenanceResultClaim, **values) -> None:
        changed = await self.session.scalar(
            update(outbox)
            .where(*self._result_owned(claim))
            .values(
                **values,
                lease_owner=None,
                lease_expires_at=None,
                updated_at=func.clock_timestamp(),
            )
            .returning(outbox.c.message_id)
        )
        if changed is None:
            raise MaintenanceLeaseLostError("Result delivery claim expired or replaced")

    async def mark_published(self, claim: MaintenanceResultClaim) -> None:
        await self._mark(claim, status="published", published_at=func.clock_timestamp(), error_code=None)

    async def retry_result(self, claim: MaintenanceResultClaim) -> None:
        # Attempts never cap delivery; bound exponent even after prolonged outage.
        delay = min(60, 2 ** min(max(claim.attempts - 1, 0), 6))
        await self._mark(
            claim,
            next_attempt_at=func.clock_timestamp() + self._duration(delay),
            error_code="publish_failed",
        )
