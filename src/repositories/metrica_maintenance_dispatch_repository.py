"""Bounded durable discovery; workers, not Redis signals, own fenced claims."""

from uuid import UUID

from sqlalchemy import and_, exists, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities.metrica_maintenance import WorkKind
from models import metrica_maintenance_operations as operations
from repositories.metrica_maintenance_repository import MaintenanceGateRepository


class MaintenanceDispatchRepository(MaintenanceGateRepository):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)
        self._session = session

    async def ready_tenants(self, *, kind: WorkKind, limit: int) -> list[UUID]:
        if kind not in {WorkKind.CLEANUP, WorkKind.MERGE} or not 1 <= limit <= 1000:
            raise ValueError("Invalid maintenance discovery")
        earlier = operations.alias("earlier")
        # Never skip an older blocked/delayed operation to execute a newer one.
        blocker = exists(
            select(1).where(
                earlier.c.equestrian_id == operations.c.equestrian_id,
                earlier.c.status != "completed",
                or_(
                    and_(
                        earlier.c.kind == "cleanup",
                        or_(operations.c.kind == "merge", earlier.c.cleanup_seq < operations.c.cleanup_seq),
                    ),
                    and_(
                        earlier.c.kind == "merge",
                        operations.c.kind == "merge",
                        earlier.c.normalization_revision < operations.c.normalization_revision,
                    ),
                ),
            )
        )
        statuses = ["pending", "draining", "deleting"] if kind == WorkKind.CLEANUP else ["waiting", "running"]
        criteria = (
            operations.c.kind == kind,
            operations.c.status.in_(statuses),
            operations.c.next_attempt_at <= func.clock_timestamp(),
            or_(operations.c.lease_owner.is_(None), operations.c.lease_expires_at <= func.clock_timestamp()),
            ~blocker,
        )
        statement = (
            select(operations.c.equestrian_id)
            .where(*criteria)
            .order_by(operations.c.next_attempt_at, operations.c.id)
            .limit(limit)
        )
        tenants = list((await self._session.scalars(statement)).all())
        if tenants:
            # Rotate bounded discovery, including waiting/gapped heads, so the
            # first limit tenants cannot starve the rest. This is NOT a delivery
            # mark or a lease: NOW remains due and worker claims are unaffected.
            # Commit before Redis; a lost signal is still discoverable next tick.
            await self.lock_scope(tenants)
            await self._session.execute(
                update(operations)
                .where(operations.c.equestrian_id.in_(tenants), *criteria)
                .values(next_attempt_at=func.clock_timestamp())
            )
        return tenants
