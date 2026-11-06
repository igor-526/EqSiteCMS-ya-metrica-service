"""Short tenant/account locks and durable maintenance leases. Never commits.

This is a storage primitive, not worker integration. All participating writers
must use lock_scope before touching tasks, operations or stats. Batch callers
must roll back the whole transaction on MaintenanceLeaseLostError.
"""

from collections.abc import Iterable
from datetime import timedelta
from uuid import UUID

from sqlalchemy import exists, func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities.metrica_maintenance import BindingLifecycle, MaintenanceLease, MaintenanceLeaseLostError, WorkKind
from models import metrica_maintenance_operations as operations
from models import metrica_parse_tasks as tasks
from models import metrica_tenant_gates as gates


class MaintenanceGateRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def lock_scope(self, equestrian_ids: Iterable[UUID], account_ids: Iterable[UUID] = ()) -> None:
        # Keep the existing enqueue lock namespace for mixed-version safety.
        for prefix, ids in (("metrica-parse", equestrian_ids), ("metrica-account", account_ids)):
            for identity in sorted(set(ids)):
                await self.session.execute(
                    text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                    {"key": f"{prefix}:{identity}"},
                )

    async def _observe(self, tenant: UUID, lifecycle: BindingLifecycle):
        # completed is LOCAL ledger-owned, never a max of an uncorrelated peer result.
        statement = pg_insert(gates).values(
            equestrian_id=tenant,
            binding_generation=lifecycle.binding_generation,
            cleanup_requested_seq=lifecycle.cleanup_requested_seq,
        )
        return (
            (
                await self.session.execute(
                    statement.on_conflict_do_update(
                        index_elements=[gates.c.equestrian_id],
                        set_={
                            "binding_generation": func.greatest(
                                gates.c.binding_generation, lifecycle.binding_generation
                            ),
                            "cleanup_requested_seq": func.greatest(
                                gates.c.cleanup_requested_seq, lifecycle.cleanup_requested_seq
                            ),
                            "updated_at": func.clock_timestamp(),
                        },
                    ).returning(gates)
                )
            )
            .mappings()
            .one()
        )

    async def _blocked(self, tenant: UUID, lifecycle: BindingLifecycle, gate) -> bool:
        return (
            lifecycle.binding_generation < gate["binding_generation"]
            or lifecycle.cleanup_requested_seq < gate["cleanup_requested_seq"]  # noqa: W503
            or lifecycle.cleanup_requested_seq > lifecycle.cleanup_completed_seq  # noqa: W503
            or await self.selected_kind(tenant) != WorkKind.PARSE  # noqa: W503
        )

    async def selected_kind(self, equestrian_id: UUID) -> WorkKind:
        await self.lock_scope([equestrian_id])
        barrier = await self.session.scalar(
            select(
                exists().where(
                    gates.c.equestrian_id == equestrian_id,
                    gates.c.cleanup_requested_seq > gates.c.cleanup_completed_seq,
                )
            )
        )
        if barrier or await self._has_operation(equestrian_id, WorkKind.CLEANUP):
            return WorkKind.CLEANUP
        if await self._has_operation(equestrian_id, WorkKind.MERGE):
            return WorkKind.MERGE
        return WorkKind.PARSE

    async def _has_operation(self, tenant: UUID, kind: WorkKind) -> bool:
        return bool(
            await self.session.scalar(
                select(
                    exists().where(
                        operations.c.equestrian_id == tenant,
                        operations.c.kind == kind,
                        operations.c.status != "completed",
                    )
                )
            )
        )


class MaintenanceArbitrationRepository(MaintenanceGateRepository):
    async def _running_parse(self, tenant: UUID) -> bool:
        # Includes expired and generation=0: reconciliation must terminalize them
        # before deletion. Never interpret lease expiry alone as a safe drain.
        return bool(
            await self.session.scalar(
                select(
                    exists().where(
                        tasks.c.equestrian_id == tenant,
                        tasks.c.status == "running",
                    )
                )
            )
        )

    @staticmethod
    def _duration(seconds: int) -> timedelta:
        if seconds <= 0:
            raise ValueError("Invalid lease duration")
        return timedelta(seconds=seconds)

    async def claim(self, equestrian_id: UUID, *, owner: UUID, lease_seconds: int) -> MaintenanceLease | None:
        duration = self._duration(lease_seconds)
        kind = await self.selected_kind(equestrian_id)
        if kind == WorkKind.PARSE:
            return None
        # Do not skip a failed/delayed/collecting head operation: in particular,
        # cleanup seq N+1 cannot run ahead of seq N.
        order = operations.c.cleanup_seq if kind == WorkKind.CLEANUP else operations.c.normalization_revision
        row = (
            (
                await self.session.execute(
                    select(operations)
                    .where(
                        operations.c.equestrian_id == equestrian_id,
                        operations.c.kind == kind,
                        operations.c.status != "completed",
                    )
                    .order_by(order, operations.c.id)
                    .limit(1)
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None  # authoritative watermark arrived before its command
        executable = {"pending", "draining", "deleting"} if kind == WorkKind.CLEANUP else {"waiting", "running"}
        if row["status"] not in executable:
            return None
        if kind == WorkKind.CLEANUP:
            completed = await self.session.scalar(
                select(gates.c.cleanup_completed_seq).where(gates.c.equestrian_id == equestrian_id)
            )
            # Reordered delivery must not delete seq N+1 before N, nor replay
            # an already completed cleanup over newly parsed statistics.
            if completed is None or row["cleanup_seq"] != completed + 1:
                return None
        if await self._running_parse(equestrian_id):
            return None
        now = await self.session.scalar(select(func.clock_timestamp()))
        if row["next_attempt_at"] > now or (row["lease_expires_at"] is not None and row["lease_expires_at"] > now):
            return None
        status = "deleting" if kind == WorkKind.CLEANUP else "running"
        revision = (
            await self.session.execute(
                update(operations)
                .where(
                    operations.c.id == row["id"],
                    operations.c.equestrian_id == equestrian_id,
                    operations.c.fencing_revision == row["fencing_revision"],
                )
                .values(
                    status=status,
                    lease_owner=owner,
                    lease_expires_at=func.clock_timestamp() + duration,
                    fencing_revision=operations.c.fencing_revision + 1,
                    updated_at=func.clock_timestamp(),
                )
                .returning(operations.c.fencing_revision)
            )
        ).scalar_one()
        return MaintenanceLease(equestrian_id, row["id"], owner, revision, kind, status)

    def _owned(self, lease: MaintenanceLease) -> tuple:
        return (
            operations.c.equestrian_id == lease.equestrian_id,
            operations.c.id == lease.operation_id,
            operations.c.kind == lease.kind,
            operations.c.status == lease.status,
            operations.c.lease_owner == lease.owner,
            operations.c.fencing_revision == lease.revision,
            operations.c.lease_expires_at > func.clock_timestamp(),
        )

    async def assert_owned(self, lease: MaintenanceLease) -> None:
        if await self.selected_kind(lease.equestrian_id) != lease.kind:
            raise MaintenanceLeaseLostError("Maintenance lease preempted")
        if await self._running_parse(lease.equestrian_id):
            raise MaintenanceLeaseLostError("Parse drain is incomplete")
        if not await self.session.scalar(select(exists().where(*self._owned(lease)))):
            raise MaintenanceLeaseLostError("Maintenance lease is no longer owned")

    async def checkpoint(self, lease: MaintenanceLease, *, checkpoint: dict, lease_seconds: int) -> None:
        duration = self._duration(lease_seconds)
        await self.assert_owned(lease)
        changed = await self.session.scalar(
            update(operations)
            .where(*self._owned(lease))
            .values(
                checkpoint=checkpoint,
                lease_expires_at=func.clock_timestamp() + duration,
                updated_at=func.clock_timestamp(),
            )
            .returning(operations.c.id)
        )
        if changed is None:
            raise MaintenanceLeaseLostError("Maintenance lease expired before checkpoint")
