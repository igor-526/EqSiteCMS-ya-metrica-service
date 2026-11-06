"""Durable admission primitives. No commit, peer I/O, credentials or auto-window calculation."""

from collections.abc import Sequence
from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.entities import ParseRun, ParseTask
from core.entities.metrica_maintenance import (
    BindingLifecycle,
    IntentAdmission,
    MaintenanceConflictError,
    PendingParseIntent,
)
from core.exceptions.parse import ParseJobAlreadyExistsError
from models import metrica_parse_runs as runs
from models import metrica_pending_parse_intents as intents
from repositories.metrica_parse_run_repository import ParseRunRepository


class PendingParseIntentRepository(ParseRunRepository):
    async def _intent(self, intent: PendingParseIntent):
        row = (
            (await self.session.execute(select(intents).where(intents.c.job_id == intent.job_id).with_for_update()))
            .mappings()
            .one_or_none()
        )
        if row is not None and (
            row["equestrian_id"] != intent.equestrian_id or row["payload_hash"] != intent.payload_hash
        ):
            raise MaintenanceConflictError("Parse command identity conflict")
        return row

    async def enqueue_intent(self, intent: PendingParseIntent, *, lifecycle: BindingLifecycle) -> IntentAdmission:
        await self.lock_scope([intent.equestrian_id])
        if await self._intent(intent) is not None:
            return IntentAdmission.DUPLICATE
        # Legacy runs have no immutable command hash; keep their established rejection contract.
        if await self.exists_job(intent.job_id):
            raise ParseJobAlreadyExistsError()
        gate = await self._observe(intent.equestrian_id, lifecycle)
        blocked = await self._blocked(intent.equestrian_id, lifecycle, gate)
        if not blocked and await self.has_active_tasks(intent.equestrian_id):
            return IntentAdmission.ACTIVE_RUN
        inserted = await self.session.scalar(
            pg_insert(intents)
            .values(
                job_id=intent.job_id,
                equestrian_id=intent.equestrian_id,
                payload_hash=intent.payload_hash,
                date_mode=intent.date_mode,
                date_from=intent.date_from,
                date_to=intent.date_to,
                binding_generation=lifecycle.binding_generation,
                observed_cleanup_requested_seq=lifecycle.cleanup_requested_seq,
            )
            .on_conflict_do_nothing(index_elements=[intents.c.job_id])
            .returning(intents.c.job_id)
        )
        if inserted is None:
            # Cross-tenant concurrent use of a globally unique job_id cannot silently ACK.
            await self._intent(intent)
            return IntentAdmission.DUPLICATE
        return IntentAdmission.DEFERRED if blocked else IntentAdmission.READY

    async def prepare_admission(self, intent: PendingParseIntent, *, lifecycle: BindingLifecycle) -> IntentAdmission:
        """Check BEFORE local auto-window resolution. Keep transaction until consume_intent.

        Fetch authoritative lifecycle outside this transaction. READY is not a claim or
        permission for HTTP I/O; it holds tenant/account locks only until commit/rollback.
        Peer outage: do not call this method; leave the pending row untouched and retry.
        """
        await self.lock_scope([intent.equestrian_id], [lifecycle.account_id] if lifecycle.account_id else [])
        row = await self._intent(intent)
        if row is None:
            raise MaintenanceConflictError("Unknown parse intent")
        if row["status"] != "pending":
            return IntentAdmission.DUPLICATE
        gate = await self._observe(intent.equestrian_id, lifecycle)
        if row["observed_cleanup_requested_seq"] < gate["cleanup_requested_seq"]:
            await self._terminal(intent, "cancelled", "cleanup_cancelled")
            return IntentAdmission.CANCELLED
        if await self._blocked(intent.equestrian_id, lifecycle, gate):
            return IntentAdmission.DEFERRED
        if await self.has_active_tasks(intent.equestrian_id):
            return IntentAdmission.DEFERRED  # saved during maintenance: never reject/drop it now
        if lifecycle.account_id is None:
            await self._terminal(intent, "missing_binding", "binding_missing")
            return IntentAdmission.MISSING_BINDING
        ready = await self.session.scalar(
            select(intents.c.job_id).where(
                intents.c.job_id == intent.job_id,
                intents.c.equestrian_id == intent.equestrian_id,
                intents.c.next_attempt_at <= func.clock_timestamp(),
            )
        )
        return IntentAdmission.READY if ready else IntentAdmission.DEFERRED

    async def _terminal(self, intent: PendingParseIntent, status: str, error_code: str) -> None:
        await self.session.execute(
            update(intents)
            .where(
                intents.c.job_id == intent.job_id,
                intents.c.equestrian_id == intent.equestrian_id,
                intents.c.status == "pending",
            )
            .values(status=status, error_code=error_code, updated_at=func.clock_timestamp())
        )

    async def cancel_before_cleanup(self, tenant: UUID, *, cleanup_seq: int) -> None:
        if cleanup_seq <= 0:
            raise ValueError("Invalid cleanup sequence")
        await self.lock_scope([tenant])
        await self.session.execute(
            update(intents)
            .where(
                intents.c.equestrian_id == tenant,
                intents.c.status == "pending",
                intents.c.observed_cleanup_requested_seq < cleanup_seq,
            )
            .values(status="cancelled", error_code="cleanup_cancelled", updated_at=func.clock_timestamp())
        )

    async def consume_intent(
        self,
        intent: PendingParseIntent,
        *,
        lifecycle: BindingLifecycle,
        run: ParseRun,
        tasks: Sequence[ParseTask],
    ) -> IntentAdmission:
        """Atomically insert run/tasks and mark admitted. Caller commits all or rolls back all."""
        decision = await self.prepare_admission(intent, lifecycle=lifecycle)
        if decision != IntentAdmission.READY:
            return decision
        if (
            run.job_id != intent.job_id
            or run.equestrian_id != intent.equestrian_id  # noqa: W503
            or run.date_mode != intent.date_mode  # noqa: W503
            or run.binding_generation != lifecycle.binding_generation  # noqa: W503
            or run.observed_cleanup_requested_seq != lifecycle.cleanup_requested_seq  # noqa: W503
            or any(  # noqa: W503
                task.binding_generation != run.binding_generation
                or task.observed_cleanup_requested_seq != run.observed_cleanup_requested_seq  # noqa: W503
                or task.yandex_account_id != lifecycle.account_id  # noqa: W503
                or task.counter_id != lifecycle.counter_id  # noqa: W503
                for task in tasks
            )
        ):
            raise MaintenanceConflictError("Parse admission snapshot mismatch")
        if not await self.create_run_with_tasks(run, tasks):
            return IntentAdmission.DEFERRED
        changed = await self.session.scalar(
            update(intents)
            .where(
                intents.c.job_id == intent.job_id,
                intents.c.equestrian_id == intent.equestrian_id,
                intents.c.status == "pending",
            )
            .values(status="admitted", run_id=run.id, updated_at=func.clock_timestamp())
            .returning(intents.c.job_id)
        )
        if changed is None:
            raise MaintenanceConflictError("Parse admission lost; roll back transaction")
        return IntentAdmission.ADMITTED

    async def admitted_run(self, intent: PendingParseIntent) -> UUID | None:
        """Tenant/hash-qualified dedup lookup; never exposes another tenant's run."""
        await self.lock_scope([intent.equestrian_id])
        row = await self._intent(intent)
        if row is None or row["status"] != "admitted":
            return None
        return await self.session.scalar(
            select(runs.c.id).where(
                runs.c.equestrian_id == intent.equestrian_id,
                runs.c.id == row["run_id"],
                runs.c.job_id == intent.job_id,
            )
        )

    async def ready_intents(self, *, limit: int) -> list[PendingParseIntent]:
        """Bounded discovery only; prepare_admission must recheck every candidate."""
        if not 1 <= limit <= 1000:
            raise ValueError("Invalid intent batch limit")
        rows = (
            await self.session.execute(
                select(intents)
                .where(intents.c.status == "pending", intents.c.next_attempt_at <= func.clock_timestamp())
                .order_by(intents.c.next_attempt_at, intents.c.job_id)
                .limit(limit)
            )
        ).mappings()
        return [
            PendingParseIntent(row["job_id"], row["equestrian_id"], row["date_mode"], row["date_from"], row["date_to"])
            for row in rows
        ]

    async def defer_intent(self, intent: PendingParseIntent, *, next_attempt_at: datetime) -> None:
        if next_attempt_at.tzinfo is None or next_attempt_at.utcoffset() is None:
            raise ValueError("Retry timestamp must be timezone-aware")
        await self.lock_scope([intent.equestrian_id])
        row = await self._intent(intent)
        if row is None:
            raise MaintenanceConflictError("Unknown parse intent")
        await self.session.execute(
            update(intents)
            .where(
                intents.c.job_id == intent.job_id,
                intents.c.equestrian_id == intent.equestrian_id,
                intents.c.status == "pending",
            )
            .values(next_attempt_at=next_attempt_at, updated_at=func.clock_timestamp())
        )
