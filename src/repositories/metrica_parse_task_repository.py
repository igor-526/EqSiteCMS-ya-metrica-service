from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import case, exists, func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities import ParseErrorCode, ParseTask, TaskStatus
from core.entities.metrica_maintenance import BindingLifecycle
from core.protocols.repositories.metrica_parse import LeaseLostError
from models import metrica_maintenance_operations as operations
from models import metrica_parse_runs, metrica_parse_tasks
from models import metrica_tenant_gates as gates
from repositories.metrica_maintenance_repository import MaintenanceGateRepository


class ParseTaskRepository(MaintenanceGateRepository):
    """Never commits. A claim uses a savepoint to contain a unique-index race.

    All worker writes are fenced by locked_by and an unexpired lease. A lost
    lease raises LeaseLostError: caller must roll back the page/finalization.
    Attempts count failed executions (including expired leases), not claims.
    """

    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.table = metrica_parse_tasks

    def _no_running(self, account: Any) -> Any:
        running = self.table.alias("running_task")
        return ~exists().where(running.c.yandex_account_id == account, running.c.status == TaskStatus.RUNNING)

    def _ready(self) -> tuple[Any, ...]:
        return (
            self.table.c.status == TaskStatus.QUEUED,
            self.table.c.next_attempt_at <= func.clock_timestamp(),
            self._no_running(self.table.c.yandex_account_id),
        )

    async def list_queued_tenants(self, account_id: UUID | None = None) -> list[UUID]:
        """Discovery only; includes delayed retries so cleanup cancels them immediately."""
        stmt = (
            select(self.table.c.equestrian_id)
            .join(metrica_parse_runs, metrica_parse_runs.c.id == self.table.c.run_id)
            .where(self.table.c.status == TaskStatus.QUEUED)
        )
        if account_id is not None:
            stmt = stmt.where(self.table.c.yandex_account_id == account_id)
        return list(
            (
                await self.session.execute(
                    stmt.group_by(self.table.c.equestrian_id).order_by(
                        func.min(metrica_parse_runs.c.created_at), self.table.c.equestrian_id
                    )
                )
            ).scalars()
        )

    async def list_recovery_tenants(self, *, limit: int = 100) -> list[UUID]:
        if not 1 <= limit <= 1000:
            raise ValueError("Invalid recovery limit")
        return list(
            (
                await self.session.execute(
                    select(self.table.c.equestrian_id)
                    .where(
                        self.table.c.status == TaskStatus.RUNNING,
                        or_(self.table.c.locked_until <= func.clock_timestamp(), self.table.c.locked_until.is_(None)),
                    )
                    .group_by(self.table.c.equestrian_id)
                    .order_by(func.min(self.table.c.locked_until).nullsfirst(), self.table.c.equestrian_id)
                    .limit(limit)
                )
            ).scalars()
        )

    def _retry_cancelled(self) -> Any:
        return or_(
            self.table.c.binding_generation == 0,
            exists().where(
                gates.c.equestrian_id == self.table.c.equestrian_id,
                or_(
                    gates.c.cleanup_requested_seq > self.table.c.observed_cleanup_requested_seq,
                    gates.c.binding_generation > self.table.c.binding_generation,
                ),
            ),
            exists().where(
                operations.c.equestrian_id == self.table.c.equestrian_id,
                operations.c.kind == "cleanup",
                operations.c.cleanup_seq > self.table.c.observed_cleanup_requested_seq,
            ),
        )

    async def cancel_queued_before_cleanup(self, equestrian_id: UUID, *, cleanup_seq: int) -> int:
        """No commit. Preserve audit and never touch running or post-delete intents."""
        if cleanup_seq <= 0:
            raise ValueError("Invalid cleanup sequence")
        await self.lock_scope([equestrian_id])
        return await self._cancel_queued(equestrian_id, self.table.c.observed_cleanup_requested_seq < cleanup_seq)

    async def _cancel_queued(self, tenant: UUID, predicate: Any) -> int:
        rows = await self.session.execute(
            update(self.table)
            .where(self.table.c.equestrian_id == tenant, self.table.c.status == TaskStatus.QUEUED, predicate)
            .values(
                status=TaskStatus.CANCELLED,
                finished_at=func.clock_timestamp(),
                locked_by=None,
                locked_until=None,
                error_code=ParseErrorCode.BINDING_CHANGED,
                updated_at=func.clock_timestamp(),
            )
            .returning(self.table.c.id)
        )
        return len(rows.all())

    async def _prepare_parse(self, tenant: UUID, lifecycle: BindingLifecycle) -> bool:
        await self.lock_scope([tenant], [lifecycle.account_id] if lifecycle.account_id else [])
        gate = await self._observe(tenant, lifecycle)
        # Monotonic local observation also rejects an in-flight stale peer snapshot.
        stale = or_(
            self.table.c.observed_cleanup_requested_seq < gate["cleanup_requested_seq"],
            self.table.c.binding_generation < gate["binding_generation"],
        )
        await self._cancel_queued(tenant, stale)
        if await self._blocked(tenant, lifecycle, gate):
            return False
        if lifecycle.account_id is None:
            await self._cancel_queued(tenant, True)
            return False
        return True

    def _snapshot_matches(self, tenant: UUID, lifecycle: BindingLifecycle) -> tuple[Any, ...]:
        return (
            self.table.c.equestrian_id == tenant,
            self.table.c.binding_generation == lifecycle.binding_generation,
            self.table.c.observed_cleanup_requested_seq == lifecycle.cleanup_requested_seq,
            self.table.c.yandex_account_id == lifecycle.account_id,
            self.table.c.counter_id == lifecycle.counter_id,
        )

    async def claim(
        self,
        account_id: UUID,
        *,
        equestrian_id: UUID,
        lifecycle: BindingLifecycle,
        locked_by: str,
        lease_seconds: int,
    ) -> ParseTask | None:
        if lease_seconds <= 0 or not locked_by or len(locked_by) > 128:
            raise ValueError("Invalid lease")
        if not await self._prepare_parse(equestrian_id, lifecycle) or lifecycle.account_id != account_id:
            return None
        candidate = (
            select(self.table.c.id)
            .join(metrica_parse_runs, metrica_parse_runs.c.id == self.table.c.run_id)
            .where(
                self.table.c.yandex_account_id == account_id,
                *self._ready(),
                *self._snapshot_matches(equestrian_id, lifecycle),
            )
            .order_by(metrica_parse_runs.c.created_at, self.table.c.sequence, self.table.c.created_at, self.table.c.id)
            .with_for_update(skip_locked=True, of=self.table)
            .limit(1)
            .scalar_subquery()
        )
        stmt = (
            update(self.table)
            .where(self.table.c.id == candidate)
            .values(
                status=TaskStatus.RUNNING,
                locked_by=locked_by,
                locked_until=func.clock_timestamp() + timedelta(seconds=lease_seconds),
                started_at=func.coalesce(self.table.c.started_at, func.clock_timestamp()),
                updated_at=func.clock_timestamp(),
            )
            .returning(self.table)
        )
        try:
            async with self.session.begin_nested():
                row = (await self.session.execute(stmt)).mappings().one_or_none()
        except IntegrityError as exc:
            # Do not swallow other integrity failures. asyncpg wraps SQLSTATE.
            if getattr(exc.orig, "sqlstate", None) != "23505":
                raise
            if "ux_metrica_parse_tasks_running_account" not in str(exc.orig):
                raise
            return None
        return ParseTask.model_validate(row) if row is not None else None

    async def list_dispatchable_accounts(self, *, equestrian_id: UUID, lifecycle: BindingLifecycle) -> list[UUID]:
        if not await self._prepare_parse(equestrian_id, lifecycle):
            return []
        return list(
            (
                await self.session.execute(
                    select(self.table.c.yandex_account_id)
                    .where(*self._ready(), *self._snapshot_matches(equestrian_id, lifecycle))
                    .distinct()
                )
            )
            .scalars()
            .all()
        )

    async def assert_writable(self, task_id: UUID, *, locked_by: str) -> None:
        # Discover immutable scope before locks; never take a task row lock before
        # tenant -> account. Re-read ownership after waiting for the scope locks.
        scope = (
            (
                await self.session.execute(
                    select(self.table.c.equestrian_id, self.table.c.yandex_account_id).where(
                        self.table.c.id == task_id
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        if scope is None:
            raise LeaseLostError("Task lease is no longer owned")
        tenant = scope["equestrian_id"]
        await self.lock_scope([tenant], [scope["yandex_account_id"]])
        # Pending cleanup/merge (including an authoritative watermark preceding
        # NATS) is NOT a write prohibition: the current running attempt drains.
        # Unknown/failed/executing maintenance states fail closed. Maintenance
        # itself cannot claim until every running task has been terminalized.
        incompatible = exists().where(
            operations.c.equestrian_id == tenant,
            operations.c.status.not_in(("pending", "draining", "collecting", "waiting", "completed")),
        )
        cleaned = exists().where(
            gates.c.equestrian_id == tenant,
            gates.c.cleanup_completed_seq > self.table.c.observed_cleanup_requested_seq,
        )
        owned = await self.session.scalar(
            select(self.table.c.id)
            .where(
                self.table.c.id == task_id,
                self.table.c.equestrian_id == tenant,
                self.table.c.status == TaskStatus.RUNNING,
                self.table.c.locked_by == locked_by,
                self.table.c.locked_until > func.clock_timestamp(),
                ~incompatible,
                ~cleaned,
            )
            .with_for_update(of=self.table)
        )
        if owned is None:
            raise LeaseLostError("Task lease is no longer writable")

    async def _owned_update(self, task_id: UUID, owner: str, **values: Any) -> None:
        await self.assert_writable(task_id, locked_by=owner)
        stmt = (
            update(self.table)
            .where(
                self.table.c.id == task_id,
                self.table.c.status == TaskStatus.RUNNING,
                self.table.c.locked_by == owner,
                self.table.c.locked_until > func.clock_timestamp(),
            )
            .values(**values, updated_at=func.clock_timestamp())
            .returning(self.table.c.id)
        )
        if (await self.session.execute(stmt)).scalar_one_or_none() is None:
            raise LeaseLostError("Task lease is no longer owned")

    async def renew_lease(self, task_id: UUID, *, locked_by: str, lease_seconds: int) -> None:
        if lease_seconds <= 0:
            raise ValueError("Invalid lease duration")
        await self._owned_update(
            task_id, locked_by, locked_until=func.clock_timestamp() + timedelta(seconds=lease_seconds)
        )

    async def save_checkpoint(
        self,
        task_id: UUID,
        *,
        locked_by: str,
        next_offset: int,
        lease_seconds: int,
        pages_fetched: int = 1,
        rows_fetched: int = 0,
        rows_inserted: int = 0,
        rows_updated: int = 0,
        rows_skipped: int = 0,
        sampled: bool = False,
    ) -> None:
        counters = {
            "pages_fetched": pages_fetched,
            "rows_fetched": rows_fetched,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "rows_skipped": rows_skipped,
        }
        if next_offset < 1 or lease_seconds <= 0 or any(value < 0 for value in counters.values()):
            raise ValueError("Invalid checkpoint")
        await self._owned_update(
            task_id,
            locked_by,
            next_offset=next_offset,
            locked_until=func.clock_timestamp() + timedelta(seconds=lease_seconds),
            sampled=self.table.c.sampled | sampled,
            **{name: self.table.c[name] + value for name, value in counters.items()},
        )

    async def mark_succeeded(self, task_id: UUID, *, locked_by: str, rows_deleted: int = 0) -> None:
        if rows_deleted < 0:
            raise ValueError("Invalid deleted count")
        await self._owned_update(
            task_id,
            locked_by,
            status=TaskStatus.SUCCEEDED,
            locked_by=None,
            locked_until=None,
            finished_at=func.clock_timestamp(),
            rows_deleted=self.table.c.rows_deleted + rows_deleted,
            error_code=None,
            error_status=None,
        )

    async def schedule_retry(
        self,
        task_id: UUID,
        *,
        locked_by: str,
        next_attempt_at: datetime,
        error_code: ParseErrorCode,
        error_status: int | None = None,
        lifecycle: BindingLifecycle | None = None,
    ) -> None:
        await self.assert_writable(task_id, locked_by=locked_by)
        if lifecycle is not None:
            tenant = await self.session.scalar(select(self.table.c.equestrian_id).where(self.table.c.id == task_id))
            if tenant is None:
                raise LeaseLostError("Task disappeared during retry")
            await self._observe(tenant, lifecycle)
        cancelled = self._retry_cancelled()
        exhausted = self.table.c.attempts + 1 >= self.table.c.max_attempts
        await self._owned_update(
            task_id,
            locked_by,
            status=case(
                (cancelled, TaskStatus.CANCELLED.value),
                (exhausted, TaskStatus.FAILED.value),
                else_=TaskStatus.QUEUED.value,
            ),
            attempts=self.table.c.attempts + 1,
            next_attempt_at=next_attempt_at,
            locked_by=None,
            locked_until=None,
            finished_at=case((or_(cancelled, exhausted), func.clock_timestamp()), else_=None),
            error_code=error_code,
            error_status=error_status,
        )

    async def mark_failed(
        self,
        task_id: UUID,
        *,
        locked_by: str,
        error_code: ParseErrorCode,
        error_status: int | None = None,
    ) -> None:
        await self._owned_update(
            task_id,
            locked_by,
            status=TaskStatus.FAILED,
            attempts=self.table.c.attempts + 1,
            locked_by=None,
            locked_until=None,
            finished_at=func.clock_timestamp(),
            error_code=error_code,
            error_status=error_status,
        )

    async def reap_expired_leases(
        self, *, equestrian_id: UUID | None = None, lifecycle: BindingLifecycle | None = None
    ) -> int:
        if lifecycle is not None and equestrian_id is None:
            raise ValueError("Lifecycle requires tenant scope")
        # Never lock a task row before tenant -> account scope.
        tenants = [equestrian_id] if equestrian_id is not None else await self.list_recovery_tenants()
        await self.lock_scope(tenants)
        accounts = list(
            (
                await self.session.execute(
                    select(self.table.c.yandex_account_id)
                    .where(self.table.c.equestrian_id.in_(tenants), self.table.c.status == TaskStatus.RUNNING)
                    .distinct()
                )
            ).scalars()
        )
        await self.lock_scope([], accounts)
        changed = 0
        for tenant in tenants:
            if lifecycle is not None:
                await self._observe(tenant, lifecycle)
            expired = or_(self.table.c.locked_until <= func.clock_timestamp(), self.table.c.locked_until.is_(None))
            cancelled = self._retry_cancelled()
            exhausted = self.table.c.attempts + 1 >= self.table.c.max_attempts
            rows = await self.session.execute(
                update(self.table)
                .where(self.table.c.equestrian_id == tenant, self.table.c.status == TaskStatus.RUNNING, expired)
                .values(
                    status=case(
                        (cancelled, TaskStatus.CANCELLED.value),
                        (exhausted, TaskStatus.FAILED.value),
                        else_=TaskStatus.QUEUED.value,
                    ),
                    attempts=self.table.c.attempts + 1,
                    next_attempt_at=func.clock_timestamp(),
                    locked_by=None,
                    locked_until=None,
                    finished_at=case((or_(cancelled, exhausted), func.clock_timestamp()), else_=None),
                    error_code=ParseErrorCode.LEASE_EXPIRED,
                    error_status=None,
                    updated_at=func.clock_timestamp(),
                )
                .returning(self.table.c.id)
            )
            changed += len(rows.all())
        return changed

    async def fail_queued_by_account(
        self,
        account_id: UUID,
        *,
        error_code: ParseErrorCode = ParseErrorCode.CREDENTIALS_INVALID,
        error_status: int | None = None,
    ) -> int:
        stmt = (
            update(self.table)
            .where(self.table.c.yandex_account_id == account_id, self.table.c.status == TaskStatus.QUEUED)
            .values(
                status=TaskStatus.FAILED,
                finished_at=func.clock_timestamp(),
                locked_by=None,
                locked_until=None,
                error_code=error_code,
                error_status=error_status,
                updated_at=func.clock_timestamp(),
            )
            .returning(self.table.c.id)
        )
        return len((await self.session.execute(stmt)).all())
