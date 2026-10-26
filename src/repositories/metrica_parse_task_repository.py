from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import case, exists, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities import ParseErrorCode, ParseTask, TaskStatus
from core.protocols.repositories.metrica_parse import LeaseLostError
from models import metrica_parse_runs, metrica_parse_tasks


class ParseTaskRepository:
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

    async def claim(self, account_id: UUID, *, locked_by: str, lease_seconds: int) -> ParseTask | None:
        if lease_seconds <= 0 or not locked_by or len(locked_by) > 128:
            raise ValueError("Invalid lease")
        candidate = (
            select(self.table.c.id)
            .join(metrica_parse_runs, metrica_parse_runs.c.id == self.table.c.run_id)
            .where(self.table.c.yandex_account_id == account_id, *self._ready())
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

    async def list_dispatchable_accounts(self) -> list[UUID]:
        return list(
            (await self.session.execute(select(self.table.c.yandex_account_id).where(*self._ready()).distinct()))
            .scalars()
            .all()
        )

    async def _owned_update(self, task_id: UUID, owner: str, **values: Any) -> None:
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
    ) -> None:
        exhausted = self.table.c.attempts + 1 >= self.table.c.max_attempts
        await self._owned_update(
            task_id,
            locked_by,
            status=case((exhausted, TaskStatus.FAILED), else_=TaskStatus.QUEUED),
            attempts=self.table.c.attempts + 1,
            next_attempt_at=next_attempt_at,
            locked_by=None,
            locked_until=None,
            finished_at=case((exhausted, func.clock_timestamp()), else_=None),
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

    async def reap_expired_leases(self) -> int:
        expired = (
            select(self.table.c.id)
            .where(self.table.c.status == TaskStatus.RUNNING, self.table.c.locked_until <= func.clock_timestamp())
            .with_for_update(skip_locked=True)
        )
        exhausted = self.table.c.attempts + 1 >= self.table.c.max_attempts
        stmt = (
            update(self.table)
            .where(self.table.c.id.in_(expired))
            .values(
                status=case((exhausted, TaskStatus.FAILED), else_=TaskStatus.QUEUED),
                attempts=self.table.c.attempts + 1,
                next_attempt_at=func.clock_timestamp(),
                locked_by=None,
                locked_until=None,
                finished_at=case((exhausted, func.clock_timestamp()), else_=None),
                error_code=ParseErrorCode.LEASE_EXPIRED,
                error_status=None,
                updated_at=func.clock_timestamp(),
            )
            .returning(self.table.c.id)
        )
        return len((await self.session.execute(stmt)).all())

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
