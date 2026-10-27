"""Caller owns commit/rollback. LeaseLostError requires rollback of the whole page."""

from collections.abc import Sequence
from datetime import datetime
from typing import Protocol
from uuid import UUID

from core.entities import ParseErrorCode, ParseRun, ParseTask


class LeaseLostError(RuntimeError):
    """The caller must roll back any writes performed with this stale lease."""


class ParseRunRepositoryProtocol(Protocol):
    async def lock_equestrian(self, equestrian_id: UUID) -> None: ...

    async def has_active_tasks(self, equestrian_id: UUID) -> bool: ...

    async def exists_job(self, job_id: UUID) -> bool: ...

    async def create_run_with_tasks(self, run: ParseRun, tasks: Sequence[ParseTask]) -> bool:
        """False for active tasks; raise ParseJobAlreadyExistsError for a duplicate job."""
        ...


class ParseTaskRepositoryProtocol(Protocol):
    async def claim(self, account_id: UUID, *, locked_by: str, lease_seconds: int) -> ParseTask | None: ...

    async def list_dispatchable_accounts(self) -> list[UUID]: ...

    async def renew_lease(self, task_id: UUID, *, locked_by: str, lease_seconds: int) -> None: ...

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
    ) -> None: ...

    async def mark_succeeded(self, task_id: UUID, *, locked_by: str, rows_deleted: int = 0) -> None: ...

    async def schedule_retry(
        self,
        task_id: UUID,
        *,
        locked_by: str,
        next_attempt_at: datetime,
        error_code: ParseErrorCode,
        error_status: int | None = None,
    ) -> None: ...

    async def mark_failed(
        self,
        task_id: UUID,
        *,
        locked_by: str,
        error_code: ParseErrorCode,
        error_status: int | None = None,
    ) -> None: ...

    async def reap_expired_leases(self) -> int: ...

    async def fail_queued_by_account(
        self,
        account_id: UUID,
        *,
        error_code: ParseErrorCode = ParseErrorCode.CREDENTIALS_INVALID,
        error_status: int | None = None,
    ) -> int: ...
