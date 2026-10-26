"""Repositories share the caller's session/transaction; only the caller commits."""

from collections.abc import Sequence
from datetime import date
from typing import NamedTuple, Protocol
from uuid import UUID

from core.entities import StatRow


class UpsertCounts(NamedTuple):
    inserted: int
    updated: int


class StatsRepositoryProtocol(Protocol):
    """One repository instance per parse kind. Inputs are aggregated per page/key."""

    async def upsert(self, rows: Sequence[StatRow]) -> UpsertCounts: ...

    async def reconcile_window(
        self, *, equestrian_id: UUID, date_from: date, date_to: date, parse_task_id: UUID
    ) -> int: ...

    async def get_last_stat_date(self, equestrian_id: UUID) -> date | None: ...

    async def list_for_equestrian(self, *, equestrian_id: UUID, date_from: date, date_to: date) -> list[StatRow]: ...
