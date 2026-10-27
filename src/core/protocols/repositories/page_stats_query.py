"""Read-only queries, sharing the caller's session without committing."""

from collections.abc import Sequence
from datetime import date
from typing import Protocol
from uuid import UUID

from core.entities.metrica_stats_query import Direction, Metric, PageStatsValue, StatsBucket


class PageStatsQueryRepositoryProtocol(Protocol):
    async def table(
        self, *, equestrian_id: UUID, metric: Metric, buckets: Sequence[StatsBucket], page_ids: Sequence[UUID]
    ) -> list[PageStatsValue]: ...

    async def ranking(
        self, *, equestrian_id: UUID, metric: Metric, date_from: date, date_to: date, direction: Direction
    ) -> list[UUID]: ...
