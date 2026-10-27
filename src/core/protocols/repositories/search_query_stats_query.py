"""Read-only queries, sharing the caller's session without committing."""

from collections.abc import Sequence
from datetime import date
from typing import Protocol
from uuid import UUID

from core.entities.metrica_stats_query import Direction, Metric, StatsBucket
from core.entities.search_query_stats_query import SearchQueryStatsValue


class SearchQueryStatsQueryRepositoryProtocol(Protocol):
    async def table(
        self, *, equestrian_id: UUID, metric: Metric, buckets: Sequence[StatsBucket], search_query_ids: Sequence[UUID]
    ) -> list[SearchQueryStatsValue]: ...

    async def ranking(
        self, *, equestrian_id: UUID, metric: Metric, date_from: date, date_to: date, direction: Direction
    ) -> list[UUID]: ...
