from uuid import UUID

from core.entities.metrica_stats_query import Direction, Metric
from core.exceptions import ClientError
from core.protocols.repositories.search_query_stats_query import SearchQueryStatsQueryRepositoryProtocol
from core.schemas.api.page_stats import parse_bucket, parse_date
from core.schemas.api.search_query_stats import (
    SearchQueryStatsRankingInDto,
    SearchQueryStatsRankingOutDto,
    SearchQueryStatsTableInDto,
    SearchQueryStatsTableOutDto,
    SearchQueryStatsValueOutDto,
)


def _required(value: str | None) -> str:
    if not value:
        raise ValueError("Missing parameter")
    return value


class SearchQueryStatsQueryService:
    def __init__(self, repository: SearchQueryStatsQueryRepositoryProtocol) -> None:
        self._repository = repository

    async def table(self, *, data: SearchQueryStatsTableInDto) -> SearchQueryStatsTableOutDto:
        try:
            equestrian_id = UUID(_required(data.equestrian_id))
            metric = Metric(_required(data.metric))
            if not 1 <= len(data.bucket) <= 31:
                raise ValueError("Invalid bucket count")
            buckets = [parse_bucket(value) for value in data.bucket]
            if any(right.date_from <= left.date_to for left, right in zip(buckets, buckets[1:], strict=False)):
                raise ValueError("Invalid bucket order")
            ids = list(dict.fromkeys(UUID(value) for value in data.search_query_ids))
            if not 1 <= len(ids) <= 100:
                raise ValueError("Invalid UUID count")
        except ValueError:
            raise ClientError("Некорректные параметры статистики запросов") from None
        values = await self._repository.table(
            equestrian_id=equestrian_id, metric=metric, buckets=buckets, search_query_ids=ids
        )
        return SearchQueryStatsTableOutDto(
            metric=metric,
            values=[
                SearchQueryStatsValueOutDto(search_query_id=row.search_query_id, bucket=row.bucket, value=row.value)
                for row in values
            ],
        )

    async def ranking(self, *, data: SearchQueryStatsRankingInDto) -> SearchQueryStatsRankingOutDto:
        try:
            equestrian_id = UUID(_required(data.equestrian_id))
            metric = Metric(_required(data.metric))
            date_from = parse_date(_required(data.date_from))
            date_to = parse_date(_required(data.date_to))
            direction = Direction(_required(data.direction))
            if not 0 <= (date_to - date_from).days < 366:
                raise ValueError("Invalid ranking bounds")
        except ValueError:
            raise ClientError("Некорректные параметры статистики запросов") from None
        ids = await self._repository.ranking(
            equestrian_id=equestrian_id, metric=metric, date_from=date_from, date_to=date_to, direction=direction
        )
        return SearchQueryStatsRankingOutDto(metric=metric, direction=direction, search_query_ids=ids)
