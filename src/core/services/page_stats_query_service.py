from core.protocols.repositories.page_stats_query import PageStatsQueryRepositoryProtocol
from core.schemas.api.page_stats import (
    PageStatsRankingInDto,
    PageStatsRankingOutDto,
    PageStatsTableInDto,
    PageStatsTableOutDto,
    PageStatsValueOutDto,
)


class PageStatsQueryService:
    def __init__(self, repository: PageStatsQueryRepositoryProtocol) -> None:
        self._repository = repository

    async def table(self, *, data: PageStatsTableInDto) -> PageStatsTableOutDto:
        values = await self._repository.table(
            equestrian_id=data.equestrian_id, metric=data.metric, buckets=data.buckets, page_ids=data.page_ids
        )
        return PageStatsTableOutDto(
            metric=data.metric,
            values=[PageStatsValueOutDto(page_id=row.page_id, bucket=row.bucket, value=row.value) for row in values],
        )

    async def ranking(self, *, data: PageStatsRankingInDto) -> PageStatsRankingOutDto:
        pages = await self._repository.ranking(
            equestrian_id=data.equestrian_id,
            metric=data.metric,
            date_from=data.date_from,
            date_to=data.date_to,
            direction=data.direction,
        )
        return PageStatsRankingOutDto(metric=data.metric, direction=data.direction, page_ids=pages)
