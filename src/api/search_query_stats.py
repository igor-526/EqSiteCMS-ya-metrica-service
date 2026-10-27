from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from containers.application import create_search_query_stats_services
from core.schemas.api.search_query_stats import (
    SearchQueryStatsRankingInDto,
    SearchQueryStatsRankingOutDto,
    SearchQueryStatsTableInDto,
    SearchQueryStatsTableOutDto,
)
from core.services.search_query_stats_query_service import SearchQueryStatsQueryService
from utils.database import worker_session

router = APIRouter(prefix="/search-query-stats")


async def get_search_query_stats_service() -> AsyncIterator[SearchQueryStatsQueryService]:
    """Fresh read-only session, no commit or process-global provider override."""
    async with worker_session() as session:
        yield create_search_query_stats_services(session).search_query_stats_query_service()


@router.get("/table", response_model=SearchQueryStatsTableOutDto)
async def table(
    data: Annotated[SearchQueryStatsTableInDto, Query()],
    service: Annotated[SearchQueryStatsQueryService, Depends(get_search_query_stats_service, scope="function")],
) -> SearchQueryStatsTableOutDto:
    return await service.table(data=data)


@router.get("/ranking", response_model=SearchQueryStatsRankingOutDto)
async def ranking(
    data: Annotated[SearchQueryStatsRankingInDto, Query()],
    service: Annotated[SearchQueryStatsQueryService, Depends(get_search_query_stats_service, scope="function")],
) -> SearchQueryStatsRankingOutDto:
    return await service.ranking(data=data)
