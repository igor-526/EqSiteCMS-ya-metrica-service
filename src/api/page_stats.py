from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from containers.application import create_page_stats_services
from core.schemas.api.page_stats import (
    PageStatsRankingInDto,
    PageStatsRankingOutDto,
    PageStatsTableInDto,
    PageStatsTableOutDto,
)
from core.services.page_stats_query_service import PageStatsQueryService
from utils.database import worker_session

router = APIRouter(prefix="/page-stats")


async def get_page_stats_service() -> AsyncIterator[PageStatsQueryService]:
    """Read-only request graph: no commit or shared session override."""
    async with worker_session() as session:
        yield create_page_stats_services(session).page_stats_query_service()


@router.get("/table", response_model=PageStatsTableOutDto)
async def table(
    data: Annotated[PageStatsTableInDto, Query()],
    service: Annotated[PageStatsQueryService, Depends(get_page_stats_service)],
) -> PageStatsTableOutDto:
    return await service.table(data=data)


@router.get("/ranking", response_model=PageStatsRankingOutDto)
async def ranking(
    data: Annotated[PageStatsRankingInDto, Query()],
    service: Annotated[PageStatsQueryService, Depends(get_page_stats_service)],
) -> PageStatsRankingOutDto:
    return await service.ranking(data=data)
