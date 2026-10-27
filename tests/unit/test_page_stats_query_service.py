"""UT-MP-02: one call and unmodified future/arbitrary SEO bounds."""

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from core.entities.metrica_stats_query import Direction, Metric, PageStatsValue
from core.protocols.repositories.page_stats_query import PageStatsQueryRepositoryProtocol
from core.schemas.api.page_stats import PageStatsRankingInDto, PageStatsTableInDto
from core.services.page_stats_query_service import PageStatsQueryService

EQ, PAGE = UUID(int=1), UUID(int=2)


@pytest.mark.parametrize("empty", [False, True])
async def test_table_exact_bounds_and_single_call(empty):
    repository = AsyncMock(spec=PageStatsQueryRepositoryProtocol)
    repository.table.return_value = [] if empty else [PageStatsValue(PAGE, date(2099, 10, 8), Decimal("1.12345678"))]
    data = PageStatsTableInDto(
        equestrian_id=EQ,
        metric=Metric.PAGE_DEPTH,
        bucket=["2099-10-08..2099-10-09", "2099-11-03..2101-02-04"],
        page_ids=[PAGE],
    )
    result = await PageStatsQueryService(repository).table(data=data)
    repository.table.assert_awaited_once_with(
        equestrian_id=EQ, metric=Metric.PAGE_DEPTH, buckets=data.buckets, page_ids=[PAGE]
    )
    repository.ranking.assert_not_called()
    assert result.metric == Metric.PAGE_DEPTH
    assert len(result.values) == (0 if empty else 1)
    if not empty:
        assert result.values[0].value == Decimal("1.12345678")
        assert result.values[0].bucket == date(2099, 10, 8)


@pytest.mark.parametrize("direction", list(Direction))
async def test_ranking_exact_bounds_and_single_call(direction):
    repository = AsyncMock(spec=PageStatsQueryRepositoryProtocol)
    repository.ranking.return_value = [PAGE, EQ]
    data = PageStatsRankingInDto(
        equestrian_id=EQ,
        metric=Metric.VISITS,
        date_from=date(2099, 10, 8),
        date_to=date(2099, 10, 9),
        direction=direction,
    )
    result = await PageStatsQueryService(repository).ranking(data=data)
    repository.ranking.assert_awaited_once_with(
        equestrian_id=EQ, metric=Metric.VISITS, date_from=data.date_from, date_to=data.date_to, direction=direction
    )
    repository.table.assert_not_called()
    assert result.page_ids == [PAGE, EQ]
    assert result.metric == data.metric
    assert result.direction == direction
