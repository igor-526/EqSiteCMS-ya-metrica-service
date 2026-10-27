"""UT-MQ-01: exact bounds, UUID normalization, numeric DTO and local read graph."""

from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from containers import repositories_container
from containers.application import create_search_query_stats_services
from core.entities.metrica_stats_query import Direction, Metric, StatsBucket
from core.entities.search_query_stats_query import SearchQueryStatsValue
from core.exceptions import ClientError
from core.protocols.repositories.search_query_stats_query import SearchQueryStatsQueryRepositoryProtocol
from core.schemas.api.search_query_stats import (
    SearchQueryStatsRankingInDto,
    SearchQueryStatsTableInDto,
    SearchQueryStatsValueOutDto,
)
from core.services.search_query_stats_query_service import SearchQueryStatsQueryService
from repositories.search_query_stats_query_repository import SearchQueryStatsQueryRepository

EQ, QUERY = UUID(int=1), UUID(int=2)


@pytest.mark.parametrize("metric", list(Metric))
@pytest.mark.parametrize("empty", [True, False])
async def test_table_single_call_exact_arbitrary_bounds(metric, empty):
    repository = AsyncMock(spec=SearchQueryStatsQueryRepositoryProtocol)
    repository.table.return_value = (
        [] if empty else [SearchQueryStatsValue(QUERY, date(2099, 10, 8), Decimal("1.23456789"))]
    )
    data = SearchQueryStatsTableInDto(
        equestrian_id=str(EQ),
        metric=metric.value,
        bucket=["2099-10-08..2099-10-09", "2099-11-03..2101-02-04"],
        search_query_ids=[str(QUERY), QUERY.hex] * 101,
    )
    result = await SearchQueryStatsQueryService(repository).table(data=data)
    repository.table.assert_awaited_once_with(
        equestrian_id=EQ,
        metric=metric,
        buckets=[StatsBucket(date(2099, 10, 8), date(2099, 10, 9)), StatsBucket(date(2099, 11, 3), date(2101, 2, 4))],
        search_query_ids=[QUERY],
    )
    repository.ranking.assert_not_called()
    assert result.metric == metric
    assert result.values == (
        []
        if empty
        else [
            SearchQueryStatsValueOutDto(search_query_id=QUERY, bucket=date(2099, 10, 8), value=Decimal("1.23456789"))
        ]
    )


async def test_maximum_batch_and_normalized_uuid_dedup():
    repository = AsyncMock(spec=SearchQueryStatsQueryRepositoryProtocol)
    repository.table.return_value = []
    start = date(2099, 1, 1)
    buckets = [f"{start + timedelta(days=i * 2)}..{start + timedelta(days=i * 2)}" for i in range(31)]
    ids = [str(UUID(int=i)) for i in range(100)]
    await SearchQueryStatsQueryService(repository).table(
        data=SearchQueryStatsTableInDto(
            equestrian_id=EQ.hex, metric="visits", bucket=buckets, search_query_ids=ids + ids
        )
    )
    kwargs = repository.table.call_args.kwargs
    assert len(kwargs["buckets"]) == 31
    assert kwargs["search_query_ids"] == [UUID(int=i) for i in range(100)]


@pytest.mark.parametrize("days", [1, 2, 366])
@pytest.mark.parametrize("direction", list(Direction))
@pytest.mark.parametrize("metric", list(Metric))
async def test_ranking_single_call_no_python_sort(days, direction, metric):
    repository = AsyncMock(spec=SearchQueryStatsQueryRepositoryProtocol)
    repository.ranking.return_value = [QUERY, EQ]
    start = date(2099, 1, 1)
    end = start + timedelta(days=days - 1)
    result = await SearchQueryStatsQueryService(repository).ranking(
        data=SearchQueryStatsRankingInDto(
            equestrian_id=str(EQ),
            metric=metric.value,
            date_from=str(start),
            date_to=str(end),
            direction=direction.value,
        )
    )
    repository.ranking.assert_awaited_once_with(
        equestrian_id=EQ, metric=metric, date_from=start, date_to=end, direction=direction
    )
    repository.table.assert_not_called()
    assert result.search_query_ids == [QUERY, EQ]
    assert result.metric == metric
    assert result.direction == direction


@pytest.mark.parametrize(
    "route, data",
    [
        ("table", SearchQueryStatsTableInDto()),
        ("ranking", SearchQueryStatsRankingInDto()),
    ],
)
async def test_service_validation_client_error_before_repository(route, data):
    repository = AsyncMock(spec=SearchQueryStatsQueryRepositoryProtocol)
    with pytest.raises(ClientError):
        await getattr(SearchQueryStatsQueryService(repository), route)(data=data)
    repository.table.assert_not_called()
    repository.ranking.assert_not_called()


@pytest.mark.parametrize(
    "value, expected",
    [
        (12, 12),
        (Decimal("114.86666667"), 114.86666667),
        (Decimal("1.123456789"), 1.12345679),
        (Decimal("0"), 0.0),
    ],
)
def test_numeric_output(value, expected):
    data = SearchQueryStatsValueOutDto(search_query_id=QUERY, bucket=date(2026, 10, 19), value=value)
    assert data.model_dump(mode="json") == {
        "search_query_id": str(QUERY),
        "bucket": "2026-10-19",
        "value": expected,
    }
    assert type(data.model_dump(mode="json")["value"]) is type(expected)


def test_query_graph_request_local_and_lazy():
    session, other = MagicMock(), MagicMock()
    original = repositories_container.session.overridden
    graph = create_search_query_stats_services(session)
    first, second = graph.search_query_stats_query_service(), graph.search_query_stats_query_service()
    assert isinstance(first, SearchQueryStatsQueryService)
    assert isinstance(first._repository, SearchQueryStatsQueryRepository)
    assert first is not second
    assert first._repository is not second._repository
    assert first._repository.session is second._repository.session is session
    assert create_search_query_stats_services(other).search_query_stats_query_service()._repository.session is other
    assert graph.search_query_stats_query_service()._repository.session is session
    assert repositories_container.session.overridden == original
