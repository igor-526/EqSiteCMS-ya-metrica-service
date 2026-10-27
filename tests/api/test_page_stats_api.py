"""AT-MP-01..04: trusted peer access, validation and safe failures."""

from contextlib import asynccontextmanager
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

import api.page_stats as page_stats_api
import main
from core.entities.metrica_stats_query import Metric, PageStatsValue, StatsBucket
from core.protocols.repositories.page_stats_query import PageStatsQueryRepositoryProtocol
from core.services.page_stats_query_service import PageStatsQueryService

EQ, PAGE = UUID(int=1), UUID(int=2)
PREFIX = "/api/metrica/page-stats/"
HEADERS = [{}, {"Cookie": "access_token=foreign", "Authorization": "Bearer foreign"}]
TABLE = [
    ("equestrian_id", str(EQ)),
    ("metric", "visits"),
    ("bucket", "2026-10-19..2026-10-25"),
    ("bucket", "2026-10-26..2026-10-26"),
    ("page_ids", str(PAGE)),
    ("page_ids", str(PAGE)),
]
RANKING = [
    ("equestrian_id", str(EQ)),
    ("metric", "visits"),
    ("date_from", "2026-10-19"),
    ("date_to", "2026-10-25"),
    ("direction", "desc"),
]


@pytest.fixture
def env(monkeypatch):
    repository = AsyncMock(spec=PageStatsQueryRepositoryProtocol)
    repository.table.return_value = [PageStatsValue(PAGE, date(2026, 10, 19), 12)]
    repository.ranking.return_value = [PAGE]
    events = []
    session = object()

    @asynccontextmanager
    async def request_session():
        events.append("open")
        try:
            yield session
        finally:
            events.append("close")

    graph = Mock(return_value=SimpleNamespace(page_stats_query_service=lambda: PageStatsQueryService(repository)))
    monkeypatch.setattr(page_stats_api, "worker_session", request_session)
    monkeypatch.setattr(page_stats_api, "create_page_stats_services", graph)
    monkeypatch.setattr(main, "close_database", AsyncMock())
    with TestClient(main.app, raise_server_exceptions=False) as client:
        yield SimpleNamespace(**locals())


@pytest.mark.parametrize("route, params", [("table", TABLE), ("ranking", RANKING)])
def test_anonymous_and_credentials_identical(env, route, params):
    responses = [env.client.get(PREFIX + route, params=params, headers=headers) for headers in HEADERS]
    assert [r.status_code for r in responses] == [200, 200]
    expected = (
        {"metric": "visits", "values": [{"page_id": str(PAGE), "bucket": "2026-10-19", "value": 12}]}
        if route == "table"
        else {"metric": "visits", "direction": "desc", "page_ids": [str(PAGE)]}
    )
    assert responses[0].json() == responses[1].json() == expected
    assert env.events == ["open", "close"] * 2
    assert env.graph.call_count == 2
    env.graph.assert_called_with(env.session)
    if route == "table":
        env.repository.table.assert_awaited_with(
            equestrian_id=EQ,
            metric=Metric.VISITS,
            buckets=[
                StatsBucket(date(2026, 10, 19), date(2026, 10, 25)),
                StatsBucket(date(2026, 10, 26), date(2026, 10, 26)),
            ],
            page_ids=[PAGE],
        )
    else:
        assert env.repository.ranking.await_count == 2


@pytest.mark.parametrize("metric", list(Metric))
def test_numeric_projection_and_empty_results(env, metric):
    additive = metric in [Metric.VISITS, Metric.USERS, Metric.PAGEVIEWS, Metric.BOUNCES, Metric.VISIT_DURATION_SECONDS]
    value = 12 if additive else Decimal("114.86666667")
    env.repository.table.return_value = [PageStatsValue(PAGE, date(2026, 10, 19), value)]
    params = [(key, metric.value if key == "metric" else val) for key, val in TABLE]
    response = env.client.get(PREFIX + "table", params=params)
    assert response.status_code == 200
    actual = response.json()["values"][0]["value"]
    assert actual == (12 if additive else 114.86666667)
    assert type(actual) is (int if additive else float)
    env.repository.table.return_value = []
    assert env.client.get(PREFIX + "table", params=params).json() == {"metric": metric.value, "values": []}
    env.repository.ranking.return_value = []
    assert env.client.get(PREFIX + "ranking", params=RANKING).json()["page_ids"] == []


TABLE_ERRORS = [
    ("equestrian_id", []),
    ("equestrian_id", ["SECRET_MARKER"]),
    ("metric", []),
    ("metric", ["SECRET_MARKER"]),
    ("bucket", []),
    ("bucket", ["SECRET_MARKER"]),
    ("bucket", ["2026-10-19..2026-10-19"] * 32),
    ("bucket", ["2026-10-20..2026-10-19"]),
    ("bucket", ["2026-10-19..2026-10-20", "2026-10-20..2026-10-21"]),
    ("bucket", ["2026-10-21..2026-10-22", "2026-10-19..2026-10-20"]),
    ("bucket", ["2026-1-01..2026-01-02"]),
    ("bucket", ["2026-02-30..2026-03-01"]),
    ("page_ids", []),
    ("page_ids", ["SECRET_MARKER"]),
    ("page_ids", [str(UUID(int=i)) for i in range(101)]),
]
RANKING_ERRORS = [
    ("equestrian_id", []),
    ("equestrian_id", ["SECRET_MARKER"]),
    ("metric", []),
    ("metric", ["SECRET_MARKER"]),
    ("date_from", []),
    ("date_to", []),
    ("direction", []),
    ("direction", ["SECRET_MARKER"]),
    ("date_from", ["SECRET_MARKER"]),
    ("date_to", ["SECRET_MARKER"]),
    ("date_from", ["2026-10-26"]),
    ("date_from", ["2025-10-19"]),
    ("date_from", ["2026-1-01"]),
    ("date_to", ["2026-02-30"]),
]


@pytest.mark.parametrize("headers", HEADERS)
@pytest.mark.parametrize(
    "route, source, key, values",
    [
        (route, source, key, values)
        for route, source, errors in [("table", TABLE, TABLE_ERRORS), ("ranking", RANKING, RANKING_ERRORS)]
        for key, values in errors
    ],
)
def test_validation_safe_and_no_read(env, route, source, key, values, headers, caplog):
    params = [(k, v) for k, v in source if k != key] + [(key, value) for value in values]
    response = env.client.get(PREFIX + route, params=params, headers=headers)
    assert response.status_code == 400
    assert "SECRET_MARKER" not in response.text + caplog.text
    assert all(set(error) == {"loc", "type"} for error in response.json()["detail"])
    env.repository.table.assert_not_awaited()
    env.repository.ranking.assert_not_awaited()


@pytest.mark.parametrize("headers", HEADERS)
@pytest.mark.parametrize("route, params", [("table", TABLE), ("ranking", RANKING)])
def test_database_error_safe_and_session_closed(env, route, params, headers, caplog):
    getattr(env.repository, route).side_effect = SQLAlchemyError("SECRET_MARKER SELECT password FROM db")
    response = env.client.get(PREFIX + route, params=params, headers=headers)
    assert response.status_code == 400
    assert response.json() == {"detail": "Ошибка обработки запроса"}
    assert "SECRET_MARKER" not in response.text + caplog.text
    assert env.events == ["open", "close"]


@pytest.mark.parametrize("headers", HEADERS)
@pytest.mark.parametrize("route, params", [("table", TABLE), ("ranking", RANKING)])
@pytest.mark.parametrize("mode", ["method", "slash"])
def test_framework_errors_safe_without_service(env, route, params, headers, mode):
    response = (
        env.client.post(PREFIX + route, params=params, headers=headers)
        if mode == "method"
        else env.client.get(PREFIX + route + "/", params=params, headers=headers)
    )
    assert response.status_code == 400
    assert response.json() == {"detail": "Ошибка HTTP-запроса"}
    assert not response.history
    env.graph.assert_not_called()
    assert not env.events
