"""UT-M-02: registered production route, no external lifespan or database."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

import api.metrica_tasks as tasks_api
import containers.application as application
import main
from core.entities.metrica_task_list import (
    MetricaTaskListItem,
    MetricaTaskListPage,
    MetricaTaskStatus,
    MetricaTaskType,
)
from core.protocols.repositories.metrica_task_list import MetricaTaskListRepositoryProtocol

EQ = UUID(int=1)
URL = "/api/metrica/tasks"
CREDENTIALS = [{}, {"Cookie": "access_token=foreign", "Authorization": "Bearer foreign"}]


@pytest.fixture
def env(monkeypatch):
    repository = AsyncMock(spec=MetricaTaskListRepositoryProtocol)
    repository.list_tasks.return_value = MetricaTaskListPage(
        [
            MetricaTaskListItem(
                "merge:example",
                MetricaTaskType.MERGE,
                MetricaTaskStatus.RUNNING,
                True,
                datetime(2026, 10, 20, tzinfo=UTC),
            )
        ],
        1,
    )
    session = Mock()
    events = []

    @asynccontextmanager
    async def request_session():
        events.append("open")
        try:
            yield session
        finally:
            events.append("close")

    factory = Mock(return_value=repository)
    monkeypatch.setattr(tasks_api, "worker_session", request_session)
    monkeypatch.setattr(application, "MetricaTaskListRepository", factory)
    # Deliberately no context manager: ASGI lifespan must never start NATS.
    client = TestClient(main.app, raise_server_exceptions=False)
    yield SimpleNamespace(**locals())
    client.close()


@pytest.mark.parametrize("credentials", CREDENTIALS)
@pytest.mark.parametrize("selector", [None, "", "SECRET_MARKER", "123", "null"])
@pytest.mark.parametrize("query", ["", "?limit=bad&offset=-1"])
def test_selector_precedes_query_and_storage(env, selector, query, credentials, caplog):
    headers = dict(credentials)
    if selector is not None:
        headers["X-Equestrian-Id"] = selector
    response = env.client.get(URL + query, headers=headers)
    assert response.status_code == 401
    assert response.json() == {"detail": "Некорректный selector конюшни"}
    assert "SECRET_MARKER" not in response.text + caplog.text
    env.factory.assert_not_called()
    env.repository.list_tasks.assert_not_awaited()
    assert not env.events


@pytest.mark.parametrize("credentials", CREDENTIALS)
@pytest.mark.parametrize(
    "query",
    [
        "limit=0",
        "limit=101",
        "limit=-1",
        "limit=1.0",
        "limit=1e1",
        "limit=true",
        "limit=",
        "limit=%201",
        "limit=%2B1",
        "limit=SECRET_MARKER",
        "limit=１",
        "limit=1_0",
        "offset=-1",
        "offset=0.5",
        "offset=",
        "offset=false",
        "offset=NaN",
        "offset=1e2",
        "offset=%200",
        "offset=%2B0",
        "offset=SECRET_MARKER",
        "limit=" + "9" * 5000,
    ],
)
def test_invalid_query_never_opens_storage(env, credentials, query, caplog):
    response = env.client.get(URL + "?" + query, headers={**credentials, "X-Equestrian-Id": str(EQ)})
    assert response.status_code == 400
    assert "SECRET_MARKER" not in response.text + caplog.text
    env.factory.assert_not_called()
    env.repository.list_tasks.assert_not_awaited()
    assert not env.events


@pytest.mark.parametrize("credentials", CREDENTIALS)
@pytest.mark.parametrize(
    "query,limit,offset", [("", 20, 0), ("?limit=1&offset=0", 1, 0), ("?limit=100&offset=200", 100, 200)]
)
def test_success_uses_selector_not_credentials_or_query(env, credentials, query, limit, offset):
    separator = "&" if query else "?"
    response = env.client.get(
        URL + query + separator + "equestrian_id=" + str(UUID(int=2)),
        headers={**credentials, "X-Equestrian-Id": str(EQ)},
    )
    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {
                "id": "merge:example",
                "type": "merge",
                "status": "running",
                "is_active": True,
                "created_at": "2026-10-20T00:00:00Z",
            }
        ],
        "total": 1,
    }
    env.repository.list_tasks.assert_awaited_once_with(equestrian_id=EQ, limit=limit, offset=offset)
    env.factory.assert_called_once_with(env.session)
    assert env.events == ["open", "close"]
    assert not env.session.mock_calls


@pytest.mark.parametrize("total", [0, 7])
def test_empty_and_offset_beyond_end(env, total):
    env.repository.list_tasks.return_value = MetricaTaskListPage([], total)
    response = env.client.get(URL + "?offset=100", headers={"X-Equestrian-Id": str(EQ)})
    assert response.status_code == 200
    assert response.json() == {"items": [], "total": total}


@pytest.mark.parametrize("credentials", CREDENTIALS)
@pytest.mark.parametrize("failure", [SQLAlchemyError("SECRET_MARKER SELECT token"), RuntimeError("SECRET_MARKER")])
def test_safe_failure_unwinds_request_session(env, credentials, failure, caplog):
    env.repository.list_tasks.side_effect = failure
    response = env.client.get(URL, headers={**credentials, "X-Equestrian-Id": str(EQ)})
    assert response.status_code == 400
    assert response.json() == {"detail": "Ошибка обработки запроса"}
    assert "SECRET_MARKER" not in response.text + caplog.text
    assert env.events == ["open", "close"]
    assert not env.session.mock_calls
