from contextlib import asynccontextmanager
from datetime import UTC, date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

import api.parse as parse_api
import main
from core.exceptions import PeerServiceException
from core.schemas.clients.yandex_service import MetricaBinding
from core.schemas.parse import ParseEnqueueInDto
from core.services import DateRangeResolver, ParseEnqueueService, TaskDecomposer

PATH = "/api/metrica/stats/parse"


@pytest.fixture(params=[{}, {"Cookie": "session=foreign", "Authorization": "Bearer foreign"}])
def env(request, monkeypatch):
    eq, account = uuid4(), uuid4()
    events = []

    @asynccontextmanager
    async def begin():
        events.append("begin")
        try:
            yield
        except Exception:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    session = SimpleNamespace(begin=begin)
    peer = AsyncMock()
    peer.get_metrica_binding.return_value = MetricaBinding.model_validate(
        {"counter": {"counter_id": "42", "equestrian_id": eq, "yandex_account": {"id": account}}}
    )
    runs = AsyncMock()
    runs.has_active_tasks.return_value = False
    runs.create_run_with_tasks.return_value = True
    pages, queries, dispatcher = AsyncMock(), AsyncMock(), AsyncMock()
    pages.get_last_stat_date.return_value = date(2026, 10, 25)
    queries.get_last_stat_date.return_value = date(2026, 10, 24)

    async def dispatch(account_id):
        assert account_id == account
        assert events[-1] == "commit"
        events.append("dispatch")

    dispatcher.dispatch_account.side_effect = dispatch
    service = ParseEnqueueService(
        yandex_client=peer,
        transaction=session,
        run_repository=runs,
        pages_repository=pages,
        search_queries_repository=queries,
        date_resolver=DateRangeResolver(clock=lambda: datetime(2026, 10, 26, 12, tzinfo=UTC)),
        decomposer=TaskDecomposer(),
        dispatcher=dispatcher,
        max_attempts=3,
    )
    enqueue = AsyncMock(wraps=service.enqueue)
    service.enqueue = enqueue

    @asynccontextmanager
    async def request_session():
        events.append("open")
        try:
            yield session
        finally:
            events.append("close")

    graph = Mock(return_value=SimpleNamespace(parse_enqueue_service=lambda: service))
    monkeypatch.setattr(parse_api, "worker_session", request_session)
    monkeypatch.setattr(parse_api, "create_parse_services", graph)
    monkeypatch.setattr(main, "close_database", AsyncMock())
    with TestClient(main.app, headers=request.param, raise_server_exceptions=False) as client:
        yield SimpleNamespace(**locals())


def payload(env, **extra):
    return {"equestrian_id": str(env.eq), **extra}


@pytest.mark.parametrize("explicit", [True, False])
def test_enqueue_contract_and_request_scoped_graph(env, explicit):
    dates = {"date_from": "2026-10-21", "date_to": "2026-10-25"} if explicit else {}
    response = env.client.post(PATH, json=payload(env, **dates))
    assert response.status_code == 200
    data = response.json()
    assert set(data) == {"run_id", "equestrian_id", "date_mode", "clamped", "ranges", "tasks"}
    assert data["date_mode"] == ("explicit" if explicit else "auto")
    assert data["clamped"] is False
    assert data["equestrian_id"] == str(env.eq)
    assert set(data["ranges"]) == {"pages", "search_queries"}
    assert len(data["tasks"]) == 2
    for sequence, task in enumerate(data["tasks"], 1):
        assert set(task) == {"id", "kind", "date_from", "date_to", "sequence", "status"}
        assert task["status"] == "queued"
        assert task["sequence"] == sequence
    assert str(env.account) not in response.text
    env.graph.assert_called_once_with(env.session)
    assert type(env.enqueue.await_args.kwargs["data"]) is ParseEnqueueInDto
    assert env.events == ["open", "begin", "commit", "dispatch", "close"]
    if not explicit:
        assert data["ranges"]["pages"] == {"date_from": "2026-10-25", "date_to": "2026-10-26"}
        assert data["ranges"]["search_queries"]["date_from"] == "2026-10-24"


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"equestrian_id": "SECRET_MARKER"},
        {"equestrian_id": None},
        {"date_from": "SECRET_MARKER"},
        {"access_token": "SECRET_MARKER"},
        [],
        None,
    ],
)
def test_structural_validation_is_safe(env, body, caplog):
    if isinstance(body, dict) and "equestrian_id" not in body and body:
        body = payload(env, **body)
    response = env.client.post(PATH, json=body)
    assert response.status_code == 400
    assert "SECRET_MARKER" not in response.text + caplog.text
    assert all(set(error) == {"loc", "type"} for error in response.json()["detail"])
    env.enqueue.assert_not_awaited()
    env.runs.create_run_with_tasks.assert_not_awaited()


@pytest.mark.parametrize(
    "dates",
    [
        {"date_from": "2026-10-21"},
        {"date_to": "2026-10-25"},
        {"date_from": "2026-10-25", "date_to": "2026-10-21"},
        {"date_from": "2026-10-27", "date_to": "2026-10-31"},
        {"date_from": "2020-01-01", "date_to": "2023-12-31"},
    ],
)
def test_range_errors(env, dates):
    response = env.client.post(PATH, json=payload(env, **dates))
    assert response.status_code == 400
    assert response.json() == {"detail": "Некорректный диапазон дат"}
    env.runs.create_run_with_tasks.assert_not_awaited()
    env.dispatcher.dispatch_account.assert_not_awaited()
    assert env.events[-1] == "close"


@pytest.mark.parametrize(
    ("mode", "detail"),
    [
        ("missing", "Счётчик Яндекс.Метрики не привязан к конюшне"),
        ("peer", "Не удалось получить данные интеграции Яндекс"),
        ("active", "Парсинг конюшни уже выполняется"),
    ],
)
def test_domain_errors_safe(env, mode, detail, caplog):
    if mode == "missing":
        env.peer.get_metrica_binding.return_value = MetricaBinding(counter=None)
    elif mode == "peer":
        env.peer.get_metrica_binding.side_effect = PeerServiceException("yandex-service", 503, "http")
    else:
        env.runs.has_active_tasks.return_value = True
    response = env.client.post(PATH, json=payload(env))
    assert response.status_code == 400
    assert response.json() == {"detail": detail}
    env.runs.create_run_with_tasks.assert_not_awaited()
    env.dispatcher.dispatch_account.assert_not_awaited()
    assert env.events[-1] == "close"


@pytest.mark.parametrize(
    ("start", "end", "expected_start", "expected_end"),
    [
        ("2026-10-20", "2026-10-31", "2026-10-20", "2026-10-26"),
        ("2023-01-01", "2024-11-03", "2024-10-26", "2024-11-03"),
    ],
)
def test_clamped_ranges(env, start, end, expected_start, expected_end):
    response = env.client.post(PATH, json=payload(env, date_from=start, date_to=end))
    assert response.status_code == 200
    data = response.json()
    assert data["clamped"] is True
    assert data["date_mode"] == "explicit"
    assert all(r == {"date_from": expected_start, "date_to": expected_end} for r in data["ranges"].values())
    assert all(expected_start <= t["date_from"] <= t["date_to"] <= expected_end for t in data["tasks"])
