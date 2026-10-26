import logging
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from core.entities.metrica_parse import ParseKind
from core.exceptions import (
    ClientError,
    MetricaBindingMissingError,
    ParseAlreadyActiveError,
    ParseRangeError,
    PeerServiceException,
    YandexIntegrationUnavailableError,
)
from core.schemas.clients.yandex_service import MetricaBinding
from core.schemas.parse import ParseEnqueueInDto
from core.services.parse_date_resolver import DateRangeResolver
from core.services.parse_decomposer import TaskDecomposer
from core.services.parse_enqueue_service import ParseEnqueueService


class FakeTransaction:
    def __init__(self, events):
        self.events = events

    @asynccontextmanager
    async def begin(self):
        self.events.append("begin")
        try:
            yield
        except Exception:
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")


@pytest.fixture
def env():
    events = []
    eq, account = uuid4(), uuid4()
    binding = MetricaBinding.model_validate(
        {
            "counter": {
                "counter_id": "123",
                "equestrian_id": eq,
                "yandex_account": {"id": account},
            }
        }
    )

    async def get_binding(**kwargs):
        assert kwargs == {"equestrian_id": eq}
        events.append("y7")
        return binding

    async def lock(equestrian_id):
        assert equestrian_id == eq
        events.append("lock")

    async def active(equestrian_id):
        assert equestrian_id == eq
        events.append("active")
        return False

    async def last(equestrian_id):
        assert equestrian_id == eq
        events.append("last")
        return date(2026, 10, 20)

    async def create(run, tasks):
        assert run.equestrian_id == eq
        events.append("create")
        return True

    async def dispatch(account_id):
        assert account_id == account
        events.append("dispatch")

    client = SimpleNamespace(get_metrica_binding=AsyncMock(side_effect=get_binding))
    runs = SimpleNamespace(
        lock_equestrian=AsyncMock(side_effect=lock),
        has_active_tasks=AsyncMock(side_effect=active),
        create_run_with_tasks=AsyncMock(side_effect=create),
    )
    pages = SimpleNamespace(get_last_stat_date=AsyncMock(side_effect=last))
    search = SimpleNamespace(get_last_stat_date=AsyncMock(return_value=date(2026, 10, 25)))
    dispatcher = SimpleNamespace(dispatch_account=AsyncMock(side_effect=dispatch))
    service = ParseEnqueueService(
        yandex_client=client,
        transaction=FakeTransaction(events),
        run_repository=runs,
        pages_repository=pages,
        search_queries_repository=search,
        date_resolver=DateRangeResolver(clock=lambda: datetime(2026, 10, 26, 12, tzinfo=UTC)),
        decomposer=TaskDecomposer(),
        dispatcher=dispatcher,
        max_attempts=4,
    )
    return SimpleNamespace(**locals())


async def test_enqueue_auto_transaction_and_safe_output(env):
    result = await env.service.enqueue(data=ParseEnqueueInDto(equestrian_id=env.eq))
    assert env.events == ["y7", "begin", "lock", "active", "last", "create", "commit", "dispatch"]
    env.search.get_last_stat_date.assert_awaited_once_with(env.eq)
    assert result.ranges[ParseKind.PAGES].date_from == date(2026, 10, 20)
    assert result.ranges[ParseKind.SEARCH_QUERIES].date_from == date(2026, 10, 25)
    run, tasks = env.runs.create_run_with_tasks.call_args.args
    assert result.run_id == run.id
    assert [t.id for t in result.tasks] == [t.id for t in tasks]
    assert len(tasks) == 3
    for task in tasks:
        assert task.equestrian_id == env.eq
        assert task.run_id == run.id
        assert task.counter_id == "123"
        assert task.yandex_account_id == env.account
        assert task.max_attempts == 4
        assert task.status == "queued"
    output = result.model_dump_json()
    assert str(env.account) not in output
    assert "counter_id" not in output
    assert "token" not in output


async def test_explicit_clamp_logged_without_auto_queries(env, caplog):
    with caplog.at_level(logging.INFO):
        result = await env.service.enqueue(
            data=ParseEnqueueInDto(
                equestrian_id=env.eq,
                date_from=date(2026, 10, 20),
                date_to=date(2027, 1, 1),
            )
        )
    assert result.clamped
    assert result.date_mode == "explicit"
    assert result.ranges[ParseKind.PAGES].date_to == date(2026, 10, 26)
    env.pages.get_last_stat_date.assert_not_awaited()
    env.search.get_last_stat_date.assert_not_awaited()
    assert "clamped" in caplog.text
    assert "enqueued" in caplog.text


@pytest.mark.parametrize(
    "category,status",
    [("timeout", None), ("transport", None), ("http", 503), ("http", 400), ("http", 404), ("response", 200)],
)
async def test_y7_failure_before_transaction(env, category, status):
    env.client.get_metrica_binding.side_effect = PeerServiceException("yandex", status, category)
    with pytest.raises(YandexIntegrationUnavailableError, match="Не удалось получить данные интеграции Яндекс") as exc:
        await env.service.enqueue(data=ParseEnqueueInDto(equestrian_id=env.eq))
    assert isinstance(exc.value, ClientError)
    assert env.events == []
    env.runs.create_run_with_tasks.assert_not_awaited()
    env.dispatcher.dispatch_account.assert_not_awaited()


async def test_missing_binding(env):
    env.client.get_metrica_binding.side_effect = None
    env.client.get_metrica_binding.return_value = MetricaBinding(counter=None)
    with pytest.raises(MetricaBindingMissingError, match="Счётчик Яндекс.Метрики не привязан к конюшне"):
        await env.service.enqueue(data=ParseEnqueueInDto(equestrian_id=env.eq))
    assert env.events == []


async def test_wrong_equestrian_binding(env):
    env.binding.counter.equestrian_id = uuid4()
    with pytest.raises(YandexIntegrationUnavailableError):
        await env.service.enqueue(data=ParseEnqueueInDto(equestrian_id=env.eq))
    assert env.events == ["y7"]


@pytest.mark.parametrize("at_create", [False, True])
async def test_active_run_rejected_and_rolled_back(env, at_create):
    mock = env.runs.create_run_with_tasks if at_create else env.runs.has_active_tasks
    mock.side_effect = None
    mock.return_value = not at_create
    with pytest.raises(ParseAlreadyActiveError, match="Парсинг конюшни уже выполняется"):
        await env.service.enqueue(data=ParseEnqueueInDto(equestrian_id=env.eq))
    assert env.events[-1] == "rollback"
    env.dispatcher.dispatch_account.assert_not_awaited()
    if not at_create:
        env.pages.get_last_stat_date.assert_not_awaited()
        env.runs.create_run_with_tasks.assert_not_awaited()


@pytest.mark.parametrize(
    "start,end", [(date(2026, 1, 1), None), (None, date(2026, 1, 1)), (date(2027, 1, 1), date(2027, 1, 2))]
)
async def test_bad_dates_rollback_without_create(env, start, end):
    with pytest.raises(ParseRangeError):
        await env.service.enqueue(data=ParseEnqueueInDto(equestrian_id=env.eq, date_from=start, date_to=end))
    assert env.events[-1] == "rollback"
    env.runs.create_run_with_tasks.assert_not_awaited()
    env.dispatcher.dispatch_account.assert_not_awaited()


async def test_database_failure_rolls_back(env):
    env.runs.create_run_with_tasks.side_effect = RuntimeError("database failure")
    with pytest.raises(RuntimeError):
        await env.service.enqueue(data=ParseEnqueueInDto(equestrian_id=env.eq))
    assert env.events[-1] == "rollback"
    env.dispatcher.dispatch_account.assert_not_awaited()


async def test_commit_failure_does_not_dispatch(env):
    @asynccontextmanager
    async def fail_commit():
        env.events.append("begin")
        yield
        raise RuntimeError("commit failure")

    env.service._transaction = SimpleNamespace(begin=fail_commit)
    with pytest.raises(RuntimeError):
        await env.service.enqueue(data=ParseEnqueueInDto(equestrian_id=env.eq))
    env.dispatcher.dispatch_account.assert_not_awaited()


async def test_dispatch_failure_after_commit_is_only_safe_warning(env, caplog):
    env.dispatcher.dispatch_account.side_effect = RuntimeError("redis://secret-token")
    with caplog.at_level(logging.WARNING):
        result = await env.service.enqueue(data=ParseEnqueueInDto(equestrian_id=env.eq))
    assert result.tasks
    assert env.events[-1] == "commit"
    assert "dispatch failed after commit" in caplog.text
    assert "secret-token" not in caplog.text
    assert "Traceback" not in caplog.text
