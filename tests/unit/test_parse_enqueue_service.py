import logging
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from core.entities.metrica_maintenance import IntentAdmission
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
            "binding_generation": 3,
            "cleanup_requested_seq": 2,
            "cleanup_completed_seq": 2,
            "counter": {
                "counter_id": "123",
                "equestrian_id": eq,
                "yandex_account": {"id": account},
            },
        }
    )

    async def get_binding(**kwargs):
        assert kwargs == {"equestrian_id": eq}
        events.append("y7")
        return binding

    async def enqueue_intent(intent, *, lifecycle):
        assert intent.equestrian_id == eq
        assert lifecycle.binding_generation == 3
        assert lifecycle.cleanup_requested_seq == 2
        events.append("intent")
        return IntentAdmission.READY

    async def prepare(intent, *, lifecycle):
        events.append("prepare")
        if lifecycle.account_id is None:
            return IntentAdmission.MISSING_BINDING
        return IntentAdmission.READY

    async def last(equestrian_id):
        assert equestrian_id == eq
        events.append("last")
        return date(2026, 10, 20)

    async def consume(intent, *, lifecycle, run, tasks):
        assert run.equestrian_id == eq
        assert run.job_id == intent.job_id
        assert run.binding_generation == lifecycle.binding_generation == 3
        assert run.observed_cleanup_requested_seq == lifecycle.cleanup_requested_seq == 2
        assert all(t.binding_generation == 3 and t.observed_cleanup_requested_seq == 2 for t in tasks)
        events.append("consume")
        return IntentAdmission.ADMITTED

    async def dispatch(account_id):
        assert account_id == account
        events.append("dispatch")

    client = SimpleNamespace(get_metrica_binding=AsyncMock(side_effect=get_binding))
    intents = SimpleNamespace(
        enqueue_intent=AsyncMock(side_effect=enqueue_intent),
        prepare_admission=AsyncMock(side_effect=prepare),
        consume_intent=AsyncMock(side_effect=consume),
    )
    pages = SimpleNamespace(get_last_stat_date=AsyncMock(side_effect=last))
    search = SimpleNamespace(get_last_stat_date=AsyncMock(return_value=date(2026, 10, 25)))
    dispatcher = SimpleNamespace(dispatch_account=AsyncMock(side_effect=dispatch))
    service = ParseEnqueueService(
        yandex_client=client,
        transaction=FakeTransaction(events),
        intent_repository=intents,
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
    assert env.events == ["y7", "begin", "intent", "prepare", "last", "consume", "commit", "dispatch"]
    env.search.get_last_stat_date.assert_awaited_once_with(env.eq)
    assert result is not None
    assert result.ranges[ParseKind.PAGES].date_from == date(2026, 10, 20)
    assert result.ranges[ParseKind.SEARCH_QUERIES].date_from == date(2026, 10, 25)
    run, tasks = (env.intents.consume_intent.call_args.kwargs[k] for k in ("run", "tasks"))
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
    assert result is not None
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
    env.intents.consume_intent.assert_not_awaited()
    env.dispatcher.dispatch_account.assert_not_awaited()


async def test_missing_binding(env):
    env.client.get_metrica_binding.side_effect = None
    env.client.get_metrica_binding.return_value = MetricaBinding(
        counter=None, binding_generation=3, cleanup_requested_seq=2, cleanup_completed_seq=2
    )
    with pytest.raises(MetricaBindingMissingError, match="Счётчик Яндекс.Метрики не привязан к конюшне"):
        await env.service.enqueue(data=ParseEnqueueInDto(equestrian_id=env.eq))
    assert env.events == ["begin", "intent", "prepare", "commit"]


async def test_wrong_equestrian_binding(env):
    env.binding.counter.equestrian_id = uuid4()
    with pytest.raises(YandexIntegrationUnavailableError):
        await env.service.enqueue(data=ParseEnqueueInDto(equestrian_id=env.eq))
    assert env.events == ["y7"]


async def test_active_run_rejected_and_rolled_back(env):
    env.intents.enqueue_intent.side_effect = None
    env.intents.enqueue_intent.return_value = IntentAdmission.ACTIVE_RUN
    with pytest.raises(ParseAlreadyActiveError):
        await env.service.enqueue(data=ParseEnqueueInDto(equestrian_id=env.eq))
    assert env.events[-1] == "rollback"
    env.intents.prepare_admission.assert_not_awaited()
    env.pages.get_last_stat_date.assert_not_awaited()
    env.dispatcher.dispatch_account.assert_not_awaited()


@pytest.mark.parametrize(
    "start,end", [(date(2026, 1, 1), None), (None, date(2026, 1, 1)), (date(2027, 1, 1), date(2027, 1, 2))]
)
async def test_bad_dates_rollback_without_create(env, start, end):
    with pytest.raises(ParseRangeError):
        await env.service.enqueue(data=ParseEnqueueInDto(equestrian_id=env.eq, date_from=start, date_to=end))
    assert "commit" not in env.events
    env.intents.consume_intent.assert_not_awaited()
    env.dispatcher.dispatch_account.assert_not_awaited()


async def test_database_failure_rolls_back(env):
    env.intents.consume_intent.side_effect = RuntimeError("database failure")
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
    assert result is not None
    assert result.tasks
    assert env.events[-1] == "commit"
    assert "dispatch failed after commit" in caplog.text
    assert "secret-token" not in caplog.text
    assert "Traceback" not in caplog.text


async def test_ut_mp_04_job_id_persisted_and_legacy_nulls_unchanged(env):
    job_id = uuid4()
    await env.service.enqueue(data=ParseEnqueueInDto(equestrian_id=env.eq), job_id=job_id)
    assert env.intents.consume_intent.call_args.kwargs["run"].job_id == job_id
    await env.service.enqueue(data=ParseEnqueueInDto(equestrian_id=env.eq))
    assert env.intents.consume_intent.call_args.kwargs["run"].job_id is not None


async def test_ut_mp_04_redelivery_no_tasks_no_dispatch(env):
    from core.exceptions.parse import ParseJobAlreadyExistsError

    env.intents.enqueue_intent.side_effect = None
    env.intents.enqueue_intent.return_value = IntentAdmission.DUPLICATE
    with pytest.raises(ParseJobAlreadyExistsError):
        await env.service.enqueue(data=ParseEnqueueInDto(equestrian_id=env.eq), job_id=uuid4())
    env.intents.prepare_admission.assert_not_awaited()
    env.intents.consume_intent.assert_not_awaited()
    env.pages.get_last_stat_date.assert_not_awaited()
    env.dispatcher.dispatch_account.assert_not_awaited()
    assert env.events[-1] == "rollback"


async def test_ut_mp_04_concurrent_duplicate_no_dispatch(env):
    from core.exceptions.parse import ParseJobAlreadyExistsError

    env.intents.consume_intent.side_effect = ParseJobAlreadyExistsError()
    with pytest.raises(ParseJobAlreadyExistsError):
        await env.service.enqueue(data=ParseEnqueueInDto(equestrian_id=env.eq), job_id=uuid4())
    env.dispatcher.dispatch_account.assert_not_awaited()
    assert env.events[-1] == "rollback"


@pytest.mark.parametrize("last", [date(2026, 10, 25), None, date(2026, 10, 21), date(2023, 1, 1)])
async def test_ut_mp_03_auto_command_windows_cover_every_day(env, last):
    import json
    from datetime import timedelta

    from clients.nats.handlers.parse_run import ParseRunHandler
    from core.schemas.messaging import ParseCommand

    env.pages.get_last_stat_date.side_effect = None
    env.pages.get_last_stat_date.return_value = last
    env.search.get_last_stat_date.return_value = last
    body = {
        "schema_version": 1,
        "occurred_at": "2026-10-26T00:00:03Z",
        "job_id": str(uuid4()),
        "provider": "ya_metrica",
        "equestrian_id": str(env.eq),
        "trigger": "schedule",
        "window": {"mode": "auto"},
        "params": {},
    }
    await ParseRunHandler(service=env.service).handle(payload=json.dumps(body).encode())
    run, tasks = (env.intents.consume_intent.call_args.kwargs[k] for k in ("run", "tasks"))
    assert run.job_id == ParseCommand.model_validate(body).job_id
    assert run.date_mode == "auto"
    assert not run.clamped
    lower, today = date(2024, 10, 26), date(2026, 10, 26)
    start = max(last, lower) if last is not None else lower
    assert set(task.kind for task in tasks) == set(ParseKind)
    for kind in ParseKind:
        windows = [task for task in tasks if task.kind == kind]
        assert windows[0].date_from == start
        assert windows[-1].date_to == today
        cursor = start
        for task in windows:
            assert task.status == "queued"
            assert task.date_from == cursor
            assert 1 <= (task.date_to - task.date_from).days + 1 <= 5
            cursor = task.date_to + timedelta(days=1)
        assert cursor == today + timedelta(days=1)
        if last == date(2026, 10, 25):
            assert len(windows) == 1
            assert (windows[0].date_to - windows[0].date_from).days == 1
    env.dispatcher.dispatch_account.assert_awaited_once_with(env.account)


@pytest.mark.parametrize("with_counter", [False, True])
async def test_ut_d04_d06_maintenance_wait_commits_original_auto_intent(env, with_counter):
    import json

    from clients.nats.handlers.parse_run import ParseRunHandler
    from tests.unit.test_parse_command_schema import command_payload

    env.binding.cleanup_completed_seq = 1
    if not with_counter:
        env.binding.counter = None
    env.intents.enqueue_intent.side_effect = None
    env.intents.enqueue_intent.return_value = IntentAdmission.DEFERRED
    env.intents.prepare_admission.side_effect = None
    env.intents.prepare_admission.return_value = IntentAdmission.DEFERRED
    body = command_payload(equestrian_id=str(env.eq))
    await ParseRunHandler(service=env.service).handle(payload=json.dumps(body).encode())
    assert env.events == ["y7", "begin", "commit"]
    intent = env.intents.enqueue_intent.call_args.args[0]
    snapshot = env.intents.enqueue_intent.call_args.kwargs["lifecycle"]
    assert str(intent.job_id) == body["job_id"]
    assert intent.date_mode == "auto" and intent.date_from is None and intent.date_to is None
    assert snapshot.cleanup_requested_seq == 2 and snapshot.cleanup_completed_seq == 1
    env.pages.get_last_stat_date.assert_not_awaited()
    env.search.get_last_stat_date.assert_not_awaited()
    env.intents.consume_intent.assert_not_awaited()
    env.dispatcher.dispatch_account.assert_not_awaited()


async def test_ut_d04_maintenance_wait_commit_failure_never_acks(env):
    import json

    from clients.nats.handlers.parse_run import ParseRunHandler
    from tests.unit.test_parse_command_schema import command_payload

    @asynccontextmanager
    async def fail_commit():
        yield
        raise RuntimeError("commit failed")

    env.service._transaction = SimpleNamespace(begin=fail_commit)
    env.intents.prepare_admission.side_effect = None
    env.intents.prepare_admission.return_value = IntentAdmission.DEFERRED
    with pytest.raises(RuntimeError, match="commit failed"):
        await ParseRunHandler(service=env.service).handle(
            payload=json.dumps(command_payload(equestrian_id=str(env.eq))).encode()
        )
    env.dispatcher.dispatch_account.assert_not_awaited()


@pytest.mark.parametrize("decision", [IntentAdmission.DEFERRED, IntentAdmission.CANCELLED])
async def test_ut_d04_consume_recheck_does_not_signal_unadmitted_work(env, decision):
    env.intents.consume_intent.side_effect = None
    env.intents.consume_intent.return_value = decision
    assert await env.service.enqueue(data=ParseEnqueueInDto(equestrian_id=env.eq), job_id=uuid4()) is None
    assert env.events[-1] == "commit"
    env.dispatcher.dispatch_account.assert_not_awaited()


async def test_ut_d06_replay_fetches_fresh_binding_and_auto_window(env):
    from core.entities.metrica_maintenance import PendingParseIntent

    intent = PendingParseIntent(uuid4(), env.eq, "auto")
    # Maintenance has removed historical rows, so admission must re-read dates.
    env.pages.get_last_stat_date.side_effect = None
    env.pages.get_last_stat_date.return_value = None
    env.search.get_last_stat_date.return_value = None
    result = await env.service.replay_intent(intent)
    assert result is not None
    assert env.events == ["y7", "begin", "prepare", "consume", "commit", "dispatch"]
    assert result.ranges[ParseKind.PAGES].date_from == date(2024, 10, 26)
    run = env.intents.consume_intent.call_args.kwargs["run"]
    assert run.job_id == intent.job_id
    assert run.binding_generation == 3
    env.intents.enqueue_intent.assert_not_awaited()


@pytest.mark.parametrize("decision", [IntentAdmission.DEFERRED, IntentAdmission.CANCELLED, IntentAdmission.DUPLICATE])
async def test_ut_d06_replay_rechecks_gate_active_run_and_terminal_state(env, decision):
    from core.entities.metrica_maintenance import PendingParseIntent

    env.intents.prepare_admission.side_effect = None
    env.intents.prepare_admission.return_value = decision
    assert await env.service.replay_intent(PendingParseIntent(uuid4(), env.eq, "auto")) is None
    assert env.events == ["y7", "begin", "commit"]
    env.intents.enqueue_intent.assert_not_awaited()
    env.pages.get_last_stat_date.assert_not_awaited()
    env.intents.consume_intent.assert_not_awaited()
    env.dispatcher.dispatch_account.assert_not_awaited()


async def test_ut_d06_replay_missing_binding_commits_terminal(env):
    from core.entities.metrica_maintenance import PendingParseIntent

    env.binding.counter = None
    with pytest.raises(MetricaBindingMissingError):
        await env.service.replay_intent(PendingParseIntent(uuid4(), env.eq, "auto"))
    assert env.events == ["y7", "begin", "prepare", "commit"]
    env.intents.enqueue_intent.assert_not_awaited()


async def test_ut_d06_replay_outage_failclosed_before_transaction(env):
    from core.entities.metrica_maintenance import PendingParseIntent

    env.client.get_metrica_binding.side_effect = PeerServiceException("yandex", 503, "http")
    with pytest.raises(YandexIntegrationUnavailableError):
        await env.service.replay_intent(PendingParseIntent(uuid4(), env.eq, "auto"))
    assert env.events == []
    env.intents.prepare_admission.assert_not_awaited()


async def test_ut_d06_replay_commit_failure_never_dispatches(env):
    from core.entities.metrica_maintenance import PendingParseIntent

    @asynccontextmanager
    async def fail_commit():
        yield
        raise RuntimeError("commit failed")

    env.service._transaction = SimpleNamespace(begin=fail_commit)
    with pytest.raises(RuntimeError, match="commit failed"):
        await env.service.replay_intent(PendingParseIntent(uuid4(), env.eq, "auto"))
    env.dispatcher.dispatch_account.assert_not_awaited()
