import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from nats.js.errors import BadRequestError
from sqlalchemy.dialects import postgresql

import main
from core.entities.metrica_maintenance import WorkKind
from core.services.maintenance_dispatch_service import MaintenanceDispatchService
from repositories.metrica_maintenance_dispatch_repository import MaintenanceDispatchRepository
from workers import maintenance_runtime as runtime
from workers.dispatcher import CeleryMaintenanceDispatcher
from workers.tasks import metrica_maintenance as tasks


@pytest.fixture
def graph(monkeypatch):
    active = []

    @asynccontextmanager
    async def session():
        active.append(True)
        try:
            yield object()
        finally:
            active.pop()

    worker = SimpleNamespace(tick=AsyncMock(return_value="progress"))
    dispatch = SimpleNamespace(dispatch_ready=AsyncMock())
    replay = SimpleNamespace(replay_ready=AsyncMock())
    service = SimpleNamespace(
        cleanup_worker_service=lambda: worker,
        merge_worker_service=lambda: None,
        maintenance_dispatch_service=lambda: dispatch,
        parse_intent_replay_service=lambda: replay,
    )
    factory = MagicMock(return_value=service)
    monkeypatch.setattr(tasks, "worker_session", session)
    monkeypatch.setattr(tasks, "create_parse_services", factory)
    monkeypatch.setattr(tasks.settings, "maintenance_runtime_enabled", True)
    return worker, dispatch, replay, service, factory, active


def test_tick_is_bounded_and_new_owner_per_redelivery(graph):
    worker, _, _, _, factory, active = graph
    tenant = str(uuid4())
    assert tasks.cleanup_tick.run(tenant) == "progress"
    assert tasks.cleanup_tick.run(tenant) == "progress"
    assert worker.tick.await_count == factory.call_count == 2
    calls = worker.tick.await_args_list
    assert calls[0].args == (UUID(tenant),)
    assert calls[0].kwargs["owner"] != calls[1].kwargs["owner"]
    assert not active


def test_merge_not_stub_success_but_supports_real_handler(graph):
    worker, _, _, service, _, _ = graph
    tenant = str(uuid4())
    assert tasks.merge_tick.run(tenant) == "unavailable"
    worker.tick.assert_not_awaited()
    service.merge_worker_service = lambda: worker
    assert tasks.merge_tick.run(tenant) == "progress"
    worker.tick.assert_awaited_once()


def test_periodic_dispatch_and_deferred_replay_limits(graph):
    _, dispatch, replay, _, _, active = graph
    tasks.dispatch_maintenance.run()
    tasks.replay_intents.run()
    dispatch.dispatch_ready.assert_awaited_once_with(limit=100)
    replay.replay_ready.assert_awaited_once_with(limit=100)
    assert not active


def test_runtime_off_does_not_create_sessions_or_graph(graph, monkeypatch):
    _, _, _, _, factory, active = graph
    monkeypatch.setattr(tasks.settings, "maintenance_runtime_enabled", False)
    assert tasks.cleanup_tick.run(str(uuid4())) == "disabled"
    assert tasks.merge_tick.run(str(uuid4())) == "disabled"
    tasks.dispatch_maintenance.run()
    tasks.replay_intents.run()
    factory.assert_not_called()
    assert not active


def test_worker_error_is_not_completed_and_session_closes(graph):
    worker, _, _, _, _, active = graph
    worker.tick.side_effect = ConnectionError("secret")
    with pytest.raises(ConnectionError):
        tasks.cleanup_tick.run(str(uuid4()))
    assert not active


@pytest.mark.parametrize("kind", [WorkKind.CLEANUP, WorkKind.MERGE])
async def test_queue_bindings(kind):
    app, tenant = MagicMock(), uuid4()
    await CeleryMaintenanceDispatcher(app).dispatch_maintenance(tenant, kind=kind)
    app.send_task.assert_called_once_with(f"metrica.{kind}_tick", args=[str(tenant)], queue=f"ya_metrica_{kind}")


@pytest.mark.parametrize("merge_available", [False, True])
async def test_durable_rediscovery_after_lost_signal_and_commit_before_broker(merge_available, caplog):
    active = []

    @asynccontextmanager
    async def begin():
        active.append(True)
        try:
            yield
        finally:
            active.pop()

    tenants = [uuid4(), uuid4()]
    repository = SimpleNamespace(ready_tenants=AsyncMock(return_value=tenants))
    dispatcher = SimpleNamespace(dispatch_maintenance=AsyncMock())

    async def send(tenant, *, kind):
        assert not active
        if tenant == tenants[0]:
            raise ConnectionError("private-secret")

    dispatcher.dispatch_maintenance.side_effect = send
    service = MaintenanceDispatchService(
        repository=repository,
        transaction=SimpleNamespace(begin=begin),
        dispatcher=dispatcher,
        merge_available=merge_available,
    )
    assert await service.dispatch_ready() == 1 + int(merge_available)
    assert await service.dispatch_ready() == 1 + int(merge_available)
    assert repository.ready_tenants.await_count == 2 * (1 + int(merge_available))
    assert "private-secret" not in caplog.text
    assert repository.ready_tenants.await_args_list[0].kwargs == {"kind": WorkKind.CLEANUP, "limit": 100}


@pytest.mark.parametrize("kind", [WorkKind.CLEANUP, WorkKind.MERGE])
async def test_discovery_sql_bounded_respects_lease_priority_blocked_head(kind):
    session = SimpleNamespace(scalars=AsyncMock(return_value=MagicMock()), execute=AsyncMock())
    session.scalars.return_value.all.return_value = [uuid4()]
    repository = MaintenanceDispatchRepository(session)
    assert len(await repository.ready_tenants(kind=kind, limit=100)) == 1
    statement = session.scalars.await_args.args[0]
    sql = str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    assert "LIMIT 100" in sql
    assert "lease_expires_at <= clock_timestamp()" in sql
    assert "next_attempt_at <= clock_timestamp()" in sql
    assert "NOT (EXISTS" in sql
    assert "earlier.status != 'completed'" in sql
    assert "earlier.kind = 'cleanup'" in sql
    assert "OFFSET" not in sql and "FOR UPDATE" not in sql


async def test_supervisor_retry_partial_startup_then_cancel(monkeypatch, caplog):
    client = AsyncMock()
    consumers = [AsyncMock(), AsyncMock(), AsyncMock()]
    consumers[1].start.side_effect = [ConnectionError("private-secret"), None]
    started = asyncio.Event()

    async def wait():
        started.set()
        await asyncio.Event().wait()

    relay = AsyncMock(side_effect=wait)
    for consumer in consumers:
        consumer.wait.side_effect = wait
    original_sleep = asyncio.sleep
    delays = []

    async def sleep(delay):
        delays.append(delay)
        await original_sleep(0)

    monkeypatch.setattr(runtime.asyncio, "sleep", sleep)
    supervisor = asyncio.create_task(runtime.supervise_maintenance(client, consumers, relay))
    await asyncio.wait_for(started.wait(), timeout=2)
    supervisor.cancel()
    with pytest.raises(asyncio.CancelledError):
        await supervisor
    assert delays == [1]
    assert client.setup_maintenance.await_count == 2
    assert client.close.await_count == 2
    assert all(consumer.stop.await_count == 2 for consumer in consumers)
    assert "private-secret" not in caplog.text


@pytest.mark.parametrize("failed_child", ["consumer", "relay"])
async def test_child_failure_restarts_entire_group(failed_child, monkeypatch):
    client, consumer = AsyncMock(), AsyncMock()
    relay = AsyncMock()
    starts = 0

    async def wait():
        await asyncio.Event().wait()

    async def fail_then_stop():
        nonlocal starts
        starts += 1
        if starts == 1:
            raise RuntimeError()
        raise BadRequestError()

    consumer.wait.side_effect = fail_then_stop if failed_child == "consumer" else wait
    relay.side_effect = fail_then_stop if failed_child == "relay" else wait
    monkeypatch.setattr(runtime.asyncio, "sleep", AsyncMock())
    await runtime.supervise_maintenance(client, [consumer], relay)
    assert consumer.start.await_count == client.close.await_count == 2


async def test_bad_config_stops_without_starting_relay():
    client, consumer, relay = AsyncMock(), AsyncMock(), AsyncMock()
    client.setup_maintenance.side_effect = BadRequestError()
    await runtime.supervise_maintenance(client, [consumer], relay)
    consumer.start.assert_not_awaited()
    consumer.stop.assert_awaited_once()
    client.close.assert_awaited_once()
    relay.assert_not_called()


async def test_runtime_off_lifespan_is_lazy(monkeypatch):
    monkeypatch.setattr(main.settings, "maintenance_runtime_enabled", False)
    factory = MagicMock(side_effect=AssertionError("must stay lazy"))
    monkeypatch.setattr(main, "maintenance_nats_client_provider", factory)
    monkeypatch.setattr(main, "supervise_nats", AsyncMock())
    monkeypatch.setattr(main, "nats_client_provider", MagicMock())
    monkeypatch.setattr(main, "parse_run_consumer_provider", MagicMock())
    monkeypatch.setattr(main, "close_database", AsyncMock())
    async with main.lifespan(main.app):
        await asyncio.sleep(0)
    factory.assert_not_called()


async def test_runtime_enabled_lifespan_starts_and_stops_maintenance_group(monkeypatch):
    started, stopped = asyncio.Event(), asyncio.Event()
    client = MagicMock()
    consumers = [MagicMock(), MagicMock(), MagicMock()]

    async def supervise(actual_client, actual_consumers, relay):
        assert actual_client is client and actual_consumers is consumers
        assert callable(relay)
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    monkeypatch.setattr(main.settings, "maintenance_runtime_enabled", True)
    monkeypatch.setattr(main, "maintenance_nats_client_provider", lambda: client)
    monkeypatch.setattr(main, "create_maintenance_consumers", lambda *_: consumers)
    monkeypatch.setattr(main, "supervise_maintenance", supervise)
    monkeypatch.setattr(main, "supervise_nats", AsyncMock())
    monkeypatch.setattr(main, "nats_client_provider", MagicMock())
    monkeypatch.setattr(main, "parse_run_consumer_provider", MagicMock())
    monkeypatch.setattr(main, "close_database", AsyncMock())
    async with main.lifespan(main.app):
        await asyncio.wait_for(started.wait(), timeout=2)
    assert stopped.is_set()


async def test_relay_loop_owns_fresh_sessions_claim_owners_and_retries_via_supervisor(monkeypatch):
    sessions, closed, graphs = [], [], []

    @asynccontextmanager
    async def session_scope():
        session = object()
        sessions.append(session)
        try:
            yield session
        finally:
            closed.append(session)

    def factory(session):
        relay = SimpleNamespace(relay_once=AsyncMock(return_value=False))
        graph = SimpleNamespace(
            maintenance_result_publisher=MagicMock(), maintenance_result_relay=lambda: relay, relay=relay
        )
        graphs.append(graph)
        return graph

    calls = 0

    async def sleep(delay):
        nonlocal calls
        assert delay == 3
        assert closed == sessions
        calls += 1
        if calls == 2:
            raise asyncio.CancelledError()

    publisher = MagicMock()
    monkeypatch.setattr(runtime, "worker_session", session_scope)
    monkeypatch.setattr(runtime, "create_parse_services", factory)
    monkeypatch.setattr(runtime.asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await runtime.relay_results(publisher, interval=3)
    assert len(graphs) == len(sessions) == 2
    owners = []
    for graph in graphs:
        graph.maintenance_result_publisher.override.assert_called_once_with(publisher)
        owners.append(graph.relay.relay_once.await_args.kwargs["owner"])
    assert owners[0] != owners[1]


def test_actual_di_default_merge_gate_and_independent_transport():
    from containers.application import (
        create_parse_services,
        maintenance_nats_client_provider,
        nats_client_provider,
    )

    graph = create_parse_services(MagicMock())
    graph.celery_app.override(MagicMock())
    assert graph.merge_worker_service()._transaction is graph.repositories_container.session()
    assert graph.maintenance_dispatch_service()._merge_available is True
    assert graph.maintenance_dispatch_service()._repository._session is graph.repositories_container.session()
    assert maintenance_nats_client_provider() is not nats_client_provider()


@pytest.mark.parametrize("limit", [0, 1001])
async def test_dispatch_rejects_unbounded_discovery(limit):
    service = MaintenanceDispatchService(repository=MagicMock(), transaction=MagicMock(), dispatcher=MagicMock())
    with pytest.raises(ValueError):
        await service.dispatch_ready(limit=limit)
