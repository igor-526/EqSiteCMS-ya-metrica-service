import asyncio
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient
from nats.js.errors import BadRequestError

import main
from clients.nats.lifecycle import NatsConnectionErrorPolicy


def fake_transport():
    client, consumer = AsyncMock(), AsyncMock()
    client.error_policy = NatsConnectionErrorPolicy(service_name="test", report_after_attempts=1)
    return client, consumer


async def test_lifespan_nats_down_health_and_shutdown(monkeypatch):
    client, consumer = fake_transport()
    client.connect.side_effect = OSError("private-url")
    monkeypatch.setattr(main, "nats_client_provider", lambda: client)
    monkeypatch.setattr(main, "parse_run_consumer_provider", lambda: consumer)
    close_db = AsyncMock()
    monkeypatch.setattr(main, "close_database", close_db)
    async with main.lifespan(main.app):
        await asyncio.sleep(0)
        async with AsyncClient(transport=ASGITransport(app=main.app), base_url="http://test") as http:
            response = await http.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
    client.connect.assert_awaited_once()
    consumer.start.assert_not_awaited()
    consumer.stop.assert_awaited_once()
    client.close.assert_awaited_once()
    close_db.assert_awaited_once()


async def test_supervisor_backoff_recovers_then_shutdown(monkeypatch):
    client, consumer = fake_transport()
    client.connect.side_effect = [OSError(), OSError(), None]
    started = asyncio.Event()
    waits = []
    real_sleep = asyncio.sleep

    async def sleep(delay):
        waits.append(delay)
        await real_sleep(0)

    async def wait():
        started.set()
        await asyncio.Event().wait()

    consumer.wait.side_effect = wait
    monkeypatch.setattr(main.asyncio, "sleep", sleep)
    task = asyncio.create_task(main.supervise_nats(client, consumer))
    await started.wait()
    assert waits == [1, 2]
    assert client.connect.await_count == 3
    client.setup.assert_awaited_once()
    consumer.start.assert_awaited_once()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert consumer.stop.await_count == client.close.await_count == 3


async def test_bad_configuration_is_not_retried():
    client, consumer = fake_transport()
    client.setup.side_effect = BadRequestError()
    await main.supervise_nats(client, consumer)
    client.connect.assert_awaited_once()
    consumer.start.assert_not_awaited()
    client.close.assert_awaited_once()


async def test_running_consumer_failure_restarts_transport(monkeypatch):
    client, consumer = fake_transport()
    consumer.wait.side_effect = [RuntimeError(), asyncio.CancelledError()]
    sleep = AsyncMock()
    monkeypatch.setattr(main.asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await main.supervise_nats(client, consumer)
    assert consumer.start.await_count == 2
    assert client.close.await_count == consumer.stop.await_count == 2
    sleep.assert_awaited_once_with(1.0)


async def test_shutdown_during_setup_closes_transport():
    client, consumer = fake_transport()
    setting_up = asyncio.Event()

    async def setup():
        setting_up.set()
        await asyncio.Event().wait()

    client.setup.side_effect = setup
    task = asyncio.create_task(main.supervise_nats(client, consumer))
    await setting_up.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    consumer.start.assert_not_awaited()
    consumer.stop.assert_awaited_once()
    client.close.assert_awaited_once()


async def test_supervisor_backoff_is_capped(monkeypatch):
    client, consumer = fake_transport()
    client.connect.side_effect = OSError()
    delays = []

    async def sleep(delay):
        delays.append(delay)
        if len(delays) == 8:
            raise asyncio.CancelledError()

    monkeypatch.setattr(main.asyncio, "sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await main.supervise_nats(client, consumer)
    assert delays == [1, 2, 4, 8, 16, 30, 30, 30]
    assert client.close.await_count == 8
