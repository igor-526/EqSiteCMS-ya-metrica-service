import logging
from unittest.mock import AsyncMock, MagicMock, patch

import nats.errors
import pytest
from nats.js.api import AckPolicy, DeliverPolicy
from nats.js.errors import BadRequestError, NotFoundError

from clients.nats.client import NatsJetstreamClient
from clients.nats.lifecycle import NatsConnectionErrorPolicy
from settings import NatsSettings


def client_with_js(**kwargs):
    settings = NatsSettings(_env_file=None, nats_setup_backoff_seconds=0, **kwargs)
    client = NatsJetstreamClient(settings=settings)
    client._jetstream = AsyncMock()
    return client


async def test_setup_owned_durable_without_stream_creation():
    client = client_with_js()
    await client.setup()
    js = client.jetstream
    js.add_stream.assert_not_called()
    call = js.add_consumer.await_args.kwargs
    assert call["stream"] == "PARSE_COMMANDS"
    config = call["config"]
    assert config.durable_name == "ya-metrica-parse-run"
    assert config.filter_subject == "commands.parse.ya_metrica.run"
    assert config.deliver_policy == DeliverPolicy.ALL
    assert config.ack_policy == AckPolicy.EXPLICIT
    assert (config.ack_wait, config.max_deliver) == (30, 5)


async def test_retry_not_found_then_success():
    client = client_with_js(nats_setup_max_attempts=3)
    client.jetstream.add_consumer.side_effect = [NotFoundError(), NotFoundError(), None]
    with patch("clients.nats.client.asyncio.sleep", new_callable=AsyncMock) as sleep:
        await client.setup_consumers()
    assert client.jetstream.add_consumer.await_count == 3
    assert sleep.await_count == 2


@pytest.mark.parametrize("error,attempts", [(NotFoundError(), 3), (BadRequestError(), 1)])
async def test_bounded_retry_only_not_found(error, attempts):
    client = client_with_js(nats_setup_max_attempts=3)
    client.jetstream.add_consumer.side_effect = error
    with pytest.raises(type(error)):
        await client.setup_consumers()
    assert client.jetstream.add_consumer.await_count == attempts


async def test_connect_close_and_reconnect_safe_drain():
    settings = NatsSettings(_env_file=None)
    client = NatsJetstreamClient(settings=settings)
    connection = MagicMock(is_connected=True, is_closed=False)
    connection.connect = AsyncMock()
    connection.drain = AsyncMock(side_effect=nats.errors.ConnectionReconnectingError())
    connection.close = AsyncMock()
    with patch("clients.nats.client.NATS", return_value=connection):
        await client.connect()
        await client.connect()
    connection.connect.assert_awaited_once()
    assert connection.connect.await_args.kwargs["servers"] == ["nats://eqsitecms-nats:4222"]
    assert client.jetstream is connection.jetstream.return_value
    await client.close()
    connection.close.assert_awaited_once()
    assert not client.is_connected
    await client.close()
    with pytest.raises(RuntimeError):
        _ = client.jetstream


async def test_failed_connect_closes_partial_connection():
    client = NatsJetstreamClient(settings=NatsSettings(_env_file=None))
    connection = MagicMock(is_closed=False)
    connection.connect = AsyncMock(side_effect=nats.errors.NoServersError())
    connection.drain = AsyncMock(side_effect=nats.errors.ConnectionClosedError())
    connection.close = AsyncMock()
    with patch("clients.nats.client.NATS", return_value=connection):
        with pytest.raises(nats.errors.NoServersError):
            await client.connect()
    await client.close()
    connection.close.assert_awaited_once()


async def test_error_policy_one_escalation_per_incident_without_secrets(caplog):
    policy = NatsConnectionErrorPolicy(service_name="metrica", report_after_attempts=2)
    with caplog.at_level(logging.INFO):
        for _ in range(6):
            await policy.on_error(RuntimeError("secret-token nats://password@host"))
        assert sum(record.levelno == logging.ERROR for record in caplog.records) == 1
        await policy.on_disconnected()
        await policy.on_closed()
        assert policy.escalated
        await policy.on_reconnected()
        assert not policy.escalated
        assert policy.consecutive_failures == 0
        for _ in range(3):
            await policy.on_error(RuntimeError("secret-token"))
    assert sum(record.levelno == logging.ERROR for record in caplog.records) == 2
    assert "secret-token" not in caplog.text
    assert "password" not in caplog.text


async def test_setup_retry_cancel_interrupts_wait():
    import asyncio

    client = client_with_js(nats_setup_max_attempts=3)
    client.jetstream.add_consumer.side_effect = NotFoundError()
    with patch("clients.nats.client.asyncio.sleep", side_effect=asyncio.CancelledError()):
        with pytest.raises(asyncio.CancelledError):
            await client.setup_consumers()
    assert client.jetstream.add_consumer.await_count == 1


def test_sentry_ignores_sdk_logger_even_when_disabled(monkeypatch):
    from utils import configure_sentry as module

    ignore, initialize = MagicMock(), MagicMock()
    monkeypatch.setattr(module, "ignore_logger", ignore)
    monkeypatch.setattr(module.sentry_sdk, "init", initialize)
    monkeypatch.setattr(module, "settings", MagicMock(sentry_enabled=False))
    module.configure_sentry()
    ignore.assert_called_once_with("nats.aio.client")
    initialize.assert_not_called()
