import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from nats.errors import TimeoutError

from clients.nats.consumers.parse_run import ParseRunConsumer
from clients.nats.handlers.parse_run import PermanentCommandError
from settings import NatsSettings


def make_consumer():
    settings = NatsSettings(_env_file=None)
    client = MagicMock()
    client.jetstream.pull_subscribe = AsyncMock()
    handler = AsyncMock()
    consumer = ParseRunConsumer(client=client, settings=settings, handler=handler)
    return consumer, client, handler


@pytest.mark.parametrize("error,action", [(None, "ack"), (PermanentCommandError(), "term"), (RuntimeError(), "nak")])
async def test_delivery_outcome_and_actual_subject(error, action):
    consumer, _, handler = make_consumer()
    handler.handle.side_effect = error
    message = MagicMock(data=b"command", headers={"X-Equestrian-Id": "not-a-credential"}, subject="actual.subject")
    message.ack, message.term, message.nak = AsyncMock(), AsyncMock(), AsyncMock()
    await consumer._process_message(message)
    handler.handle.assert_awaited_once_with(payload=b"command", headers=message.headers, subject="actual.subject")
    for method in ("ack", "nak", "term"):
        assert getattr(message, method).await_count == int(method == action)


async def test_cancellation_never_acknowledges_delivery():
    consumer, _, handler = make_consumer()
    handler.handle.side_effect = asyncio.CancelledError()
    message = MagicMock(data=b"command", headers=None, subject="subject")
    message.ack, message.term, message.nak = AsyncMock(), AsyncMock(), AsyncMock()
    with pytest.raises(asyncio.CancelledError):
        await consumer._process_message(message)
    for method in ("ack", "term", "nak"):
        getattr(message, method).assert_not_awaited()


async def test_pull_timeout_continues_start_idempotent_stop_cancels():
    consumer, client, _ = make_consumer()
    subscription = client.jetstream.pull_subscribe.return_value
    fetched = asyncio.Event()

    async def fetch(**kwargs):
        assert kwargs == {"batch": 1, "timeout": 5}
        if not fetched.is_set():
            fetched.set()
            raise TimeoutError()
        await asyncio.Event().wait()

    subscription.fetch.side_effect = fetch
    await consumer.start()
    await consumer.start()
    client.jetstream.pull_subscribe.assert_awaited_once_with(
        subject="commands.parse.ya_metrica.run", durable="ya-metrica-parse-run", stream="PARSE_COMMANDS"
    )
    await fetched.wait()
    await asyncio.sleep(0)
    assert subscription.fetch.await_count == 2
    assert consumer.is_running
    await consumer.stop()
    await consumer.stop()
    assert not consumer.is_running
    subscription.unsubscribe.assert_awaited_once()


async def test_fetch_failure_propagates_to_supervisor():
    consumer, client, _ = make_consumer()
    client.jetstream.pull_subscribe.return_value.fetch.side_effect = RuntimeError("transport")
    await consumer.start()
    with pytest.raises(RuntimeError):
        await consumer.wait()
    await consumer.stop()
