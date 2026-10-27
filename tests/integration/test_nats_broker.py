"""IT-NATS-M1: real JetStream, isolated resources; missing infrastructure is a failure."""

import asyncio
import os
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from uuid import uuid4

import pytest
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, DeliverPolicy, RetentionPolicy, StorageType, StreamConfig

from clients.nats.client import NatsJetstreamClient
from clients.nats.consumers.parse_run import ParseRunConsumer
from clients.nats.handlers.parse_run import PermanentCommandError
from settings import NatsSettings

pytestmark = pytest.mark.infrastructure


@dataclass
class Broker:
    client: NatsJetstreamClient
    settings: NatsSettings
    other_subject: str


@pytest.fixture
async def broker() -> AsyncIterator[Broker]:
    url = os.environ.get("TEST_NATS_URL")
    if not url:
        pytest.fail("TEST_NATS_URL is required; IT-NATS-M1 never skips missing infrastructure")
    token = uuid4().hex
    stream = f"MT4_{token}"
    prefix = f"test.mt4.{token}.commands.parse"
    settings = NatsSettings(
        NATS_SERVERS=url,
        NATS_STREAM_PARSE_COMMANDS=stream,
        NATS_SUBJECT_PARSE_RUN=f"{prefix}.ya_metrica.run",
        NATS_CONSUMER_PARSE_RUN=f"mt4-{token}",
        # Short ack_wait makes non-redelivery assertions bounded and meaningful.
        NATS_CONSUMER_ACK_WAIT_SECONDS=0.2,
        NATS_CONSUMER_MAX_DELIVER=5,
        NATS_SETUP_MAX_ATTEMPTS=1,
    )
    client = NatsJetstreamClient(settings=settings)
    created = False
    try:
        async with asyncio.timeout(10):
            await client.connect()
            # Same StreamConfig as seo-service; only names/subject namespace differ.
            await client.jetstream.add_stream(
                config=StreamConfig(
                    name=stream,
                    subjects=[f"{prefix}.>"],
                    storage=StorageType.FILE,
                    retention=RetentionPolicy.LIMITS,
                    max_age=604800,
                )
            )
            created = True
            await client.setup()
        yield Broker(client, settings, f"{prefix}.demo.run")
    finally:
        try:
            if created:
                # Delete only this fixture's exact UUID-named stream and its consumer.
                assert stream == f"MT4_{token}" and len(token) == 32
                async with asyncio.timeout(5):
                    await client.jetstream.delete_stream(stream)
        finally:
            async with asyncio.timeout(10):
                await client.close()


class OutcomeHandler:
    def __init__(self, outcome: str) -> None:
        self.outcome = outcome
        self.calls = 0

    async def handle(self, *, payload: bytes, headers: Mapping[str, str] | None, subject: str) -> None:
        assert payload == b"command"
        assert headers is not None and headers["Nats-Msg-Id"]
        assert subject.endswith(".ya_metrica.run")
        self.calls += 1
        if self.outcome == "nak":
            raise RuntimeError("transient failure")
        if self.outcome == "term":
            raise PermanentCommandError("invalid command")


async def subscribe(broker: Broker):
    return await broker.client.jetstream.pull_subscribe(
        subject=broker.settings.nats_subject_parse_run,
        durable=broker.settings.nats_consumer_parse_run,
        stream=broker.settings.nats_stream_parse_commands,
    )


async def assert_no_delivery(subscription) -> None:
    # Longer than test ack_wait: a missing ack/term would cause redelivery.
    with pytest.raises(NatsTimeoutError):
        await subscription.fetch(batch=1, timeout=0.6)


async def test_durable_filter_and_explicit_ack(broker: Broker) -> None:
    """Durable survives rebind; unacked delivery returns, acked/foreign delivery does not."""
    js = broker.client.jetstream
    settings = broker.settings
    info = await js.consumer_info(settings.nats_stream_parse_commands, settings.nats_consumer_parse_run)
    assert info.config.durable_name == settings.nats_consumer_parse_run
    assert info.config.filter_subject == settings.nats_subject_parse_run
    assert info.config.ack_policy == AckPolicy.EXPLICIT
    assert info.config.deliver_policy == DeliverPolicy.ALL
    assert info.config.ack_wait == settings.nats_consumer_ack_wait_seconds
    assert info.config.max_deliver == settings.nats_consumer_max_deliver
    stream = await js.stream_info(settings.nats_stream_parse_commands)
    assert stream.config.storage == StorageType.FILE
    assert stream.config.retention == RetentionPolicy.LIMITS
    assert stream.config.max_age == 604800
    subscription = await subscribe(broker)
    try:
        await js.publish(broker.other_subject, b"other provider")
        await assert_no_delivery(subscription)
        ack = await js.publish(settings.nats_subject_parse_run, b"unacked")
        first = (await subscription.fetch(batch=1, timeout=2))[0]
        assert first.metadata.sequence.stream == ack.seq
        assert first.metadata.num_delivered == 1
        # Unsubscribe does not delete durable; ack_wait expiry causes another delivery.
        await subscription.unsubscribe()
        subscription = await subscribe(broker)
        again = (await subscription.fetch(batch=1, timeout=2))[0]
        assert again.metadata.sequence.stream == ack.seq
        assert again.metadata.num_delivered == 2
        await again.ack_sync(timeout=2)
        await assert_no_delivery(subscription)
        info = await js.consumer_info(settings.nats_stream_parse_commands, settings.nats_consumer_parse_run)
        assert info.num_ack_pending == 0
        assert info.num_pending == 0
    finally:
        await subscription.unsubscribe()


@pytest.mark.parametrize("outcome", ["ack", "nak", "term"])
async def test_consumer_ack_nak_max_deliver_and_term(broker: Broker, outcome: str) -> None:
    """Exercise runtime outcome mapping with actual broker messages, not mocked Msg."""
    settings = broker.settings
    js = broker.client.jetstream
    handler = OutcomeHandler(outcome)
    consumer = ParseRunConsumer(client=broker.client, settings=settings, handler=handler)
    subscription = await subscribe(broker)
    try:
        published = await js.publish(
            settings.nats_subject_parse_run, b"command", headers={"Nats-Msg-Id": str(uuid4())}
        )
        attempts = settings.nats_consumer_max_deliver if outcome == "nak" else 1
        for delivery in range(1, attempts + 1):
            message = (await subscription.fetch(batch=1, timeout=2))[0]
            assert message.metadata.sequence.stream == published.seq
            assert message.metadata.num_delivered == delivery
            await consumer._process_message(message)
        assert handler.calls == attempts
        await assert_no_delivery(subscription)
        info = await js.consumer_info(settings.nats_stream_parse_commands, settings.nats_consumer_parse_run)
        assert info.num_pending == 0
        if outcome != "nak":
            assert info.num_ack_pending == 0
        # LIMITS retention keeps the message; lack of redelivery is not stream deletion.
        assert (await js.stream_info(settings.nats_stream_parse_commands)).state.messages == 1
    finally:
        await subscription.unsubscribe()
