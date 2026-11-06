"""IT-NATS-M1: real JetStream, isolated resources; missing infrastructure is a failure."""

import asyncio
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from uuid import uuid4

import pytest
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import (
    AckPolicy,
    ConsumerConfig,
    DeliverPolicy,
    DiscardPolicy,
    RetentionPolicy,
    StorageType,
)

from clients.nats.client import MaintenanceTopologyError, NatsJetstreamClient, _maintenance_stream
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
async def broker(qa_infra_context) -> AsyncIterator[Broker]:
    """NATS-01: root lifecycle belongs exclusively to infra runner, even on failure."""
    context = qa_infra_context
    resource = context["resource"]
    stream, prefix = resource["stream"], resource["subject"]
    settings = NatsSettings(
        NATS_SERVERS=context["url"],
        NATS_STREAM_PARSE_COMMANDS=stream,
        NATS_SUBJECT_PARSE_RUN=f"{prefix}.ya_metrica.run",
        NATS_CONSUMER_PARSE_RUN=resource["durable"],
        NATS_STREAM_MAINTENANCE_RESULTS=stream,
        NATS_CONSUMER_ACK_WAIT_SECONDS=0.2,
        NATS_CONSUMER_MAX_DELIVER=5,
        NATS_SETUP_MAX_ATTEMPTS=1,
    )
    client = NatsJetstreamClient(settings=settings)
    try:
        async with asyncio.timeout(10):
            await client.connect()
            js = client.jetstream
            # Reset runner probe or preceding node's children; never another durable/root.
            from nats.js.errors import NotFoundError

            try:
                await js.delete_consumer(stream, resource["durable"])
            except NotFoundError:
                pass
            await js.purge_stream(stream)
            config = (await js.stream_info(stream)).config
            assert config.subjects == [prefix + ".>"]
            config.storage = StorageType.FILE
            config.retention = RetentionPolicy.LIMITS
            config.max_age = 604800
            config.duplicate_window = 120
            config.discard = DiscardPolicy.OLD
            await js.update_stream(config=config)
            await client.setup()
        yield Broker(client, settings, f"{prefix}.demo.run")
    finally:
        # No stream deletion here: confirmed root ownership/cleanup is runner-only.
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


def maintenance_config(broker):
    """The production startup validator, with only owned stream/subject renamed."""
    return _maintenance_stream(
        broker.settings.nats_stream_parse_commands,
        [broker.settings.nats_subject_parse_run.rsplit(".", 2)[0] + ".>"],
    )


def maintenance_consumer(broker):
    return ConsumerConfig(
        durable_name=broker.settings.nats_consumer_parse_run,
        filter_subject=broker.settings.nats_subject_parse_run,
        deliver_policy=DeliverPolicy.ALL,
        ack_policy=AckPolicy.EXPLICIT,
        ack_wait=30,
        max_deliver=-1,
    )


async def test_existing_compatible_maintenance_topology_reused(broker):
    js = broker.client.jetstream
    expected = maintenance_config(broker)
    config = (await js.stream_info(expected.name)).config
    config.max_age, config.discard = 0, DiscardPolicy.NEW
    await js.update_stream(config=config)
    await js.delete_consumer(expected.name, broker.settings.nats_consumer_parse_run)
    consumer = maintenance_consumer(broker)
    await js.add_consumer(expected.name, config=consumer)
    before_stream = (await js.stream_info(expected.name)).config
    before_consumer = (await js.consumer_info(expected.name, consumer.durable_name)).config
    async with asyncio.timeout(5):
        for _ in range(2):
            await broker.client._maintenance_stream(expected)
            await broker.client._maintenance_consumer(expected.name, consumer)
    assert (await js.stream_info(expected.name)).config == before_stream
    assert (await js.consumer_info(expected.name, consumer.durable_name)).config == before_consumer


@pytest.mark.parametrize("field", ["max_age", "discard"])
async def test_existing_incompatible_stream_not_repaired(broker, field):
    js = broker.client.jetstream
    expected = maintenance_config(broker)
    config = (await js.stream_info(expected.name)).config
    config.max_age = 300 if field == "max_age" else 0
    config.discard = DiscardPolicy.OLD if field == "discard" else DiscardPolicy.NEW
    await js.update_stream(config=config)
    before = (await js.stream_info(expected.name)).config
    async with asyncio.timeout(5):
        with pytest.raises(MaintenanceTopologyError, match=field):
            await broker.client._maintenance_stream(expected)
    assert (await js.stream_info(expected.name)).config == before


@pytest.mark.parametrize("field", ["filter_subject", "ack_wait"])
async def test_existing_incompatible_durable_not_repaired(broker, field):
    js = broker.client.jetstream
    stream = broker.settings.nats_stream_parse_commands
    expected = maintenance_consumer(broker)
    await js.delete_consumer(stream, expected.durable_name)
    actual = maintenance_consumer(broker)
    setattr(actual, field, broker.other_subject if field == "filter_subject" else 60)
    await js.add_consumer(stream, config=actual)
    before = (await js.consumer_info(stream, expected.durable_name)).config
    async with asyncio.timeout(5):
        with pytest.raises(MaintenanceTopologyError, match=field):
            await broker.client._maintenance_consumer(stream, expected)
    assert (await js.consumer_info(stream, expected.durable_name)).config == before


async def test_canonical_result_serializer_headers_replay_and_duplicate(broker):
    """Real publisher -> real broker: namespace adapter changes subject ONLY.

    Production serializer bytes and Nats-Msg-Id pass untouched; this is not paired
    SEO handler evidence (WIRE-02 owns that separate property).
    """
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from clients.nats.maintenance_result_publisher import MaintenanceResultPublisher
    from core.entities.metrica_maintenance import MaintenanceResultClaim
    from core.schemas.messaging.maintenance import CLEANUP_COMPLETED, CleanupCompleted, maintenance_message_id

    operation = uuid4()
    message_id = maintenance_message_id(CLEANUP_COMPLETED, operation, "completed")
    payload = CleanupCompleted(
        schema_version=1,
        message_id=message_id,
        operation_id=operation,
        equestrian_id=uuid4(),
        occurred_at=datetime.now(UTC),
        cleanup_seq=1,
        status="completed",
    ).model_dump(mode="json")
    claim = MaintenanceResultClaim(message_id, CLEANUP_COMPLETED, payload, uuid4(), 1, 0)

    class OwnedTransport:
        async def publish(self, subject, body, *, headers, timeout):  # noqa: ASYNC109 - production API adapter
            assert subject == CLEANUP_COMPLETED
            return await broker.client.jetstream.publish(
                broker.settings.nats_subject_parse_run, body, headers=headers, timeout=timeout
            )

    publisher = MaintenanceResultPublisher(
        client=SimpleNamespace(jetstream=OwnedTransport()), settings=broker.settings
    )
    subscription = await subscribe(broker)
    try:
        assert await publisher.publish_result(claim) is False
        first = (await subscription.fetch(batch=1, timeout=2))[0]
        assert CleanupCompleted.model_validate_json(first.data).model_dump(mode="json") == payload
        assert first.headers["Nats-Msg-Id"] == str(message_id)
        assert await publisher.publish_result(claim) is True
        await subscription.unsubscribe()
        subscription = await subscribe(broker)
        replay = (await subscription.fetch(batch=1, timeout=2))[0]
        assert replay.data == first.data and replay.headers == first.headers
        assert replay.metadata.sequence.stream == first.metadata.sequence.stream
        assert replay.metadata.num_delivered == 2
        await replay.ack_sync(timeout=2)
        await assert_no_delivery(subscription)
        assert (
            await broker.client.jetstream.stream_info(broker.settings.nats_stream_parse_commands)
        ).state.messages == 1
    finally:
        await subscription.unsubscribe()
