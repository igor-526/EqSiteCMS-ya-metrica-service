"""FIX-C02: immutable canonical startup topology, independent of other fix suites."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import nats.errors
import pytest
from nats.js.api import (
    AckPolicy,
    ConsumerConfig,
    DeliverPolicy,
    DiscardPolicy,
    RetentionPolicy,
    StorageType,
    StreamConfig,
)
from nats.js.errors import BadRequestError, NotFoundError, ServiceUnavailableError

from clients.nats.client import MaintenanceTopologyError, NatsJetstreamClient
from settings import NatsSettings


def canonical(name, subjects):
    return StreamConfig(
        name=name,
        subjects=subjects,
        storage=StorageType.FILE,
        retention=RetentionPolicy.LIMITS,
        max_age=0,
        max_msgs=-1,
        max_bytes=-1,
        discard=DiscardPolicy.NEW,
        duplicate_window=120,
    )


def topology(settings):
    s = settings
    results = canonical(
        s.nats_stream_maintenance_results,
        [
            "events.metrica.cleanup.completed",
            "events.metrica.pages.merge.completed",
        ],
    )
    seo = canonical(s.nats_stream_seo_maintenance, ["commands.seo.pages.merge", "events.seo.pages.merge.finalized"])
    if hasattr(s, "nats_stream_yandex_lifecycle"):
        owner = results
        foreign = [canonical(s.nats_stream_yandex_lifecycle, ["events.yandex.metrica.cleanup.requested"]), seo]
        routes = [
            (foreign[0].name, "events.yandex.metrica.cleanup.requested", s.nats_consumer_cleanup_requested),
            (seo.name, "commands.seo.pages.merge", s.nats_consumer_pages_merge),
            (seo.name, "events.seo.pages.merge.finalized", s.nats_consumer_pages_merge_finalized),
        ]
    else:
        owner, foreign = seo, [results]
        routes = [
            (results.name, "events.metrica.pages.merge.completed", s.nats_consumer_merge_completed),
            (results.name, "events.metrica.cleanup.completed", s.nats_consumer_cleanup_completed),
        ]
    return owner, foreign, routes


def fixture_client():
    settings = NatsSettings(_env_file=None, nats_setup_max_attempts=3, nats_setup_backoff_seconds=0)
    client = NatsJetstreamClient(settings=settings)
    owner, foreign, routes = topology(settings)
    streams = {c.name: deepcopy(c) for c in [owner, *foreign]}
    consumers = {}

    async def stream_info(name):
        if name not in streams:
            raise NotFoundError()
        return SimpleNamespace(config=deepcopy(streams[name]))

    async def add_stream(*, config):
        streams[config.name] = deepcopy(config)
        return SimpleNamespace(config=deepcopy(config))

    async def consumer_info(stream, durable):
        if (stream, durable) not in consumers:
            raise NotFoundError()
        return SimpleNamespace(config=deepcopy(consumers[stream, durable]))

    async def add_consumer(*, stream, config):
        consumers[stream, config.durable_name] = deepcopy(config)
        return SimpleNamespace(config=deepcopy(config))

    js = SimpleNamespace(
        stream_info=AsyncMock(side_effect=stream_info),
        add_stream=AsyncMock(side_effect=add_stream),
        consumer_info=AsyncMock(side_effect=consumer_info),
        add_consumer=AsyncMock(side_effect=add_consumer),
    )
    client._connection = SimpleNamespace(is_connected=True)
    client._jetstream = js
    return client, js, streams, consumers, owner, foreign, routes


async def test_valid_owner_creation_and_existing_durables_are_not_updated():
    client, js, streams, consumers, owner, foreign, routes = fixture_client()
    del streams[owner.name]
    await client.setup_maintenance()
    js.add_stream.assert_awaited_once()
    assert js.add_stream.await_args.kwargs["config"] == owner
    assert js.add_consumer.await_count == len(routes)
    for stream, subject, durable in routes:
        c = consumers[stream, durable]
        assert (c.filter_subject, c.deliver_policy, c.ack_policy, c.ack_wait, c.max_deliver) == (
            subject,
            DeliverPolicy.ALL,
            AckPolicy.EXPLICIT,
            30,
            -1,
        )
    await client.setup_maintenance()
    assert js.add_consumer.await_count == len(routes)
    assert js.add_stream.await_count == 1
    assert {call.args[0] for call in js.stream_info.await_args_list} == {owner.name, *(c.name for c in foreign)}


@pytest.mark.parametrize("index", [0, 1, 2])
@pytest.mark.parametrize(
    "field,value",
    [
        ("max_age", 60),
        ("discard", DiscardPolicy.OLD),
        ("storage", StorageType.MEMORY),
        ("retention", RetentionPolicy.INTEREST),
        ("max_msgs", 100),
        ("max_bytes", 10000),
        ("duplicate_window", 60),
        ("subjects", ["wrong.subject"]),
        ("max_msgs_per_subject", 1),
        ("max_msg_size", 1024),
        ("no_ack", True),
        ("sealed", True),
        ("allow_msg_ttl", True),
    ],
)
async def test_every_stream_rejects_incompatible_config_before_consumer_start(index, field, value):
    client, js, streams, _, owner, foreign, _ = fixture_client()
    configs = [owner, *foreign]
    target = configs[index % len(configs)]
    setattr(streams[target.name], field, value)
    with pytest.raises(MaintenanceTopologyError, match=field):
        await client.setup_maintenance()
    js.add_stream.assert_not_awaited()
    js.add_consumer.assert_not_awaited()
    assert js.stream_info.await_count <= len(configs)  # permanent mismatch is never retried


@pytest.mark.parametrize("route_index", [0, 1, 2])
@pytest.mark.parametrize(
    "field,value",
    [
        ("filter_subject", "wrong.subject"),
        ("ack_policy", AckPolicy.NONE),
        ("ack_wait", 60),
        ("max_deliver", 5),
        ("deliver_policy", DeliverPolicy.NEW),
        ("deliver_subject", "push.subject"),
        ("filter_subjects", ["wrong.subject"]),
        ("backoff", [30, 60]),
        ("headers_only", True),
    ],
)
async def test_existing_durable_is_validated_not_silently_updated(route_index, field, value):
    client, js, _, consumers, _, _, routes = fixture_client()
    await client.setup_maintenance()
    stream, _, durable = routes[route_index % len(routes)]
    setattr(consumers[stream, durable], field, value)
    js.add_consumer.reset_mock()
    js.stream_info.reset_mock()
    with pytest.raises(MaintenanceTopologyError, match=field):
        await client.setup_maintenance()
    js.add_consumer.assert_not_awaited()
    assert js.stream_info.await_count <= 3


@pytest.mark.parametrize(
    "error",
    [NotFoundError(), ServiceUnavailableError(), nats.errors.TimeoutError(), nats.errors.ConnectionClosedError()],
)
async def test_transient_owner_lookup_retry_and_exhaustion(error):
    client, js, _, _, owner, foreign, _ = fixture_client()
    lookup = js.stream_info.side_effect
    failures = 0

    async def fail_foreign(name):
        nonlocal failures
        if name == foreign[0].name and failures < 2:
            failures += 1
            raise error
        return await lookup(name)

    js.stream_info.side_effect = fail_foreign
    await client.setup_maintenance()
    assert failures == 2
    js.add_stream.assert_not_awaited()
    js.add_consumer.reset_mock()

    async def absent_foreign(name):
        if name == foreign[0].name:
            raise error
        return await lookup(name)

    js.stream_info.side_effect = absent_foreign
    js.stream_info.reset_mock()
    with pytest.raises(type(error)):
        await client.setup_maintenance()
    assert js.stream_info.await_count == 6
    js.add_stream.assert_not_awaited()
    js.add_consumer.assert_not_awaited()


async def test_consumer_bad_request_is_permanent_and_returned_config_is_validated():
    client, js, *_ = fixture_client()
    js.add_consumer.side_effect = BadRequestError()
    with pytest.raises(BadRequestError):
        await client.setup_maintenance()
    assert js.add_consumer.await_count == 1
    client, js, *_ = fixture_client()
    js.add_consumer.side_effect = None
    js.add_consumer.return_value = SimpleNamespace(config=ConsumerConfig(ack_policy=AckPolicy.NONE))
    with pytest.raises(MaintenanceTopologyError):
        await client.setup_maintenance()
    assert js.add_consumer.await_count == 1


def test_topology_error_uses_existing_supervisor_permanent_error_class():
    assert isinstance(MaintenanceTopologyError("incompatible max_age"), BadRequestError)
