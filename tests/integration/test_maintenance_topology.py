"""FIX-C02 real JetStream: unique isolated streams, immutable startup validation."""

import asyncio
import os
from contextlib import asynccontextmanager
from copy import deepcopy
from uuid import uuid4

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
from nats.js.errors import NotFoundError

from clients.nats.client import MaintenanceTopologyError, NatsJetstreamClient
from settings import NatsSettings

pytestmark = pytest.mark.infrastructure


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


@asynccontextmanager
async def isolated_topology():
    url = os.environ.get("TEST_NATS_URL")
    if not url:
        pytest.fail("TEST_NATS_URL is required: absence is not acceptance")
    prefix = "FC02_" + uuid4().hex
    values = dict(
        nats_servers_raw=url,
        nats_setup_max_attempts=2,
        nats_setup_backoff_seconds=0,
        nats_stream_maintenance_results=prefix + "_RESULTS",
        nats_stream_seo_maintenance=prefix + "_SEO",
    )
    if "nats_stream_yandex_lifecycle" in NatsSettings.model_fields:
        values["nats_stream_yandex_lifecycle"] = prefix + "_Y"
    settings = NatsSettings(_env_file=None, **values)
    client = NatsJetstreamClient(settings=settings)
    owner, foreign, routes = topology(settings)
    configs = [owner, *foreign]
    await asyncio.wait_for(client.connect(), timeout=10)
    js = client.jetstream
    created = set()
    try:
        for config in configs:
            assert config.name.startswith(prefix)
            with pytest.raises(NotFoundError):
                await js.stream_info(config.name)

        async def create(config):
            assert config.name.startswith(prefix)
            await js.add_stream(config=config)
            created.add(config.name)

        yield client, configs, routes, create
    finally:
        # The owner may have been created by setup; names are unique and checked absent.
        for config in configs:
            assert config.name.startswith(prefix)
            try:
                await js.delete_stream(config.name)
            except NotFoundError:
                pass
        await client.close()


async def test_real_valid_topology_owner_discard_new_and_durable_reuse():
    async with isolated_topology() as (client, configs, routes, create):
        for config in configs[1:]:
            await create(config)
        await asyncio.wait_for(client.setup_maintenance(), timeout=10)
        for expected in configs:
            c = (await client.jetstream.stream_info(expected.name)).config
            assert c.discard == DiscardPolicy.NEW
            assert (c.storage, c.retention, c.max_age, c.max_msgs, c.max_bytes, c.duplicate_window) == (
                StorageType.FILE,
                RetentionPolicy.LIMITS,
                0,
                -1,
                -1,
                120,
            )
            assert set(c.subjects) == set(expected.subjects)
        for stream, subject, durable in routes:
            c = (await client.jetstream.consumer_info(stream, durable)).config
            assert (c.filter_subject, c.deliver_policy, c.ack_policy, c.ack_wait, c.max_deliver) == (
                subject,
                DeliverPolicy.ALL,
                AckPolicy.EXPLICIT,
                30,
                -1,
            )
            assert c.deliver_subject is None
        await asyncio.wait_for(client.setup_maintenance(), timeout=10)


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
    ],
)
async def test_real_incompatible_stream_fails_without_repair_or_consumer(index, field, value):
    async with isolated_topology() as (client, configs, routes, create):
        target = configs[index % len(configs)].name
        for config in configs:
            changed = deepcopy(config)
            if config.name == target:
                setattr(changed, field, value)
                if field == "max_age":
                    changed.duplicate_window = 60  # Broker requires dedup <= nonzero TTL.
            await create(changed)
        with pytest.raises(MaintenanceTopologyError, match=field):
            await asyncio.wait_for(client.setup_maintenance(), timeout=10)
        actual = (await client.jetstream.stream_info(target)).config
        assert getattr(actual, field) == value
        for stream, _, durable in routes:
            with pytest.raises(NotFoundError):
                await client.jetstream.consumer_info(stream, durable)


@pytest.mark.parametrize(
    "field,value",
    [
        ("filter_subject", ""),
        ("ack_policy", AckPolicy.NONE),
        ("ack_wait", 60),
        ("max_deliver", 5),
    ],
)
async def test_real_incompatible_existing_durable_fails_without_update(field, value):
    async with isolated_topology() as (client, configs, routes, create):
        for config in configs:
            await create(config)
        stream, subject, durable = routes[0]
        config = ConsumerConfig(
            durable_name=durable,
            filter_subject=subject,
            deliver_policy=DeliverPolicy.ALL,
            ack_policy=AckPolicy.EXPLICIT,
            ack_wait=30,
            max_deliver=-1,
        )
        setattr(config, field, value)
        await client.jetstream.add_consumer(stream=stream, config=config)
        before = (await client.jetstream.consumer_info(stream, durable)).config
        with pytest.raises(MaintenanceTopologyError):
            await asyncio.wait_for(client.setup_maintenance(), timeout=10)
        after = (await client.jetstream.consumer_info(stream, durable)).config
        assert after == before
