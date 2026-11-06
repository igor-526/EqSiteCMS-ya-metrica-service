"""UT-D-03 / UT-N-02/04. Mocked transport is NOT real-broker acceptance."""

import asyncio
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
import yaml  # type: ignore[import-untyped]
from nats.js.api import (
    AckPolicy,
    ConsumerConfig,
    ConsumerInfo,
    DeliverPolicy,
    DiscardPolicy,
    RetentionPolicy,
    StorageType,
    StreamConfig,
    StreamInfo,
    StreamState,
)
from nats.js.errors import BadRequestError, NotFoundError
from pydantic import ValidationError

from clients.nats.client import NatsJetstreamClient
from clients.nats.consumers.maintenance import MaintenanceConsumer
from clients.nats.handlers.maintenance import MaintenanceHandler
from clients.nats.handlers.parse_run import PermanentCommandError
from core.entities.metrica_maintenance import MaintenanceConflictError, WorkKind
from core.schemas.messaging.maintenance import (
    CLEANUP_COMPLETED,
    CLEANUP_REQUESTED,
    MAX_BODY_BYTES,
    MERGE_COMMAND,
    MERGE_COMPLETED,
    MERGE_FINALIZED,
    CleanupCompleted,
    CleanupRequested,
    MergeCompleted,
    MergeFinalized,
    MergeMappings,
    MergeSeal,
    maintenance_message_id,
)
from core.services.maintenance_delivery_service import MaintenanceDeliveryService
from settings import NatsSettings


def make_settings(**kwargs: Any) -> NatsSettings:
    # Pydantic-settings accepts _env_file and field-name population at runtime.
    return NatsSettings(**{"_env_file": None, **kwargs})


def body(subject=CLEANUP_REQUESTED, *, operation=None, tenant=None, **fields):
    operation = operation or uuid4()
    extra = {
        CLEANUP_REQUESTED: dict(cleanup_seq=1, binding_generation=2, reason="counter_unlinked"),
        MERGE_COMMAND: dict(normalization_revision=1, kind="seal", part_count=0),
        MERGE_FINALIZED: dict(normalization_revision=1, result_message_id=str(uuid4())),
    }[subject]
    extra.update(fields)
    kind = {
        CLEANUP_REQUESTED: "requested",
        MERGE_COMMAND: extra.get("part_index", "seal"),
        MERGE_FINALIZED: f"finalized:{extra.get('result_message_id')}",
    }[subject]
    return dict(
        schema_version=1,
        message_id=str(maintenance_message_id(subject, operation, kind)),
        operation_id=str(operation),
        equestrian_id=str(tenant or uuid4()),
        occurred_at="2026-10-21T00:00:00Z",
        **extra,
    )


def message(data, subject=CLEANUP_REQUESTED):
    msg = MagicMock(
        data=json.dumps(data).encode(),
        subject=subject,
        headers={"Nats-Msg-Id": data["message_id"], "X-Equestrian-Id": "ignored"},
    )
    msg.metadata.num_delivered = 1
    msg.ack, msg.nak, msg.term = AsyncMock(), AsyncMock(), AsyncMock()
    return msg


def consumer(handler, subject=CLEANUP_REQUESTED):
    return MaintenanceConsumer(
        client=MagicMock(),
        settings=make_settings(),
        handler=handler,
        route=("stream", subject, "durable"),
    )


@pytest.mark.parametrize(
    "subject,model",
    [(CLEANUP_REQUESTED, CleanupRequested), (MERGE_COMMAND, MergeSeal), (MERGE_FINALIZED, MergeFinalized)],
)
async def test_maintenance_valid_handler_uses_payload_tenant_and_no_peer_io(subject, model):
    service = AsyncMock()
    msg = message(body(subject), subject)
    await consumer(MaintenanceHandler(service=service, consumer="durable"), subject)._process_message(msg)
    service.receive.assert_awaited_once_with(model.model_validate_json(msg.data), consumer="durable")
    msg.ack.assert_awaited_once()
    msg.nak.assert_not_awaited()
    msg.term.assert_not_awaited()


@pytest.mark.parametrize(
    "changes",
    [
        {"schema_version": 2},
        {"schema_version": True},
        {"schema_version": "1"},
        {"equestrian_id": "invalid"},
        {"cleanup_seq": 0},
        {"cleanup_seq": True},
        {"cleanup_seq": "1"},
        {"cleanup_seq": 2**63},
        {"binding_generation": -1},
        {"reason": "unknown"},
        {"secret": "SECRET"},
        {"occurred_at": "2026-10-21T00:00:00"},
        {"occurred_at": 12345678},
        {"occurred_at": "12345678"},
        {"occurred_at": "2026-10-21T01:00:00+01:00"},
        {"message_id": str(uuid4())},
    ],
)
async def test_maintenance_invalid_schema_term_without_storage(changes, caplog):
    service = AsyncMock()
    data = body()
    data.update(changes)
    msg = message(data)
    await consumer(MaintenanceHandler(service=service, consumer="durable"))._process_message(msg)
    msg.term.assert_awaited_once()
    msg.ack.assert_not_awaited()
    msg.nak.assert_not_awaited()
    service.receive.assert_not_awaited()
    assert "SECRET" not in caplog.text


@pytest.mark.parametrize("headers", [None, {}, {"Nats-Msg-Id": str(uuid4())}])
async def test_maintenance_header_identity_required(headers):
    handler = MaintenanceHandler(service=AsyncMock(), consumer="durable")
    with pytest.raises(PermanentCommandError):
        await handler.handle(payload=json.dumps(body()).encode(), headers=headers, subject=CLEANUP_REQUESTED)


async def test_maintenance_body_limit_and_wrong_subject():
    handler = MaintenanceHandler(service=AsyncMock(), consumer="durable")
    for payload, subject in [
        (b" " * (MAX_BODY_BYTES + 1), CLEANUP_REQUESTED),
        (b"{}", "unknown"),
        (b"{", MERGE_COMMAND),
    ]:
        with pytest.raises(PermanentCommandError):
            await handler.handle(payload=payload, headers=None, subject=subject)
    msg = message(body(), MERGE_COMMAND)
    await consumer(handler)._process_message(msg)
    msg.term.assert_awaited_once()


@pytest.mark.parametrize("attempt,delay", [(1, 1), (2, 2), (5, 16), (7, 60), (100000, 60)])
async def test_maintenance_temporary_storage_error_nak_without_leaking(attempt, delay, caplog):
    service = AsyncMock()
    service.receive.side_effect = RuntimeError("SECRET")
    msg = message(body())
    msg.metadata.num_delivered = attempt
    await consumer(MaintenanceHandler(service=service, consumer="durable"))._process_message(msg)
    msg.nak.assert_awaited_once_with(delay=delay)
    msg.ack.assert_not_awaited()
    msg.term.assert_not_awaited()
    assert "SECRET" not in caplog.text
    assert caplog.records[-1].levelname == ("ERROR" if attempt >= 5 else "WARNING")


async def test_maintenance_hash_or_foreign_correlation_term_and_cancel_no_ack():
    service = AsyncMock()
    handler = MaintenanceHandler(service=service, consumer="durable")
    msg = message(body())
    service.receive.side_effect = MaintenanceConflictError("SECRET")
    await consumer(handler)._process_message(msg)
    msg.term.assert_awaited_once()
    service.receive.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await consumer(handler)._process_message(msg)
    msg.ack.assert_not_awaited()
    msg.nak.assert_not_awaited()


class Transaction:
    def __init__(self, events, fail=False):
        self.events, self.fail = events, fail

    @asynccontextmanager
    async def begin(self):
        self.events.append("begin")
        try:
            yield
            if self.fail:
                raise RuntimeError("commit failed")
        except BaseException:
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")


@pytest.mark.parametrize(
    "subject,model,method",
    [
        (CLEANUP_REQUESTED, CleanupRequested, None),
        (MERGE_COMMAND, MergeSeal, "seal"),
        (MERGE_FINALIZED, MergeFinalized, "finalize"),
    ],
)
@pytest.mark.parametrize("fresh", [True, False])
async def test_maintenance_commit_before_ack_duplicate_inbox_no_effect(subject, model, method, fresh):
    events = []
    repo = AsyncMock()
    repo.save_inbox.return_value = fresh
    data = body(subject)
    service = MaintenanceDeliveryService(repository=repo, transaction=Transaction(events))
    msg = message(data, subject)
    msg.ack.side_effect = lambda: events.append("ack")
    await consumer(MaintenanceHandler(service=service, consumer="durable"), subject)._process_message(msg)
    assert events == ["begin", "commit", "ack"]
    if method:
        assert getattr(repo, method).await_count == int(fresh)
    if subject == MERGE_FINALIZED:
        repo.save_operation.assert_not_awaited()  # Unknown finalized may never invent a ledger row.
    else:
        kwargs = repo.save_operation.await_args.kwargs
        assert kwargs["kind"] == (WorkKind.CLEANUP if subject == CLEANUP_REQUESTED else WorkKind.MERGE)
    assert repo.save_inbox.await_args.kwargs["payload"] == model.model_validate(data).model_dump(mode="json")


async def test_maintenance_commit_failure_rolls_back_and_nak_not_ack():
    events = []
    service = MaintenanceDeliveryService(repository=AsyncMock(), transaction=Transaction(events, fail=True))
    msg = message(body())
    await consumer(MaintenanceHandler(service=service, consumer="durable"))._process_message(msg)
    assert events == ["begin", "rollback"]
    msg.nak.assert_awaited_once_with(delay=1)
    msg.ack.assert_not_awaited()


async def test_maintenance_mappings_saved_bounded_after_inbox():
    events = []
    repo = AsyncMock()
    data = body(
        MERGE_COMMAND,
        kind="mappings",
        part_index=0,
        mappings=[dict(source_page_id=str(uuid4()), target_page_id=str(uuid4()))],
    )
    data.pop("part_count")
    command = MergeMappings.model_validate(data)
    repo.save_inbox.return_value = True
    service = MaintenanceDeliveryService(repository=repo, transaction=Transaction(events))
    await service.receive(command, consumer="durable")
    assert [call[0] for call in repo.mock_calls] == ["save_operation", "save_inbox", "save_chunk"]
    assert repo.save_chunk.await_args.kwargs["mappings"] == data["mappings"]


@pytest.mark.parametrize("issue", ["empty", "too_many", "self", "duplicate_source", "extra", "negative"])
def test_maintenance_mapping_schema_rejects_conflicts(issue):
    pair = dict(source_page_id=str(uuid4()), target_page_id=str(uuid4()))
    pairs = [pair]
    if issue == "empty":
        pairs = []
    if issue == "too_many":
        pairs *= 501
    if issue == "self":
        pair["target_page_id"] = pair["source_page_id"]
    if issue == "duplicate_source":
        pairs *= 2
    if issue == "extra":
        pair["extra"] = "x"
    data = body(MERGE_COMMAND, kind="mappings", part_index=-1 if issue == "negative" else 0, mappings=pairs)
    data.pop("part_count")
    with pytest.raises(ValidationError):
        MergeMappings.model_validate(data)


def canonical_topology_mock() -> MagicMock:
    """Model owner creation and foreign lookup with real SDK info/config objects."""
    subjects = {
        "METRICA_MAINTENANCE_RESULTS": ["events.metrica.cleanup.completed", "events.metrica.pages.merge.completed"],
        "SEO_MAINTENANCE": ["commands.seo.pages.merge", "events.seo.pages.merge.finalized"],
        "YANDEX_LIFECYCLE": ["events.yandex.metrica.cleanup.requested"],
    }
    owner = "METRICA_MAINTENANCE_RESULTS"
    streams = {
        name: StreamConfig(
            name=name,
            subjects=items,
            storage=StorageType.FILE,
            retention=RetentionPolicy.LIMITS,
            max_age=0,
            max_msgs=-1,
            max_bytes=-1,
            discard=DiscardPolicy.NEW,
            duplicate_window=120,
        )
        for name, items in subjects.items()
        if name != owner
    }
    consumers: dict[tuple[str, str], ConsumerConfig] = {}

    def stream_result(config: StreamConfig) -> StreamInfo:
        return StreamInfo(config=deepcopy(config), state=StreamState(0, 0, 0, 0, 0))

    async def stream_info(name: str) -> StreamInfo:
        if name not in streams:
            raise NotFoundError()
        return stream_result(streams[name])

    async def add_stream(*, config: StreamConfig) -> StreamInfo:
        assert config.name is not None
        streams[config.name] = deepcopy(config)
        return stream_result(config)

    def consumer_result(stream: str, durable: str, config: ConsumerConfig) -> ConsumerInfo:
        return ConsumerInfo(
            name=durable, stream_name=stream, config=deepcopy(config), created=datetime(2026, 1, 1, tzinfo=UTC)
        )

    async def consumer_info(stream: str, durable: str) -> ConsumerInfo:
        if (stream, durable) not in consumers:
            raise NotFoundError()
        return consumer_result(stream, durable, consumers[stream, durable])

    async def add_consumer(*, stream: str, config: ConsumerConfig) -> ConsumerInfo:
        assert config.durable_name is not None
        consumers[stream, config.durable_name] = deepcopy(config)
        return consumer_result(stream, config.durable_name, config)

    return MagicMock(
        stream_info=AsyncMock(side_effect=stream_info),
        add_stream=AsyncMock(side_effect=add_stream),
        consumer_info=AsyncMock(side_effect=consumer_info),
        add_consumer=AsyncMock(side_effect=add_consumer),
    )


async def test_maintenance_owner_topology_and_contract_mirror():
    settings = make_settings(nats_setup_backoff_seconds=0)
    client = NatsJetstreamClient(settings=settings)
    js = canonical_topology_mock()
    client._jetstream = js
    await client.setup_maintenance()
    assert [c[0] for c in js.mock_calls[:4]] == ["stream_info", "add_stream", "stream_info", "stream_info"]
    js.update_stream.assert_not_called()
    config = js.add_stream.await_args.kwargs["config"]
    assert (config.name, config.storage, config.retention, config.discard) == (
        "METRICA_MAINTENANCE_RESULTS",
        StorageType.FILE,
        RetentionPolicy.LIMITS,
        DiscardPolicy.NEW,
    )
    assert config.subjects == [CLEANUP_COMPLETED, MERGE_COMPLETED]
    assert (config.max_age, config.max_msgs, config.max_bytes, config.duplicate_window) == (0, -1, -1, 120)
    doc = yaml.safe_load((Path(__file__).parents[2] / "docs/asyncapi.yaml").read_text())
    for call, (stream, subject, durable) in zip(
        js.add_consumer.await_args_list, client.maintenance_routes(), strict=True
    ):
        config = call.kwargs["config"]
        assert call.kwargs["stream"] == stream
        assert (config.durable_name, config.filter_subject) == (durable, subject)
        assert (config.ack_policy, config.deliver_policy, config.ack_wait, config.max_deliver) == (
            AckPolicy.EXPLICIT,
            DeliverPolicy.ALL,
            30,
            -1,
        )
        channel = doc["channels"][subject]
        assert channel["x-stream"] == stream
        assert channel["x-consumers"][0]["durable"] == durable
        assert "subscribe" in channel
    for subject in (CLEANUP_COMPLETED, MERGE_COMPLETED):
        assert "publish" in doc["channels"][subject]
    assert doc["x-streams"]["METRICA_MAINTENANCE_RESULTS"]["subjects"] == [CLEANUP_COMPLETED, MERGE_COMPLETED]


@pytest.mark.parametrize("error,attempts", [(NotFoundError(), 3), (BadRequestError(), 1)])
async def test_maintenance_foreign_owner_bounded_retry_no_foreign_stream_creation(error, attempts):
    client = NatsJetstreamClient(settings=make_settings(nats_setup_max_attempts=3, nats_setup_backoff_seconds=0))
    js = canonical_topology_mock()
    client._jetstream = js
    lookup = js.stream_info.side_effect

    async def unavailable_foreign(name):
        if name == "YANDEX_LIFECYCLE":
            raise error
        return await lookup(name)

    js.stream_info.side_effect = unavailable_foreign
    with pytest.raises(type(error)):
        await client.setup_maintenance()
    assert sum(c.args == ("YANDEX_LIFECYCLE",) for c in js.stream_info.await_args_list) == attempts
    js.consumer_info.assert_not_awaited()
    js.add_consumer.assert_not_awaited()
    js.update_stream.assert_not_called()
    js.add_stream.assert_awaited_once()
    assert js.add_stream.await_args.kwargs["config"].name == "METRICA_MAINTENANCE_RESULTS"


def test_maintenance_results_strict_correlations_and_identity_subject_separation():
    request = body()
    common = {
        key: request[key] for key in ("schema_version", "message_id", "operation_id", "equestrian_id", "occurred_at")
    }
    CleanupCompleted.model_validate(dict(common, cleanup_seq=1, status="completed"))
    MergeCompleted.model_validate(dict(common, normalization_revision=1, status="completed", part_count=0))
    with pytest.raises(ValidationError):
        MergeCompleted.model_validate(dict(common, normalization_revision=1, status="completed", part_count=None))
    with pytest.raises(ValidationError):
        MergeCompleted.model_validate(
            dict(common, normalization_revision=1, status="superseded_by_delete", part_count=None)
        )
    operation = uuid4()
    ids = {maintenance_message_id(subject, operation, "completed") for subject in (CLEANUP_COMPLETED, MERGE_COMPLETED)}
    assert len(ids) == 2


async def test_maintenance_seal_before_parts_preserves_order_and_same_operation():
    tenant, operation = uuid4(), uuid4()
    seal = MergeSeal.model_validate(body(MERGE_COMMAND, tenant=tenant, operation=operation, part_count=1))
    data = body(
        MERGE_COMMAND,
        tenant=tenant,
        operation=operation,
        kind="mappings",
        part_index=0,
        mappings=[dict(source_page_id=str(uuid4()), target_page_id=str(uuid4()))],
    )
    data.pop("part_count")
    mappings = MergeMappings.model_validate(data)
    repo = AsyncMock()
    repo.save_inbox.return_value = True
    service = MaintenanceDeliveryService(repository=repo, transaction=Transaction([]))
    await service.receive(seal, consumer="merge")
    await service.receive(mappings, consumer="merge")
    assert [call[0] for call in repo.mock_calls] == [
        "save_operation",
        "save_inbox",
        "seal",
        "save_operation",
        "save_inbox",
        "save_chunk",
    ]
    assert repo.seal.await_args.args == repo.save_chunk.await_args.args == (tenant, operation)


async def test_maintenance_delivery_graph_fresh_sessions_lazy_factory(monkeypatch):
    from containers import application
    from core.services.maintenance_delivery_service import MaintenanceDeliveryService

    sessions = []

    @asynccontextmanager
    async def session_scope():
        session = AsyncMock()
        sessions.append(session)
        yield session

    monkeypatch.setattr(application, "worker_session", session_scope)
    receives = AsyncMock()
    monkeypatch.setattr(MaintenanceDeliveryService, "receive", receives)
    settings = make_settings()
    client = NatsJetstreamClient(settings=settings)
    consumers = application.create_maintenance_consumers(client, settings)
    assert len(consumers) == 3
    assert not any(item.is_running for item in consumers)
    assert sessions == []
    handler = application.DeliveryMaintenanceHandler(consumer=settings.nats_consumer_cleanup_requested)
    data = body()
    for _ in range(2):
        await handler.handle(
            payload=json.dumps(data).encode(),
            headers={"Nats-Msg-Id": str(data["message_id"])},
            subject=CLEANUP_REQUESTED,
        )
    assert len(sessions) == 2 and sessions[0] is not sessions[1]
    assert receives.await_count == 2


@pytest.mark.parametrize("failure", ["save_operation", "save_inbox", "seal", "finalize"])
async def test_maintenance_storage_failure_rolls_back_ledger_and_inbox(failure):
    events = []
    repo = AsyncMock()
    repo.save_inbox.return_value = True
    getattr(repo, failure).side_effect = RuntimeError("database unavailable")
    subject = MERGE_FINALIZED if failure == "finalize" else MERGE_COMMAND
    service = MaintenanceDeliveryService(repository=repo, transaction=Transaction(events))
    msg = message(body(subject), subject)
    await consumer(MaintenanceHandler(service=service, consumer="durable"), subject)._process_message(msg)
    assert events == ["begin", "rollback"]
    msg.ack.assert_not_awaited()
    msg.term.assert_not_awaited()
    msg.nak.assert_awaited_once_with(delay=1)


def test_maintenance_asyncapi_mirrors_strict_payload_properties():
    doc = yaml.safe_load((Path(__file__).parents[2] / "docs/asyncapi.yaml").read_text())
    schemas = doc["components"]["schemas"]
    for model in (CleanupRequested, CleanupCompleted, MergeMappings, MergeSeal, MergeFinalized, MergeCompleted):
        runtime = model.model_json_schema(ref_template="#/components/schemas/{model}")
        mirror = schemas[model.__name__]
        assert mirror["properties"] == runtime["properties"]
        assert mirror["required"] == runtime["required"]
        assert mirror["additionalProperties"] is False
    assert schemas["CleanupRequested"]["properties"]["cleanup_seq"]["maximum"] == 2**63 - 1


async def test_maintenance_pull_binds_exact_durable_and_stop_unsubscribes():
    client = MagicMock()
    client.jetstream.pull_subscribe = AsyncMock()
    subscription = client.jetstream.pull_subscribe.return_value
    entered = asyncio.Event()

    async def fetch(**kwargs):
        entered.set()
        await asyncio.Event().wait()

    subscription.fetch.side_effect = fetch
    item = MaintenanceConsumer(
        client=client,
        settings=make_settings(),
        handler=AsyncMock(),
        route=("SEO_MAINTENANCE", MERGE_COMMAND, "ya-metrica-pages-merge"),
    )
    await item.start()
    await entered.wait()
    client.jetstream.pull_subscribe.assert_awaited_once_with(
        stream="SEO_MAINTENANCE", subject=MERGE_COMMAND, durable="ya-metrica-pages-merge"
    )
    await item.stop()
    assert not item.is_running
    subscription.unsubscribe.assert_awaited_once()
