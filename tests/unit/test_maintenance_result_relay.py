"""Finalization/result recovery contracts; mocks are not real PG acceptance."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql

from clients.nats.maintenance_result_publisher import MaintenanceResultPublisher
from core.entities.metrica_maintenance import (
    MaintenanceLease,
    MaintenanceLeaseLostError,
    MaintenanceResultClaim,
    WorkKind,
)
from core.schemas.messaging.maintenance import CLEANUP_COMPLETED, CleanupCompleted, maintenance_message_id
from core.services.maintenance_result_relay import MaintenanceResultRelay
from core.services.metrica_cleanup_service import CleanupFinalizerService
from repositories.metrica_cleanup_repository import CleanupBatchRepository
from repositories.metrica_result_outbox_repository import MaintenanceResultOutboxRepository
from settings import NatsSettings


def result_claim(attempts=1):
    tenant, operation = uuid4(), uuid4()
    result = CleanupCompleted(
        schema_version=1,
        message_id=maintenance_message_id(CLEANUP_COMPLETED, operation, "completed"),
        operation_id=operation,
        equestrian_id=tenant,
        occurred_at=datetime.now(UTC),
        cleanup_seq=1,
        status="completed",
    )
    return MaintenanceResultClaim(
        result.message_id, CLEANUP_COMPLETED, result.model_dump(mode="json"), uuid4(), 2, attempts
    )


def transaction(events, fail=0):
    count = 0

    @asynccontextmanager
    async def begin():
        nonlocal count
        count += 1
        events.append("begin")
        try:
            yield
            if count == fail:
                raise RuntimeError("commit failure")
            events.append("commit")
        except BaseException:
            events.append("rollback")
            raise

    tx = MagicMock()
    tx.begin = begin
    return tx


@pytest.mark.parametrize("remaining", [[], [True], [False, True], [False, False]])
async def test_finalizer_rechecks_fences_and_both_tables(remaining):
    session = AsyncMock()
    repo = CleanupBatchRepository(session)
    repo.assert_owned = AsyncMock()
    repo._operation = AsyncMock(return_value={"cleanup_seq": 1})
    repo.save_result, repo.finish_work = AsyncMock(), AsyncMock()
    session.scalar.side_effect = remaining
    lease = MaintenanceLease(uuid4(), uuid4(), uuid4(), 1, WorkKind.CLEANUP, "deleting")
    if not remaining:
        repo.assert_owned.side_effect = MaintenanceLeaseLostError()
        with pytest.raises(MaintenanceLeaseLostError):
            await repo.finalize_cleanup(lease)
    else:
        assert await repo.finalize_cleanup(lease) is (remaining == [False, False])
    repo.assert_owned.assert_awaited_once_with(lease)
    if remaining == [False, False]:
        result = repo.save_result.call_args.args[0]
        assert result.message_id == maintenance_message_id(CLEANUP_COMPLETED, lease.operation_id, "completed")
        repo.finish_work.assert_awaited_once_with(lease, status="completed", result_message_id=result.message_id)
    else:
        repo.save_result.assert_not_awaited()
        repo.finish_work.assert_not_awaited()
    for call in session.scalar.call_args_list:
        assert lease.equestrian_id in call.args[0].compile().params.values()
    session.commit.assert_not_awaited()


@pytest.mark.parametrize("failure", [None, "outbox", "terminal", "commit"])
async def test_finalizer_atomic_transaction(failure):
    events = []
    session = AsyncMock()
    repo = CleanupBatchRepository(session)
    repo.assert_owned = AsyncMock()
    repo._operation = AsyncMock(return_value={"cleanup_seq": 1})
    session.scalar.return_value = False

    async def save(result):
        events.append("outbox")
        if failure == "outbox":
            raise RuntimeError()

    async def finish(*args, **kwargs):
        events.append("terminal")
        if failure == "terminal":
            raise MaintenanceLeaseLostError()

    repo.save_result, repo.finish_work = save, finish
    service = CleanupFinalizerService(repository=repo, transaction=transaction(events, int(failure == "commit")))
    lease = MaintenanceLease(uuid4(), uuid4(), uuid4(), 1, WorkKind.CLEANUP, "deleting")
    if failure:
        with pytest.raises(RuntimeError):
            await service.finalize(lease)
        assert events[-1] == "rollback"
    else:
        assert await service.finalize(lease)
        assert events == ["begin", "outbox", "terminal", "commit"]


@pytest.mark.parametrize("failure", [None, "publish", "claim_commit", "mark_commit"])
async def test_relay_transaction_boundaries_puback_and_retry(failure, caplog):
    events = []
    repo, publisher = AsyncMock(), AsyncMock()
    claim = result_claim(5)
    repo.claim_result.return_value = claim

    async def publish(value):
        assert value is claim and events == ["begin", "commit"]
        events.append("publish")
        if failure == "publish":
            raise TimeoutError("sensitive exception not logged")
        return True

    async def mark(value):
        events.append("mark")

    publisher.publish_result.side_effect, repo.mark_published.side_effect = publish, mark
    relay = MaintenanceResultRelay(
        repository=repo,
        publisher=publisher,
        transaction=transaction(events, {"claim_commit": 1, "mark_commit": 2}.get(failure, 0)),
    )
    if failure in {"claim_commit", "mark_commit"}:
        with pytest.raises(RuntimeError):
            await relay.relay_once(owner=claim.owner)
        assert events[-1] == "rollback"
    else:
        assert await relay.relay_once(owner=claim.owner)
    if failure == "claim_commit":
        publisher.publish_result.assert_not_awaited()
    elif failure == "publish":
        repo.retry_result.assert_awaited_once_with(claim)
        repo.mark_published.assert_not_awaited()
        assert "requires attention" in caplog.text and "sensitive exception" not in caplog.text
    else:
        repo.mark_published.assert_awaited_once_with(claim)
        repo.retry_result.assert_not_awaited()
        assert "deduplicated by broker" in caplog.text


async def test_relay_no_work_or_process_cancellation_keeps_recoverable_claim():
    import asyncio

    repo, publisher = AsyncMock(), AsyncMock()
    repo.claim_result.return_value = None
    relay = MaintenanceResultRelay(repository=repo, publisher=publisher, transaction=transaction([]))
    assert not await relay.relay_once(owner=uuid4())
    publisher.publish_result.assert_not_awaited()
    repo.claim_result.return_value = result_claim()
    publisher.publish_result.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await relay.relay_once(owner=uuid4())
    repo.mark_published.assert_not_awaited()
    repo.retry_result.assert_not_awaited()


@pytest.mark.parametrize("duplicate", [False, True])
async def test_publisher_validates_puback_and_preserves_identity(duplicate):
    client, settings = MagicMock(), NatsSettings()
    client.jetstream.publish = AsyncMock(
        return_value=SimpleNamespace(stream=settings.nats_stream_maintenance_results, seq=2, duplicate=duplicate)
    )
    publisher = MaintenanceResultPublisher(client=client, settings=settings)
    claim = result_claim()
    assert await publisher.publish_result(claim) is duplicate
    first = client.jetstream.publish.call_args
    await publisher.publish_result(claim)
    assert client.jetstream.publish.call_args == first
    assert first.kwargs["headers"] == {"Nats-Msg-Id": str(claim.message_id)}
    for ack in (
        SimpleNamespace(stream="wrong", seq=1),
        SimpleNamespace(stream=settings.nats_stream_maintenance_results, seq=0),
    ):
        client.jetstream.publish.return_value = ack
        with pytest.raises(RuntimeError):
            await publisher.publish_result(claim)


async def test_outbox_bounded_skip_locked_expired_claim_recovery():
    session, claim = AsyncMock(), result_claim()
    result = MagicMock()
    result.mappings.return_value.one_or_none.return_value = dict(
        message_id=claim.message_id, subject=claim.subject, payload=claim.payload, fencing_revision=4, attempts=3
    )
    session.execute.return_value = result
    recovered = await MaintenanceResultOutboxRepository(session).claim_result(owner=claim.owner, lease_seconds=60)
    assert recovered is not None
    assert recovered.revision == 5 and recovered.attempts == 4
    sql = str(session.execute.call_args_list[0].args[0].compile(dialect=postgresql.dialect()))
    assert "FOR UPDATE SKIP LOCKED" in sql and "LIMIT" in sql
    assert "lease_expires_at <= clock_timestamp()" in sql
    assert "next_attempt_at <= clock_timestamp()" in sql
    session.commit.assert_not_awaited()


@pytest.mark.parametrize("attempt,delay", [(1, 1), (2, 2), (6, 32), (7, 60), (100000, 60)])
async def test_retry_backoff_without_attempt_cap(attempt, delay):
    repo = MaintenanceResultOutboxRepository(AsyncMock())
    repo._mark = AsyncMock()
    await repo.retry_result(result_claim(attempt))
    expr = repo._mark.call_args.kwargs["next_attempt_at"]
    assert expr.compile().params["clock_timestamp_1"].total_seconds() == delay


@pytest.mark.parametrize("method", ["mark_published", "retry_result"])
async def test_marks_reject_stale_owner_revision_and_expiry(method):
    session = AsyncMock()
    session.scalar.return_value = None
    repo, claim = MaintenanceResultOutboxRepository(session), result_claim()
    with pytest.raises(MaintenanceLeaseLostError):
        await getattr(repo, method)(claim)
    statement = session.scalar.call_args.args[0]
    sql = str(statement.compile(dialect=postgresql.dialect())).split("WHERE")[1]
    assert all(
        column in sql for column in ("message_id", "status", "lease_owner", "fencing_revision", "lease_expires_at")
    )
    assert claim.owner in statement.compile().params.values()


def test_lazy_di_factories_share_session():
    from dependency_injector import providers

    from containers.repositories import RepositoriesContainer
    from containers.services import ServicesContainer
    from containers.settings import SettingsContainer

    session = AsyncMock()
    repositories = RepositoriesContainer(session=providers.Object(session))
    services = ServicesContainer(
        repositories_container=repositories,
        settings_container=SettingsContainer(),
        maintenance_result_publisher=providers.Object(AsyncMock()),
    )
    for service in (services.cleanup_finalizer_service(), services.maintenance_result_relay()):
        assert isinstance(service._repository, MaintenanceResultOutboxRepository)
        assert service._repository.session is session and service._transaction is session
    session.execute.assert_not_called()


@pytest.mark.parametrize("conflict", [False, True])
async def test_result_outbox_immutable_envelope_hash_after_duplicate_window(conflict):
    from core.entities.metrica_maintenance import MaintenanceConflictError
    from repositories.metrica_maintenance_storage_repository import payload_hash

    claim = result_claim()
    result = CleanupCompleted.model_validate(claim.payload)
    session = AsyncMock()
    repo = MaintenanceResultOutboxRepository(session)
    repo._operation = AsyncMock()
    session.scalar.return_value = None  # Unique conflict, even after broker window.
    stored = dict(
        message_id=claim.message_id,
        equestrian_id=result.equestrian_id,
        operation_id=result.operation_id,
        subject=claim.subject,
        payload=claim.payload,
        payload_hash=payload_hash(claim.payload),
    )
    if conflict:
        stored["payload_hash"] = "0" * 64
    response = MagicMock()
    response.mappings.return_value.one.return_value = stored
    session.execute.return_value = response
    if conflict:
        with pytest.raises(MaintenanceConflictError):
            await repo.save_result(result)
    else:
        await repo.save_result(result)
    session.commit.assert_not_awaited()


async def test_result_outbox_rejects_nondeterministic_message_id_before_sql():
    from core.entities.metrica_maintenance import MaintenanceConflictError

    session = AsyncMock()
    result = CleanupCompleted.model_validate(result_claim().payload).model_copy(update={"message_id": uuid4()})
    with pytest.raises(MaintenanceConflictError):
        await MaintenanceResultOutboxRepository(session).save_result(result)
    session.execute.assert_not_awaited()
