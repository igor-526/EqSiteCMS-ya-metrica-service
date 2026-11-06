"""UT-N-03/04/06: bounded transactions, fencing, rollback, result barrier and DI."""

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import DBAPIError

from core.entities.metrica_maintenance import (
    MaintenanceLease,
    MaintenanceLeaseLostError,
    MergeBatch,
    MergeOverflowError,
    MergeStorageError,
    WorkKind,
)
from core.schemas.messaging.maintenance import MERGE_COMPLETED, maintenance_message_id
from core.services.metrica_merge_service import MergeWorkerService
from repositories.metrica_merge_repository import MergeBatchRepository
from repositories.metrica_merge_worker_repository import MergeWorkerRepository


class Transaction:
    def __init__(self, events):
        self.events = events

    @asynccontextmanager
    async def begin(self):
        self.events.append("begin")
        try:
            yield
        except BaseException:
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")


DEFAULT_BATCH = MergeBatch(0, 0, 1, False)


def worker(batch=DEFAULT_BATCH):
    events = []
    lease = MaintenanceLease(uuid4(), uuid4(), uuid4(), 3, WorkKind.MERGE, "running")
    repo = AsyncMock()
    repo.claim_merge.return_value = lease
    repo.move_batch.return_value = batch
    repo.finalize_merge.return_value = True
    repo.fail_merge.return_value = "failed_blocked"
    service = MergeWorkerService(repository=repo, transaction=Transaction(events), batch_size=2)
    return service, repo, lease, events


@pytest.mark.parametrize("batch", [None, MergeBatch(0, 1, 0, True), MergeBatch(0, 0, 2, False)])
async def test_tick_one_batch_fresh_transactions_and_never_releases_parse_gate(batch):
    service, repo, lease, events = worker(batch)
    result = await service.tick(lease.equestrian_id, owner=lease.owner)
    finalizing = batch is None or batch.part_completed
    assert result == ("awaiting_seo" if finalizing else "progress")
    assert events == ["begin", "commit"] * 3
    repo.move_batch.assert_awaited_once_with(lease, batch_size=2, lease_seconds=60)
    assert repo.finalize_merge.await_count == int(finalizing)
    assert repo.yield_merge.await_count == int(not finalizing)
    repo.finalize.assert_not_awaited()


async def test_tick_waiting_or_running_parse_has_no_effect():
    service, repo, lease, events = worker()
    repo.claim_merge.return_value = None
    assert await service.tick(lease.equestrian_id, owner=lease.owner) == "waiting"
    assert events == ["begin", "commit"]
    repo.move_batch.assert_not_awaited()
    repo.finalize_merge.assert_not_awaited()


async def test_part_completed_is_not_operation_completion():
    service, repo, lease, _ = worker(MergeBatch(0, 0, 0, True))
    repo.finalize_merge.return_value = False
    assert await service.tick(lease.equestrian_id, owner=lease.owner) == "progress"
    repo.yield_merge.assert_awaited_once_with(lease)


@pytest.mark.parametrize("overflow", [False, True])
async def test_failure_recorded_only_after_effect_rollback_in_fresh_transaction(overflow):
    service, repo, lease, events = worker()
    repo.move_batch.side_effect = MergeOverflowError() if overflow else MergeStorageError()

    async def fail(*args, **kwargs):
        assert events == ["begin", "commit", "begin", "rollback", "begin"]
        return "failed_blocked" if overflow else "retry"

    repo.fail_merge.side_effect = fail
    assert await service.tick(lease.equestrian_id, owner=lease.owner) == ("failed_blocked" if overflow else "retry")
    repo.fail_merge.assert_awaited_once_with(lease, overflow=overflow, max_attempts=5)
    repo.finalize_merge.assert_not_awaited()
    repo.yield_merge.assert_not_awaited()
    assert events[-1] == "commit"


@pytest.mark.parametrize("phase", ["move_batch", "finalize_merge", "yield_merge", "fail_merge"])
async def test_preemption_or_expiry_never_finishes_or_retries_as_stale_owner(phase):
    service, repo, lease, events = worker(None)
    repo.finalize_merge.return_value = False
    if phase == "fail_merge":
        repo.move_batch.side_effect = MergeOverflowError()
    getattr(repo, phase).side_effect = MaintenanceLeaseLostError()
    assert await service.tick(lease.equestrian_id, owner=lease.owner) == "lease_lost"
    assert events[-1] == "rollback"
    if phase == "move_batch":
        repo.finalize_merge.assert_not_awaited()
    if phase != "fail_merge":
        repo.fail_merge.assert_not_awaited()


@pytest.mark.parametrize("error", [RuntimeError(), asyncio.CancelledError()])
async def test_crash_leaves_durable_lease_to_expire_not_unsafe_finalization(error):
    service, repo, lease, events = worker()
    repo.move_batch.side_effect = error
    with pytest.raises(type(error)):
        await service.tick(lease.equestrian_id, owner=lease.owner)
    assert events[-1] == "rollback"
    repo.yield_merge.assert_not_awaited()
    repo.finalize_merge.assert_not_awaited()


def sql(statement):
    return str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))


@pytest.mark.parametrize("kind", [WorkKind.PARSE, WorkKind.CLEANUP, WorkKind.MERGE])
async def test_claim_respects_cleanup_priority_reaps_expired_and_does_not_cancel_queued(kind):
    session = AsyncMock()
    repo = MergeWorkerRepository(session)
    repo.selected_kind = AsyncMock(return_value=kind)
    repo.claim = AsyncMock(return_value=None)
    tenant, owner = uuid4(), uuid4()
    with patch("repositories.metrica_merge_worker_repository.ParseTaskRepository") as tasks:
        tasks.return_value = AsyncMock()
        assert await repo.claim_merge(tenant, owner=owner, lease_seconds=20) is None
        assert tasks.call_count == int(kind == WorkKind.MERGE)
        if kind == WorkKind.MERGE:
            tasks.return_value.reap_expired_leases.assert_awaited_once_with(equestrian_id=tenant)
            tasks.return_value.cancel_queued_before_cleanup.assert_not_awaited()
            repo.claim.assert_awaited_once_with(tenant, owner=owner, lease_seconds=20)
        else:
            repo.claim.assert_not_awaited()


@pytest.mark.parametrize("part_count,finished", [(None, 0), (2, 1), (0, 0), (2, 2)])
async def test_finalizer_counts_in_database_no_mapping_load_and_atomic_correlated_outbox(part_count, finished):
    _, _, lease, _ = worker()
    session = AsyncMock()
    repo = MergeWorkerRepository(session)
    repo.assert_owned = AsyncMock()
    repo._operation = AsyncMock(return_value={"part_count": part_count, "normalization_revision": 8})
    repo.save_result, repo.finish_work = AsyncMock(), AsyncMock()
    session.scalar.return_value = finished
    complete = part_count is not None and finished == part_count
    assert await repo.finalize_merge(lease) == complete
    statement = sql(session.scalar.call_args.args[0])
    assert "count(*)" in statement and "mappings" not in statement and str(lease.equestrian_id) in statement
    if complete:
        result = repo.save_result.await_args.args[0]
        assert result.status == "completed" and result.part_count == part_count
        assert result.normalization_revision == 8
        assert result.message_id == maintenance_message_id(MERGE_COMPLETED, lease.operation_id, "completed")
        repo.finish_work.assert_awaited_once_with(lease, status="awaiting_seo", result_message_id=result.message_id)
    else:
        repo.save_result.assert_not_awaited()
        repo.finish_work.assert_not_awaited()
    session.commit.assert_not_awaited()


@pytest.mark.parametrize("overflow,attempts,max_attempts", [(True, 0, 5), (False, 0, 5), (False, 4, 5)])
async def test_failure_is_fenced_delayed_retry_or_durable_block_without_outbox(overflow, attempts, max_attempts):
    _, _, lease, _ = worker()
    session = AsyncMock()
    repo = MergeWorkerRepository(session)
    repo.assert_owned = AsyncMock()
    repo._operation = AsyncMock(return_value={"attempts": attempts})
    session.scalar.return_value = lease.operation_id
    blocked = overflow or attempts + 1 >= max_attempts
    assert await repo.fail_merge(lease, overflow=overflow, max_attempts=max_attempts) == (
        "failed_blocked" if blocked else "retry"
    )
    statement = session.scalar.call_args.args[0]
    query = sql(statement)
    assert "lease_expires_at > clock_timestamp()" in query and "fencing_revision + 1" in query
    assert str(lease.equestrian_id) in query and "lease_owner=NULL" in query
    assert statement.compile().params["attempts"] == attempts + 1
    assert statement.compile().params["status"] == ("failed_blocked" if blocked else "running")
    assert "outbox" not in query
    repo.assert_owned.assert_awaited_once_with(lease)


@pytest.mark.parametrize("state,error_class", [("22003", MergeOverflowError), ("40001", MergeStorageError)])
async def test_sql_errors_mapped_to_safe_domain_diagnostic(state, error_class):
    session = AsyncMock()
    repo = MergeWorkerRepository(session)
    _, _, lease, _ = worker()
    original = MagicMock(sqlstate=state)
    error = DBAPIError("sql", {"sensitive": "value"}, original)
    with patch.object(MergeBatchRepository, "move_batch", new=AsyncMock(side_effect=error)):
        with pytest.raises(error_class) as caught:
            await repo.move_batch(lease, batch_size=1, lease_seconds=20)
    assert "secret" not in str(caught.value) and "value" not in str(caught.value)


async def test_merge_di_real_handler_shared_session_and_dispatch_ready_no_io_or_activation():
    from containers.application import create_parse_services
    from settings import settings

    session = MagicMock()
    graph = create_parse_services(session)
    graph.celery_app.override(MagicMock())
    service = graph.merge_worker_service()
    assert isinstance(service, MergeWorkerService)
    assert isinstance(service._repository, MergeWorkerRepository)
    assert service._repository.session is service._transaction is session
    assert graph.maintenance_dispatch_service()._merge_available is True
    assert settings.maintenance_runtime_enabled is False
    session.execute.assert_not_called()


@pytest.mark.parametrize("changed", [None, "operation"])
async def test_yield_and_failure_reject_expired_release_cas(changed):
    _, _, lease, _ = worker()
    session = AsyncMock()
    repo = MergeWorkerRepository(session)
    repo.assert_owned = AsyncMock()
    session.scalar.return_value = changed
    if changed is None:
        with pytest.raises(MaintenanceLeaseLostError):
            await repo.yield_merge(lease)
    else:
        await repo.yield_merge(lease)
    query = sql(session.scalar.call_args.args[0])
    assert "lease_expires_at > clock_timestamp()" in query and "fencing_revision + 1" in query
    assert "attempts=0" in query and "error_code=NULL" in query


@pytest.mark.parametrize("superseded", [False, True])
async def test_correlated_finalized_only_completes_own_operation_and_preserves_other_barriers(superseded):
    _, _, lease, _ = worker()
    session = AsyncMock()
    repo = MergeWorkerRepository(session)
    message, cleanup = uuid4(), uuid4()
    repo._operation = AsyncMock(
        side_effect=[
            dict(
                kind="merge",
                status="awaiting_seo",
                normalization_revision=4,
                result_message_id=message,
                cleanup_operation_id=cleanup if superseded else None,
                part_count=2,
            ),
            dict(status="completed"),
        ]
    )
    assert await repo.finalize(
        lease.equestrian_id, lease.operation_id, normalization_revision=4, result_message_id=message
    )
    query = sql(session.execute.call_args.args[0])
    assert "status='completed'" in query
    assert str(lease.equestrian_id) in query and str(lease.operation_id) in query
    assert "tenant_gates" not in query and "chunks" not in query and "stats" not in query
    assert repo._operation.await_count == 1 + int(superseded)


async def test_prepared_merge_task_invokes_real_di_service_without_starting_runtime(monkeypatch):
    from types import SimpleNamespace

    from dependency_injector import providers

    from containers.application import create_parse_services
    from workers.tasks import metrica_maintenance as tasks

    service, repo, lease, events = worker()
    graph = create_parse_services(MagicMock())
    graph.repositories_container.session.override(providers.Object(service._transaction))
    graph.repositories_container.merge_worker_repository.override(providers.Object(repo))

    @asynccontextmanager
    async def session_scope():
        yield service._transaction

    monkeypatch.setattr(tasks, "settings", SimpleNamespace(maintenance_runtime_enabled=True))
    monkeypatch.setattr(tasks, "worker_session", session_scope)
    monkeypatch.setattr(tasks, "create_parse_services", lambda session: graph)
    assert await tasks._tick(lease.equestrian_id, merge=True) == "progress"
    repo.claim_merge.assert_awaited_once()
    assert repo.claim_merge.await_args.args == (lease.equestrian_id,)
    assert repo.claim_merge.await_args.kwargs["owner"] != lease.owner
    repo.move_batch.assert_awaited_once()
    assert events == ["begin", "commit"] * 3
