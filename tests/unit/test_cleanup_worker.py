"""UT-D-05 / UT-N-06 bounded cleanup/drain and admission-time preemption."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql

from core.entities.metrica_maintenance import CleanupBatch, MaintenanceLease, MaintenanceLeaseLostError, WorkKind
from core.services.metrica_cleanup_service import CleanupWorkerService
from repositories.metrica_cleanup_worker_repository import CleanupWorkerRepository
from repositories.metrica_maintenance_delivery_repository import MaintenanceDeliveryRepository
from repositories.metrica_result_outbox_repository import MaintenanceResultOutboxRepository


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


def worker(exhausted=False, completed=True):
    events = []
    lease = MaintenanceLease(uuid4(), uuid4(), uuid4(), 1, WorkKind.CLEANUP, "deleting")
    repo, batch, finalizer = AsyncMock(), AsyncMock(), AsyncMock()
    repo.claim_cleanup.return_value = lease
    batch.run_batch.return_value = CleanupBatch("search_queries", 0, exhausted)
    batch.run_batch.side_effect = lambda *_: events.append("batch_committed") or batch.run_batch.return_value
    finalizer.finalize.return_value = completed
    service = CleanupWorkerService(
        repository=repo, transaction=Transaction(events), batch_service=batch, finalizer=finalizer
    )
    return service, repo, batch, finalizer, lease, events


@pytest.mark.parametrize("exhausted,completed", [(False, True), (True, False), (True, True)])
async def test_worker_one_batch_commits_claim_before_effect_and_fresh_finalizer(exhausted, completed):
    service, repo, batch, finalizer, lease, events = worker(exhausted, completed)
    result = await service.tick(lease.equestrian_id, owner=lease.owner)
    assert result == ("completed" if exhausted and completed else "progress")
    assert events[:3] == ["begin", "commit", "batch_committed"]
    batch.run_batch.assert_awaited_once_with(lease)
    assert finalizer.finalize.await_count == int(exhausted)
    assert repo.yield_cleanup.await_count == int(result == "progress")
    if result == "progress":
        assert events[-2:] == ["begin", "commit"]


async def test_worker_waits_without_deleting_or_finalizing_running_parse_or_blocked_head():
    service, repo, batch, finalizer, lease, events = worker()
    repo.claim_cleanup.return_value = None
    assert await service.tick(lease.equestrian_id, owner=lease.owner) == "waiting"
    assert events == ["begin", "commit"]
    batch.run_batch.assert_not_awaited()
    finalizer.finalize.assert_not_awaited()
    repo.yield_cleanup.assert_not_awaited()


@pytest.mark.parametrize("phase", ["batch", "finalize", "yield"])
async def test_worker_stale_lease_stops_without_more_effects(phase):
    service, repo, batch, finalizer, lease, events = worker(exhausted=True, completed=False)
    call = {"batch": batch.run_batch, "finalize": finalizer.finalize, "yield": repo.yield_cleanup}[phase]
    call.side_effect = MaintenanceLeaseLostError()
    assert await service.tick(lease.equestrian_id, owner=lease.owner) == "lease_lost"
    assert batch.run_batch.await_count == 1
    if phase == "batch":
        finalizer.finalize.assert_not_awaited()
    if phase != "yield":
        repo.yield_cleanup.assert_not_awaited()
    else:
        assert events[-1] == "rollback"


@pytest.mark.parametrize("phase", ["batch", "finalize", "yield"])
async def test_worker_failure_records_in_fresh_transaction(phase):
    service, repo, batch, finalizer, lease, events = worker(exhausted=True, completed=False)
    call = {"batch": batch.run_batch, "finalize": finalizer.finalize, "yield": repo.yield_cleanup}[phase]
    call.side_effect = RuntimeError("storage failed: sensitive details")
    repo.fail_cleanup.return_value = "retry"
    assert await service.tick(lease.equestrian_id, owner=lease.owner) == "retry"
    repo.fail_cleanup.assert_awaited_once_with(lease, max_attempts=5)
    assert events[-2:] == ["begin", "commit"]
    if phase == "yield":
        assert events[-4:] == ["begin", "rollback", "begin", "commit"]


async def test_worker_failure_transition_lease_loss_does_not_mask_fence():
    service, repo, batch, _, lease, events = worker()
    batch.run_batch.side_effect = RuntimeError("failed")
    repo.fail_cleanup.side_effect = MaintenanceLeaseLostError()
    assert await service.tick(lease.equestrian_id, owner=lease.owner) == "lease_lost"
    assert events[-1] == "rollback"


def sql(statement):
    return str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))


@pytest.mark.parametrize("running", [True, False])
async def test_cleanup_claim_cancels_old_queued_reaps_expired_then_waits_or_claims(running):
    tenant = uuid4()
    session = AsyncMock()
    row = {"id": uuid4(), "status": "pending", "cleanup_seq": 3}
    selected = MagicMock()
    selected.mappings.return_value.one_or_none.return_value = row
    session.execute.return_value = selected
    repo = CleanupWorkerRepository(session)
    repo.lock_scope = AsyncMock()
    repo._running_parse = AsyncMock(return_value=running)
    repo.claim = AsyncMock(return_value="lease")
    with (
        patch("repositories.metrica_cleanup_worker_repository.ParseTaskRepository") as tasks,
        patch("repositories.metrica_cleanup_worker_repository.PendingParseIntentRepository") as intents,
    ):
        tasks.return_value = AsyncMock()
        intents.return_value = AsyncMock()
        result = await repo.claim_cleanup(tenant, owner=uuid4(), lease_seconds=30)
        tasks.return_value.cancel_queued_before_cleanup.assert_awaited_once_with(tenant, cleanup_seq=3)
        intents.return_value.cancel_before_cleanup.assert_awaited_once_with(tenant, cleanup_seq=3)
        tasks.return_value.reap_expired_leases.assert_awaited_once_with(equestrian_id=tenant)
    assert result == (None if running else "lease")
    assert repo.claim.await_count == int(not running)
    query = sql(session.execute.call_args_list[0].args[0])
    assert str(tenant) in query and "LIMIT 1" in query and "ORDER BY" in query
    if running:
        transition = sql(session.execute.call_args.args[0])
        assert "status='draining'" in transition and str(tenant) in transition
        assert "('pending', 'draining')" in transition
    session.commit.assert_not_awaited()


@pytest.mark.parametrize("changed", [None, "id"])
async def test_yield_is_fenced_cas_and_invalidates_old_owner(changed):
    session = AsyncMock()
    repo = CleanupWorkerRepository(session)
    repo.assert_owned = AsyncMock()
    session.scalar.return_value = changed
    lease = worker()[4]
    if changed is None:
        with pytest.raises(MaintenanceLeaseLostError):
            await repo.yield_cleanup(lease)
    else:
        await repo.yield_cleanup(lease)
    query = sql(session.scalar.call_args.args[0])
    assert "fencing_revision + 1" in query and "lease_expires_at > clock_timestamp()" in query
    assert "lease_owner=NULL" in query and str(lease.equestrian_id) in query
    repo.assert_owned.assert_awaited_once_with(lease)


@pytest.mark.parametrize("kind", [WorkKind.CLEANUP, WorkKind.MERGE])
@pytest.mark.parametrize("part_count", [None, 2])
async def test_admission_immediate_supersession_correlated_result_atomic_no_stats(kind, part_count):
    tenant, operation, merge_id, cleanup_id = uuid4(), uuid4(), uuid4(), uuid4()
    session = AsyncMock()
    cleanup_result, merge_result = MagicMock(), MagicMock()
    cleanup_result.mappings.return_value.one_or_none.return_value = {"id": cleanup_id, "cleanup_seq": 2}
    merge_result.mappings.return_value.__iter__.return_value = iter(
        [{"id": merge_id, "normalization_revision": 7, "part_count": part_count}]
    )
    session.execute.side_effect = [cleanup_result, merge_result]
    repo = MaintenanceDeliveryRepository(session)
    repo.supersede = AsyncMock(return_value=True)
    repo.save_result = AsyncMock()
    repo.record_superseded_result = AsyncMock()
    with patch.object(MaintenanceResultOutboxRepository, "save_operation", new=AsyncMock(return_value=True)):
        assert await repo.save_operation(tenant, operation, kind=kind)
    repo.supersede.assert_awaited_once_with(tenant, merge_id, cleanup_operation=cleanup_id)
    result = repo.save_result.await_args.args[0]
    assert (result.equestrian_id, result.operation_id, result.normalization_revision) == (tenant, merge_id, 7)
    assert (result.cleanup_operation_id, result.cleanup_seq, result.part_count) == (cleanup_id, 2, part_count)
    assert result.status == "superseded_by_delete" and result.occurred_at <= datetime.now(UTC)
    repo.record_superseded_result.assert_awaited_once_with(tenant, merge_id, result_message_id=result.message_id)
    queries = [sql(call.args[0]) for call in session.execute.call_args_list]
    assert all(str(tenant) in query and "metrica_page_stats" not in query for query in queries)
    assert "awaiting_seo" not in queries[1]  # Already completed work awaits its original SEO result.
    assert (f"id = '{operation}'" in queries[1]) == (kind == WorkKind.MERGE)
    session.commit.assert_not_awaited()


async def test_delivery_di_uses_preempting_repository_worker_is_lazy_same_session():
    from containers.application import create_parse_services

    session = MagicMock()
    graph = create_parse_services(session)
    service = graph.cleanup_worker_service()
    delivery = graph.maintenance_delivery_service()
    assert isinstance(delivery._repository, MaintenanceDeliveryRepository)
    assert isinstance(service._repository, CleanupWorkerRepository)
    assert service._transaction is service._repository.session is service._batch_service._transaction is session
    assert service._finalizer._transaction is delivery._transaction is session
    session.execute.assert_not_called()


async def test_admission_without_pending_cleanup_does_not_report_or_preempt():
    session = AsyncMock()
    selected = MagicMock()
    selected.mappings.return_value.one_or_none.return_value = None
    session.execute.return_value = selected
    repo = MaintenanceDeliveryRepository(session)
    repo.supersede, repo.save_result = AsyncMock(), AsyncMock()
    with patch.object(MaintenanceResultOutboxRepository, "save_operation", new=AsyncMock(return_value=False)):
        assert not await repo.save_operation(uuid4(), uuid4(), kind=WorkKind.MERGE, normalization_revision=1)
    repo.supersede.assert_not_awaited()
    repo.save_result.assert_not_awaited()
    assert session.execute.await_count == 1


async def test_worker_cancellation_leaves_claim_for_recovery_without_unsafe_release():
    import asyncio

    service, repo, batch, finalizer, lease, _ = worker()
    batch.run_batch.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await service.tick(lease.equestrian_id, owner=lease.owner)
    repo.yield_cleanup.assert_not_awaited()
    finalizer.finalize.assert_not_awaited()
