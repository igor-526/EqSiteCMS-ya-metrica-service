"""IT-D-02 / UT-N-06 real transaction fixtures. Collection is not acceptance."""

import asyncio
from datetime import date, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities import DateMode, ParseKind, ParseRun, ParseTask
from core.entities.metrica_maintenance import (
    BindingLifecycle,
    MaintenanceConflictError,
    MaintenanceLeaseLostError,
    WorkKind,
)
from core.services.metrica_cleanup_service import CleanupBatchService, CleanupFinalizerService, CleanupWorkerService
from models import metrica_maintenance_operations as operations
from models import metrica_maintenance_outbox as outbox
from models import metrica_parse_tasks as tasks
from models import metrica_tenant_gates as gates
from repositories.metrica_cleanup_worker_repository import CleanupWorkerRepository
from repositories.metrica_maintenance_delivery_repository import MaintenanceDeliveryRepository
from repositories.metrica_parse_run_repository import ParseRunRepository
from repositories.metrica_parse_task_repository import ParseTaskRepository

pytestmark = pytest.mark.infrastructure


def worker(session):
    repo = CleanupWorkerRepository(session)
    return CleanupWorkerService(
        repository=repo,
        transaction=session,
        batch_service=CleanupBatchService(repository=repo, transaction=session, batch_size=1),
        finalizer=CleanupFinalizerService(repository=repo, transaction=session),
    )


@pytest.mark.parametrize("active", [False, True])
async def test_cleanup_immediately_reports_before_drain_and_fences_active_merge(pg_engine, active):
    tenant, merge_id, cleanup_id = uuid4(), uuid4(), uuid4()
    async with AsyncSession(pg_engine) as session:
        repo = MaintenanceDeliveryRepository(session)
        await repo.save_operation(tenant, merge_id, kind=WorkKind.MERGE, normalization_revision=1)
        await repo.seal(tenant, merge_id, part_count=0)
        lease = await repo.claim(tenant, owner=uuid4(), lease_seconds=60) if active else None
        await session.commit()
        async with session.begin():
            await repo.save_operation(tenant, cleanup_id, kind=WorkKind.CLEANUP, cleanup_seq=1, binding_generation=1)
        result = (await session.execute(select(outbox))).mappings().one()
        assert result["payload"]["status"] == "superseded_by_delete"
        assert result["payload"]["cleanup_operation_id"] == str(cleanup_id)
        assert await session.scalar(select(gates.c.cleanup_completed_seq)) == 0
        assert await session.scalar(select(operations.c.status).where(operations.c.id == merge_id)) == "awaiting_seo"
        await session.rollback()
        if lease:
            with pytest.raises(MaintenanceLeaseLostError):
                async with session.begin():
                    await repo.checkpoint(lease, checkpoint={"illegal": True}, lease_seconds=60)
        with pytest.raises(MaintenanceConflictError):
            async with session.begin():
                await repo.finalize(tenant, merge_id, normalization_revision=1, result_message_id=result["message_id"])
        # Empty pages then empty search table; one batch per call, no eager completion.
        assert await worker(session).tick(tenant, owner=uuid4()) == "progress"
        assert await worker(session).tick(tenant, owner=uuid4()) == "completed"
        assert await worker(session).tick(tenant, owner=uuid4()) == "waiting"
        assert await session.scalar(select(func.count()).select_from(outbox)) == 2


async def test_new_merge_during_cleanup_late_parts_never_execute_duplicate_report_stable(pg_engine):
    tenant, cleanup_id, merge_id = uuid4(), uuid4(), uuid4()
    async with AsyncSession(pg_engine) as session:
        repo = MaintenanceDeliveryRepository(session)
        await repo.save_operation(tenant, cleanup_id, kind=WorkKind.CLEANUP, cleanup_seq=1, binding_generation=1)
        await repo.save_operation(tenant, merge_id, kind=WorkKind.MERGE, normalization_revision=1)
        await session.commit()
        result = (await session.execute(select(outbox))).mappings().one()
        assert result["payload"]["part_count"] is None
        await session.rollback()
        async with session.begin():
            await repo.save_operation(tenant, merge_id, kind=WorkKind.MERGE, normalization_revision=1)
            await repo.save_chunk(
                tenant,
                merge_id,
                part_index=0,
                mappings=[{"source_page_id": str(uuid4()), "target_page_id": str(uuid4())}],
            )
            await repo.seal(tenant, merge_id, part_count=1)
        assert await session.scalar(select(func.count()).select_from(outbox)) == 1
        assert (await session.execute(select(outbox.c.payload))).scalar_one() == result["payload"]
        assert await session.scalar(select(operations.c.status).where(operations.c.id == merge_id)) == "awaiting_seo"


async def test_report_and_supersession_roll_back_with_failed_delivery(pg_engine):
    tenant, merge_id, cleanup_id = uuid4(), uuid4(), uuid4()
    async with AsyncSession(pg_engine) as session:
        repo = MaintenanceDeliveryRepository(session)
        await repo.save_operation(tenant, merge_id, kind=WorkKind.MERGE, normalization_revision=1)
        await session.commit()
        with pytest.raises(RuntimeError):
            async with session.begin():
                await repo.save_operation(
                    tenant, cleanup_id, kind=WorkKind.CLEANUP, cleanup_seq=1, binding_generation=1
                )
                raise RuntimeError("crash before inbox commit")
        assert not session.in_transaction()
        async with AsyncSession(pg_engine) as observer:
            assert await observer.scalar(select(func.count()).select_from(outbox)) == 0
            assert (
                await observer.scalar(select(operations.c.status).where(operations.c.id == merge_id)) == "collecting"
            )
            assert await observer.scalar(select(gates.c.cleanup_requested_seq)) == 0


async def test_running_drain_expired_terminal_cancellation_and_cleanup_restart(pg_engine):
    day = date(2026, 10, 23)
    async with AsyncSession(pg_engine) as session:
        run = ParseRun(equestrian_id=uuid4(), date_mode=DateMode.EXPLICIT)
        task = ParseTask(
            run_id=run.id,
            equestrian_id=run.equestrian_id,
            yandex_account_id=uuid4(),
            counter_id="123",
            kind=ParseKind.PAGES,
            date_from=day,
            date_to=day,
            sequence=0,
            binding_generation=1,
            max_attempts=3,
        )
        assert await ParseRunRepository(session).create_run_with_tasks(run, [task])
        task_repo = ParseTaskRepository(session)
        assert await task_repo.claim(
            task.yandex_account_id,
            equestrian_id=task.equestrian_id,
            lifecycle=BindingLifecycle(1, 0, 0, task.yandex_account_id, "123"),
            locked_by="parse",
            lease_seconds=120,
        )
        cleanup_id = uuid4()
        await MaintenanceDeliveryRepository(session).save_operation(
            task.equestrian_id, cleanup_id, kind=WorkKind.CLEANUP, cleanup_seq=1, binding_generation=1
        )
        await session.commit()
        assert await worker(session).tick(task.equestrian_id, owner=uuid4()) == "waiting"
        assert await session.scalar(select(operations.c.status).where(operations.c.id == cleanup_id)) == "draining"
        assert await session.scalar(select(tasks.c.status).where(tasks.c.id == task.id)) == "running"
        await session.execute(
            tasks.update()
            .where(tasks.c.id == task.id)
            .values(locked_until=func.clock_timestamp() - timedelta(seconds=1))
        )
        await session.commit()
        repo = CleanupWorkerRepository(session)
        async with session.begin():
            old = await repo.claim_cleanup(task.equestrian_id, owner=uuid4(), lease_seconds=60)
        assert old is not None
        assert await session.scalar(select(tasks.c.status).where(tasks.c.id == task.id)) == "cancelled"
        # Crash after claim. Restart may only recover after expiry, increasing the fence.
        await session.execute(
            operations.update()
            .where(operations.c.id == cleanup_id)
            .values(lease_expires_at=func.clock_timestamp() - timedelta(seconds=1))
        )
        await session.commit()
        assert await worker(session).tick(task.equestrian_id, owner=uuid4()) == "progress"
        with pytest.raises(MaintenanceLeaseLostError):
            async with session.begin():
                await repo.delete_batch(old, batch_size=1, lease_seconds=60)
        assert await worker(session).tick(task.equestrian_id, owner=uuid4()) == "completed"


async def test_bounded_ticks_both_tables_contiguous_cleanup_and_foreign_rows(pg_engine):
    from models import metrica_page_stats as pages
    from models import metrica_search_query_stats as queries

    tenant, foreign = uuid4(), uuid4()
    async with AsyncSession(pg_engine) as session:
        repo = MaintenanceDeliveryRepository(session)
        # Reordered command is durable, but cannot execute before the missing prefix.
        await repo.save_operation(tenant, uuid4(), kind=WorkKind.CLEANUP, cleanup_seq=2, binding_generation=2)
        await session.commit()
        assert await worker(session).tick(tenant, owner=uuid4()) == "waiting"
        await repo.save_operation(tenant, uuid4(), kind=WorkKind.CLEANUP, cleanup_seq=1, binding_generation=1)
        for table, entity in ((pages, "page_id"), (queries, "search_query_id")):
            for owner, count in ((tenant, 3), (foreign, 1)):
                for index in range(count):
                    await session.execute(
                        table.insert().values(
                            **{
                                "id": uuid4(),
                                "equestrian_id": owner,
                                entity: uuid4(),
                                "stat_date": date(2026, 1, index + 1),
                                "visits": 1,
                                "users": 1,
                                "pageviews": 1,
                                "bounces": 0,
                                "visit_duration_seconds": 1,
                            }
                        )
                    )
        await session.commit()
        previous, completed = 6, 0
        for _ in range(14):
            result = await worker(session).tick(tenant, owner=uuid4())
            remaining = sum(
                [
                    await session.scalar(
                        select(func.count()).select_from(table).where(table.c.equestrian_id == tenant)
                    )
                    for table in (pages, queries)
                ]
            )
            assert previous - remaining in (0, 1)
            previous = remaining
            completed += int(result == "completed")
            await session.rollback()
            if completed == 2:
                break
        assert completed == 2 and previous == 0
        assert await session.scalar(select(gates.c.cleanup_completed_seq).where(gates.c.equestrian_id == tenant)) == 2
        for table in (pages, queries):
            assert (
                await session.scalar(select(func.count()).select_from(table).where(table.c.equestrian_id == foreign))
                == 1  # noqa: W503
            )


@pytest.mark.parametrize("phase", ["batch", "finalizer"])
async def test_cleanup_failed_transaction_durable_attempts_backoff_and_blocked(pg_engine, phase):
    from sqlalchemy import text

    from models import metrica_page_stats as pages

    tenant, operation = uuid4(), uuid4()
    async with AsyncSession(pg_engine) as session:
        repo = CleanupWorkerRepository(session)
        async with session.begin():
            await repo.save_operation(tenant, operation, kind=WorkKind.CLEANUP, cleanup_seq=1, binding_generation=1)
            if phase == "batch":
                await session.execute(
                    pages.insert().values(
                        id=uuid4(),
                        equestrian_id=tenant,
                        stat_date=date(2026, 1, 1),
                        page_id=uuid4(),
                        visits=1,
                        users=1,
                        pageviews=1,
                        bounces=0,
                        visit_duration_seconds=1,
                    )
                )
        service = CleanupWorkerService(
            repository=repo,
            transaction=session,
            batch_service=CleanupBatchService(repository=repo, transaction=session, batch_size=1),
            finalizer=CleanupFinalizerService(repository=repo, transaction=session),
        )
        if phase == "finalizer":
            assert await service.tick(tenant, owner=uuid4()) == "progress"
        original = repo.delete_batch if phase == "batch" else repo.finalize_cleanup

        async def failed_effect(*args, **kwargs):
            await original(*args, **kwargs)
            # Real aborted PG transaction AFTER effect/checkpoint/outbox writes.
            await session.execute(text("SELECT 1 / 0"))

        if phase == "batch":
            repo.delete_batch = failed_effect
        else:
            repo.finalize_cleanup = failed_effect
        for attempt in range(1, 6):
            assert await service.tick(tenant, owner=uuid4()) == ("retry" if attempt < 5 else "failed_blocked")
            assert not session.in_transaction()  # failed effect rolled back; diagnostics committed
            # A brand-new session/connection on every attempt, never identity-map state.
            async with AsyncSession(pg_engine) as observer:
                row = (await observer.execute(select(operations).where(operations.c.id == operation))).mappings().one()
                assert row["attempts"] == attempt and row["error_code"] == "cleanup_storage_failed"
                assert row["lease_owner"] is row["lease_expires_at"] is None
                delay = await observer.scalar(select(row["next_attempt_at"] - func.clock_timestamp()))
                assert 0 < delay.total_seconds() <= min(60, 2 ** (attempt - 1))
                assert await observer.scalar(select(func.count()).select_from(pages)) == int(phase == "batch")
                assert await observer.scalar(select(func.count()).select_from(outbox)) == 0
                assert await observer.scalar(select(gates.c.cleanup_completed_seq)) == 0
                assert row["checkpoint"] == ({} if phase == "batch" else {"table": "exhausted"})
            assert await service.tick(tenant, owner=uuid4()) == "waiting"
            async with session.begin():
                await session.execute(
                    operations.update()
                    .where(operations.c.id == operation)
                    .values(next_attempt_at=func.clock_timestamp() - timedelta(seconds=1))
                )
        assert await service.tick(tenant, owner=uuid4()) == "waiting"
        async with session.begin():
            assert await repo.selected_kind(tenant) == WorkKind.CLEANUP
            assert await repo.claim(tenant, owner=uuid4(), lease_seconds=60) is None


@pytest.mark.parametrize("reason", ["expired", "reclaimed", "foreign"])
async def test_cleanup_failure_transition_rejects_stale_or_foreign_lease(pg_engine, reason):
    from dataclasses import replace

    tenant, operation = uuid4(), uuid4()
    async with AsyncSession(pg_engine) as session:
        repo = CleanupWorkerRepository(session)
        async with session.begin():
            await repo.save_operation(tenant, operation, kind=WorkKind.CLEANUP, cleanup_seq=1, binding_generation=1)
            lease = await repo.claim_cleanup(tenant, owner=uuid4(), lease_seconds=60)
        assert lease is not None
        if reason in {"expired", "reclaimed"}:
            async with session.begin():
                await session.execute(
                    operations.update()
                    .where(operations.c.id == operation)
                    .values(lease_expires_at=func.clock_timestamp() - timedelta(seconds=1))
                )
            if reason == "reclaimed":
                async with session.begin():
                    assert await repo.claim_cleanup(tenant, owner=uuid4(), lease_seconds=60) is not None
        else:
            lease = replace(lease, equestrian_id=uuid4())
        with pytest.raises(MaintenanceLeaseLostError):
            async with session.begin():
                await repo.fail_cleanup(lease, max_attempts=5)
        async with session.begin():
            row = (await session.execute(select(operations).where(operations.c.id == operation))).mappings().one()
            assert row["attempts"] == 0 and row["error_code"] is None


async def test_cleanup_two_sessions_contend_then_reclaim_fences_old_owner(pg_engine):
    """PG-02: actual advisory-lock wait, bounded release and independent recovery."""
    from sqlalchemy import text

    tenant, operation = uuid4(), uuid4()
    async with AsyncSession(pg_engine) as first, AsyncSession(pg_engine) as second:
        repo1, repo2 = CleanupWorkerRepository(first), CleanupWorkerRepository(second)
        async with first.begin():
            await repo1.save_operation(tenant, operation, kind=WorkKind.CLEANUP, cleanup_seq=1, binding_generation=1)
        entered = asyncio.Event()

        async def contend():
            async with second.begin():
                await second.execute(text("SET LOCAL statement_timeout = '4000'"))
                pid = await second.scalar(select(func.pg_backend_pid()))
                entered.set()
                lease = await repo2.claim_cleanup(tenant, owner=uuid4(), lease_seconds=60)
                return pid, lease

        async with asyncio.timeout(8):
            async with first.begin():
                pid1 = await first.scalar(select(func.pg_backend_pid()))
                old = await repo1.claim_cleanup(tenant, owner=uuid4(), lease_seconds=60)
                assert old is not None
                contender = asyncio.create_task(contend())
                try:
                    await entered.wait()
                    # Observe a real server lock wait, not an arbitrary sleep proving nothing.
                    for _ in range(100):
                        blocked = await first.scalar(
                            text(
                                "SELECT count(*) FROM pg_stat_activity "
                                "WHERE pid <> pg_backend_pid() AND wait_event = 'advisory' "
                                "AND pg_backend_pid() = ANY(pg_blocking_pids(pid))"
                            )
                        )
                        if blocked:
                            break
                        await asyncio.sleep(0.02)
                    assert blocked == 1 and not contender.done()
                except BaseException:
                    contender.cancel()
                    await asyncio.gather(contender, return_exceptions=True)
                    raise
            try:
                pid2, denied = await contender
                assert pid1 != pid2 and denied is None
                async with second.begin():
                    await second.execute(
                        operations.update()
                        .where(operations.c.id == operation)
                        .values(lease_expires_at=func.clock_timestamp() - timedelta(seconds=1))
                    )
                async with second.begin():
                    recovered = await repo2.claim_cleanup(tenant, owner=uuid4(), lease_seconds=60)
                assert recovered is not None and recovered.revision > old.revision
                with pytest.raises(MaintenanceLeaseLostError):
                    async with first.begin():
                        await repo1.fail_cleanup(old, max_attempts=5)
                async with first.begin():
                    row = (
                        (await first.execute(select(operations).where(operations.c.id == operation))).mappings().one()
                    )
                    assert row["lease_owner"] == recovered.owner
                    assert row["fencing_revision"] == recovered.revision
                    assert row["attempts"] == 0 and row["error_code"] is None
            finally:
                if not contender.done():
                    contender.cancel()
                await asyncio.gather(contender, return_exceptions=True)
