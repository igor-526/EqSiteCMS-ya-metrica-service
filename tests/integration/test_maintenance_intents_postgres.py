"""Real PostgreSQL admission prerequisites for IT-D-02; not batch/worker acceptance."""

import os
from datetime import date
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities import DateMode, ParseKind, ParseRun, ParseTask, TaskStatus
from core.entities.metrica_maintenance import (
    BindingLifecycle,
    IntentAdmission,
    MaintenanceConflictError,
    PendingParseIntent,
    WorkKind,
)
from models import metrica_parse_runs as runs
from models import metrica_parse_tasks as tasks_table
from models import metrica_pending_parse_intents as intents
from repositories.metrica_maintenance_storage_repository import MaintenanceStorageRepository
from repositories.metrica_pending_parse_repository import PendingParseIntentRepository

pytestmark = pytest.mark.infrastructure


@pytest.fixture(autouse=True)
def require_maintenance_test_database():
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.fail("BLOCKED: dedicated TEST_DATABASE_URL required; skips are not acceptance")


def resolved(intent, lifecycle):
    run = ParseRun(
        job_id=intent.job_id,
        equestrian_id=intent.equestrian_id,
        date_mode=DateMode(intent.date_mode),
        binding_generation=lifecycle.binding_generation,
        observed_cleanup_requested_seq=lifecycle.cleanup_requested_seq,
    )
    tasks = [
        ParseTask(
            run_id=run.id,
            equestrian_id=run.equestrian_id,
            yandex_account_id=lifecycle.account_id,
            counter_id=lifecycle.counter_id,
            binding_generation=run.binding_generation,
            observed_cleanup_requested_seq=run.observed_cleanup_requested_seq,
            kind=ParseKind.PAGES,
            date_from=date(2026, 1, 1),
            date_to=date(2026, 1, 1),
            sequence=0,
            max_attempts=3,
        )
    ]
    return run, tasks


async def test_maintenance_intent_deferred_until_local_and_authoritative_cleanup_complete(pg_session):
    tenant, operation, account = uuid4(), uuid4(), uuid4()
    intent = PendingParseIntent(uuid4(), tenant, "auto")
    snapshot = BindingLifecycle(2, 1, 0, account, "42")
    repo = PendingParseIntentRepository(pg_session)
    assert await repo.enqueue_intent(intent, lifecycle=snapshot) == IntentAdmission.DEFERRED
    await pg_session.commit()  # ACK before local cleanup command delivery is safe
    assert await repo.ready_intents(limit=10) == [intent]
    assert await pg_session.scalar(select(func.count()).select_from(runs)) == 0
    assert await pg_session.scalar(select(intents.c.date_from)) is None
    complete = BindingLifecycle(2, 1, 1, account, "42")
    # A peer snapshot cannot manufacture LOCAL completion.
    assert await repo.prepare_admission(intent, lifecycle=complete) == IntentAdmission.DEFERRED
    storage = MaintenanceStorageRepository(pg_session)
    await storage.save_operation(tenant, operation, kind=WorkKind.CLEANUP, cleanup_seq=1, binding_generation=1)
    lease = await storage.claim(tenant, owner=uuid4(), lease_seconds=30)
    assert lease is not None
    await storage.finish_work(lease, status="completed", result_message_id=uuid4())
    await pg_session.commit()
    assert await repo.prepare_admission(intent, lifecycle=snapshot) == IntentAdmission.DEFERRED
    run, tasks = resolved(intent, complete)
    assert await repo.consume_intent(intent, lifecycle=complete, run=run, tasks=tasks) == IntentAdmission.ADMITTED
    await pg_session.commit()
    assert await repo.admitted_run(intent) == run.id
    assert await repo.enqueue_intent(intent, lifecycle=complete) == IntentAdmission.DUPLICATE
    assert await pg_session.scalar(select(func.count()).select_from(runs)) == 1


async def test_maintenance_intent_run_task_and_consumption_rollback_together(pg_session):
    intent = PendingParseIntent(uuid4(), uuid4(), "auto")
    snapshot = BindingLifecycle(1, 0, 0, uuid4(), "42")
    repo = PendingParseIntentRepository(pg_session)
    assert await repo.enqueue_intent(intent, lifecycle=snapshot) == IntentAdmission.READY
    await pg_session.commit()
    run, tasks = resolved(intent, snapshot)
    assert await repo.consume_intent(intent, lifecycle=snapshot, run=run, tasks=tasks) == IntentAdmission.ADMITTED
    await pg_session.rollback()  # crash before commit
    assert await pg_session.scalar(select(func.count()).select_from(runs)) == 0
    assert await pg_session.scalar(select(func.count()).select_from(tasks_table)) == 0
    assert await pg_session.scalar(select(intents.c.status)) == "pending"
    assert await repo.consume_intent(intent, lifecycle=snapshot, run=run, tasks=tasks) == IntentAdmission.ADMITTED
    await pg_session.commit()  # crash after commit/redelivery
    assert await repo.consume_intent(intent, lifecycle=snapshot, run=run, tasks=tasks) == IntentAdmission.DUPLICATE
    assert await pg_session.scalar(select(func.count()).select_from(tasks_table)) == 1


async def test_maintenance_intent_cleanup_cancels_old_not_post_delete_observation(pg_session):
    tenant = uuid4()
    repo = PendingParseIntentRepository(pg_session)
    old = PendingParseIntent(uuid4(), tenant, "auto")
    new = PendingParseIntent(uuid4(), tenant, "auto")
    await repo.enqueue_intent(old, lifecycle=BindingLifecycle(1, 0, 0, uuid4(), "42"))
    await repo.enqueue_intent(new, lifecycle=BindingLifecycle(2, 1, 0, uuid4(), "43"))
    await repo.cancel_before_cleanup(tenant, cleanup_seq=1)
    assert await pg_session.scalar(select(intents.c.status).where(intents.c.job_id == old.job_id)) == "cancelled"
    assert await pg_session.scalar(select(intents.c.status).where(intents.c.job_id == new.job_id)) == "pending"
    assert await repo.prepare_admission(old, lifecycle=BindingLifecycle(2, 1, 0)) == IntentAdmission.DUPLICATE


async def test_maintenance_intent_missing_binding_is_terminal_without_run(pg_session):
    intent = PendingParseIntent(uuid4(), uuid4(), "auto")
    repo = PendingParseIntentRepository(pg_session)
    await repo.enqueue_intent(intent, lifecycle=BindingLifecycle(1, 0, 0))
    assert await repo.prepare_admission(intent, lifecycle=BindingLifecycle(1, 0, 0)) == IntentAdmission.MISSING_BINDING
    await pg_session.commit()
    assert await repo.ready_intents(limit=10) == []
    assert await pg_session.scalar(select(intents.c.error_code)) == "binding_missing"
    assert await pg_session.scalar(select(func.count()).select_from(runs)) == 0


async def test_maintenance_intent_conflict_and_foreign_tenant_isolation(pg_engine):
    intent = PendingParseIntent(uuid4(), uuid4(), "auto")
    lifecycle = BindingLifecycle(1, 0, 0)
    async with AsyncSession(pg_engine) as first, AsyncSession(pg_engine) as second:
        a, b = PendingParseIntentRepository(first), PendingParseIntentRepository(second)
        await a.enqueue_intent(intent, lifecycle=lifecycle)
        await first.commit()
        for altered in [
            PendingParseIntent(intent.job_id, uuid4(), "auto"),
            PendingParseIntent(intent.job_id, intent.equestrian_id, "explicit", date(2026, 1, 1), date(2026, 1, 2)),
        ]:
            with pytest.raises(MaintenanceConflictError):
                await b.enqueue_intent(altered, lifecycle=lifecycle)
            await second.rollback()
        assert await a.enqueue_intent(intent, lifecycle=lifecycle) == IntentAdmission.DUPLICATE


async def test_maintenance_saved_intent_waits_active_run_but_new_command_rejected(pg_session):
    repo = PendingParseIntentRepository(pg_session)
    lifecycle = BindingLifecycle(1, 0, 0, uuid4(), "42")
    active = PendingParseIntent(uuid4(), uuid4(), "auto")
    deferred = PendingParseIntent(uuid4(), active.equestrian_id, "auto")
    await repo.enqueue_intent(active, lifecycle=lifecycle)
    await repo.enqueue_intent(deferred, lifecycle=lifecycle)
    run, tasks = resolved(active, lifecycle)
    await repo.consume_intent(active, lifecycle=lifecycle, run=run, tasks=tasks)
    await pg_session.commit()
    assert await repo.prepare_admission(deferred, lifecycle=lifecycle) == IntentAdmission.DEFERRED
    rejected = PendingParseIntent(uuid4(), active.equestrian_id, "auto")
    assert await repo.enqueue_intent(rejected, lifecycle=lifecycle) == IntentAdmission.ACTIVE_RUN
    assert await pg_session.scalar(select(intents.c.job_id).where(intents.c.job_id == rejected.job_id)) is None
    await pg_session.execute(
        tasks_table.update().where(tasks_table.c.run_id == run.id).values(status=TaskStatus.SUCCEEDED)
    )
    new_run, new_tasks = resolved(deferred, lifecycle)
    assert (
        await repo.consume_intent(deferred, lifecycle=lifecycle, run=new_run, tasks=new_tasks)
        == IntentAdmission.ADMITTED  # noqa: W503
    )


@pytest.mark.parametrize("stage", ["enqueue", "replay"])
@pytest.mark.parametrize("contender", ["duplicate", "foreign_tenant", "different_hash"])
async def test_concurrent_intent_to_run_admission_exactly_once(pg_engine, stage, contender):
    import asyncio

    intent = PendingParseIntent(uuid4(), uuid4(), "auto")
    lifecycle = BindingLifecycle(1, 0, 0, uuid4(), "42")
    other = intent
    if contender == "foreign_tenant":
        other = PendingParseIntent(intent.job_id, uuid4(), "auto")
    elif contender == "different_hash":
        other = PendingParseIntent(intent.job_id, intent.equestrian_id, "explicit", date(2026, 1, 1), date(2026, 1, 2))
    async with AsyncSession(pg_engine) as first:
        repo = PendingParseIntentRepository(first)
        assert await repo.enqueue_intent(intent, lifecycle=lifecycle) == IntentAdmission.READY
        if stage == "replay":
            await first.commit()
            assert await repo.prepare_admission(intent, lifecycle=lifecycle) == IntentAdmission.READY
        started = asyncio.Event()

        async def compete():
            async with AsyncSession(pg_engine) as second:
                duplicate = PendingParseIntentRepository(second)
                started.set()
                if contender != "duplicate":
                    with pytest.raises(MaintenanceConflictError):
                        await duplicate.enqueue_intent(other, lifecycle=lifecycle)
                    await second.rollback()
                    return None
                assert await duplicate.enqueue_intent(other, lifecycle=lifecycle) == IntentAdmission.DUPLICATE
                run, tasks = resolved(other, lifecycle)
                assert (
                    await duplicate.consume_intent(other, lifecycle=lifecycle, run=run, tasks=tasks)
                    == IntentAdmission.DUPLICATE  # noqa: W503
                )
                admitted = await duplicate.admitted_run(other)
                await second.commit()
                return admitted

        task = asyncio.create_task(compete())
        try:
            await started.wait()
            # Winner retains the tenant/identity transaction until commit.
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(task), timeout=0.1)
            run, tasks = resolved(intent, lifecycle)
            assert (
                await repo.consume_intent(intent, lifecycle=lifecycle, run=run, tasks=tasks)
                == IntentAdmission.ADMITTED  # noqa: W503
            )
            await first.commit()
            result = await asyncio.wait_for(task, timeout=5)
            assert result == (run.id if contender == "duplicate" else None)
        finally:
            await first.rollback()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        assert await first.scalar(select(func.count()).select_from(intents)) == 1
        assert await first.scalar(select(func.count()).select_from(runs)) == 1
        assert await first.scalar(select(func.count()).select_from(tasks_table)) == 1
        assert await first.scalar(select(runs.c.equestrian_id)) == intent.equestrian_id
        assert await repo.admitted_run(intent) == run.id
