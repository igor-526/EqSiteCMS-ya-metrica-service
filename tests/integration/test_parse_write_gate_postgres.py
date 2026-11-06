"""IT-D-02 fencing prerequisites; not cleanup batch/recovery acceptance."""

import os
from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities import DateMode, ParseKind, ParseRun, ParseTask, StatRow
from core.entities.metrica_maintenance import BindingLifecycle
from core.protocols.repositories.metrica_parse import LeaseLostError
from models import metrica_maintenance_operations as operations
from models import metrica_parse_tasks as tasks
from models import metrica_tenant_gates as gates
from repositories import ParseRunRepository, ParseTaskRepository, StatsRepository
from repositories.metrica_maintenance_repository import MaintenanceArbitrationRepository

pytestmark = pytest.mark.infrastructure
DAY = date(2026, 10, 23)


@pytest.fixture(autouse=True)
def require_database():
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.fail("BLOCKED: dedicated TEST_DATABASE_URL required; skips are not acceptance")


async def seed(session, kind):
    run = ParseRun(equestrian_id=uuid4(), date_mode=DateMode.EXPLICIT)
    task = ParseTask(
        run_id=run.id,
        equestrian_id=run.equestrian_id,
        yandex_account_id=uuid4(),
        counter_id="123",
        kind=kind,
        date_from=DAY,
        date_to=DAY,
        sequence=0,
        binding_generation=1,
        max_attempts=3,
    )
    assert await ParseRunRepository(session).create_run_with_tasks(run, [task])
    repo = ParseTaskRepository(session)
    claimed = await repo.claim(
        task.yandex_account_id,
        equestrian_id=task.equestrian_id,
        lifecycle=BindingLifecycle(1, 0, 0, task.yandex_account_id, "123"),
        locked_by="worker",
        lease_seconds=120,
    )
    assert claimed is not None
    return claimed


def stat(task, *, current=True):
    return StatRow(
        equestrian_id=task.equestrian_id,
        stat_date=DAY,
        entity_id=uuid4(),
        parse_task_id=task.id if current else None,
        visits=3,
        users=2,
        pageviews=4,
        bounces=1,
        visit_duration_seconds=30,
    )


@pytest.mark.parametrize("kind", list(ParseKind))
@pytest.mark.parametrize("maintenance", ["watermark", "cleanup", "merge"])
async def test_running_drains_pending_barriers_then_maintenance_can_claim(pg_session, kind, maintenance):
    task = await seed(pg_session, kind)
    if maintenance != "merge":
        await pg_session.execute(
            gates.update()
            .where(gates.c.equestrian_id == task.equestrian_id)
            .values(binding_generation=2, cleanup_requested_seq=1)
        )
    if maintenance != "watermark":
        values = (
            {"cleanup_seq": 1, "binding_generation": 1} if maintenance == "cleanup" else {"normalization_revision": 1}
        )
        await pg_session.execute(
            operations.insert().values(
                id=uuid4(),
                equestrian_id=task.equestrian_id,
                kind=maintenance,
                status="pending" if maintenance == "cleanup" else "waiting",
                **values,
            )
        )
    arbitration = MaintenanceArbitrationRepository(pg_session)
    assert await arbitration.claim(task.equestrian_id, owner=uuid4(), lease_seconds=30) is None
    repo, stats = ParseTaskRepository(pg_session), StatsRepository(pg_session, kind)
    await repo.assert_writable(task.id, locked_by="worker")
    await stats.upsert([stat(task)])
    await repo.save_checkpoint(task.id, locked_by="worker", next_offset=2, lease_seconds=120)
    await repo.assert_writable(task.id, locked_by="worker")
    assert (
        await stats.reconcile_window(
            equestrian_id=task.equestrian_id, date_from=DAY, date_to=DAY, parse_task_id=task.id
        )
        == 0  # noqa: W503
    )
    await repo.mark_succeeded(task.id, locked_by="worker")
    lease = await arbitration.claim(task.equestrian_id, owner=uuid4(), lease_seconds=30)
    assert (lease is None) == (maintenance == "watermark")


@pytest.mark.parametrize("kind", list(ParseKind))
@pytest.mark.parametrize("phase", ["page", "reconcile"])
async def test_expiry_after_effect_rolls_back_stats_and_checkpoint(pg_engine, kind, phase):
    async with AsyncSession(pg_engine) as session:
        task = await seed(session, kind)
        stats = StatsRepository(session, kind)
        # Baseline row must survive rollback of reconcile; page creates a new row.
        baseline = stat(task)
        await stats.upsert([baseline])
        await session.commit()
        repo = ParseTaskRepository(session)
        with pytest.raises(LeaseLostError):
            async with session.begin():
                await repo.assert_writable(task.id, locked_by="worker")
                if phase == "page":
                    await stats.upsert([stat(task)])
                else:
                    # Reconcile would delete baseline if its attribution changed.
                    await session.execute(update(stats.table).values(parse_task_id=None))
                    assert (
                        await stats.reconcile_window(
                            equestrian_id=task.equestrian_id, date_from=DAY, date_to=DAY, parse_task_id=task.id
                        )
                        == 1  # noqa: W503
                    )
                # Deterministic DB-clock lease loss after stats effects, before CAS.
                await session.execute(
                    tasks.update()
                    .where(tasks.c.id == task.id)
                    .values(locked_until=func.clock_timestamp() - timedelta(seconds=1))
                )
                if phase == "page":
                    await repo.save_checkpoint(task.id, locked_by="worker", next_offset=2, lease_seconds=120)
                else:
                    await repo.mark_succeeded(task.id, locked_by="worker")
        assert await session.scalar(select(func.count()).select_from(stats.table)) == 1
        row = (await session.execute(select(tasks).where(tasks.c.id == task.id))).mappings().one()
        assert row["status"] == "running" and row["next_offset"] == 1
        assert await session.scalar(select(stats.table.c.parse_task_id)) == task.id


@pytest.mark.parametrize("reason", ["expired", "owner", "terminal", "executing", "cleaned"])
async def test_late_worker_cannot_acquire_write_guard(pg_session, reason):
    task = await seed(pg_session, ParseKind.PAGES)
    if reason == "expired":
        await pg_session.execute(tasks.update().values(locked_until=func.clock_timestamp() - timedelta(seconds=1)))
    elif reason == "terminal":
        await pg_session.execute(tasks.update().values(status="cancelled", locked_by=None, locked_until=None))
    elif reason == "cleaned":
        await pg_session.execute(
            gates.update()
            .where(gates.c.equestrian_id == task.equestrian_id)
            .values(cleanup_requested_seq=1, cleanup_completed_seq=1, binding_generation=2)
        )
    elif reason == "executing":
        await pg_session.execute(
            operations.insert().values(
                id=uuid4(),
                equestrian_id=task.equestrian_id,
                kind="cleanup",
                status="deleting",
                cleanup_seq=1,
                binding_generation=1,
            )
        )
    with pytest.raises(LeaseLostError):
        await ParseTaskRepository(pg_session).assert_writable(
            task.id, locked_by="other" if reason == "owner" else "worker"
        )


async def test_guard_holds_tenant_lock_until_commit_without_global_lock(pg_engine):
    async with AsyncSession(pg_engine) as setup:
        task = await seed(setup, ParseKind.PAGES)
        await setup.commit()
    async with AsyncSession(pg_engine) as writer, AsyncSession(pg_engine) as maintenance:
        await ParseTaskRepository(writer).assert_writable(task.id, locked_by="worker")
        await maintenance.execute(text("SET LOCAL lock_timeout = '100ms'"))
        arbitration = MaintenanceArbitrationRepository(maintenance)
        await arbitration.lock_scope([uuid4()])  # unrelated tenant is not blocked
        with pytest.raises(DBAPIError) as failure:
            await arbitration.lock_scope([task.equestrian_id])
        assert getattr(failure.value.orig, "sqlstate", None) == "55P03"
        await maintenance.rollback()
        await writer.commit()
        await arbitration.lock_scope([task.equestrian_id])


@pytest.mark.parametrize("method", ["retry", "reap"])
@pytest.mark.parametrize("reason", ["watermark", "generation", "legacy", "merge", "ordinary"])
async def test_terminal_recovery_preserves_audit_and_unblocks_maintenance(pg_session, method, reason):
    from core.entities import ParseErrorCode

    task = await seed(pg_session, ParseKind.PAGES)
    repo = ParseTaskRepository(pg_session)
    await repo.save_checkpoint(task.id, locked_by="worker", next_offset=101, lease_seconds=120, rows_fetched=100)
    lifecycle = BindingLifecycle(1, 0, 0, task.yandex_account_id, "123")
    if reason == "watermark":
        lifecycle = BindingLifecycle(2, 1, 0, None, None)
    elif reason == "generation":
        lifecycle = BindingLifecycle(2, 0, 0, task.yandex_account_id, "123")
    elif reason == "legacy":
        await pg_session.execute(tasks.update().where(tasks.c.id == task.id).values(binding_generation=0))
    if reason in {"watermark", "merge"}:
        await pg_session.execute(
            operations.insert().values(
                id=uuid4(),
                equestrian_id=task.equestrian_id,
                kind="cleanup" if reason == "watermark" else "merge",
                status="pending" if reason == "watermark" else "waiting",
                **(
                    {"cleanup_seq": 1, "binding_generation": 1}
                    if reason == "watermark"
                    else {"normalization_revision": 1}
                ),
            )
        )
    if method == "retry":
        await repo.schedule_retry(
            task.id,
            locked_by="worker",
            next_attempt_at=datetime.now(UTC),
            error_code=ParseErrorCode.PROVIDER_UNAVAILABLE,
            error_status=503,
            lifecycle=lifecycle,
        )
    else:
        await pg_session.execute(
            tasks.update()
            .where(tasks.c.id == task.id)
            .values(locked_until=func.clock_timestamp() - timedelta(seconds=1))
        )
        assert await repo.reap_expired_leases(equestrian_id=task.equestrian_id, lifecycle=lifecycle) == 1
        assert await repo.reap_expired_leases(equestrian_id=task.equestrian_id, lifecycle=lifecycle) == 0
    row = (await pg_session.execute(select(tasks).where(tasks.c.id == task.id))).mappings().one()
    cancelled = reason not in {"ordinary", "merge"}
    assert row["status"] == ("cancelled" if cancelled else "queued")
    assert row["attempts"] == 1 and row["next_offset"] == 101 and row["rows_fetched"] == 100
    assert row["locked_by"] is None and row["locked_until"] is None
    assert (row["finished_at"] is not None) == cancelled
    assert row["error_code"] == ("provider_unavailable" if method == "retry" else "lease_expired")
    with pytest.raises(LeaseLostError):
        await repo.assert_writable(task.id, locked_by="worker")
    if reason in {"watermark", "merge"}:
        assert (
            await MaintenanceArbitrationRepository(pg_session).claim(
                task.equestrian_id, owner=uuid4(), lease_seconds=30
            )
            is not None
        )


async def test_legacy_null_lease_recovery_is_terminal_without_touching_live_other_tenant(pg_session):
    legacy = await seed(pg_session, ParseKind.PAGES)
    other = await seed(pg_session, ParseKind.SEARCH_QUERIES)
    await pg_session.execute(
        tasks.update().where(tasks.c.id == legacy.id).values(binding_generation=0, locked_until=None)
    )
    repo = ParseTaskRepository(pg_session)
    assert legacy.equestrian_id in await repo.list_recovery_tenants()
    assert other.equestrian_id not in await repo.list_recovery_tenants()
    assert await repo.reap_expired_leases() == 1
    assert await pg_session.scalar(select(tasks.c.status).where(tasks.c.id == legacy.id)) == "cancelled"
    assert await pg_session.scalar(select(tasks.c.status).where(tasks.c.id == other.id)) == "running"
    await repo.assert_writable(other.id, locked_by="worker")


async def test_recovery_observation_and_task_transition_rollback_together(pg_session):
    task = await seed(pg_session, ParseKind.PAGES)
    await pg_session.execute(
        tasks.update().where(tasks.c.id == task.id).values(locked_until=func.clock_timestamp() - timedelta(seconds=1))
    )
    repo = ParseTaskRepository(pg_session)
    with pytest.raises(RuntimeError):
        async with pg_session.begin_nested():
            assert (
                await repo.reap_expired_leases(
                    equestrian_id=task.equestrian_id, lifecycle=BindingLifecycle(2, 1, 0, None, None)
                )
                == 1  # noqa: W503
            )
            raise RuntimeError("simulated crash before commit")
    assert await pg_session.scalar(select(tasks.c.status).where(tasks.c.id == task.id)) == "running"
    assert (
        await pg_session.scalar(
            select(gates.c.cleanup_requested_seq).where(gates.c.equestrian_id == task.equestrian_id)
        )
        == 0  # noqa: W503
    )
    assert (
        await repo.reap_expired_leases(
            equestrian_id=task.equestrian_id, lifecycle=BindingLifecycle(2, 1, 0, None, None)
        )
        == 1  # noqa: W503
    )
