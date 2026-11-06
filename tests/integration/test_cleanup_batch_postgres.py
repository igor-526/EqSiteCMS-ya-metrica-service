"""IT-D-02 storage slice: real PG only, no operational worker acceptance."""

import os
from datetime import date, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities.metrica_maintenance import MaintenanceLeaseLostError, WorkKind
from core.services.metrica_cleanup_service import CleanupBatchService
from models import metrica_maintenance_operations as operations
from models import metrica_page_stats as pages
from models import metrica_parse_runs as runs
from models import metrica_parse_tasks as tasks
from models import metrica_search_query_stats as queries
from models import metrica_tenant_gates as gates
from repositories.metrica_cleanup_repository import CleanupBatchRepository

pytestmark = pytest.mark.infrastructure


@pytest.fixture(autouse=True)
def require_cleanup_test_database():
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.fail("BLOCKED: dedicated TEST_DATABASE_URL required; skips are not acceptance")


async def seed(session, *, size=7):
    tenant, foreign, operation = uuid4(), uuid4(), uuid4()
    repository = CleanupBatchRepository(session)
    await repository.save_operation(tenant, operation, kind=WorkKind.CLEANUP, cleanup_seq=1, binding_generation=1)
    for table, entity in ((pages, "page_id"), (queries, "search_query_id")):
        for identity, count in ((tenant, size), (foreign, 3)):
            if count:
                await session.execute(
                    table.insert(),
                    [
                        dict(
                            id=uuid4(),
                            equestrian_id=identity,
                            stat_date=date(2026, 1, 1),
                            **{entity: uuid4()},
                            visits=1,
                            users=1,
                            pageviews=1,
                            bounces=0,
                            visit_duration_seconds=1,
                        )
                        for _ in range(count)
                    ],
                )
    lease = await repository.claim(tenant, owner=uuid4(), lease_seconds=60)
    assert lease is not None
    await session.commit()
    return tenant, foreign, operation, lease


async def counts(session, tenant):
    return tuple(
        [
            await session.scalar(select(func.count()).select_from(table).where(table.c.equestrian_id == tenant))
            for table in (pages, queries)
        ]
    )


@pytest.mark.parametrize("size", [0, 6, 7])
async def test_cleanup_both_tables_multiple_bounded_commits_and_other_tenant(pg_engine, size):
    async with AsyncSession(pg_engine) as session:
        tenant, foreign, operation, lease = await seed(session, size=size)
        service = CleanupBatchService(
            repository=CleanupBatchRepository(session), transaction=session, batch_size=2, lease_seconds=60
        )
        deleted = 0
        for _ in range(12):
            outcome = await service.run_batch(lease)
            assert 0 <= outcome.deleted_rows <= 2
            deleted += outcome.deleted_rows
            if outcome.exhausted:
                break
        else:
            pytest.fail("cleanup did not exhaust within bounded number of batches")
        assert deleted == size * 2
        assert await counts(session, tenant) == (0, 0)
        assert await counts(session, foreign) == (3, 3)
        row = (await session.execute(select(operations).where(operations.c.id == operation))).mappings().one()
        assert row["checkpoint"] == {"table": "exhausted"}
        assert row["status"] == "deleting"  # Empty is NOT completed; no result published.
        assert row["result_message_id"] is None
        assert await session.scalar(select(gates.c.cleanup_completed_seq)) == 0


async def test_cleanup_crash_before_commit_rolls_back_effect_checkpoint_and_renewal(pg_engine):
    async with AsyncSession(pg_engine) as session:
        tenant, _, operation, lease = await seed(session)
        repository = CleanupBatchRepository(session)
        before = await session.scalar(select(operations.c.lease_expires_at).where(operations.c.id == operation))
        await session.rollback()
        with pytest.raises(RuntimeError):
            async with session.begin():
                await repository.delete_batch(lease, batch_size=2, lease_seconds=120)
                raise RuntimeError("simulated crash before commit")
        assert await counts(session, tenant) == (7, 7)
        row = (await session.execute(select(operations).where(operations.c.id == operation))).mappings().one()
        assert row["checkpoint"] == {} and row["lease_expires_at"] == before


async def test_cleanup_restart_after_commit_resumes_remaining_keys(pg_engine):
    async with AsyncSession(pg_engine) as session:
        tenant, _, _, lease = await seed(session)
        service = CleanupBatchService(repository=CleanupBatchRepository(session), transaction=session, batch_size=2)
        assert (await service.run_batch(lease)).deleted_rows == 2
    # Same durable lease, new process/session: do not repeat a key or skip a row.
    async with AsyncSession(pg_engine) as session:
        service = CleanupBatchService(repository=CleanupBatchRepository(session), transaction=session, batch_size=2)
        assert (await service.run_batch(lease)).deleted_rows == 2
        assert await counts(session, tenant) == (3, 7)


async def test_cleanup_new_owner_fences_stale_batch_and_other_tenant_lease(pg_engine):
    async with AsyncSession(pg_engine) as session:
        tenant, _, operation, old = await seed(session)
        await session.execute(
            operations.update()
            .where(operations.c.id == operation)
            .values(lease_expires_at=func.clock_timestamp() - timedelta(seconds=1))
        )
        await session.commit()
        repo = CleanupBatchRepository(session)
        new = await repo.claim(tenant, owner=uuid4(), lease_seconds=60)
        assert new is not None and new.revision > old.revision
        await session.commit()
        service = CleanupBatchService(repository=repo, transaction=session, batch_size=2)
        with pytest.raises(MaintenanceLeaseLostError):
            await service.run_batch(old)
        assert await counts(session, tenant) == (7, 7)
        await session.rollback()
        assert (await service.run_batch(new)).deleted_rows == 2


async def test_cleanup_expiry_after_effect_rolls_back_deleted_rows(pg_engine):
    class ExpiringRepository(CleanupBatchRepository):
        async def checkpoint(self, lease, *, checkpoint, lease_seconds):
            # Controlled DB-clock expiry between effect and checkpoint CAS.
            await self.session.execute(
                operations.update()
                .where(operations.c.id == lease.operation_id)
                .values(lease_expires_at=func.clock_timestamp() - timedelta(seconds=1))
            )
            await super().checkpoint(lease, checkpoint=checkpoint, lease_seconds=lease_seconds)

    async with AsyncSession(pg_engine) as session:
        tenant, _, operation, lease = await seed(session)
        service = CleanupBatchService(repository=ExpiringRepository(session), transaction=session, batch_size=2)
        with pytest.raises(MaintenanceLeaseLostError):
            await service.run_batch(lease)
        assert await counts(session, tenant) == (7, 7)
        assert await session.scalar(select(operations.c.checkpoint).where(operations.c.id == operation)) == {}


@pytest.mark.parametrize("expired", [False, True])
async def test_cleanup_running_even_expired_blocks_effect_until_terminal(pg_engine, expired):
    async with AsyncSession(pg_engine) as session:
        tenant, _, _, lease = await seed(session)
        run, task = uuid4(), uuid4()
        await session.execute(runs.insert().values(id=run, equestrian_id=tenant, date_mode="explicit"))
        await session.execute(
            tasks.insert().values(
                id=task,
                run_id=run,
                equestrian_id=tenant,
                counter_id="123",
                yandex_account_id=uuid4(),
                kind="pages",
                date_from=date(2026, 1, 1),
                date_to=date(2026, 1, 1),
                sequence=0,
                status="running",
                max_attempts=3,
                locked_by="old-worker",
                locked_until=func.clock_timestamp() + timedelta(seconds=-1 if expired else 60),
            )
        )
        await session.commit()
        service = CleanupBatchService(repository=CleanupBatchRepository(session), transaction=session, batch_size=2)
        with pytest.raises(MaintenanceLeaseLostError):
            await service.run_batch(lease)
        assert await counts(session, tenant) == (7, 7)
        await session.rollback()
        # Worker/recovery must terminalize first. Expiry by itself is NOT drain.
        await session.execute(tasks.update().where(tasks.c.id == task).values(status="cancelled"))
        await session.commit()
        assert (await service.run_batch(lease)).deleted_rows == 2
