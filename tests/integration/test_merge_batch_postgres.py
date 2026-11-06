"""IT-N-02 storage fixtures. Collection is NOT real-PostgreSQL acceptance."""

import os
from datetime import date, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities.metrica_maintenance import MaintenanceLeaseLostError, WorkKind
from core.entities.metrica_parse import ParseKind
from core.entities.metrica_stats import StatRow
from models import metrica_maintenance_chunks as chunks
from models import metrica_maintenance_operations as operations
from models import metrica_page_stats as pages
from models import metrica_parse_runs as runs
from models import metrica_parse_tasks as tasks
from models import metrica_search_query_stats as queries
from repositories.metrica_merge_repository import MergeBatchRepository
from repositories.metrica_stats_repository import StatsRepository

pytestmark = pytest.mark.infrastructure


@pytest.fixture(autouse=True)
def require_merge_database():
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.fail("BLOCKED: dedicated TEST_DATABASE_URL required; skips are not acceptance")


async def seed(session, *, visits=2, target_visits=8, dates=5, part_count=1):
    tenant, foreign, operation, source, target, second = (uuid4() for _ in range(6))
    repo = MergeBatchRepository(session)
    await repo.save_operation(tenant, operation, kind=WorkKind.MERGE, normalization_revision=1)
    await repo.save_chunk(
        tenant,
        operation,
        part_index=0,
        mappings=[
            {"source_page_id": str(source), "target_page_id": str(target)},
            {"source_page_id": str(second), "target_page_id": str(target)},
        ],
    )
    for part_index in range(1, part_count):
        await repo.save_chunk(
            tenant,
            operation,
            part_index=part_index,
            mappings=[{"source_page_id": str(uuid4()), "target_page_id": str(target)}],
        )
    await repo.seal(tenant, operation, part_count=part_count)
    old_run, old_task = uuid4(), uuid4()
    await session.execute(runs.insert().values(id=old_run, equestrian_id=tenant, date_mode="explicit"))
    await session.execute(
        tasks.insert().values(
            id=old_task,
            run_id=old_run,
            equestrian_id=tenant,
            counter_id="123",
            yandex_account_id=uuid4(),
            kind="pages",
            date_from=date(2026, 1, 1),
            date_to=date(2026, 1, 5),
            sequence=0,
            max_attempts=3,
            status="succeeded",
        )
    )
    for index in range(dates):
        await session.execute(
            pages.insert().values(
                equestrian_id=tenant,
                page_id=source,
                stat_date=date(2026, 1, index + 1),
                visits=visits,
                users=3,
                pageviews=9,
                bounces=1,
                visit_duration_seconds=200,
            )
        )
    for identity, page, value in ((tenant, target, target_visits), (tenant, second, 3), (foreign, source, 7)):
        await session.execute(
            pages.insert().values(
                equestrian_id=identity,
                page_id=page,
                stat_date=date(2026, 1, 1),
                parse_task_id=old_task if identity == tenant else None,
                visits=value,
                users=5,
                pageviews=8,
                bounces=0,
                visit_duration_seconds=80,
            )
        )
    await session.execute(
        queries.insert().values(
            equestrian_id=tenant,
            search_query_id=source,
            stat_date=date(2026, 1, 1),
            visits=1,
            users=1,
            pageviews=1,
            bounces=0,
            visit_duration_seconds=1,
        )
    )
    lease = await repo.claim(tenant, owner=uuid4(), lease_seconds=60)
    assert lease is not None
    await session.commit()
    return tenant, foreign, operation, source, target, lease


async def list_pages(session, tenant):
    return (
        (await session.execute(select(pages).where(pages.c.equestrian_id == tenant).order_by(pages.c.stat_date)))
        .mappings()
        .all()
    )


async def test_multiple_batches_two_sources_collision_generated_ratios_restart_and_retry(pg_engine):
    async with AsyncSession(pg_engine) as session:
        tenant, foreign, operation, source, target, lease = await seed(session)
        repo = MergeBatchRepository(session)
        async with session.begin():
            first = await repo.move_batch(lease, batch_size=2, lease_seconds=60)
            assert first is not None and first.moved_rows == 2
    # Simulated process restart: same durable lease + persisted checkpoint.
    async with AsyncSession(pg_engine) as session:
        repo = MergeBatchRepository(session)
        for _ in range(8):
            async with session.begin():
                result = await repo.move_batch(lease, batch_size=2, lease_seconds=60)
            if result is None:
                break
        else:
            pytest.fail("merge did not exhaust within bounded ticks")
        rows = await list_pages(session, tenant)
        assert len(rows) == 5 and {row["page_id"] for row in rows} == {target}
        row = rows[0]
        assert (row["visits"], row["users"], row["pageviews"], row["bounces"], row["visit_duration_seconds"]) == (
            13,
            13,
            25,
            1,
            360,
        )
        assert row["bounce_rate"] == Decimal("7.69230769")
        assert row["page_depth"] == Decimal("1.92307692")
        assert row["avg_visit_duration_seconds"] == Decimal("27.69230769")
        assert all(row["parse_task_id"] is None for row in rows)
        assert len(await list_pages(session, foreign)) == 1
        assert await session.scalar(select(func.count()).select_from(queries)) == 1
        assert await session.scalar(select(chunks.c.status).where(chunks.c.operation_id == operation)) == "completed"
        assert await session.scalar(select(operations.c.status).where(operations.c.id == operation)) == "running"
        # Merely finishing storage is not completion/result/gate release.
        await session.rollback()
        async with session.begin():
            assert await repo.move_batch(lease, batch_size=2, lease_seconds=60) is None
        assert rows == await list_pages(session, tenant)


@pytest.mark.parametrize("failure", ["crash", "expired_checkpoint"])
async def test_effect_and_checkpoint_rollback_together(pg_engine, failure):
    class ExpiringRepository(MergeBatchRepository):
        async def checkpoint_chunk(self, lease, **kwargs):
            await self.session.execute(
                operations.update()
                .where(operations.c.id == lease.operation_id)
                .values(lease_expires_at=func.clock_timestamp() - timedelta(seconds=1))
            )
            await super().checkpoint_chunk(lease, **kwargs)

    async with AsyncSession(pg_engine) as session:
        tenant, _, operation, _, _, lease = await seed(session)
        before = await list_pages(session, tenant)
        await session.rollback()
        repo = (ExpiringRepository if failure == "expired_checkpoint" else MergeBatchRepository)(session)
        with pytest.raises((RuntimeError, MaintenanceLeaseLostError)):
            async with session.begin():
                await repo.move_batch(lease, batch_size=2, lease_seconds=60)
                raise RuntimeError("simulated crash")
        assert before == await list_pages(session, tenant)
        assert await session.scalar(select(chunks.c.checkpoint).where(chunks.c.operation_id == operation)) == {}


async def test_stale_owner_fenced_before_move(pg_engine):
    async with AsyncSession(pg_engine) as session:
        tenant, _, operation, _, _, old = await seed(session)
        await session.execute(
            operations.update()
            .where(operations.c.id == operation)
            .values(lease_expires_at=func.clock_timestamp() - timedelta(seconds=1))
        )
        await session.commit()
        repo = MergeBatchRepository(session)
        new = await repo.claim(tenant, owner=uuid4(), lease_seconds=60)
        assert new is not None and new.revision > old.revision
        await session.commit()
        with pytest.raises(MaintenanceLeaseLostError):
            async with session.begin():
                await repo.move_batch(old, batch_size=2, lease_seconds=60)
        async with session.begin():
            result = await repo.move_batch(new, batch_size=2, lease_seconds=60)
            assert result is not None and result.moved_rows == 2


async def test_integer_overflow_rolls_back_target_source_and_checkpoint(pg_engine):
    async with AsyncSession(pg_engine) as session:
        tenant, _, operation, _, _, lease = await seed(session, visits=2**31 - 1)
        before = await list_pages(session, tenant)
        await session.rollback()
        with pytest.raises(DBAPIError):
            async with session.begin():
                await MergeBatchRepository(session).move_batch(lease, batch_size=2, lease_seconds=60)
        assert before == await list_pages(session, tenant)
        assert await session.scalar(select(chunks.c.checkpoint).where(chunks.c.operation_id == operation)) == {}


async def test_fresh_parse_replaces_merge_aggregate_and_reconciles_other_dates(pg_engine):
    async with AsyncSession(pg_engine) as session:
        tenant, _, _, _, target, lease = await seed(session)
        repo = MergeBatchRepository(session)
        for _ in range(8):
            async with session.begin():
                result = await repo.move_batch(lease, batch_size=2, lease_seconds=60)
            if result is None:
                break
        # Storage-only fixture: worker/finalized gate admission is verified later.
        run, task = uuid4(), uuid4()
        async with session.begin():
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
                    date_to=date(2026, 1, 5),
                    sequence=0,
                    max_attempts=3,
                )
            )
            stats = StatsRepository(session, ParseKind.PAGES)
            await stats.upsert(
                [
                    StatRow(
                        equestrian_id=tenant,
                        stat_date=date(2026, 1, 1),
                        entity_id=target,
                        parse_task_id=task,
                        visits=4,
                        users=2,
                        pageviews=12,
                        bounces=0,
                        visit_duration_seconds=40,
                    )
                ]
            )
            reconciled = await stats.reconcile_window(
                equestrian_id=tenant, date_from=date(2026, 1, 1), date_to=date(2026, 1, 5), parse_task_id=task
            )
            assert reconciled == 4
        rows = await list_pages(session, tenant)
        assert len(rows) == 1 and rows[0]["visits"] == 4 and rows[0]["parse_task_id"] == task
        assert rows[0]["page_depth"] == Decimal("3.00000000")


async def test_empty_source_mappings_are_success_without_synthetic_rows(pg_engine):
    async with AsyncSession(pg_engine) as session:
        tenant, _, _, _, target, lease = await seed(session, dates=0)
        repo = MergeBatchRepository(session)
        async with session.begin():
            empty = await repo.move_batch(lease, batch_size=2, lease_seconds=60)
            assert empty is not None and empty.moved_rows == 0 and not empty.part_completed
        async with session.begin():
            second = await repo.move_batch(lease, batch_size=2, lease_seconds=60)
            assert second is not None and second.moved_rows == 1 and second.part_completed
        rows = await list_pages(session, tenant)
        assert len(rows) == 1 and rows[0]["page_id"] == target and rows[0]["visits"] == 11
