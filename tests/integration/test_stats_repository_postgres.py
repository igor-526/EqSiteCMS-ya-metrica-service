"""IT-MS-02..06: real PostgreSQL upsert, normalized collisions and isolation."""

from datetime import date, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import insert, select

from core.entities import ParseKind, StatRow
from models import metrica_parse_runs, metrica_parse_tasks
from repositories import StatsRepository

DAY = date(2026, 10, 23)


async def seed_task(session, equestrian_id, *, kind=ParseKind.PAGES):
    run_id, task_id = uuid4(), uuid4()
    await session.execute(
        insert(metrica_parse_runs).values(id=run_id, equestrian_id=equestrian_id, date_mode="explicit")
    )
    await session.execute(
        insert(metrica_parse_tasks).values(
            id=task_id,
            run_id=run_id,
            equestrian_id=equestrian_id,
            counter_id="123",
            yandex_account_id=uuid4(),
            kind=kind,
            date_from=DAY,
            date_to=DAY,
            sequence=0,
            max_attempts=3,
        )
    )
    return task_id


def stat(equestrian_id, task_id, *, entity_id=None, day=DAY, visits=3, users=3, pageviews=6, bounces=0, duration=300):
    return StatRow(
        equestrian_id=equestrian_id,
        parse_task_id=task_id,
        stat_date=day,
        entity_id=entity_id or uuid4(),
        visits=visits,
        users=users,
        pageviews=pageviews,
        bounces=bounces,
        visit_duration_seconds=duration,
    )


@pytest.mark.parametrize("kind", list(ParseKind))
async def test_it_ms_02_03_04_upsert_generated_and_replace(pg_session, kind):
    eq = uuid4()
    task = await seed_task(pg_session, eq, kind=kind)
    repo = StatsRepository(pg_session, kind)
    # /services/rides/tours?utm_source=a and /services/rides/tours -> same SEO UUID.
    first = stat(eq, task)
    zero = stat(eq, task, visits=0, users=0, pageviews=0, duration=0)
    assert await repo.upsert([]) == (0, 0)
    assert await repo.upsert([first, zero]) == (2, 0)
    other_page = stat(eq, task, entity_id=first.entity_id, visits=1, users=1, pageviews=1, bounces=1, duration=20)
    assert await repo.upsert([other_page]) == (0, 1)
    rows = (await pg_session.execute(select(repo.table))).mappings().all()
    merged = next(row for row in rows if row[repo.entity_column] == first.entity_id)
    assert (
        merged["visits"],
        merged["users"],
        merged["pageviews"],
        merged["bounces"],
        merged["visit_duration_seconds"],
    ) == (4, 4, 7, 1, 320)
    assert (merged["bounce_rate"], merged["page_depth"], merged["avg_visit_duration_seconds"]) == (
        Decimal("25"),
        Decimal("1.75"),
        Decimal("80"),
    )
    empty = next(row for row in rows if row[repo.entity_column] == zero.entity_id)
    assert (empty["bounce_rate"], empty["page_depth"], empty["avg_visit_duration_seconds"]) == (0, 0, 0)
    new_task = await seed_task(pg_session, eq, kind=kind)
    replacement = stat(
        eq, new_task, entity_id=first.entity_id, visits=30, users=30, pageviews=59, bounces=5, duration=3446
    )
    assert await repo.upsert([replacement]) == (0, 1)
    row = (await pg_session.execute(select(repo.table).where(repo.table.c.id == first.id))).mappings().one()
    assert row["visits"] == 30
    assert row["parse_task_id"] == new_task
    assert (row["bounce_rate"], row["page_depth"], row["avg_visit_duration_seconds"]) == (
        Decimal("16.66666667"),
        Decimal("1.96666667"),
        Decimal("114.86666667"),
    )
    assert len(await repo.list_for_equestrian(equestrian_id=eq, date_from=DAY, date_to=DAY)) == 2


@pytest.mark.parametrize("kind", list(ParseKind))
async def test_it_ms_05_reconcile_window(pg_session, kind):
    eq, foreign = uuid4(), uuid4()
    old, current = await seed_task(pg_session, eq), await seed_task(pg_session, eq)
    repo = StatsRepository(pg_session, kind)
    other_kind = StatsRepository(pg_session, ParseKind.SEARCH_QUERIES if kind == ParseKind.PAGES else ParseKind.PAGES)
    await repo.upsert(
        [stat(eq, old), stat(eq, current), stat(eq, old, day=DAY - timedelta(days=1)), stat(foreign, old)]
    )
    await other_kind.upsert([stat(eq, old)])
    # IS DISTINCT FROM must also remove rows with a deleted/null task reference.
    await pg_session.execute(
        insert(repo.table).values(
            equestrian_id=eq,
            stat_date=DAY,
            **{repo.entity_column: uuid4()},
            visits=0,
            users=0,
            pageviews=0,
            bounces=0,
            visit_duration_seconds=0,
        )
    )
    assert await repo.reconcile_window(equestrian_id=eq, date_from=DAY, date_to=DAY, parse_task_id=current) == 2
    assert len((await pg_session.execute(select(repo.table))).all()) == 3
    assert len((await pg_session.execute(select(other_kind.table))).all()) == 1


async def test_it_ms_06_isolation_and_dates(pg_session):
    eq, foreign = uuid4(), uuid4()
    task = await seed_task(pg_session, eq)
    pages = StatsRepository(pg_session, ParseKind.PAGES)
    queries = StatsRepository(pg_session, ParseKind.SEARCH_QUERIES)
    assert await pages.get_last_stat_date(eq) is None
    assert await queries.get_last_stat_date(eq) is None
    await pages.upsert([stat(eq, task), stat(foreign, task, day=DAY + timedelta(days=5))])
    await queries.upsert([stat(eq, task, day=DAY - timedelta(days=3))])
    assert await pages.get_last_stat_date(eq) == DAY
    assert await queries.get_last_stat_date(eq) == DAY - timedelta(days=3)
    own = await pages.list_for_equestrian(equestrian_id=eq, date_from=DAY, date_to=DAY + timedelta(days=9))
    assert len(own) == 1 and own[0].equestrian_id == eq


async def test_large_api_page_uses_atomic_sql_batches(pg_engine):
    """An API page larger than asyncpg's bind ceiling still belongs to one transaction."""
    from sqlalchemy import func
    from sqlalchemy.ext.asyncio import AsyncSession

    async with AsyncSession(pg_engine) as session:
        eq = uuid4()
        task = await seed_task(session, eq)
        await session.commit()
        repo = StatsRepository(session, ParseKind.PAGES)
        rows = [stat(eq, task) for _ in range(3500)]
        assert await repo.upsert(rows) == (3500, 0)
        await session.rollback()
        assert await session.scalar(select(func.count()).select_from(repo.table)) == 0
        assert await repo.upsert(rows) == (3500, 0)
        await session.commit()
        assert await repo.upsert(rows) == (0, 3500)
