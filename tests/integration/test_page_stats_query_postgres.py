"""IT-MP-01..06: actual PostgreSQL aggregation and constant statement count."""

import asyncio
import logging
import os
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import insert, select

from core.entities.metrica_stats_query import Direction, Metric, StatsBucket
from models import metrica_page_stats
from repositories import PageStatsQueryRepository

DAY = date(2026, 10, 5)
ADDITIVE = list(Metric)[:5]
DERIVED = list(Metric)[5:]


async def seed(session, eq, page, day=DAY, *, visits=30, users=30, pageviews=59, bounces=5, duration=3446):
    await session.execute(
        insert(metrica_page_stats).values(
            equestrian_id=eq,
            page_id=page,
            stat_date=day,
            visits=visits,
            users=users,
            pageviews=pageviews,
            bounces=bounces,
            visit_duration_seconds=duration,
        )
    )


@pytest.mark.parametrize("metric,total", zip(ADDITIVE, [60, 60, 118, 10, 6892], strict=True))
@pytest.mark.parametrize(
    "bounds",
    [
        (DAY, DAY + timedelta(days=6)),
        (date(2026, 10, 1), date(2026, 10, 31)),
        (date(2026, 1, 1), date(2026, 12, 31)),
    ],
)
async def test_it_mp_01_additive(pg_session, metric, total, bounds):
    eq, page = uuid4(), uuid4()
    await seed(pg_session, eq, page)
    await seed(pg_session, eq, page, DAY + timedelta(days=1))
    values = await PageStatsQueryRepository(pg_session).table(
        equestrian_id=eq,
        metric=metric,
        buckets=[StatsBucket(*bounds)],
        page_ids=[page],
    )
    assert len(values) == 1
    assert values[0].value == total and isinstance(values[0].value, int)
    assert values[0].bucket == bounds[0]


@pytest.mark.parametrize(
    "metric,expected", zip(DERIVED, [Decimal("1"), Decimal("1.99"), Decimal("99.2")], strict=True)
)
async def test_it_mp_02_weighted_and_zero(pg_session, metric, expected):
    eq, page, zero = uuid4(), uuid4(), uuid4()
    await seed(pg_session, eq, page, visits=99, users=99, pageviews=198, bounces=0, duration=9900)
    await seed(pg_session, eq, page, DAY + timedelta(days=1), visits=1, users=1, pageviews=1, bounces=1, duration=20)
    await seed(pg_session, eq, zero, visits=0, users=0, pageviews=0, bounces=0, duration=0)
    values = await PageStatsQueryRepository(pg_session).table(
        equestrian_id=eq,
        metric=metric,
        buckets=[StatsBucket(DAY, DAY + timedelta(days=1))],
        page_ids=[page, zero],
    )
    assert {v.page_id: v.value for v in values} == {page: expected, zero: 0}


@pytest.mark.parametrize("metric", list(Metric))
async def test_it_mp_03_daily_generated(pg_session, metric):
    eq, page = uuid4(), uuid4()
    await seed(pg_session, eq, page)
    expected = await pg_session.scalar(select(metrica_page_stats.c[metric.value]))
    values = await PageStatsQueryRepository(pg_session).table(
        equestrian_id=eq,
        metric=metric,
        buckets=[StatsBucket(DAY, DAY)],
        page_ids=[page],
    )
    assert values[0].value == expected


async def test_it_mp_04_isolation_sparse_exact_boundaries(pg_session):
    eq, foreign, page, missing, excluded = [uuid4() for _ in range(5)]
    for day in (
        DAY,
        DAY + timedelta(days=1),
        DAY + timedelta(days=2),
        DAY + timedelta(days=3),
        DAY + timedelta(days=5),
        DAY - timedelta(days=1),
    ):
        await seed(pg_session, eq, page, day)
    await seed(pg_session, foreign, page, visits=900)
    await seed(pg_session, foreign, missing)
    await seed(pg_session, eq, excluded)
    buckets = [
        StatsBucket(DAY, DAY + timedelta(days=1)),
        StatsBucket(DAY + timedelta(days=3), DAY + timedelta(days=4)),
    ]
    values = await PageStatsQueryRepository(pg_session).table(
        equestrian_id=eq,
        metric=Metric.VISITS,
        buckets=buckets,
        page_ids=[page, missing],
    )
    assert [(v.page_id, v.bucket, v.value) for v in values] == [(page, DAY, 60), (page, buckets[1].key, 30)]


@pytest.mark.parametrize("metric", list(Metric))
@pytest.mark.parametrize("direction", list(Direction))
async def test_it_mp_05_ranking(pg_session, metric, direction):
    eq, foreign = uuid4(), uuid4()
    low, tie, high, outside = [UUID(int=n) for n in range(1, 5)]
    for page in (low, tie):
        await seed(pg_session, eq, page, visits=1, users=1, pageviews=1, bounces=0, duration=1)
        await seed(
            pg_session, eq, page, DAY + timedelta(days=1), visits=1, users=1, pageviews=1, bounces=0, duration=1
        )
    await seed(pg_session, eq, high, visits=10, users=10, pageviews=30, bounces=5, duration=100)
    await seed(pg_session, foreign, low, visits=1000)
    await seed(pg_session, foreign, outside)
    await seed(pg_session, eq, outside, DAY + timedelta(days=2))
    result = await PageStatsQueryRepository(pg_session).ranking(
        equestrian_id=eq,
        metric=metric,
        date_from=DAY,
        date_to=DAY + timedelta(days=1),
        direction=direction,
    )
    assert result == ([low, tie, high] if direction == Direction.ASC else [high, low, tie])
    empty = await PageStatsQueryRepository(pg_session).ranking(
        equestrian_id=uuid4(),
        metric=metric,
        date_from=DAY,
        date_to=DAY,
        direction=direction,
    )
    assert empty == []


async def test_it_mp_06_one_statement(pg_session, sql_statements, caplog):
    eq = uuid4()
    pages = [uuid4() for _ in range(100)]
    buckets = [StatsBucket(DAY + timedelta(days=i), DAY + timedelta(days=i)) for i in range(31)]
    # All 3100 pairs exist; seed outside the measured window.
    await pg_session.execute(
        insert(metrica_page_stats),
        [
            dict(
                equestrian_id=eq,
                page_id=page,
                stat_date=bucket.key,
                visits=1,
                users=1,
                pageviews=1,
                bounces=0,
                visit_duration_seconds=1,
            )
            for page in pages
            for bucket in buckets
        ],
    )
    repo = PageStatsQueryRepository(pg_session)
    sql_statements.clear()
    caplog.clear()
    result = await repo.table(equestrian_id=eq, metric=Metric.VISITS, buckets=buckets, page_ids=pages)
    assert len(result) == 3100
    assert len(sql_statements) == 1 and "metrica_page_stats" in sql_statements[0]
    table_sql = caplog.text
    sql_statements.clear()
    caplog.clear()
    ranking = await repo.ranking(
        equestrian_id=eq,
        metric=Metric.VISITS,
        date_from=buckets[0].key,
        date_to=buckets[-1].key,
        direction=Direction.DESC,
    )
    assert ranking == sorted(pages)
    assert len(sql_statements) == 1 and "metrica_page_stats" in sql_statements[0]
    evidence = os.environ.get("MP_SQL_EVIDENCE")
    if evidence:
        await asyncio.to_thread(
            Path(evidence).write_text,
            "M3: 1 statement, 100 pages x 31 buckets\n" + table_sql + "\nM4: 1 statement\n" + caplog.text,
        )
    logging.getLogger(__name__).info("IT-MP-06: M3=1, M4=1 SQL statements")
