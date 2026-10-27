"""IT-MQ-02..04: query-only SQL aggregates, isolation and bounded reads."""

import os
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Numeric, case, cast, func, insert
from sqlalchemy.dialects import postgresql

from core.entities.metrica_stats_query import Direction, Metric, StatsBucket
from models import metrica_page_stats, metrica_parse_runs, metrica_parse_tasks, metrica_search_query_stats
from repositories.page_stats_query_repository import METRIC_EXPRESSIONS as PAGE_EXPRESSIONS
from repositories.search_query_stats_query_repository import SearchQueryStatsQueryRepository

DAY = date(2026, 10, 5)
EXPECTED = [100, 80, 208, 1, 1090, Decimal("1"), Decimal("2.08"), Decimal("10.9")]


async def seed(session, eq, query, day=DAY, *, visits=30, users=30, pageviews=59, bounces=5, duration=3446, task=None):
    await session.execute(
        insert(metrica_search_query_stats).values(
            equestrian_id=eq,
            search_query_id=query,
            stat_date=day,
            visits=visits,
            users=users,
            pageviews=pageviews,
            bounces=bounces,
            visit_duration_seconds=duration,
            parse_task_id=task,
        )
    )


def assert_query_sql(statements):
    assert len(statements) == 1
    sql = statements[0].lower()
    assert "metrica_search_query_stats" in sql
    for forbidden in ("metrica_page_stats", "metrica_parse_tasks", "metrica_parse_runs", "counters"):
        assert forbidden not in sql
    return sql


def evidence(name, content):
    directory = os.environ.get("MQ_SQL_EVIDENCE_DIR")
    if directory:
        Path(directory, name).write_text(content)


@pytest.mark.parametrize("metric,expected", list(zip(Metric, EXPECTED, strict=True)))
async def test_it_mq_02_weighted_zero_sparse(pg_session, sql_statements, metric, expected):
    eq, query, zero, missing = [uuid4() for _ in range(4)]
    await seed(pg_session, eq, query, visits=99, users=79, pageviews=198, bounces=0, duration=990)
    await seed(
        pg_session, eq, query, DAY + timedelta(days=1), visits=1, users=1, pageviews=10, bounces=1, duration=100
    )
    await seed(pg_session, eq, zero, visits=0, users=0, pageviews=0, bounces=0, duration=0)
    sql_statements.clear()
    result = await SearchQueryStatsQueryRepository(pg_session).table(
        equestrian_id=eq,
        metric=metric,
        buckets=[
            StatsBucket(DAY, DAY + timedelta(days=1)),
            StatsBucket(DAY + timedelta(days=3), DAY + timedelta(days=3)),
        ],
        search_query_ids=[query, zero, missing, query],
    )
    assert {(v.search_query_id, v.bucket): v.value for v in result} == {(query, DAY): expected, (zero, DAY): 0}
    assert len(result) == 2  # Sparse pairs and duplicate UUID input never multiply rows.
    assert all(isinstance(v.value, int if metric in list(Metric)[:5] else Decimal) for v in result)
    sql = assert_query_sql(sql_statements)
    evidence(f"weighted-{metric.value}.sql.txt", "M5: 1 statement; weighted + zero + missing + duplicate UUID\n" + sql)


@pytest.mark.parametrize(
    "metric,expected",
    [
        (Metric.VISITS, 4_000_000_000),
        (Metric.USERS, 4_000_000_000),
        (Metric.VISIT_DURATION_SECONDS, 2_000_000_000_000),
        (Metric.BOUNCE_RATE, Decimal("33.33333333")),
        (Metric.PAGE_DEPTH, Decimal("1.66666667")),
        (Metric.AVG_VISIT_DURATION_SECONDS, Decimal("0.33333333")),
    ],
)
async def test_it_mq_02_large_sums_and_rounding(pg_session, metric, expected):
    eq, query = uuid4(), uuid4()
    for offset in (0, 1):
        if metric in list(Metric)[:5]:
            await seed(
                pg_session,
                eq,
                query,
                DAY + timedelta(days=offset),
                visits=2_000_000_000,
                users=2_000_000_000,
                pageviews=2_000_000_000,
                bounces=0,
                duration=1_000_000_000_000,
            )
        else:
            await seed(
                pg_session,
                eq,
                query,
                DAY + timedelta(days=offset),
                visits=3,
                users=1,
                pageviews=5,
                bounces=1,
                duration=1,
            )
    result = await SearchQueryStatsQueryRepository(pg_session).table(
        equestrian_id=eq,
        metric=metric,
        buckets=[StatsBucket(DAY, DAY + timedelta(days=1))],
        search_query_ids=[query],
    )
    assert result[0].value == expected
    assert isinstance(result[0].value, int if metric in list(Metric)[:5] else Decimal)


async def test_it_mq_03_tenant_tasks_and_exact_dates(pg_session, sql_statements):
    eq, foreign, query, missing, excluded, run = [uuid4() for _ in range(6)]
    tasks = [uuid4(), uuid4()]
    await pg_session.execute(insert(metrica_parse_runs).values(id=run, equestrian_id=eq, date_mode="explicit"))
    for sequence, task in enumerate(tasks):
        await pg_session.execute(
            insert(metrica_parse_tasks).values(
                id=task,
                run_id=run,
                equestrian_id=eq,
                counter_id=str(sequence),
                yandex_account_id=uuid4(),
                kind="search_queries",
                date_from=DAY,
                date_to=DAY + timedelta(days=1),
                sequence=sequence,
                max_attempts=3,
            )
        )
        await seed(pg_session, eq, query, DAY + timedelta(days=sequence), task=task)
    for offset in (-1, 2, 3, 5):
        await seed(pg_session, eq, query, DAY + timedelta(days=offset))
    await seed(pg_session, foreign, query, visits=900)
    await seed(pg_session, foreign, missing)
    await seed(pg_session, eq, excluded)
    buckets = [
        StatsBucket(DAY, DAY + timedelta(days=1)),
        StatsBucket(DAY + timedelta(days=3), DAY + timedelta(days=4)),
    ]
    sql_statements.clear()
    result = await SearchQueryStatsQueryRepository(pg_session).table(
        equestrian_id=eq,
        metric=Metric.VISITS,
        buckets=buckets,
        search_query_ids=[query, missing],
    )
    assert [(v.search_query_id, v.bucket, v.value) for v in result] == [(query, DAY, 60), (query, buckets[1].key, 30)]
    assert_query_sql(sql_statements)


@pytest.mark.parametrize("metric", list(Metric))
@pytest.mark.parametrize("direction", list(Direction))
async def test_it_mq_04_numeric_rank_ties(pg_session, sql_statements, metric, direction):
    eq, foreign = uuid4(), uuid4()
    low, tie, high, outside = [UUID(int=n) for n in range(1, 5)]
    for query in (low, tie):
        for offset in (0, 1):
            await seed(
                pg_session,
                eq,
                query,
                DAY + timedelta(days=offset),
                visits=1,
                users=1,
                pageviews=1,
                bounces=0,
                duration=1,
            )
    await seed(pg_session, eq, high, visits=10, users=10, pageviews=30, bounces=5, duration=100)
    await seed(pg_session, foreign, low, visits=1000)
    await seed(pg_session, foreign, outside)
    await seed(pg_session, eq, outside, DAY - timedelta(days=1))
    await seed(pg_session, eq, outside, DAY + timedelta(days=2))
    repo = SearchQueryStatsQueryRepository(pg_session)
    sql_statements.clear()
    result = await repo.ranking(
        equestrian_id=eq, metric=metric, date_from=DAY, date_to=DAY + timedelta(days=1), direction=direction
    )
    assert result == ([low, tie, high] if direction == Direction.ASC else [high, low, tie])
    sql = assert_query_sql(sql_statements)
    assert " join " not in sql
    evidence(f"ranking-{metric.value}-{direction.value}.sql.txt", "M6: 1 statement; numeric + ties UUID asc\n" + sql)
    sql_statements.clear()
    assert (
        await repo.ranking(equestrian_id=uuid4(), metric=metric, date_from=DAY, date_to=DAY, direction=direction) == []
    )
    assert_query_sql(sql_statements)


@pytest.mark.parametrize("size,days", [(1, 1), (5, 1), (50, 31), (100, 31)])
async def test_it_mq_04_one_statement_at_scale(pg_session, sql_statements, size, days):
    eq = uuid4()
    queries = [uuid4() for _ in range(size)]
    buckets = [StatsBucket(DAY + timedelta(days=i), DAY + timedelta(days=i)) for i in range(days)]
    await pg_session.execute(
        insert(metrica_search_query_stats),
        [
            dict(
                equestrian_id=eq,
                search_query_id=query,
                stat_date=bucket.key,
                visits=1,
                users=1,
                pageviews=1,
                bounces=0,
                visit_duration_seconds=1,
            )
            for query in queries
            for bucket in buckets
        ],
    )
    repo = SearchQueryStatsQueryRepository(pg_session)
    sql_statements.clear()
    result = await repo.table(equestrian_id=eq, metric=Metric.VISITS, buckets=buckets, search_query_ids=queries)
    assert len(result) == size * days
    assert len({(v.search_query_id, v.bucket) for v in result}) == size * days
    assert all(v.value == 1 for v in result)
    table_sql = assert_query_sql(sql_statements)
    sql_statements.clear()
    result = await repo.ranking(
        equestrian_id=eq,
        metric=Metric.VISITS,
        date_from=buckets[0].key,
        date_to=buckets[-1].key,
        direction=Direction.DESC,
    )
    assert result == sorted(queries)
    ranking_sql = assert_query_sql(sql_statements)
    evidence(
        f"count-{size}x{days}.sql.txt",
        f"M5: 1 statement, {size} UUID x {days} buckets\n{table_sql}\nM6: 1 statement\n{ranking_sql}",
    )


def test_page_whitelist_sql_unchanged():
    """Compare helper output to the former page expressions, including numeric casts."""
    stats = metrica_page_stats.c
    visits = func.sum(stats.visits)
    original = {metric: func.sum(stats[metric.value]) for metric in list(Metric)[:5]}
    for metric, numerator in (
        (Metric.BOUNCE_RATE, func.sum(stats.bounces) * 100),
        (Metric.PAGE_DEPTH, func.sum(stats.pageviews)),
        (Metric.AVG_VISIT_DURATION_SECONDS, func.sum(stats.visit_duration_seconds)),
    ):
        original[metric] = case((visits > 0, func.round(cast(numerator, Numeric) / visits, 8)), else_=0)
    assert set(original) == set(PAGE_EXPRESSIONS) == set(Metric)
    for metric in Metric:
        before = original[metric].compile(dialect=postgresql.dialect())
        after = PAGE_EXPRESSIONS[metric].compile(dialect=postgresql.dialect())
        assert str(before) == str(after)
        assert before.params == after.params
        assert type(original[metric].type) is type(PAGE_EXPRESSIONS[metric].type)
