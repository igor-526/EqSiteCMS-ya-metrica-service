"""Fresh parse/reconcile SQL contract after merge. No database acceptance here."""

from datetime import date
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql

from core.entities.metrica_metrics import BASE_METRICS
from core.entities.metrica_parse import ParseKind
from core.entities.metrica_stats import StatRow
from repositories.metrica_stats_repository import StatsRepository


@pytest.mark.parametrize("kind,entity", [(ParseKind.PAGES, "page_id"), (ParseKind.SEARCH_QUERIES, "search_query_id")])
async def test_fresh_parse_uses_sql_null_safe_replacement_not_merge_addition(kind, entity):
    session = AsyncMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = [False]
    session.execute.return_value = result
    repo = StatsRepository(session, kind)
    row = StatRow(
        equestrian_id=uuid4(),
        entity_id=uuid4(),
        parse_task_id=uuid4(),
        stat_date=date(2026, 1, 1),
        visits=4,
        users=2,
        pageviews=12,
        bounces=1,
        visit_duration_seconds=40,
    )
    counts = await repo.upsert([row])
    statement = session.execute.await_args.args[0]
    sql = str(statement.compile(dialect=postgresql.dialect()))
    table = repo.table.name
    assert f"ON CONFLICT (equestrian_id, stat_date, {entity})" in sql
    for metric in BASE_METRICS:
        # NULL parse_task_id from merge makes equality UNKNOWN, hence ELSE
        # replaces the aggregate. Only subsequent pages of this task add.
        assert (
            f"{metric} = CASE WHEN ({table}.parse_task_id = excluded.parse_task_id) "
            f"THEN {table}.{metric} + excluded.{metric} ELSE excluded.{metric} END"
        ) in sql
    assert "parse_task_id = excluded.parse_task_id" in sql
    assert "bounce_rate =" not in sql and "page_depth =" not in sql
    assert counts.inserted == 0 and counts.updated == 1
    session.commit.assert_not_called()


async def test_reconcile_removes_null_aggregate_only_in_own_tenant_and_window():
    session = AsyncMock()
    session.execute.return_value = MagicMock()
    session.execute.return_value.all.return_value = [(uuid4(),)]
    tenant, task = uuid4(), uuid4()
    start, end = date(2026, 1, 1), date(2026, 1, 4)
    assert (
        await StatsRepository(session, ParseKind.PAGES).reconcile_window(
            equestrian_id=tenant, date_from=start, date_to=end, parse_task_id=task
        )
        == 1  # noqa: W503
    )
    statement = session.execute.await_args.args[0]
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert "DELETE FROM metrica_page_stats" in sql
    assert "parse_task_id IS DISTINCT FROM" in sql  # NULL aggregates must be removed too
    assert "equestrian_id =" in sql and "stat_date BETWEEN" in sql
    assert set(statement.compile().params.values()) == {tenant, task, start, end}
    session.commit.assert_not_called()
