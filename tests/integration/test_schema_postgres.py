"""IT-MS-01/07: migrations and PostgreSQL-specific schema guarantees."""

from datetime import date
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlalchemy import inspect, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from models import metrica_page_stats, metrica_parse_runs, metrica_parse_tasks, metrica_search_query_stats
from utils.basemodel import metadata

CONFIG_PATH = Path(__file__).parents[2] / "src" / "alembic.ini"
TABLES = {"metrica_parse_runs", "metrica_parse_tasks", "metrica_page_stats", "metrica_search_query_stats"}


def _migrate(connection, target, *, downgrade=False):
    """Use Alembic's revision chain with the caller's isolated connection, not env.py settings."""
    script = ScriptDirectory.from_config(Config(str(CONFIG_PATH)))

    def steps(revision, context):
        if downgrade:
            return script._downgrade_revs(target, revision)
        return script._upgrade_revs(target, revision)

    context = MigrationContext.configure(connection, opts={"fn": steps})
    with Operations.context(context), context.begin_transaction():
        context.run_migrations()


async def test_it_ms_01_upgrade_downgrade_upgrade(pg_schema_engine):
    async with pg_schema_engine.begin() as connection:
        await connection.run_sync(lambda conn: _migrate(conn, "head"))
        assert set(await connection.run_sync(lambda conn: inspect(conn).get_table_names())) == TABLES | {
            "alembic_version"
        }
        assert await connection.scalar(text("SELECT version_num FROM alembic_version")) == "20261026_0002"
        assert (
            await connection.run_sync(lambda conn: compare_metadata(MigrationContext.configure(conn), metadata)) == []
        )

        def inspect_schema(conn):
            inspector = inspect(conn)
            for name, entity in (("metrica_page_stats", "page_id"), ("metrica_search_query_stats", "search_query_id")):
                columns = {column["name"]: column for column in inspector.get_columns(name)}
                assert set(columns) == {
                    "id",
                    "equestrian_id",
                    "stat_date",
                    entity,
                    "visits",
                    "users",
                    "pageviews",
                    "bounces",
                    "visit_duration_seconds",
                    "bounce_rate",
                    "page_depth",
                    "avg_visit_duration_seconds",
                    "parse_task_id",
                    "created_at",
                    "updated_at",
                }
                for rate in ("bounce_rate", "page_depth", "avg_visit_duration_seconds"):
                    assert columns[rate]["computed"]["persisted"] is True
                assert inspector.get_unique_constraints(name)[0]["column_names"] == [
                    "equestrian_id",
                    "stat_date",
                    entity,
                ]
                fk = inspector.get_foreign_keys(name)[0]
                assert fk["referred_table"] == "metrica_parse_tasks"
                assert fk["options"]["ondelete"] == "SET NULL"
                assert inspector.get_check_constraints(name)
            indexes = {index["name"]: index for index in inspector.get_indexes("metrica_parse_tasks")}
            running = indexes["ux_metrica_parse_tasks_running_account"]
            assert running["unique"] is True
            assert running["column_names"] == ["yandex_account_id"]
            assert "running" in str(running["dialect_options"]["postgresql_where"])
            assert indexes["ix_metrica_parse_tasks_claim"]["column_names"] == [
                "yandex_account_id",
                "next_attempt_at",
                "created_at",
                "sequence",
            ]
            assert "queued" in str(indexes["ix_metrica_parse_tasks_claim"]["dialect_options"]["postgresql_where"])
            assert indexes["ix_metrica_parse_tasks_active_eq"]["column_names"] == ["equestrian_id"]
            active_predicate = str(indexes["ix_metrica_parse_tasks_active_eq"]["dialect_options"]["postgresql_where"])
            assert "queued" in active_predicate and "running" in active_predicate
            assert inspector.get_foreign_keys("metrica_parse_tasks")[0]["options"]["ondelete"] == "CASCADE"
            assert inspector.get_unique_constraints("metrica_parse_tasks")[0]["column_names"] == ["run_id", "sequence"]

        await connection.run_sync(inspect_schema)
        await connection.run_sync(lambda conn: _migrate(conn, "base", downgrade=True))
        assert set(await connection.run_sync(lambda conn: inspect(conn).get_table_names())) == {"alembic_version"}
        assert await connection.scalar(text("SELECT count(*) FROM alembic_version")) == 0
        await connection.run_sync(lambda conn: _migrate(conn, "head"))
        assert set(await connection.run_sync(lambda conn: inspect(conn).get_table_names())) == TABLES | {
            "alembic_version"
        }


async def _run_and_task(session):
    run = dict(id=uuid4(), equestrian_id=uuid4(), date_mode="explicit")
    await session.execute(metrica_parse_runs.insert().values(**run))
    task = dict(
        id=uuid4(),
        run_id=run["id"],
        equestrian_id=run["equestrian_id"],
        counter_id="105850153",
        yandex_account_id=uuid4(),
        kind="pages",
        date_from=date(2026, 10, 21),
        date_to=date(2026, 10, 25),
        sequence=0,
        max_attempts=3,
    )
    await session.execute(metrica_parse_tasks.insert().values(**task))
    return run, task


@pytest.mark.parametrize(
    "table,entity", [(metrica_page_stats, "page_id"), (metrica_search_query_stats, "search_query_id")]
)
async def test_generated_values_uniqueness_and_delete_actions(pg_session, table, entity):
    run, task = await _run_and_task(pg_session)
    row = dict(
        equestrian_id=run["equestrian_id"],
        stat_date=date(2026, 10, 23),
        **{entity: uuid4()},
        visits=30,
        users=30,
        pageviews=59,
        bounces=5,
        visit_duration_seconds=3446,
        parse_task_id=task["id"],
    )
    result = (await pg_session.execute(table.insert().values(**row).returning(table))).mappings().one()
    assert result["bounce_rate"] == Decimal("16.66666667")
    assert result["page_depth"] == Decimal("1.96666667")
    assert result["avg_visit_duration_seconds"] == Decimal("114.86666667")
    assert result["created_at"].tzinfo is not None
    with pytest.raises(IntegrityError):
        async with pg_session.begin_nested():
            await pg_session.execute(table.insert().values(**row))
    await pg_session.execute(
        table.update().values(visits=0, users=0, bounces=0, pageviews=0, visit_duration_seconds=0)
    )
    result = (await pg_session.execute(select(table))).mappings().one()
    assert result["bounce_rate"] == result["page_depth"] == result["avg_visit_duration_seconds"] == 0
    assert result["updated_at"] is not None
    # A run deletion cascades to tasks and unlinks (does not delete) statistics.
    await pg_session.execute(metrica_parse_runs.delete().where(metrica_parse_runs.c.id == run["id"]))
    assert (await pg_session.execute(select(table.c.parse_task_id))).scalar_one() is None
    assert (await pg_session.execute(select(metrica_parse_tasks))).first() is None


@pytest.mark.parametrize(
    "table,entity", [(metrica_page_stats, "page_id"), (metrica_search_query_stats, "search_query_id")]
)
@pytest.mark.parametrize(
    "changes",
    [
        {"visits": -1},
        {"users": -1},
        {"pageviews": -1},
        {"bounces": -1},
        {"visit_duration_seconds": -1},
        {"bounces": 2},
        {"bounce_rate": 1},
        {"page_depth": 1},
        {"avg_visit_duration_seconds": 1},
    ],
)
async def test_it_ms_07_invalid_stats_rejected(pg_session, table, entity, changes):
    row = dict(
        equestrian_id=uuid4(),
        stat_date=date(2026, 10, 21),
        **{entity: uuid4()},
        visits=1,
        users=1,
        pageviews=1,
        bounces=0,
        visit_duration_seconds=1,
    )
    with pytest.raises(DBAPIError):
        async with pg_session.begin_nested():
            await pg_session.execute(table.insert().values(**{**row, **changes}))
    assert (await pg_session.execute(select(table))).first() is None


@pytest.mark.parametrize(
    "changes",
    [
        {"date_to": date(2026, 10, 26)},
        {"date_to": date(2026, 10, 20)},
        {"status": "invalid"},
        {"kind": "invalid"},
        {"next_offset": 0},
        {"attempts": -1},
        {"max_attempts": 0},
        {"rows_fetched": -1},
        {"sequence": -1},
    ],
)
async def test_it_ms_07_invalid_tasks_rejected(pg_session, changes):
    _, task = await _run_and_task(pg_session)
    with pytest.raises(IntegrityError):
        async with pg_session.begin_nested():
            await pg_session.execute(
                metrica_parse_tasks.insert().values(**{**task, "id": uuid4(), "sequence": 1, **changes})
            )


async def test_running_account_and_run_sequence_constraints(pg_session):
    _, task = await _run_and_task(pg_session)
    with pytest.raises(IntegrityError):
        async with pg_session.begin_nested():
            await pg_session.execute(metrica_parse_tasks.insert().values(**{**task, "id": uuid4()}))
    await pg_session.execute(metrica_parse_tasks.update().values(status="running"))
    with pytest.raises(IntegrityError):
        async with pg_session.begin_nested():
            await pg_session.execute(
                metrica_parse_tasks.insert().values(**{**task, "id": uuid4(), "sequence": 1, "status": "running"})
            )
    await pg_session.execute(
        metrica_parse_tasks.insert().values(
            **{**task, "id": uuid4(), "sequence": 1, "status": "running", "yandex_account_id": uuid4()}
        )
    )
