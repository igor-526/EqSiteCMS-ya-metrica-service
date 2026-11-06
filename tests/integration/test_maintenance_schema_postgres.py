"""Storage prerequisites for IT-D-02/IT-N-02, not their worker/batch acceptance."""

import os
from datetime import date
from uuid import uuid4

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from models import (
    metrica_maintenance_chunks,
    metrica_maintenance_operations,
    metrica_parse_runs,
    metrica_parse_tasks,
    metrica_pending_parse_intents,
    metrica_tenant_gates,
)
from tests.integration.test_schema_postgres import _migrate
from utils.basemodel import metadata

pytestmark = pytest.mark.infrastructure


@pytest.fixture(autouse=True)
def require_maintenance_test_database():
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.fail("BLOCKED: dedicated TEST_DATABASE_URL is required; schema acceptance is not a skipped PASS")


async def test_maintenance_upgrade_preserves_legacy_rows_and_matches_metadata(pg_schema_engine):
    tenant, run_id, task_id = uuid4(), uuid4(), uuid4()
    async with pg_schema_engine.begin() as connection:
        await connection.run_sync(lambda conn: _migrate(conn, "20261028_0003"))
        await connection.execute(
            text("INSERT INTO metrica_parse_runs (id, equestrian_id, date_mode) VALUES (:id, :tenant, 'auto')"),
            {"id": run_id, "tenant": tenant},
        )
        await connection.execute(
            text(
                "INSERT INTO metrica_parse_tasks "
                "(id, run_id, equestrian_id, counter_id, yandex_account_id, kind, date_from, date_to, "
                "sequence, status, attempts, max_attempts, rows_fetched) "
                "VALUES (:id, :run, :tenant, '123', :account, 'pages', :day, :day, 0, 'failed', 2, 3, 17)"
            ),
            {"id": task_id, "run": run_id, "tenant": tenant, "account": uuid4(), "day": date(2026, 10, 1)},
        )
        await connection.run_sync(lambda conn: _migrate(conn, "head"))
        assert (
            await connection.run_sync(lambda conn: compare_metadata(MigrationContext.configure(conn), metadata)) == []
        )
        row = (await connection.execute(select(metrica_parse_tasks))).mappings().one()
        assert row["id"] == task_id and row["run_id"] == run_id
        assert row["status"] == "failed" and row["attempts"] == 2 and row["rows_fetched"] == 17
        assert row["binding_generation"] == row["observed_cleanup_requested_seq"] == row["fencing_revision"] == 0
        assert (await connection.execute(select(metrica_parse_runs.c.binding_generation))).scalar_one() == 0
        await connection.execute(metrica_parse_tasks.update().values(status="cancelled"))


async def test_maintenance_constraints_tenant_dedup_and_unresolved_auto_intent(pg_session):
    tenant, foreign_tenant, operation_id = uuid4(), uuid4(), uuid4()
    await pg_session.execute(
        metrica_tenant_gates.insert(), [{"equestrian_id": tenant}, {"equestrian_id": foreign_tenant}]
    )
    operation = dict(
        id=operation_id, equestrian_id=tenant, kind="merge", status="collecting", normalization_revision=1
    )
    await pg_session.execute(metrica_maintenance_operations.insert().values(**operation))
    chunk = dict(
        equestrian_id=tenant,
        operation_id=operation_id,
        part_index=0,
        payload_hash="a" * 64,
        mappings=[{"source_page_id": str(uuid4()), "target_page_id": str(uuid4())}],
    )
    await pg_session.execute(metrica_maintenance_chunks.insert().values(**chunk))
    for changes in ({}, {"part_index": 1, "equestrian_id": foreign_tenant}, {"part_index": 1, "payload_hash": "bad"}):
        with pytest.raises(IntegrityError):
            async with pg_session.begin_nested():
                await pg_session.execute(metrica_maintenance_chunks.insert().values(**{**chunk, **changes}))
    intent = dict(
        job_id=uuid4(),
        equestrian_id=tenant,
        payload_hash="b" * 64,
        date_mode="auto",
        binding_generation=2,
        observed_cleanup_requested_seq=1,
    )
    await pg_session.execute(metrica_pending_parse_intents.insert().values(**intent))
    with pytest.raises(IntegrityError):
        async with pg_session.begin_nested():
            await pg_session.execute(metrica_pending_parse_intents.insert().values(**intent))
    with pytest.raises(IntegrityError):
        async with pg_session.begin_nested():
            await pg_session.execute(
                metrica_pending_parse_intents.insert().values(
                    **{**intent, "job_id": uuid4(), "date_from": date.today()}
                )
            )
    assert (await pg_session.execute(select(metrica_pending_parse_intents.c.date_from))).scalar_one() is None
