"""Real PostgreSQL only; never fall back to application database settings."""

import importlib
import logging
import os
import sys
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from sqlalchemy import event
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

import models  # noqa: F401
from utils.basemodel import metadata


@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(items):
    """Infrastructure is transitive: default suite must never enter permit fixtures.

    Mark before pytest's -m deselection, including older PG tests without a marker.
    Explicit acceptance still fails closed through qa_infra_context; no skips added.
    """
    for item in items:
        if "qa_infra_context" in getattr(item, "fixturenames", ()):
            item.add_marker(pytest.mark.infrastructure)


def infra_support():
    """Explicit test-support only; standalone unit collection never imports infra."""
    raw = os.environ.get("QA_TEST_SUPPORT_ROOT")
    if not raw:
        pytest.fail("BLOCKED: explicit QA_TEST_SUPPORT_ROOT required; no application fallback")
    support = Path(raw).resolve()
    if not (support / "infra" / "contracts.py").is_file():
        pytest.fail("BLOCKED: shared infrastructure support unavailable")
    sys.path.insert(0, str(support))
    return importlib.import_module("infra")


@pytest.fixture
def qa_infra_context(request):
    """INF-01: permit + confirmed runner journal are mandatory before any connection.

    Use scripts/qa/infra/run.py, never direct TEST_DATABASE_URL/TEST_NATS_URL.
    PG-01/02 exact nodes live in test_cleanup_worker_postgres.py; INF-01 in
    test_acceptance_helpers.py; NATS-01 in test_nats_broker.py. The runner owns
    root setup/teardown and rejects skipped/empty/incomplete acceptance.
    """
    kind = "nats" if request.node.path.name == "test_nats_broker.py" else "pg"
    return infra_support().load_context(kind)


@pytest.fixture
async def pg_schema_engine(qa_infra_context) -> AsyncIterator[AsyncEngine]:
    context = qa_infra_context
    url = make_url(context["url"]).set(drivername="postgresql+asyncpg")
    if not url.database or not url.database.endswith("_test"):
        pytest.fail("BLOCKED: dedicated _test database required")
    engine = create_async_engine(url, poolclass=NullPool, connect_args=context["connect_args"], hide_parameters=True)
    try:
        yield engine
    finally:
        # The runner alone removes its schema, including partially-created metadata.
        await engine.dispose()


@pytest.fixture
async def pg_engine(pg_schema_engine: AsyncEngine) -> AsyncIterator[AsyncEngine]:
    async with pg_schema_engine.begin() as connection:
        await connection.run_sync(metadata.create_all)
    try:
        yield pg_schema_engine
    finally:
        # Exact nodes share one runner schema; reset only its test metadata.
        async with pg_schema_engine.begin() as connection:
            await connection.run_sync(metadata.drop_all)


@pytest.fixture
async def pg_session(pg_engine: AsyncEngine) -> AsyncIterator[AsyncSession]:
    async with AsyncSession(pg_engine) as session:
        yield session
        await session.rollback()


@pytest.fixture
def sql_statements(pg_engine, caplog):
    """Count statements during the measured call; never log bind values."""
    statements: list[str] = []
    logger = logging.getLogger("tests.page_stats.sql")
    caplog.set_level(logging.INFO, logger=logger.name)

    def capture(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)
        logger.info("SQL: %s", statement)

    event.listen(pg_engine.sync_engine, "before_cursor_execute", capture)
    try:
        yield statements
    finally:
        event.remove(pg_engine.sync_engine, "before_cursor_execute", capture)
