"""Real PostgreSQL only; never fall back to application database settings."""

import logging
import os
import re
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

import models  # noqa: F401
from utils.basemodel import metadata


@pytest.fixture
async def pg_schema_engine() -> AsyncIterator[AsyncEngine]:
    raw_url = os.environ.get("TEST_DATABASE_URL")
    if not raw_url:
        pytest.skip("TEST_DATABASE_URL is required for dedicated PostgreSQL tests")
    url = make_url(raw_url)
    if url.drivername != "postgresql+asyncpg":
        pytest.fail("TEST_DATABASE_URL must use postgresql+asyncpg")
    if not url.database or not url.database.endswith("_test"):
        pytest.fail("TEST_DATABASE_URL must point to a dedicated database ending in _test")
    schema = f"test_metrica_{uuid4().hex}"
    admin = create_async_engine(url, poolclass=NullPool)
    engine = create_async_engine(url, poolclass=NullPool, connect_args={"server_settings": {"search_path": schema}})
    created = False
    try:
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        created = True
        yield engine
    finally:
        await engine.dispose()
        if created and os.environ.get("TEST_KEEP_SCHEMA") != "1":
            # Only the generated schema can ever be removed; no public/workspace data.
            assert re.fullmatch(r"test_metrica_[0-9a-f]{32}", schema)
            async with admin.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()


@pytest.fixture
async def pg_engine(pg_schema_engine: AsyncEngine) -> AsyncIterator[AsyncEngine]:
    async with pg_schema_engine.begin() as connection:
        await connection.run_sync(metadata.create_all)
    yield pg_schema_engine


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
