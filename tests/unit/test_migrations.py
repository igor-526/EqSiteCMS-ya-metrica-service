import runpy
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from alembic import context
from sqlalchemy import pool

from settings import settings
from utils.basemodel import metadata

ENV_PATH = Path(__file__).parents[2] / "src" / "migration" / "env.py"


@pytest.mark.parametrize("logging_config", [None, "test-alembic.ini"])
def test_offline_migrations_configure_metadata_and_run(monkeypatch, logging_config):
    config = MagicMock(config_file_name=logging_config)
    configure = MagicMock()
    run = MagicMock()
    file_config = MagicMock()
    monkeypatch.setattr(context, "config", config, raising=False)
    monkeypatch.setattr(context, "is_offline_mode", lambda: True)
    monkeypatch.setattr(context, "configure", configure)
    monkeypatch.setattr(context, "begin_transaction", nullcontext)
    monkeypatch.setattr(context, "run_migrations", run)
    monkeypatch.setattr("logging.config.fileConfig", file_config)
    runpy.run_path(str(ENV_PATH))
    config.set_main_option.assert_called_once_with("sqlalchemy.url", settings.database_url)
    configure.assert_called_once_with(url=settings.database_url, target_metadata=metadata, literal_binds=True)
    run.assert_called_once_with()
    if logging_config:
        file_config.assert_called_once_with(logging_config)
    else:
        file_config.assert_not_called()


def test_online_migrations_connect_run_sync_and_dispose(monkeypatch):
    config = MagicMock(config_file_name=None)
    section = {"sqlalchemy.url": settings.database_url}
    config.get_section.return_value = section
    configure = MagicMock()
    run = MagicMock()
    connection = AsyncMock()

    async def run_sync(callback):
        callback("sync-connection")

    connection.run_sync.side_effect = run_sync
    connection_context = AsyncMock()
    connection_context.__aenter__.return_value = connection
    engine = MagicMock()
    engine.connect.return_value = connection_context
    engine.dispose = AsyncMock()
    engine_factory = MagicMock(return_value=engine)
    monkeypatch.setattr(context, "config", config, raising=False)
    monkeypatch.setattr(context, "is_offline_mode", lambda: False)
    monkeypatch.setattr(context, "configure", configure)
    monkeypatch.setattr(context, "begin_transaction", nullcontext)
    monkeypatch.setattr(context, "run_migrations", run)
    monkeypatch.setattr("sqlalchemy.ext.asyncio.async_engine_from_config", engine_factory)
    runpy.run_path(str(ENV_PATH))
    engine_factory.assert_called_once_with(section, prefix="sqlalchemy.", poolclass=pool.NullPool)
    configure.assert_called_once_with(connection="sync-connection", target_metadata=metadata)
    run.assert_called_once_with()
    connection.run_sync.assert_awaited_once()
    connection_context.__aexit__.assert_awaited_once_with(None, None, None)
    engine.dispose.assert_awaited_once_with()


def test_revision_is_frozen_and_has_expected_head():
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    script = ScriptDirectory.from_config(Config(str(ENV_PATH.parents[1] / "alembic.ini")))
    assert script.get_current_head() == "20261026_0002"
    revision = script.get_revision("head")
    assert revision.down_revision == "20260710_0001"
    tables = revision.module.revision_metadata.tables
    assert set(tables) == {
        "metrica_parse_runs",
        "metrica_parse_tasks",
        "metrica_page_stats",
        "metrica_search_query_stats",
    }
    for name, table in tables.items():
        assert table is not metadata.tables[name]


def test_domain_entities_and_invariants():
    from datetime import date
    from uuid import uuid4

    from pydantic import ValidationError

    from core.entities import DateMode, ParseErrorCode, ParseKind, ParseRun, ParseTask, StatRow, TaskStatus

    run = ParseRun(equestrian_id=uuid4(), date_mode=DateMode.EXPLICIT)
    assert run.clamped is False
    data = dict(
        run_id=run.id,
        equestrian_id=run.equestrian_id,
        counter_id="105850153",
        yandex_account_id=uuid4(),
        kind=ParseKind.PAGES,
        date_from=date(2026, 10, 21),
        date_to=date(2026, 10, 25),
        sequence=0,
        max_attempts=3,
    )
    task = ParseTask(**data)
    assert task.status == TaskStatus.QUEUED
    assert task.next_offset == 1
    assert task.rows_fetched == 0
    assert len(ParseErrorCode) == 13
    with pytest.raises(ValidationError, match="1 to 5 days"):
        ParseTask(**{**data, "date_to": date(2026, 10, 26)})
    row = StatRow(
        equestrian_id=run.equestrian_id,
        entity_id=uuid4(),
        stat_date=date(2026, 10, 21),
        visits=3,
        users=3,
        pageviews=6,
        bounces=1,
        visit_duration_seconds=300,
    )
    assert row.parse_task_id is None
    with pytest.raises(ValidationError, match="Bounces"):
        StatRow(**{**row.model_dump(), "bounces": 4})
