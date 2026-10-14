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
