from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.pool import AsyncAdaptedQueuePool, NullPool

from depends import utils as dependencies
from settings import settings
from utils import database


@pytest.fixture
def session_factory(monkeypatch):
    session = AsyncMock(spec=AsyncSession)
    context = AsyncMock()
    context.__aenter__.return_value = session
    factory = MagicMock(return_value=context)
    monkeypatch.setattr(database, "AsyncSessionLocal", factory)
    monkeypatch.setattr(dependencies, "AsyncSessionLocal", factory)
    return session, context, factory


async def test_get_db_commits_and_closes(session_factory):
    session, context, factory = session_factory
    generator = database.get_db()
    assert await anext(generator) is session
    session.commit.assert_not_awaited()
    with pytest.raises(StopAsyncIteration):
        await anext(generator)
    session.commit.assert_awaited_once_with()
    session.rollback.assert_not_awaited()
    session.close.assert_awaited_once_with()
    context.__aexit__.assert_awaited_once_with(None, None, None)
    factory.assert_called_once_with()


@pytest.mark.parametrize("source", ["body", "commit"])
async def test_get_db_rolls_back_failures_and_closes(session_factory, source):
    session, context, _ = session_factory
    failure = RuntimeError("transaction failed")
    generator = database.get_db()
    assert await anext(generator) is session
    with pytest.raises(RuntimeError, match="transaction failed") as error:
        if source == "body":
            await generator.athrow(failure)
        else:
            session.commit.side_effect = failure
            await anext(generator)
    assert error.value is failure
    session.rollback.assert_awaited_once_with()
    session.close.assert_awaited_once_with()
    assert context.__aexit__.await_args.args[:2] == (RuntimeError, failure)
    assert context.__aexit__.await_args.args[2] is not None


async def test_rollback_failure_still_closes(session_factory):
    session, _, _ = session_factory
    session.rollback.side_effect = RuntimeError("rollback failed")
    generator = database.get_db()
    await anext(generator)
    with pytest.raises(RuntimeError, match="rollback failed"):
        await generator.athrow(ValueError("body failed"))
    session.close.assert_awaited_once_with()


async def test_get_session_yields_and_exits_without_explicit_transaction(session_factory):
    session, context, _ = session_factory
    generator = dependencies.get_session()
    assert await anext(generator) is session
    with pytest.raises(StopAsyncIteration):
        await anext(generator)
    context.__aexit__.assert_awaited_once_with(None, None, None)
    session.commit.assert_not_awaited()
    session.rollback.assert_not_awaited()


async def test_get_session_exits_on_consumer_error(session_factory):
    _, context, _ = session_factory
    generator = dependencies.get_session()
    await anext(generator)
    with pytest.raises(ValueError, match="consumer failed"):
        await generator.athrow(ValueError("consumer failed"))
    assert context.__aexit__.await_args.args[0] is ValueError


@pytest.mark.parametrize(("use_null_pool", "pool_type"), [(True, NullPool), (False, AsyncAdaptedQueuePool)])
async def test_engine_and_session_factory_configuration(use_null_pool, pool_type):
    engine = database.make_async_engine(settings.database_url, use_null_pool=use_null_pool)
    try:
        assert isinstance(engine.pool, pool_type)
        assert str(engine.url) == settings.database_url.replace(":test@", ":***@")
        factory = database.make_async_session_factory(engine)
        assert factory.class_ is AsyncSession
        assert factory.kw == {"bind": engine, "autoflush": False, "expire_on_commit": False}
        async with factory() as session:
            assert session.bind is engine
            assert session.autoflush is False
    finally:
        await engine.dispose()


def test_global_factory_configuration():
    assert isinstance(database.async_engine.pool, AsyncAdaptedQueuePool)
    assert database.AsyncSessionLocal.kw == {
        "bind": database.async_engine,
        "autoflush": False,
        "expire_on_commit": False,
    }


async def test_database_shutdown_disposes_engine(monkeypatch):
    engine = AsyncMock()
    monkeypatch.setattr(database, "async_engine", engine)
    await database.close_database()
    engine.dispose.assert_awaited_once_with()


@pytest.mark.parametrize("failure", [False, True])
async def test_worker_session_owns_nullpool_engine_and_disposes(monkeypatch, failure):
    engine = AsyncMock()
    session = AsyncMock(spec=AsyncSession)
    context = AsyncMock()
    context.__aenter__.return_value = session
    engine_factory = MagicMock(return_value=engine)
    factory = MagicMock(return_value=context)
    monkeypatch.setattr(database, "make_async_engine", engine_factory)
    session_factory = MagicMock(return_value=factory)
    monkeypatch.setattr(database, "make_async_session_factory", session_factory)

    async def invoke():
        async with database.worker_session() as actual:
            assert actual is session
            engine.dispose.assert_not_awaited()
            if failure:
                raise ValueError("body failed")

    if failure:
        with pytest.raises(ValueError, match="body failed"):
            await invoke()
    else:
        await invoke()
    engine_factory.assert_called_once_with(settings.database_url, use_null_pool=True)
    session_factory.assert_called_once_with(engine)
    engine.dispose.assert_awaited_once_with()
    session.begin.assert_not_called()
    session.commit.assert_not_awaited()
    context.__aexit__.assert_awaited_once()


async def test_worker_disposes_when_session_factory_fails(monkeypatch):
    engine = AsyncMock()
    monkeypatch.setattr(database, "make_async_engine", lambda *args, **kwargs: engine)
    factory = MagicMock(side_effect=RuntimeError("factory failed"))
    monkeypatch.setattr(database, "make_async_session_factory", factory)
    with pytest.raises(RuntimeError, match="factory failed"):
        async with database.worker_session():
            pytest.fail("must not yield")
    engine.dispose.assert_awaited_once_with()
