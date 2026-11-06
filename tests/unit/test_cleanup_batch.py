"""Bounded cleanup/fencing contracts; not real PostgreSQL acceptance."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql

from core.entities.metrica_maintenance import (
    CleanupBatch,
    MaintenanceConflictError,
    MaintenanceLease,
    MaintenanceLeaseLostError,
    WorkKind,
)
from core.protocols.repositories.metrica_maintenance import CleanupBatchProtocol
from core.services.metrica_cleanup_service import CleanupBatchService
from repositories.metrica_cleanup_repository import CleanupBatchRepository
from settings import Settings


def lease():
    return MaintenanceLease(uuid4(), uuid4(), uuid4(), 1, WorkKind.CLEANUP, "deleting")


def repository(stage=None, deleted=2):
    session = AsyncMock()
    repo = CleanupBatchRepository(session)
    repo.assert_owned = AsyncMock()
    repo._operation = AsyncMock(return_value={"checkpoint": {} if stage is None else {"table": stage}})
    repo.checkpoint = AsyncMock()
    session.execute.return_value = MagicMock()
    session.execute.return_value.scalar_one.return_value = deleted
    session.scalar.return_value = False
    return repo, session


@pytest.mark.parametrize(
    "stage,table", [(None, "metrica_page_stats"), ("search_queries", "metrica_search_query_stats")]
)
async def test_cleanup_bounded_tenant_pk_sql_aggregate_no_materialization(stage, table):
    repo, session = repository(stage)
    owned = lease()
    contract: CleanupBatchProtocol = repo
    outcome = await contract.delete_batch(owned, batch_size=2, lease_seconds=30)
    assert outcome == CleanupBatch(stage or "pages", 2, False)
    statement = session.execute.call_args.args[0]
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert f"DELETE FROM {table}" in sql
    assert f"ORDER BY {table}.id" in sql
    assert "LIMIT" in sql and "OFFSET" not in sql
    assert "SELECT count(*)" in sql and "RETURNING" in sql
    params = statement.compile().params
    assert owned.equestrian_id in params.values() and 2 in params.values()
    assert sql.count(f"{table}.equestrian_id =") == 2
    session.execute.return_value.all.assert_not_called()
    repo.assert_owned.assert_awaited_once_with(owned)
    repo.checkpoint.assert_awaited_once_with(owned, checkpoint={"table": stage or "pages"}, lease_seconds=30)
    session.commit.assert_not_called()


@pytest.mark.parametrize(
    "stage,deleted,next_stage,exhausted",
    [
        (None, 0, "search_queries", False),
        (None, 1, "search_queries", False),
        ("search_queries", 0, "exhausted", True),
        ("search_queries", 1, "exhausted", True),
        ("exhausted", 0, "exhausted", True),
    ],
)
async def test_cleanup_stage_checkpoint_and_empty_tables(stage, deleted, next_stage, exhausted):
    repo, session = repository(stage, deleted)
    owned = lease()
    result = await repo.delete_batch(owned, batch_size=2, lease_seconds=30)
    assert result.exhausted is exhausted
    assert result.deleted_rows == deleted
    repo.checkpoint.assert_awaited_once_with(owned, checkpoint={"table": next_stage}, lease_seconds=30)
    assert session.scalar.await_count == (2 if exhausted else 0)
    assert session.execute.await_count == (0 if stage == "exhausted" else 1)


@pytest.mark.parametrize("nonempty,stage", [([True], "pages"), ([False, True], "search_queries")])
async def test_cleanup_exhausted_checkpoint_never_hides_remaining_rows(nonempty, stage):
    repo, session = repository("exhausted", 0)
    session.scalar.side_effect = nonempty
    result = await repo.delete_batch(lease(), batch_size=2, lease_seconds=30)
    assert not result.exhausted
    assert repo.checkpoint.call_args.kwargs["checkpoint"] == {"table": stage}


@pytest.mark.parametrize("batch_size,seconds", [(0, 30), (10001, 30), (True, 30), (1, 0)])
async def test_cleanup_invalid_knobs_before_effect(batch_size, seconds):
    repo, session = repository()
    with pytest.raises(ValueError):
        await repo.delete_batch(lease(), batch_size=batch_size, lease_seconds=seconds)
    repo.assert_owned.assert_not_called()
    session.execute.assert_not_called()


async def test_cleanup_wrong_kind_or_checkpoint_fail_closed():
    repo, session = repository("unknown")
    with pytest.raises(MaintenanceConflictError):
        await repo.delete_batch(lease(), batch_size=2, lease_seconds=30)
    with pytest.raises(ValueError):
        await repo.delete_batch(
            MaintenanceLease(uuid4(), uuid4(), uuid4(), 1, WorkKind.MERGE, "running"),
            batch_size=2,
            lease_seconds=30,
        )
    session.execute.assert_not_called()
    repo.checkpoint.assert_not_called()


async def test_cleanup_stale_lease_or_running_parse_blocks_before_effect():
    repo, session = repository()
    repo.assert_owned.side_effect = MaintenanceLeaseLostError()
    with pytest.raises(MaintenanceLeaseLostError):
        await repo.delete_batch(lease(), batch_size=2, lease_seconds=30)
    session.execute.assert_not_called()
    repo.checkpoint.assert_not_called()


@pytest.mark.parametrize("failure", [None, "effect", "checkpoint", "commit"])
async def test_cleanup_service_atomic_effect_checkpoint_commit_or_rollback(failure):
    repo, session = repository()
    owned = lease()
    events = []

    @asynccontextmanager
    async def begin():
        events.append("begin")
        try:
            yield
            if failure == "commit":
                raise RuntimeError("commit failure")
            events.append("commit")
        except Exception:
            events.append("rollback")
            raise

    session.begin = begin
    original_effect = session.execute.return_value

    async def effect(statement):
        events.append("effect")
        if failure == "effect":
            raise RuntimeError("storage failure")
        return original_effect

    async def checkpoint(*args, **kwargs):
        events.append("checkpoint")
        if failure == "checkpoint":
            raise MaintenanceLeaseLostError()

    session.execute.side_effect = effect
    repo.checkpoint.side_effect = checkpoint
    service = CleanupBatchService(repository=repo, transaction=session, batch_size=2, lease_seconds=30)
    if failure:
        with pytest.raises((RuntimeError, MaintenanceLeaseLostError)):
            await service.run_batch(owned)
        assert events[-1] == "rollback"
        assert "commit" not in events
    else:
        assert await service.run_batch(owned) == CleanupBatch("pages", 2, False)
        assert events == ["begin", "effect", "checkpoint", "commit"]


@pytest.mark.parametrize("size", [0, 10001, True])
def test_cleanup_service_invalid_batch_configuration(size):
    with pytest.raises(ValueError):
        CleanupBatchService(repository=AsyncMock(), transaction=AsyncMock(), batch_size=size)


def test_cleanup_settings_defaults_and_bounds():
    settings = Settings()
    assert settings.cleanup_batch_size == 1000
    assert settings.maintenance_lease_seconds == 60
    for kwargs in ({"CLEANUP_BATCH_SIZE": 0}, {"CLEANUP_BATCH_SIZE": 10001}, {"MAINTENANCE_LEASE_SECONDS": 0}):
        with pytest.raises(ValueError):
            Settings(**kwargs)


def test_cleanup_di_factory_shares_injected_session_without_activating_workers():
    from dependency_injector import providers

    from containers.repositories import RepositoriesContainer
    from containers.services import ServicesContainer
    from containers.settings import SettingsContainer

    session = AsyncMock()
    repositories = RepositoriesContainer(session=providers.Object(session))
    services = ServicesContainer(repositories_container=repositories, settings_container=SettingsContainer())
    service = services.cleanup_batch_service()
    assert isinstance(service, CleanupBatchService)
    assert service._repository.session is session
    assert service._transaction is session
