"""UT-N-03 and storage part of UT-N-04; SQL shape is not PG acceptance."""

from datetime import date
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy.dialects import postgresql

from core.entities.metrica_maintenance import (
    MaintenanceConflictError,
    MaintenanceLease,
    MaintenanceLeaseLostError,
    MergeBatch,
    WorkKind,
)
from core.entities.metrica_metrics import BASE_METRICS, add_metrics, provider_metrics
from core.protocols.repositories.metrica_maintenance import MergeBatchProtocol
from repositories.metrica_merge_repository import MergeBatchRepository


def lease():
    return MaintenanceLease(uuid4(), uuid4(), uuid4(), 1, WorkKind.MERGE, "running")


def setup_repository(count=1, checkpoint=None, mappings_count=1):
    session = AsyncMock()
    repo = MergeBatchRepository(session)
    repo.assert_owned = AsyncMock()
    repo.checkpoint_chunk = AsyncMock()
    mappings = [{"source_page_id": str(uuid4()), "target_page_id": str(uuid4())} for _ in range(mappings_count)]
    chunk = MagicMock()
    chunk.mappings.return_value.one_or_none.return_value = {
        "mappings": mappings,
        "checkpoint": checkpoint or {},
        "part_index": 7,
    }
    rows = MagicMock()
    rows.mappings.return_value.all.return_value = [
        dict(id=uuid4(), stat_date=date(2026, 1, day + 1), **dict.fromkeys(BASE_METRICS, 1)) for day in range(count)
    ]
    session.execute.side_effect = [chunk, rows, MagicMock(), MagicMock()]
    return repo, session, mappings


def sql(statement):
    return str(statement.compile(dialect=postgresql.dialect()))


def test_metrics_users_additive_and_no_python_saturation():
    assert add_metrics((2, 5, 9, 1, 200), (8, 5, 8, 0, 80)) == (10, 10, 17, 1, 280)
    assert add_metrics((2**31 - 1, 0, 0, 0, 2**63 - 1), (1, 0, 0, 0, 1)) == (2**31, 0, 0, 0, 2**63)
    assert provider_metrics([30, 30, 59, 16.66666667, 999, 114.86666667]) == (30, 30, 59, 5, 3446)
    assert provider_metrics([0, 0, 0, 90, 999, 99]) == (0, 0, 0, 0, 0)


@pytest.mark.parametrize("count,next_index,completed", [(0, 1, True), (1, 1, True), (2, 0, False)])
async def test_move_bounded_collision_adds_bases_deletes_source_then_checkpoint(count, next_index, completed):
    repo, session, mappings = setup_repository(count)
    owned = lease()
    contract: MergeBatchProtocol = repo
    assert await contract.move_batch(owned, batch_size=2, lease_seconds=60) == MergeBatch(7, 0, count, completed)
    calls = session.execute.call_args_list
    chunk_sql, source_sql = map(lambda call: sql(call.args[0]), calls[:2])
    assert "LIMIT" in chunk_sql and "FOR UPDATE" in chunk_sql and "ORDER BY" in chunk_sql
    assert "LIMIT" in source_sql and "ORDER BY metrica_page_stats.stat_date" in source_sql
    assert "OFFSET" not in source_sql and "FOR UPDATE" in source_sql
    assert owned.equestrian_id in calls[1].args[0].compile().params.values()
    assert len(calls) == (4 if count else 2)
    if count:
        upsert, deletion = calls[2].args[0], calls[3].args[0]
        text = sql(upsert)
        assert "ON CONFLICT (equestrian_id, stat_date, page_id) DO UPDATE" in text
        for name in BASE_METRICS:
            assert f"metrica_page_stats.{name} + excluded.{name}" in text
        assert "CASE" not in text and "bounce_rate" not in text and "page_depth" not in text
        assert "parse_task_id" in text and UUID(mappings[0]["target_page_id"]) in upsert.compile().params.values()
        assert "DELETE FROM metrica_page_stats" in sql(deletion)
        assert "metrica_page_stats.page_id =" in sql(deletion)
        assert owned.equestrian_id in deletion.compile().params.values()
    repo.checkpoint_chunk.assert_awaited_once_with(
        owned, part_index=7, checkpoint={"mapping_index": next_index}, completed=completed, lease_seconds=60
    )
    repo.assert_owned.assert_awaited_once_with(owned)
    session.commit.assert_not_called()


async def test_one_mapping_only_even_when_part_contains_500():
    repo, session, mappings = setup_repository(0, {"mapping_index": 499}, 500)
    outcome = await repo.move_batch(lease(), batch_size=1000, lease_seconds=60)
    assert outcome == MergeBatch(7, 499, 0, True)
    assert UUID(mappings[499]["source_page_id"]) in session.execute.call_args_list[1].args[0].compile().params.values()


async def test_no_unfinished_parts_does_not_finalize():
    repo, session, _ = setup_repository()
    result = MagicMock()
    result.mappings.return_value.one_or_none.return_value = None
    session.execute.side_effect = [result]
    assert await repo.move_batch(lease(), batch_size=1, lease_seconds=60) is None
    repo.checkpoint_chunk.assert_not_called()
    assert session.execute.await_count == 1


@pytest.mark.parametrize(
    "checkpoint", [{"mapping_index": -1}, {"mapping_index": True}, {"mapping_index": 1}, {"x": 1}]
)
async def test_invalid_checkpoint_fails_before_stats(checkpoint):
    repo, session, _ = setup_repository(checkpoint=checkpoint)
    with pytest.raises(MaintenanceConflictError):
        await repo.move_batch(lease(), batch_size=2, lease_seconds=60)
    assert session.execute.await_count == 1
    repo.checkpoint_chunk.assert_not_called()


@pytest.mark.parametrize("size,seconds", [(0, 60), (1001, 60), (True, 60), (2, 0)])
async def test_invalid_limits_before_any_lock(size, seconds):
    repo, session, _ = setup_repository()
    with pytest.raises(ValueError):
        await repo.move_batch(lease(), batch_size=size, lease_seconds=seconds)
    session.execute.assert_not_called()
    repo.assert_owned.assert_not_called()


async def test_stale_or_preempted_lease_has_no_effect():
    repo, session, _ = setup_repository()
    repo.assert_owned.side_effect = MaintenanceLeaseLostError()
    with pytest.raises(MaintenanceLeaseLostError):
        await repo.move_batch(lease(), batch_size=2, lease_seconds=60)
    session.execute.assert_not_called()


async def test_checkpoint_failure_propagates_for_caller_rollback():
    repo, session, _ = setup_repository()
    repo.checkpoint_chunk.side_effect = MaintenanceLeaseLostError()
    with pytest.raises(MaintenanceLeaseLostError):
        await repo.move_batch(lease(), batch_size=2, lease_seconds=60)
    assert session.execute.await_count == 4
    session.commit.assert_not_called()


@pytest.mark.parametrize("kind,status", [(WorkKind.CLEANUP, "deleting"), (WorkKind.MERGE, "waiting")])
async def test_wrong_lease_kind_or_status_has_no_effect(kind, status):
    repo, session, _ = setup_repository()
    invalid = MaintenanceLease(uuid4(), uuid4(), uuid4(), 1, kind, status)
    with pytest.raises(ValueError):
        await repo.move_batch(invalid, batch_size=2, lease_seconds=60)
    repo.assert_owned.assert_not_called()
    session.execute.assert_not_called()


async def test_persisted_source_equals_target_fails_before_stats():
    repo, session, mappings = setup_repository()
    mappings[0]["target_page_id"] = mappings[0]["source_page_id"]
    with pytest.raises(ValueError):
        await repo.move_batch(lease(), batch_size=2, lease_seconds=60)
    assert session.execute.await_count == 1
    repo.checkpoint_chunk.assert_not_called()
