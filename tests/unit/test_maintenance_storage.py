"""Storage prerequisites UT-N-04; worker effects are covered by later units."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql

from core.entities.metrica_maintenance import (
    MaintenanceConflictError,
    MaintenanceLease,
    MaintenanceLeaseLostError,
    WorkKind,
)
from core.protocols.repositories.metrica_maintenance import MaintenanceStorageProtocol
from repositories.metrica_maintenance_storage_repository import MaintenanceStorageRepository, payload_hash


def repository(**row):
    session = AsyncMock()
    repo = MaintenanceStorageRepository(session)
    repo.lock_scope = AsyncMock()
    repo._operation = AsyncMock(
        return_value={
            "kind": "merge",
            "status": "collecting",
            "part_count": None,
            "cleanup_operation_id": None,
            **row,
        }
    )
    return repo, session


def result(value):
    output = MagicMock()
    output.scalar_one_or_none.return_value = value
    output.mappings.return_value.one.return_value = value
    output.mappings.return_value.one_or_none.return_value = value
    return output


def pair():
    return {"source_page_id": str(uuid4()), "target_page_id": str(uuid4())}


def query(statement):
    return str(statement.compile(dialect=postgresql.dialect()))


def test_maintenance_hash_canonical_keys_but_not_array_order_and_nan_rejected():
    assert payload_hash({"a": 1, "b": 2}) == payload_hash({"b": 2, "a": 1})
    assert payload_hash([1, 2]) != payload_hash([2, 1])
    with pytest.raises(ValueError):
        payload_hash({"a": float("nan")})


@pytest.mark.parametrize(
    "mappings",
    [[], [{}], [pair()] * 501, [pair()] * 2, [{"source_page_id": str(uuid4()), "target_page_id": "invalid"}]],
)
def test_maintenance_mapping_validation(mappings):
    with pytest.raises(ValueError):
        MaintenanceStorageRepository._mappings(mappings)


async def test_maintenance_operation_invalid_identity_before_sql():
    repo, session = repository()
    contract: MaintenanceStorageProtocol = repo
    for kind in WorkKind:
        with pytest.raises(ValueError):
            await contract.save_operation(uuid4(), uuid4(), kind=kind)
    session.execute.assert_not_called()


async def test_maintenance_operation_duplicate_compares_immutable_identity():
    tenant, operation = uuid4(), uuid4()
    row = dict(
        id=operation,
        equestrian_id=tenant,
        kind=WorkKind.MERGE,
        normalization_revision=1,
        cleanup_seq=None,
        binding_generation=None,
    )
    repo, session = repository(**row)
    session.scalar.return_value = None
    assert not await repo.save_operation(tenant, operation, kind=WorkKind.MERGE, normalization_revision=1)
    with pytest.raises(MaintenanceConflictError):
        await repo.save_operation(tenant, operation, kind=WorkKind.MERGE, normalization_revision=2)
    session.commit.assert_not_called()


async def test_maintenance_cleanup_save_advances_requested_not_completed():
    repo, session = repository()
    session.scalar.return_value = uuid4()
    assert await repo.save_operation(uuid4(), uuid4(), kind=WorkKind.CLEANUP, cleanup_seq=2, binding_generation=3)
    statement = session.execute.call_args.args[0]
    assert "greatest" in query(statement) and "cleanup_completed_seq" not in query(statement)


@pytest.mark.parametrize("foreign", [None, {"equestrian_id": uuid4()}])
async def test_maintenance_operation_missing_or_foreign_rejected(foreign):
    repo, session = repository()
    del repo._operation
    session.execute.return_value = result(foreign)
    with pytest.raises(MaintenanceConflictError):
        await repo._operation(uuid4(), uuid4())


@pytest.mark.parametrize("conflict", [None, "tenant", "operation", "hash"])
async def test_maintenance_inbox_duplicate_and_identity_conflict(conflict):
    tenant, operation, message = uuid4(), uuid4(), uuid4()
    row = {"equestrian_id": tenant, "operation_id": operation, "payload_hash": payload_hash({"a": 1})}
    if conflict:
        row[{"tenant": "equestrian_id", "operation": "operation_id", "hash": "payload_hash"}[conflict]] = "other"
    repo, session = repository()
    session.scalar.return_value = None
    session.execute.return_value = result(row)
    if conflict:
        with pytest.raises(MaintenanceConflictError):
            await repo.save_inbox(tenant, operation, consumer="c", message_id=message, payload={"a": 1})
    else:
        assert not await repo.save_inbox(tenant, operation, consumer="c", message_id=message, payload={"a": 1})
    session.commit.assert_not_called()


@pytest.mark.parametrize("changed", [False, True])
async def test_maintenance_chunk_duplicate_hash_verified_even_after_completion(changed):
    body = [pair()]
    repo, session = repository(status="completed", part_count=1)
    session.execute.return_value = result("different" if changed else payload_hash(body))
    if changed:
        with pytest.raises(MaintenanceConflictError):
            await repo.save_chunk(uuid4(), uuid4(), part_index=0, mappings=body)
    else:
        assert not await repo.save_chunk(uuid4(), uuid4(), part_index=0, mappings=body)
    assert session.execute.await_count == 1


async def test_maintenance_chunk_after_seal_and_late_superseded_do_not_reopen_gate():
    repo, session = repository(part_count=2)
    session.execute.return_value = result(None)
    repo._ready = AsyncMock()
    assert await repo.save_chunk(uuid4(), uuid4(), part_index=1, mappings=[pair()])
    repo._ready.assert_awaited_once()
    with pytest.raises(MaintenanceConflictError):
        await repo.save_chunk(uuid4(), uuid4(), part_index=2, mappings=[pair()])
    repo._operation.return_value.update(status="awaiting_seo", cleanup_operation_id=uuid4())
    assert await repo.save_chunk(uuid4(), uuid4(), part_index=0, mappings=[pair()])


@pytest.mark.parametrize("count", [0, 1])
async def test_maintenance_seal_before_parts_and_empty_scan(count):
    repo, session = repository()
    session.scalar.return_value = 0
    assert await repo.seal(uuid4(), uuid4(), part_count=count)
    statement = session.execute.call_args.args[0]
    assert "collecting" in statement.compile().params.values()
    assert "SELECT count" in query(statement)


@pytest.mark.parametrize("stored,new,outside", [(2, 2, 0), (2, 1, 0), (None, 1, 1)])
async def test_maintenance_seal_duplicate_or_conflict(stored, new, outside):
    repo, session = repository(part_count=stored)
    session.scalar.return_value = outside
    if stored == new:
        assert not await repo.seal(uuid4(), uuid4(), part_count=new)
    else:
        with pytest.raises(MaintenanceConflictError):
            await repo.seal(uuid4(), uuid4(), part_count=new)


async def test_maintenance_finish_lease_fenced_and_revoked():
    repo, session = repository(part_count=0)
    repo.assert_owned = AsyncMock()
    lease = MaintenanceLease(uuid4(), uuid4(), uuid4(), 3, WorkKind.MERGE, "running")
    session.scalar.side_effect = [0, lease.operation_id]
    await repo.finish_work(lease, status="awaiting_seo", result_message_id=uuid4())
    sql = query(session.scalar.call_args.args[0])
    for field in (
        "equestrian_id",
        "lease_owner",
        "fencing_revision",
        "lease_expires_at",
        "status",
        "clock_timestamp()",
    ):
        assert field in sql
    session.scalar.side_effect = [0, None]
    with pytest.raises(MaintenanceLeaseLostError):
        await repo.finish_work(lease, status="awaiting_seo", result_message_id=uuid4())
    with pytest.raises(ValueError):
        await repo.finish_work(lease, status="completed", result_message_id=uuid4())


async def test_maintenance_supersede_revokes_revision_and_preserves_first_cleanup():
    repo, session = repository()
    repo._operation.side_effect = [
        {"kind": "merge", "status": "running", "cleanup_operation_id": None},
        {"kind": "cleanup"},
    ]
    assert await repo.supersede(uuid4(), uuid4(), cleanup_operation=uuid4())
    sql = query(session.execute.call_args.args[0])
    assert "fencing_revision +" in sql and "lease_owner" in sql
    repo._operation.side_effect = [{"kind": "merge", "cleanup_operation_id": uuid4()}, {"kind": "cleanup"}]
    assert not await repo.supersede(uuid4(), uuid4(), cleanup_operation=uuid4())


@pytest.mark.parametrize("wrong", ["revision", "result", "state"])
async def test_maintenance_finalized_requires_exact_correlation(wrong):
    message = uuid4()
    repo, session = repository(normalization_revision=3, result_message_id=message, status="awaiting_seo")
    repo._operation.return_value[
        {"revision": "normalization_revision", "result": "result_message_id", "state": "status"}[wrong]
    ] = "invalid"
    with pytest.raises(MaintenanceConflictError):
        await repo.finalize(uuid4(), uuid4(), normalization_revision=3, result_message_id=message)
    session.execute.assert_not_called()


async def test_maintenance_finalized_repeated_is_noop():
    message = uuid4()
    repo, session = repository(normalization_revision=3, result_message_id=message, status="completed")
    assert not await repo.finalize(uuid4(), uuid4(), normalization_revision=3, result_message_id=message)
    session.execute.assert_not_called()


async def test_maintenance_superseded_result_is_immutable():
    message = uuid4()
    repo, session = repository(status="superseded_by_delete", cleanup_operation_id=uuid4(), result_message_id=None)
    assert await repo.record_superseded_result(uuid4(), uuid4(), result_message_id=message)
    repo._operation.return_value.update(status="awaiting_seo", result_message_id=message)
    assert not await repo.record_superseded_result(uuid4(), uuid4(), result_message_id=message)
    with pytest.raises(MaintenanceConflictError):
        await repo.record_superseded_result(uuid4(), uuid4(), result_message_id=uuid4())


async def test_maintenance_chunk_checkpoint_renews_in_same_transaction_and_fences_replay():
    repo, session = repository()
    repo.assert_owned = AsyncMock()
    repo.checkpoint = AsyncMock()
    lease = MaintenanceLease(uuid4(), uuid4(), uuid4(), 1, WorkKind.MERGE, "running")
    session.scalar.return_value = 0
    await repo.checkpoint_chunk(lease, part_index=0, checkpoint={"cursor": 1}, completed=True, lease_seconds=30)
    repo.checkpoint.assert_awaited_once_with(lease, checkpoint={"part_index": 0}, lease_seconds=30)
    session.scalar.return_value = None
    with pytest.raises(MaintenanceConflictError):
        await repo.checkpoint_chunk(lease, part_index=0, checkpoint={}, completed=True, lease_seconds=30)
    session.commit.assert_not_called()


async def test_maintenance_finish_rejects_missing_chunks_and_cleanup_prefix_gap():
    repo, session = repository(part_count=2)
    repo.assert_owned = AsyncMock()
    lease = MaintenanceLease(uuid4(), uuid4(), uuid4(), 1, WorkKind.MERGE, "running")
    session.scalar.return_value = 1
    with pytest.raises(MaintenanceConflictError):
        await repo.finish_work(lease, status="awaiting_seo", result_message_id=uuid4())
    cleanup = MaintenanceLease(uuid4(), uuid4(), uuid4(), 1, WorkKind.CLEANUP, "deleting")
    repo._operation.return_value.update(cleanup_seq=2)
    session.scalar.side_effect = [cleanup.operation_id, None]
    with pytest.raises(MaintenanceConflictError):
        await repo.finish_work(cleanup, status="completed", result_message_id=uuid4())
    session.commit.assert_not_called()


async def test_maintenance_finalized_superseded_requires_completed_cleanup_and_seal():
    message = uuid4()
    repo, session = repository()
    row = dict(
        kind="merge",
        normalization_revision=1,
        result_message_id=message,
        status="awaiting_seo",
        cleanup_operation_id=uuid4(),
        part_count=None,
    )
    for status in ("pending", "completed"):
        repo._operation.side_effect = [row, {"status": status}]
        with pytest.raises(MaintenanceConflictError):
            await repo.finalize(uuid4(), uuid4(), normalization_revision=1, result_message_id=message)
    session.execute.assert_not_called()
