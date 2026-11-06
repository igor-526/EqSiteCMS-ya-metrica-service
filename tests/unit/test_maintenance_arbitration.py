from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy.dialects import postgresql

from core.entities.metrica_maintenance import MaintenanceLease, MaintenanceLeaseLostError, WorkKind
from core.protocols.repositories.metrica_maintenance import MaintenanceArbitrationProtocol
from repositories.metrica_maintenance_repository import MaintenanceArbitrationRepository


def sql(statement):
    return str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))


def repository():
    session = AsyncMock()
    return MaintenanceArbitrationRepository(session), session


async def test_gate_lock_order_sorted_tenants_before_accounts_and_no_global_key():
    repo, session = repository()
    a, b, c = UUID(int=1), UUID(int=2), UUID(int=3)
    await repo.lock_scope([b, a, b], [c, a])
    assert [call.args[1]["key"] for call in session.execute.call_args_list] == [
        f"metrica-parse:{a}",
        f"metrica-parse:{b}",
        f"metrica-account:{a}",
        f"metrica-account:{c}",
    ]
    assert all("pg_advisory_xact_lock" in str(call.args[0]) for call in session.execute.call_args_list)
    session.commit.assert_not_called()


@pytest.mark.parametrize(
    ("answers", "expected"),
    [
        ([True], WorkKind.CLEANUP),
        ([False, True], WorkKind.CLEANUP),
        ([False, False, True], WorkKind.MERGE),
        ([False, False, False], WorkKind.PARSE),
    ],
)
async def test_gate_priority_and_authoritative_barrier(answers, expected):
    repo, session = repository()
    session.scalar.side_effect = answers
    tenant = uuid4()
    assert await repo.selected_kind(tenant) == expected
    for call in session.scalar.call_args_list:
        assert str(tenant) in sql(call.args[0])
    assert "cleanup_requested_seq >" in sql(session.scalar.call_args_list[0].args[0])


@pytest.mark.parametrize("status", ["failed_blocked", "collecting", "awaiting_seo", "superseded_by_delete"])
async def test_maintenance_nonexecutable_head_never_bypassed(status):
    repo, session = repository()
    repo.selected_kind = AsyncMock(return_value=WorkKind.MERGE)
    result = MagicMock()
    result.mappings.return_value.one_or_none.return_value = {"status": status}
    session.execute.return_value = result
    assert await repo.claim(uuid4(), owner=uuid4(), lease_seconds=10) is None
    assert session.execute.await_count == 1


async def test_maintenance_claim_cas_increments_revision_and_uses_db_clock():
    repo, session = repository()
    tenant, operation, owner = uuid4(), uuid4(), uuid4()
    now = datetime.now(UTC)
    repo.selected_kind = AsyncMock(return_value=WorkKind.CLEANUP)
    repo._running_parse = AsyncMock(return_value=False)
    result, updated = MagicMock(), MagicMock()
    result.mappings.return_value.one_or_none.return_value = {
        "id": operation,
        "status": "pending",
        "cleanup_seq": 1,
        "next_attempt_at": now - timedelta(seconds=1),
        "lease_expires_at": None,
        "fencing_revision": 2,
    }
    updated.scalar_one.return_value = 3
    session.execute.side_effect = [result, updated]
    session.scalar.side_effect = [0, now]
    lease = await repo.claim(tenant, owner=owner, lease_seconds=10)
    assert lease == MaintenanceLease(tenant, operation, owner, 3, WorkKind.CLEANUP, "deleting")
    query = sql(session.execute.call_args_list[1].args[0])
    assert "fencing_revision + 1" in query and "fencing_revision = 2" in query
    assert "clock_timestamp()" in query and str(tenant) in query


@pytest.mark.parametrize("running", [True, False])
async def test_maintenance_running_or_live_lease_prevents_claim(running):
    repo, session = repository()
    repo.selected_kind = AsyncMock(return_value=WorkKind.CLEANUP)
    repo._running_parse = AsyncMock(return_value=running)
    now = datetime.now(UTC)
    result = MagicMock()
    result.mappings.return_value.one_or_none.return_value = {
        "status": "pending",
        "cleanup_seq": 1,
        "next_attempt_at": now,
        "lease_expires_at": now + timedelta(seconds=10),
    }
    session.execute.return_value = result
    session.scalar.side_effect = [0, now]
    assert await repo.claim(uuid4(), owner=uuid4(), lease_seconds=10) is None
    assert session.execute.await_count == 1


@pytest.mark.parametrize(
    ("selected", "running", "owned"),
    [
        (WorkKind.CLEANUP, False, True),
        (WorkKind.MERGE, True, True),
        (WorkKind.MERGE, False, False),
    ],
)
async def test_maintenance_stale_preempted_and_undrained_fenced(selected, running, owned):
    repo, session = repository()
    repo.selected_kind = AsyncMock(return_value=selected)
    repo._running_parse = AsyncMock(return_value=running)
    session.scalar.return_value = owned
    lease = MaintenanceLease(uuid4(), uuid4(), uuid4(), 1, WorkKind.MERGE, "running")
    with pytest.raises(MaintenanceLeaseLostError):
        await repo.checkpoint(lease, checkpoint={}, lease_seconds=10)
    session.commit.assert_not_called()


async def test_maintenance_checkpoint_checks_owner_revision_tenant_expiry_and_state():
    repo, session = repository()
    lease = MaintenanceLease(uuid4(), uuid4(), uuid4(), 7, WorkKind.MERGE, "running")
    repo.selected_kind = AsyncMock(return_value=WorkKind.MERGE)
    repo._running_parse = AsyncMock(return_value=False)
    session.scalar.side_effect = [True, lease.operation_id]
    await repo.checkpoint(lease, checkpoint={"cursor": "next"}, lease_seconds=30)
    statement = session.scalar.call_args.args[0]
    query = str(statement.compile(dialect=postgresql.dialect()))
    for field in ("lease_owner", "fencing_revision", "equestrian_id", "status", "lease_expires_at"):
        assert field in query
    assert "clock_timestamp()" in query
    assert statement.compile().params["checkpoint"] == {"cursor": "next"}


async def test_maintenance_expiry_during_checkpoint_requires_rollback():
    repo, session = repository()
    repo.assert_owned = AsyncMock()
    session.scalar.return_value = None
    lease = MaintenanceLease(uuid4(), uuid4(), uuid4(), 1, WorkKind.MERGE, "running")
    with pytest.raises(MaintenanceLeaseLostError):
        await repo.checkpoint(lease, checkpoint={}, lease_seconds=10)


async def test_maintenance_invalid_lease_has_no_sql_and_protocol_matches():
    repo, session = repository()
    contract: MaintenanceArbitrationProtocol = repo
    with pytest.raises(ValueError):
        await contract.claim(uuid4(), owner=uuid4(), lease_seconds=0)
    session.execute.assert_not_called()
    session.scalar.assert_not_called()


@pytest.mark.parametrize(("completed", "seq"), [(0, 2), (2, 1), (None, 1)])
async def test_maintenance_cleanup_gap_or_completed_seq_never_claimed(completed, seq):
    repo, session = repository()
    repo.selected_kind = AsyncMock(return_value=WorkKind.CLEANUP)
    result = MagicMock()
    result.mappings.return_value.one_or_none.return_value = {"status": "pending", "cleanup_seq": seq}
    session.execute.return_value = result
    session.scalar.return_value = completed
    assert await repo.claim(uuid4(), owner=uuid4(), lease_seconds=30) is None
    assert session.execute.await_count == 1


async def test_maintenance_drain_includes_expired_and_generation_zero_tasks():
    repo, session = repository()
    session.scalar.return_value = True
    assert await repo._running_parse(uuid4())
    query = sql(session.scalar.call_args.args[0])
    assert "'running'" in query
    assert "locked_until" not in query and "binding_generation" not in query
