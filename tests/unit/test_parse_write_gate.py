"""UT-D-05..06: write fencing has the same scope lock order as maintenance."""

from typing import cast
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql

from core.protocols.repositories.metrica_parse import LeaseLostError
from repositories.metrica_parse_task_repository import ParseTaskRepository


def sql(stmt):
    return str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))


def environment(*, scope_exists=True, owned=True):
    task, tenant, account = uuid4(), uuid4(), uuid4()
    scope = {"equestrian_id": tenant, "yandex_account_id": account} if scope_exists else None
    result = MagicMock()
    result.mappings.return_value.one_or_none.return_value = scope
    session = MagicMock(execute=AsyncMock(return_value=result), scalar=AsyncMock(return_value=task if owned else None))
    repo = ParseTaskRepository(session)
    repo.lock_scope = AsyncMock()
    return repo, session, task, tenant, account


async def test_guard_discovers_scope_then_locks_then_checks_ownership():
    repo, session, task, tenant, account = environment()
    events = []
    session.execute.side_effect = lambda stmt: events.append("discover") or session.execute.return_value
    cast(AsyncMock, repo.lock_scope).side_effect = lambda *args: events.append("scope")
    session.scalar.side_effect = lambda stmt: events.append("owned") or task
    await repo.assert_writable(task, locked_by="attempt-token")
    assert events == ["discover", "scope", "owned"]
    cast(AsyncMock, repo.lock_scope).assert_awaited_once_with([tenant], [account])
    discovery = sql(session.execute.call_args.args[0])
    assert "FOR UPDATE" not in discovery and str(task) in discovery
    guard = sql(session.scalar.call_args.args[0])
    for fragment in [
        str(task),
        str(tenant),
        "status = 'running'",
        "locked_by = 'attempt-token'",
        "locked_until > clock_timestamp()",
        "FOR UPDATE OF metrica_parse_tasks",
        "NOT (EXISTS",
        "metrica_maintenance_operations.equestrian_id",
        "metrica_tenant_gates.cleanup_completed_seq > metrica_parse_tasks.observed_cleanup_requested_seq",
    ]:
        assert fragment in guard
    # Pending cleanup/merge permits only the already running attempt to drain.
    assert "'pending', 'draining', 'collecting', 'waiting', 'completed'" in guard
    assert "binding_generation =" not in guard
    assert "cleanup_requested_seq =" not in guard


async def test_missing_task_fails_closed_before_locks():
    repo, session, task, _, _ = environment(scope_exists=False)
    with pytest.raises(LeaseLostError):
        await repo.assert_writable(task, locked_by="w")
    cast(AsyncMock, repo.lock_scope).assert_not_awaited()
    session.scalar.assert_not_awaited()


async def test_expired_owner_or_executing_maintenance_fails_closed():
    repo, _, task, _, _ = environment(owned=False)
    with pytest.raises(LeaseLostError):
        await repo.assert_writable(task, locked_by="stale")


async def test_owned_transition_also_locks_scope_and_rechecks_expiry():
    repo, session, task, _, _ = environment()
    repo.assert_writable = AsyncMock()
    session.execute.return_value.scalar_one_or_none.return_value = task
    await repo.mark_succeeded(task, locked_by="w")
    cast(AsyncMock, repo.assert_writable).assert_awaited_once_with(task, locked_by="w")
    statement = sql(session.execute.call_args.args[0])
    assert "locked_until > clock_timestamp()" in statement
    assert "status='succeeded'" in statement


async def test_transition_lease_loss_stops_before_update():
    repo, session, task, _, _ = environment()
    repo.assert_writable = AsyncMock(side_effect=LeaseLostError())
    with pytest.raises(LeaseLostError):
        await repo.mark_succeeded(task, locked_by="stale")
    session.execute.assert_not_awaited()
