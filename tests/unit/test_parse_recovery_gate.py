"""UT-D-04..06 recovery SQL fences; PG effects are tested separately."""

from datetime import UTC, datetime
from typing import cast
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql

from core.entities import ParseErrorCode
from core.entities.metrica_maintenance import BindingLifecycle
from repositories.metrica_parse_task_repository import ParseTaskRepository


def sql(stmt):
    return str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))


def environment():
    result = MagicMock()
    result.all.return_value = [uuid4()]
    result.scalars.return_value = [uuid4()]
    session = MagicMock(execute=AsyncMock(return_value=result), scalar=AsyncMock(return_value=uuid4()))
    repo = ParseTaskRepository(session)
    repo.lock_scope = AsyncMock()
    repo.assert_writable = AsyncMock()
    repo._observe = AsyncMock()
    return repo, session


def assert_cancellation(stmt):
    assert "binding_generation = 0" in stmt
    assert "cleanup_requested_seq > metrica_parse_tasks.observed_cleanup_requested_seq" in stmt
    assert "binding_generation > metrica_parse_tasks.binding_generation" in stmt
    assert "kind = 'cleanup'" in stmt
    assert "THEN 'cancelled'" in stmt
    assert "THEN 'failed'" in stmt
    assert "ELSE 'queued'" in stmt
    assert "attempts=(metrica_parse_tasks.attempts + 1)" in stmt
    assert "locked_by=NULL" in stmt and "locked_until=NULL" in stmt


async def test_retry_is_guarded_observes_lifecycle_then_terminalizes_old_work():
    repo, session = environment()
    task = uuid4()
    lifecycle = BindingLifecycle(2, 1, 0, None, None)
    events = []
    cast(AsyncMock, repo.assert_writable).side_effect = lambda *a, **kw: events.append("guard")
    cast(AsyncMock, repo._observe).side_effect = lambda *a: events.append("observe")
    await repo.schedule_retry(
        task,
        locked_by="owner",
        next_attempt_at=datetime.now(UTC),
        error_code=ParseErrorCode.PROVIDER_UNAVAILABLE,
        error_status=503,
        lifecycle=lifecycle,
    )
    assert events == ["guard", "observe", "guard"]
    statement = sql(session.execute.call_args.args[0])
    assert_cancellation(statement)
    assert "error_code='provider_unavailable'" in statement and "error_status=503" in statement
    assert "locked_until > clock_timestamp()" in statement


async def test_reaper_locks_scope_before_update_preserves_audit():
    repo, session = environment()
    tenant = uuid4()
    lifecycle = BindingLifecycle(2, 1, 0, None, None)
    events = []
    cast(AsyncMock, repo.lock_scope).side_effect = lambda *a: events.append("scope")
    cast(AsyncMock, repo._observe).side_effect = lambda *a: events.append("observe")
    assert await repo.reap_expired_leases(equestrian_id=tenant, lifecycle=lifecycle) == 1
    assert events == ["scope", "scope", "observe"]
    statement = sql(session.execute.call_args.args[0])
    assert_cancellation(statement)
    assert "locked_until <= clock_timestamp()" in statement and "locked_until IS NULL" in statement
    assert "status = 'running'" in statement and str(tenant) in statement
    assert "next_offset=" not in statement and "rows_fetched=" not in statement
    assert "error_code='lease_expired'" in statement


async def test_recovery_discovery_bounded_no_task_row_locks():
    repo, session = environment()
    await repo.list_recovery_tenants(limit=17)
    statement = sql(session.execute.call_args.args[0])
    assert "LIMIT 17" in statement and "FOR UPDATE" not in statement
    assert "locked_until <= clock_timestamp()" in statement
    with pytest.raises(ValueError):
        await repo.list_recovery_tenants(limit=0)
    with pytest.raises(ValueError):
        await repo.reap_expired_leases(lifecycle=BindingLifecycle(0, 0, 0, None, None))
