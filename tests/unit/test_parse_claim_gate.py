from contextlib import asynccontextmanager
from typing import cast
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql

from core.entities.metrica_maintenance import BindingLifecycle
from repositories.metrica_parse_task_repository import ParseTaskRepository


def sql(stmt):
    return str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))


def repo():
    session = MagicMock(execute=AsyncMock())
    repository = ParseTaskRepository(session)
    repository.lock_scope = AsyncMock()
    repository._observe = AsyncMock(return_value={"binding_generation": 4, "cleanup_requested_seq": 2})
    repository._blocked = AsyncMock(return_value=False)
    repository._cancel_queued = AsyncMock(return_value=3)
    return repository, session


@pytest.mark.parametrize("blocked,bound", [(True, True), (False, False)])
async def test_gate_or_missing_binding_forbids_claim_and_dispatch(blocked, bound):
    repository, session = repo()
    lifecycle = BindingLifecycle(4, 2, 2, uuid4(), "123") if bound else BindingLifecycle(4, 2, 2)
    cast(AsyncMock, repository._blocked).return_value = blocked
    for claim in [True, False]:
        tenant = uuid4()
        if claim:
            assert (
                await repository.claim(
                    uuid4(), equestrian_id=tenant, lifecycle=lifecycle, locked_by="w", lease_seconds=10
                )
                is None
            )
        else:
            assert await repository.list_dispatchable_accounts(equestrian_id=tenant, lifecycle=lifecycle) == []
    session.execute.assert_not_awaited()


async def test_guard_cancels_old_generation_and_retry_under_scope():
    repository, _ = repo()
    tenant, account = uuid4(), uuid4()
    lifecycle = BindingLifecycle(4, 2, 2, account, "123")
    assert await repository._prepare_parse(tenant, lifecycle)
    cast(AsyncMock, repository.lock_scope).assert_awaited_once_with([tenant], [account])
    predicate = sql(cast(AsyncMock, repository._cancel_queued).call_args.args[1])
    assert "observed_cleanup_requested_seq < 2" in predicate
    assert "binding_generation < 4" in predicate
    cast(AsyncMock, repository._blocked).assert_awaited_once()


async def test_cancel_preserves_audit_running_and_post_delete():
    repository = ParseTaskRepository(MagicMock(execute=AsyncMock(return_value=MagicMock(all=lambda: [1, 2]))))
    repository.lock_scope = AsyncMock()
    tenant = uuid4()
    assert await repository.cancel_queued_before_cleanup(tenant, cleanup_seq=2) == 2
    stmt = sql(cast(AsyncMock, repository.session.execute).call_args.args[0])
    where = stmt.split("WHERE")[1]
    assert str(tenant) in where and "status = 'queued'" in where
    assert "observed_cleanup_requested_seq < 2" in where
    assert "next_attempt_at" not in where
    assert "status='cancelled'" in stmt and "finished_at=clock_timestamp()" in stmt
    assert "attempts=" not in stmt and "rows_fetched=" not in stmt and "DELETE" not in stmt


async def test_claim_atomic_snapshot_and_account_exclusivity():
    repository, session = repo()
    repository._prepare_parse = AsyncMock(return_value=True)
    result = MagicMock()
    result.mappings.return_value.one_or_none.return_value = None
    session.execute.return_value = result

    @asynccontextmanager
    async def nested():
        yield

    session.begin_nested = nested
    tenant, account = uuid4(), uuid4()
    lifecycle = BindingLifecycle(4, 2, 2, account, "123")
    assert (
        await repository.claim(account, equestrian_id=tenant, lifecycle=lifecycle, locked_by="w", lease_seconds=30)
        is None
    )
    stmt = sql(session.execute.call_args.args[0])
    for fragment in [
        str(tenant),
        str(account),
        "binding_generation = 4",
        "observed_cleanup_requested_seq = 2",
        "counter_id = '123'",
        "NOT (EXISTS",
        "FOR UPDATE OF metrica_parse_tasks SKIP LOCKED",
    ]:
        assert fragment in stmt
    repository._prepare_parse.assert_awaited_once_with(tenant, lifecycle)


async def test_account_mismatch_does_not_execute_claim():
    repository, session = repo()
    repository._prepare_parse = AsyncMock(return_value=True)
    assert (
        await repository.claim(
            uuid4(),
            equestrian_id=uuid4(),
            lifecycle=BindingLifecycle(1, 0, 0, uuid4(), "1"),
            locked_by="w",
            lease_seconds=1,
        )
        is None
    )
    session.execute.assert_not_awaited()
