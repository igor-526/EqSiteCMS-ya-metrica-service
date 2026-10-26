from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from core.services.parse_dispatch_service import ParseDispatchService


async def test_reaper_commits_before_dispatchable_result():
    events = []
    account = uuid4()

    @asynccontextmanager
    async def begin():
        events.append("begin")
        yield
        events.append("commit")

    async def reap():
        events.append("reap")
        return 2

    async def ready():
        events.append("ready")
        return [account]

    repo = SimpleNamespace(
        reap_expired_leases=AsyncMock(side_effect=reap), list_dispatchable_accounts=AsyncMock(side_effect=ready)
    )
    service = ParseDispatchService(task_repository=repo, transaction=SimpleNamespace(begin=begin))
    assert await service.collect_ready_accounts() == [account]
    assert events == ["begin", "reap", "ready", "commit"]


async def test_reaper_failure_rolls_back_without_collecting():
    events = []

    @asynccontextmanager
    async def begin():
        try:
            yield
        except Exception:
            events.append("rollback")
            raise

    repo = SimpleNamespace(
        reap_expired_leases=AsyncMock(side_effect=RuntimeError()), list_dispatchable_accounts=AsyncMock()
    )
    service = ParseDispatchService(task_repository=repo, transaction=SimpleNamespace(begin=begin))
    with pytest.raises(RuntimeError):
        await service.collect_ready_accounts()
    assert events == ["rollback"]
    repo.list_dispatchable_accounts.assert_not_awaited()
