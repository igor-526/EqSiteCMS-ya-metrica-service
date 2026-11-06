from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from core.entities.metrica_maintenance import PendingParseIntent
from core.exceptions.parse import MetricaBindingMissingError
from core.services.parse_intent_replay_service import ParseIntentReplayService


@pytest.fixture
def env():
    events = []
    active = False

    @asynccontextmanager
    async def begin():
        nonlocal active
        assert not active
        active = True
        events.append("begin")
        try:
            yield
        except Exception:
            events.append("rollback")
            raise
        else:
            events.append("commit")
        finally:
            active = False

    intents = [PendingParseIntent(uuid4(), uuid4(), "auto") for _ in range(3)]
    repo = Mock(ready_intents=AsyncMock(return_value=intents), defer_intent=AsyncMock())

    async def replay(intent):
        assert not active  # no transaction held across the authoritative peer request
        events.append("peer")
        return None

    enqueue = Mock(replay_intent=AsyncMock(side_effect=replay))
    now = datetime(2026, 10, 26, tzinfo=UTC)
    service = ParseIntentReplayService(
        intent_repository=repo,
        enqueue_service=enqueue,
        transaction=Mock(begin=begin),
        clock=lambda: now,
    )
    return SimpleNamespace(**locals())


async def test_ut_d06_discovery_commits_before_peer_and_defers_waiters(env):
    assert await env.service.replay_ready(limit=3) == 0
    assert env.events[:3] == ["begin", "commit", "peer"]
    env.repo.ready_intents.assert_awaited_once_with(limit=3)
    assert env.repo.defer_intent.await_count == 3
    for call, intent in zip(env.repo.defer_intent.await_args_list, env.intents, strict=True):
        assert call.args == (intent,)
        assert call.kwargs == {"next_attempt_at": env.now + timedelta(seconds=60)}


async def test_ut_d06_outage_does_not_starve_other_tenants_or_log_secrets(env, caplog):
    env.enqueue.replay_intent.side_effect = [RuntimeError("secret-token"), object(), None]
    assert await env.service.replay_ready() == 1
    assert env.enqueue.replay_intent.await_count == 3
    assert env.repo.defer_intent.await_count == 3
    assert "secret-token" not in caplog.text
    assert "Traceback" not in caplog.text


async def test_ut_d06_terminal_missing_binding_not_deferred(env):
    env.enqueue.replay_intent.side_effect = [MetricaBindingMissingError(), None, None]
    assert await env.service.replay_ready() == 0
    assert env.repo.defer_intent.await_count == 2


async def test_ut_d06_storage_discovery_failure_propagates(env):
    env.repo.ready_intents.side_effect = RuntimeError("storage unavailable")
    with pytest.raises(RuntimeError):
        await env.service.replay_ready()
    env.enqueue.replay_intent.assert_not_awaited()
    assert env.events == ["begin", "rollback"]


async def test_ut_d06_storage_defer_failure_propagates_without_losing_intent(env):
    env.repo.defer_intent.side_effect = RuntimeError("storage unavailable")
    with pytest.raises(RuntimeError):
        await env.service.replay_ready()
    assert env.enqueue.replay_intent.await_count == 1
    assert env.events[-1] == "rollback"


@pytest.mark.parametrize("limit", [0, 1001])
async def test_ut_d06_replay_bounded_discovery(env, limit):
    with pytest.raises(ValueError):
        await env.service.replay_ready(limit=limit)
    env.repo.ready_intents.assert_not_awaited()
