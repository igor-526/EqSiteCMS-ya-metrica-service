from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from core.exceptions.peer import PeerServiceException
from core.schemas.clients.yandex_service import BindingAccount, MetricaBinding, MetricaCounterBinding
from core.services.parse_dispatch_service import ParseDispatchService


@pytest.fixture
def env():
    events = []
    tenant, account = uuid4(), uuid4()
    active = False

    @asynccontextmanager
    async def begin():
        nonlocal active
        assert not active
        active = True
        events.append("begin")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")
        finally:
            active = False

    binding = MetricaBinding(
        counter=MetricaCounterBinding(
            equestrian_id=tenant, counter_id="123", yandex_account=BindingAccount(id=account)
        ),
        binding_generation=2,
        cleanup_requested_seq=1,
        cleanup_completed_seq=1,
    )

    async def fetch(**kwargs):
        assert not active
        events.append("peer")
        return binding

    repo = Mock(
        reap_expired_leases=AsyncMock(),
        list_recovery_tenants=AsyncMock(return_value=[]),
        list_queued_tenants=AsyncMock(return_value=[tenant]),
        list_dispatchable_accounts=AsyncMock(return_value=[account]),
        claim=AsyncMock(return_value=None),
    )
    peer = Mock(get_metrica_binding=AsyncMock(side_effect=fetch))
    service = ParseDispatchService(task_repository=repo, transaction=Mock(begin=begin), yandex_client=peer)
    return SimpleNamespace(**locals())


async def test_dispatch_peer_outside_transaction_and_commit_before_signal(env):
    assert await env.service.collect_ready_accounts() == [env.account]
    assert env.events == ["begin", "commit", "begin", "commit", "peer", "begin", "commit"]
    env.repo.list_dispatchable_accounts.assert_awaited_once_with(
        equestrian_id=env.tenant,
        lifecycle=env.binding.lifecycle(),
    )


async def test_claim_fetches_fresh_snapshot_even_after_dispatch(env):
    await env.service.collect_ready_accounts()
    env.binding.binding_generation = 3
    await env.service.claim_next_for_account(env.account, locked_by="worker", lease_seconds=60)
    assert env.peer.get_metrica_binding.await_count == 2
    env.repo.claim.assert_awaited_once_with(
        env.account,
        equestrian_id=env.tenant,
        lifecycle=env.binding.lifecycle(),
        locked_by="worker",
        lease_seconds=60,
    )


@pytest.mark.parametrize("claim", [False, True])
@pytest.mark.parametrize("status", [401, 403, 503, None])
async def test_peer_failure_fail_closed_without_claim_or_dispatch(env, claim, status):
    env.peer.get_metrica_binding.side_effect = PeerServiceException("yandex", status)
    if claim:
        assert await env.service.claim_next_for_account(env.account, locked_by="w", lease_seconds=1) is None
    else:
        assert await env.service.collect_ready_accounts() == []
    env.repo.claim.assert_not_awaited()
    env.repo.list_dispatchable_accounts.assert_not_awaited()


async def test_foreign_binding_rejected(env):
    env.binding.counter.equestrian_id = uuid4()
    assert await env.service.collect_ready_accounts() == []
    env.repo.list_dispatchable_accounts.assert_not_awaited()


async def test_unavailable_tenant_does_not_block_another(env):
    env.repo.list_queued_tenants.return_value = [uuid4(), env.tenant]
    env.peer.get_metrica_binding.side_effect = [PeerServiceException("yandex", 503), env.binding]
    assert await env.service.collect_ready_accounts() == [env.account]


async def test_reaper_failure_rolls_back_after_peer_snapshot(env):
    env.repo.list_recovery_tenants.return_value = [env.tenant]
    env.repo.reap_expired_leases.side_effect = RuntimeError()
    with pytest.raises(RuntimeError):
        await env.service.collect_ready_accounts()
    assert env.events == ["begin", "commit", "peer", "begin", "rollback"]


async def test_recovery_fetches_lifecycle_without_transaction(env):
    env.repo.list_recovery_tenants.return_value = [env.tenant]
    await env.service.collect_ready_accounts()
    env.repo.reap_expired_leases.assert_awaited_once_with(equestrian_id=env.tenant, lifecycle=env.binding.lifecycle())
    assert env.events[:5] == ["begin", "commit", "peer", "begin", "commit"]


async def test_recovery_peer_outage_does_not_requeue_expired_task(env):
    env.repo.list_recovery_tenants.return_value = [env.tenant]
    env.peer.get_metrica_binding.side_effect = PeerServiceException("yandex", 503)
    await env.service.collect_ready_accounts()
    env.repo.reap_expired_leases.assert_not_awaited()
