import logging
from uuid import UUID

from core.entities import ParseTask
from core.entities.metrica_maintenance import BindingLifecycle
from core.exceptions.peer import PeerServiceException
from core.protocols.clients.yandex_service import YandexServiceClientProtocol
from core.protocols.repositories.metrica_parse import ParseTaskRepositoryProtocol
from core.services.parse_enqueue_service import ParseTransactionProtocol

logger = logging.getLogger(__name__)


class ParseDispatchService:
    def __init__(
        self,
        *,
        task_repository: ParseTaskRepositoryProtocol,
        transaction: ParseTransactionProtocol,
        yandex_client: YandexServiceClientProtocol,
    ) -> None:
        self._tasks = task_repository
        self._transaction = transaction
        self._yandex = yandex_client

    async def lifecycle_for_tenant(self, tenant: UUID) -> BindingLifecycle | None:
        # Never hold a DB transaction/advisory lock during peer HTTP.
        try:
            binding = await self._yandex.get_metrica_binding(equestrian_id=tenant)
        except PeerServiceException:
            logger.warning("Parse lifecycle unavailable: equestrian_id=%s", tenant)
            return None
        if binding.counter is not None and binding.counter.equestrian_id != tenant:
            logger.warning("Parse lifecycle identity mismatch: equestrian_id=%s", tenant)
            return None
        return binding.lifecycle()

    async def collect_ready_accounts(self) -> list[UUID]:
        async with self._transaction.begin():
            recovery_tenants = await self._tasks.list_recovery_tenants()
        for tenant in recovery_tenants:
            lifecycle = await self.lifecycle_for_tenant(tenant)
            if lifecycle is None:
                continue  # fail closed; never recover using invented lifecycle
            async with self._transaction.begin():
                await self._tasks.reap_expired_leases(equestrian_id=tenant, lifecycle=lifecycle)
        async with self._transaction.begin():
            tenants = await self._tasks.list_queued_tenants()
        accounts: set[UUID] = set()
        for tenant in tenants:
            lifecycle = await self.lifecycle_for_tenant(tenant)
            if lifecycle is None:
                continue
            async with self._transaction.begin():
                accounts.update(
                    await self._tasks.list_dispatchable_accounts(
                        equestrian_id=tenant,
                        lifecycle=lifecycle,
                    )
                )
        return sorted(accounts)

    async def claim_next_for_account(
        self,
        account_id: UUID,
        *,
        locked_by: str,
        lease_seconds: int,
    ) -> ParseTask | None:
        async with self._transaction.begin():
            tenants = await self._tasks.list_queued_tenants(account_id)
        for tenant in tenants:
            lifecycle = await self.lifecycle_for_tenant(tenant)
            if lifecycle is None:
                continue
            async with self._transaction.begin():
                task = await self._tasks.claim(
                    account_id,
                    equestrian_id=tenant,
                    lifecycle=lifecycle,
                    locked_by=locked_by,
                    lease_seconds=lease_seconds,
                )
            if task is not None:
                return task
        return None
