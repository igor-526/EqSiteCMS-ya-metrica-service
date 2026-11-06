"""Durable discovery, then disposable signals outside the storage transaction."""

import logging
from typing import Protocol
from uuid import UUID

from core.entities.metrica_maintenance import WorkKind
from core.services.parse_enqueue_service import ParseTransactionProtocol

logger = logging.getLogger(__name__)


class MaintenanceDiscoveryProtocol(Protocol):
    async def ready_tenants(self, *, kind: WorkKind, limit: int) -> list[UUID]: ...


class MaintenanceDispatcherProtocol(Protocol):
    async def dispatch_maintenance(self, tenant: UUID, *, kind: WorkKind) -> None: ...


class MergeWorkerProtocol(Protocol):
    async def tick(self, tenant: UUID, *, owner: UUID) -> str:
        """Handler must claim/fence and perform at most one bounded batch."""
        ...


class MaintenanceDispatchService:
    def __init__(
        self,
        *,
        repository: MaintenanceDiscoveryProtocol,
        transaction: ParseTransactionProtocol,
        dispatcher: MaintenanceDispatcherProtocol,
        merge_available: bool = False,
    ) -> None:
        self._repository = repository
        self._transaction = transaction
        self._dispatcher = dispatcher
        self._merge_available = merge_available

    async def dispatch_ready(self, *, limit: int = 100) -> int:
        if not 1 <= limit <= 1000:
            raise ValueError("Invalid dispatch limit")
        kinds = [WorkKind.CLEANUP, WorkKind.MERGE] if self._merge_available else [WorkKind.CLEANUP]
        sent = 0
        for kind in kinds:
            async with self._transaction.begin():
                tenants = await self._repository.ready_tenants(kind=kind, limit=limit)
            for tenant in tenants:
                try:
                    await self._dispatcher.dispatch_maintenance(tenant, kind=kind)
                    sent += 1
                except Exception:
                    # No durable mark: beat rediscovery recovers a lost signal.
                    logger.warning("Maintenance dispatch deferred: tenant=%s kind=%s", tenant, kind)
        return sent
