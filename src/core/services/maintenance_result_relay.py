"""One durable claim per tick. No transaction spans broker I/O."""

import logging
from uuid import UUID

from core.protocols.repositories.metrica_maintenance import (
    MaintenanceResultOutboxProtocol,
    MaintenanceResultPublisherProtocol,
)
from core.services.parse_enqueue_service import ParseTransactionProtocol

logger = logging.getLogger(__name__)


class MaintenanceResultRelay:
    def __init__(
        self,
        *,
        repository: MaintenanceResultOutboxProtocol,
        publisher: MaintenanceResultPublisherProtocol,
        transaction: ParseTransactionProtocol,
        lease_seconds: int = 60,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("Invalid result claim duration")
        self._repository = repository
        self._publisher = publisher
        self._transaction = transaction
        self._lease_seconds = lease_seconds

    async def relay_once(self, *, owner: UUID) -> bool:
        async with self._transaction.begin():
            claim = await self._repository.claim_result(owner=owner, lease_seconds=self._lease_seconds)
        if claim is None:
            return False
        try:
            duplicate = await self._publisher.publish_result(claim)
        except Exception:
            # No raw payload, exception text or credentials in diagnostics.
            logger.warning(
                "Maintenance result publish failed: message_id=%s attempt=%s", claim.message_id, claim.attempts
            )
            if claim.attempts >= 5:
                logger.error("Maintenance result delivery requires attention: message_id=%s", claim.message_id)
            async with self._transaction.begin():
                await self._repository.retry_result(claim)
            return True
        if duplicate:
            logger.warning("Maintenance result deduplicated by broker: message_id=%s", claim.message_id)
        async with self._transaction.begin():
            await self._repository.mark_published(claim)
        return True
