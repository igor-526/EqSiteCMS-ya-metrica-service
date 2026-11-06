"""One short cleanup transaction; scheduling/finalization belong to the worker."""

from uuid import UUID

from core.entities.metrica_maintenance import CleanupBatch, MaintenanceLease, MaintenanceLeaseLostError
from core.protocols.repositories.metrica_maintenance import (
    CleanupBatchProtocol,
    CleanupFinalizerProtocol,
    CleanupWorkerProtocol,
)
from core.services.parse_enqueue_service import ParseTransactionProtocol


class CleanupBatchService:
    def __init__(
        self,
        *,
        repository: CleanupBatchProtocol,
        transaction: ParseTransactionProtocol,
        batch_size: int = 1000,
        lease_seconds: int = 60,
    ) -> None:
        if isinstance(batch_size, bool) or not 1 <= batch_size <= 10000 or lease_seconds <= 0:
            raise ValueError("Invalid cleanup batch settings")
        self._repository = repository
        self._transaction = transaction
        self._batch_size = batch_size
        self._lease_seconds = lease_seconds

    async def run_batch(self, lease: MaintenanceLease) -> CleanupBatch:
        # Returning outside the context guarantees commit before an outcome is used.
        # Any failure (including post-effect expiry/CAS) rolls back the whole batch.
        async with self._transaction.begin():
            result = await self._repository.delete_batch(
                lease, batch_size=self._batch_size, lease_seconds=self._lease_seconds
            )
        return result


class CleanupFinalizerService:
    """A fresh short transaction; an exhausted batch is not proof of completion."""

    def __init__(self, *, repository: CleanupFinalizerProtocol, transaction: ParseTransactionProtocol) -> None:
        self._repository = repository
        self._transaction = transaction

    async def finalize(self, lease: MaintenanceLease) -> bool:
        async with self._transaction.begin():
            completed = await self._repository.finalize_cleanup(lease)
        return completed


class CleanupWorkerService:
    """One signal = one bounded batch. No loop, broker calls or runtime activation."""

    def __init__(
        self,
        *,
        repository: CleanupWorkerProtocol,
        transaction: ParseTransactionProtocol,
        batch_service: CleanupBatchService,
        finalizer: CleanupFinalizerService,
        lease_seconds: int = 60,
        max_attempts: int = 5,
    ) -> None:
        if lease_seconds <= 0 or max_attempts <= 0:
            raise ValueError("Invalid cleanup lease duration")
        self._repository = repository
        self._transaction = transaction
        self._batch_service = batch_service
        self._finalizer = finalizer
        self._lease_seconds = lease_seconds
        self._max_attempts = max_attempts

    async def tick(self, tenant: UUID, *, owner: UUID) -> str:
        async with self._transaction.begin():
            lease = await self._repository.claim_cleanup(tenant, owner=owner, lease_seconds=self._lease_seconds)
        if lease is None:
            return "waiting"
        try:
            try:
                batch = await self._batch_service.run_batch(lease)
                if batch.exhausted and await self._finalizer.finalize(lease):
                    return "completed"
                async with self._transaction.begin():
                    await self._repository.yield_cleanup(lease)
            except MaintenanceLeaseLostError:
                raise
            except Exception:
                # Effect/finalizer transaction has rolled back before this fresh
                # fenced transition. Never persist exception text or payloads.
                # Cancellation still propagates and is recovered by lease expiry.
                async with self._transaction.begin():
                    return await self._repository.fail_cleanup(lease, max_attempts=self._max_attempts)
        except MaintenanceLeaseLostError:
            return "lease_lost"
        return "progress"
