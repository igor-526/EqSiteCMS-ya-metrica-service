"""One signal, one bounded merge batch. PostgreSQL owns recovery and barriers."""

from uuid import UUID

from core.entities.metrica_maintenance import MaintenanceLeaseLostError, MergeOverflowError, MergeStorageError
from core.protocols.repositories.metrica_maintenance import MergeWorkerRepositoryProtocol
from core.services.parse_enqueue_service import ParseTransactionProtocol


class MergeWorkerService:
    def __init__(
        self,
        *,
        repository: MergeWorkerRepositoryProtocol,
        transaction: ParseTransactionProtocol,
        batch_size: int = 1000,
        lease_seconds: int = 60,
        max_attempts: int = 5,
    ) -> None:
        if type(batch_size) is not int or not 1 <= batch_size <= 1000 or lease_seconds <= 0 or max_attempts <= 0:
            raise ValueError("Invalid merge worker settings")
        self._repository = repository
        self._transaction = transaction
        self._batch_size = batch_size
        self._lease_seconds = lease_seconds
        self._max_attempts = max_attempts

    async def tick(self, tenant: UUID, *, owner: UUID) -> str:
        async with self._transaction.begin():
            lease = await self._repository.claim_merge(tenant, owner=owner, lease_seconds=self._lease_seconds)
        if lease is None:
            return "waiting"
        try:
            try:
                async with self._transaction.begin():
                    batch = await self._repository.move_batch(
                        lease, batch_size=self._batch_size, lease_seconds=self._lease_seconds
                    )
                # A separate short transaction rechecks ownership, priority and completeness.
                async with self._transaction.begin():
                    if (batch is None or batch.part_completed) and await self._repository.finalize_merge(lease):
                        return "awaiting_seo"
                    await self._repository.yield_merge(lease)
                return "progress"
            except MergeStorageError as error:
                # Never record blocked/retry in the aborted effect transaction.
                async with self._transaction.begin():
                    return await self._repository.fail_merge(
                        lease, overflow=isinstance(error, MergeOverflowError), max_attempts=self._max_attempts
                    )
        except MaintenanceLeaseLostError:
            return "lease_lost"
        # Cancellation/unexpected errors propagate; the durable lease expires and
        # rediscovery resumes from committed source deletes + chunk checkpoints.
