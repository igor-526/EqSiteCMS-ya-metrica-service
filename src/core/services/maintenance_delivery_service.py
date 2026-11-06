"""One lightweight inbox + ledger transaction. No stats work or dispatch here."""

from core.entities.metrica_maintenance import WorkKind
from core.protocols.repositories.metrica_maintenance import MaintenanceStorageProtocol
from core.schemas.messaging.maintenance import CleanupRequested, MergeFinalized, MergeMappings, MergeSeal
from core.services.parse_enqueue_service import ParseTransactionProtocol

MaintenanceCommand = CleanupRequested | MergeMappings | MergeSeal | MergeFinalized


class MaintenanceDeliveryService:
    def __init__(self, *, repository: MaintenanceStorageProtocol, transaction: ParseTransactionProtocol) -> None:
        self._repository = repository
        self._transaction = transaction

    async def receive(self, command: MaintenanceCommand, *, consumer: str) -> None:
        tenant, operation = command.equestrian_id, command.operation_id
        async with self._transaction.begin():
            if isinstance(command, CleanupRequested):
                await self._repository.save_operation(
                    tenant,
                    operation,
                    kind=WorkKind.CLEANUP,
                    cleanup_seq=command.cleanup_seq,
                    binding_generation=command.binding_generation,
                )
            elif not isinstance(command, MergeFinalized):
                await self._repository.save_operation(
                    tenant,
                    operation,
                    kind=WorkKind.MERGE,
                    normalization_revision=command.normalization_revision,
                )
            fresh = await self._repository.save_inbox(
                tenant,
                operation,
                consumer=consumer,
                message_id=command.message_id,
                payload=command.model_dump(mode="json"),
            )
            if fresh:
                if isinstance(command, MergeMappings):
                    await self._repository.save_chunk(
                        tenant,
                        operation,
                        part_index=command.part_index,
                        mappings=[pair.model_dump(mode="json") for pair in command.mappings],
                    )
                elif isinstance(command, MergeSeal):
                    await self._repository.seal(tenant, operation, part_count=command.part_count)
                elif isinstance(command, MergeFinalized):
                    await self._repository.finalize(
                        tenant,
                        operation,
                        normalization_revision=command.normalization_revision,
                        result_message_id=command.result_message_id,
                    )
        # Commit has completed before the consumer is allowed to ACK.
