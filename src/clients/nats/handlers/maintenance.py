import logging
from collections.abc import Mapping
from typing import Annotated

from pydantic import Field, TypeAdapter, ValidationError

from clients.nats.handlers.parse_run import PermanentCommandError
from core.entities.metrica_maintenance import MaintenanceConflictError
from core.schemas.messaging.maintenance import (
    CLEANUP_REQUESTED,
    MAX_BODY_BYTES,
    MERGE_COMMAND,
    MERGE_FINALIZED,
    CleanupRequested,
    MergeFinalized,
    MergeMappings,
    MergeSeal,
    identity_kind,
    maintenance_message_id,
)
from core.services.maintenance_delivery_service import MaintenanceCommand, MaintenanceDeliveryService

logger = logging.getLogger(__name__)
merge_adapter: TypeAdapter[MergeMappings | MergeSeal] = TypeAdapter(
    Annotated[MergeMappings | MergeSeal, Field(discriminator="kind")]
)


class MaintenanceHandler:
    def __init__(self, *, service: MaintenanceDeliveryService, consumer: str) -> None:
        self._service = service
        self._consumer = consumer

    async def handle(self, *, payload: bytes, headers: Mapping[str, str] | None, subject: str) -> None:
        try:
            if len(payload) > MAX_BODY_BYTES:
                raise ValueError("Body too large")
            command: MaintenanceCommand
            if subject == CLEANUP_REQUESTED:
                command = CleanupRequested.model_validate_json(payload)
            elif subject == MERGE_COMMAND:
                command = merge_adapter.validate_json(payload)
            elif subject == MERGE_FINALIZED:
                command = MergeFinalized.model_validate_json(payload)
            else:
                raise ValueError("Unknown subject")
            expected = maintenance_message_id(subject, command.operation_id, identity_kind(command))
            if command.message_id != expected or headers is None or headers.get("Nats-Msg-Id") != str(expected):
                raise ValueError("Message identity mismatch")
        except ValidationError, ValueError:
            logger.warning("Maintenance delivery rejected: invalid_schema_or_identity")
            raise PermanentCommandError("Invalid maintenance command") from None
        try:
            await self._service.receive(command, consumer=self._consumer)
        except MaintenanceConflictError:
            logger.warning("Maintenance delivery rejected: storage_correlation_conflict")
            raise PermanentCommandError("Maintenance correlation conflict") from None
