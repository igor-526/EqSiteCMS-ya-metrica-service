"""Result publisher adapter; storage marks remain the relay's responsibility."""

import json

from clients.nats.client import NatsJetstreamClient
from core.entities.metrica_maintenance import MaintenanceResultClaim
from core.schemas.messaging.maintenance import CLEANUP_COMPLETED, MERGE_COMPLETED
from settings import NatsSettings


class MaintenanceResultPublisher:
    def __init__(self, *, client: NatsJetstreamClient, settings: NatsSettings) -> None:
        self._client = client
        self._settings = settings

    async def publish_result(self, claim: MaintenanceResultClaim) -> bool:
        if claim.subject not in {CLEANUP_COMPLETED, MERGE_COMPLETED}:
            raise ValueError("Invalid maintenance result subject")
        ack = await self._client.jetstream.publish(
            claim.subject,
            json.dumps(claim.payload, sort_keys=True, separators=(",", ":")).encode(),
            headers={"Nats-Msg-Id": str(claim.message_id)},
            timeout=5,
        )
        if ack.stream != self._settings.nats_stream_maintenance_results or ack.seq <= 0:
            raise RuntimeError("Invalid maintenance result PubAck")
        return bool(ack.duplicate)
