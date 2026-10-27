import asyncio
import logging

import nats.errors
from nats.aio.client import Client as NATS
from nats.js import JetStreamContext
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy
from nats.js.errors import NotFoundError

from clients.nats.lifecycle import NatsConnectionErrorPolicy
from settings import NatsSettings

logger = logging.getLogger(__name__)


class NatsJetstreamClient:
    def __init__(self, *, settings: NatsSettings) -> None:
        self._settings = settings
        self._connection: NATS | None = None
        self._jetstream: JetStreamContext | None = None
        self.error_policy = NatsConnectionErrorPolicy(
            service_name="ya-metrica-service", report_after_attempts=settings.nats_error_report_after_attempts
        )

    @property
    def is_connected(self) -> bool:
        return self._connection is not None and self._connection.is_connected

    @property
    def jetstream(self) -> JetStreamContext:
        if self._jetstream is None:
            raise RuntimeError("NATS JetStream client is not connected")
        # Retain the context during SDK reconnect: the subscription stays valid.
        return self._jetstream

    async def connect(self) -> None:
        if self.is_connected:
            return
        self._connection = NATS()
        await self._connection.connect(
            servers=self._settings.nats_servers,
            name="ya-metrica-service",
            connect_timeout=5,
            allow_reconnect=True,
            reconnect_time_wait=2,
            max_reconnect_attempts=10,
            error_cb=self.error_policy.on_error,
            disconnected_cb=self.error_policy.on_disconnected,
            reconnected_cb=self.error_policy.on_reconnected,
            closed_cb=self.error_policy.on_closed,
        )
        self._jetstream = self._connection.jetstream()
        self.error_policy.reset()

    async def close(self) -> None:
        connection = self._connection
        try:
            if connection is not None and not connection.is_closed:
                try:
                    await connection.drain()
                except TimeoutError, nats.errors.Error:
                    logger.warning("NATS drain unavailable; closing connection")
                finally:
                    await connection.close()
        finally:
            self._connection = None
            self._jetstream = None

    async def setup(self) -> None:
        await self.setup_streams()
        await self.setup_consumers()

    async def setup_streams(self) -> None:
        """No-op: PARSE_COMMANDS belongs exclusively to seo-service."""

    async def setup_consumers(self) -> None:
        for attempt in range(1, self._settings.nats_setup_max_attempts + 1):
            try:
                await self.jetstream.add_consumer(
                    stream=self._settings.nats_stream_parse_commands,
                    config=ConsumerConfig(
                        durable_name=self._settings.nats_consumer_parse_run,
                        filter_subject=self._settings.nats_subject_parse_run,
                        deliver_policy=DeliverPolicy.ALL,
                        ack_policy=AckPolicy.EXPLICIT,
                        ack_wait=self._settings.nats_consumer_ack_wait_seconds,
                        max_deliver=self._settings.nats_consumer_max_deliver,
                    ),
                )
                return
            except NotFoundError:
                if attempt >= self._settings.nats_setup_max_attempts:
                    raise
                logger.warning("NATS command stream not yet available: attempt=%s", attempt)
                await asyncio.sleep(self._settings.nats_setup_backoff_seconds * attempt)
