import asyncio
import logging

import nats.errors
from nats.aio.client import Client as NATS
from nats.js import JetStreamContext
from nats.js.api import (
    AckPolicy,
    ConsumerConfig,
    DeliverPolicy,
    DiscardPolicy,
    RetentionPolicy,
    StorageType,
    StreamConfig,
)
from nats.js.errors import BadRequestError, NotFoundError, ServiceUnavailableError

from clients.nats.lifecycle import NatsConnectionErrorPolicy
from settings import NatsSettings

logger = logging.getLogger(__name__)


class MaintenanceTopologyError(BadRequestError):
    """Permanent mismatch; existing supervisors treat BadRequest as fail-fast."""

    def __init__(self, description: str) -> None:
        super().__init__(code=400, description=description)


def _validate_stream(actual: StreamConfig, expected: StreamConfig) -> None:
    fields = (
        "name",
        "storage",
        "retention",
        "max_age",
        "max_msgs",
        "max_bytes",
        "discard",
        "duplicate_window",
        "max_msgs_per_subject",
        "max_msg_size",
    )
    for field in fields:
        if getattr(actual, field) != getattr(expected, field):
            raise MaintenanceTopologyError(f"Maintenance stream {expected.name}: incompatible {field}")
    if set(actual.subjects or []) != set(expected.subjects or []):
        raise MaintenanceTopologyError(f"Maintenance stream {expected.name}: incompatible subjects")
    # These options can silently drop, transform or replace retained business messages.
    for field in ("no_ack", "sealed", "discard_new_per_subject", "allow_rollup_hdrs", "allow_msg_ttl"):
        if getattr(actual, field):
            raise MaintenanceTopologyError(f"Maintenance stream {expected.name}: incompatible {field}")
    for field in ("mirror", "sources", "subject_transform", "republish"):
        if getattr(actual, field):
            raise MaintenanceTopologyError(f"Maintenance stream {expected.name}: incompatible {field}")


def _validate_consumer(actual: ConsumerConfig, expected: ConsumerConfig) -> None:
    for field in (
        "durable_name",
        "filter_subject",
        "deliver_policy",
        "ack_policy",
        "ack_wait",
        "max_deliver",
        "replay_policy",
    ):
        if getattr(actual, field) != getattr(expected, field):
            raise MaintenanceTopologyError(f"Maintenance durable {expected.durable_name}: incompatible {field}")
    for field in (
        "deliver_subject",
        "deliver_group",
        "filter_subjects",
        "backoff",
        "headers_only",
        "opt_start_seq",
        "opt_start_time",
        "inactive_threshold",
        "flow_control",
        "idle_heartbeat",
    ):
        if getattr(actual, field):
            raise MaintenanceTopologyError(f"Maintenance durable {expected.durable_name}: incompatible {field}")


def _maintenance_stream(name: str, subjects: list[str]) -> StreamConfig:
    return StreamConfig(
        name=name,
        subjects=subjects,
        storage=StorageType.FILE,
        retention=RetentionPolicy.LIMITS,
        max_age=0,
        max_msgs=-1,
        max_bytes=-1,
        discard=DiscardPolicy.NEW,
        duplicate_window=120,
    )


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

    async def _maintenance_stream(self, config: StreamConfig, *, owner: bool = False) -> None:
        if not config.name:
            raise MaintenanceTopologyError("Maintenance stream name is required")
        try:
            info = await self.jetstream.stream_info(config.name)
        except NotFoundError:
            if not owner:
                raise
            info = await self.jetstream.add_stream(config=config)
        _validate_stream(info.config, config)

    async def _maintenance_consumer(self, stream: str, config: ConsumerConfig) -> None:
        if not config.durable_name:
            raise MaintenanceTopologyError("Maintenance durable name is required")
        try:
            info = await self.jetstream.consumer_info(stream, config.durable_name)
        except NotFoundError:
            info = await self.jetstream.add_consumer(stream=stream, config=config)
        _validate_consumer(info.config, config)

    async def _setup_maintenance_topology(
        self,
        owned: StreamConfig,
        foreign: list[StreamConfig],
        routes: list[tuple[str, str, str]],
    ) -> None:
        # Read all foreign configs before registering any consumer. Never update a
        # stream/durable: add_consumer would otherwise silently alter mutable fields.
        for attempt in range(1, self._settings.nats_setup_max_attempts + 1):
            try:
                await self._maintenance_stream(owned, owner=True)
                for config in foreign:
                    await self._maintenance_stream(config)
                for stream, subject, durable in routes:
                    await self._maintenance_consumer(
                        stream,
                        ConsumerConfig(
                            durable_name=durable,
                            filter_subject=subject,
                            deliver_policy=DeliverPolicy.ALL,
                            ack_policy=AckPolicy.EXPLICIT,
                            ack_wait=30,
                            max_deliver=-1,
                        ),
                    )
                return
            except (
                NotFoundError,
                ServiceUnavailableError,
                nats.errors.TimeoutError,
                nats.errors.ConnectionClosedError,
                nats.errors.ConnectionReconnectingError,
                nats.errors.NoRespondersError,
            ):
                if attempt >= self._settings.nats_setup_max_attempts:
                    raise
                logger.warning("NATS maintenance owner unavailable: attempt=%s", attempt)
                await asyncio.sleep(self._settings.nats_setup_backoff_seconds * attempt)

    async def setup_maintenance(self) -> None:
        """Opt-in maintenance startup; canonical owner configs are immutable here."""
        s = self._settings
        await self._setup_maintenance_topology(
            _maintenance_stream(
                s.nats_stream_maintenance_results,
                [s.nats_subject_cleanup_completed, s.nats_subject_pages_merge_completed],
            ),
            [
                _maintenance_stream(s.nats_stream_yandex_lifecycle, [s.nats_subject_cleanup_requested]),
                _maintenance_stream(
                    s.nats_stream_seo_maintenance,
                    [s.nats_subject_pages_merge, s.nats_subject_pages_merge_finalized],
                ),
            ],
            self.maintenance_routes(),
        )

    def maintenance_routes(self) -> list[tuple[str, str, str]]:
        s = self._settings
        return [
            (s.nats_stream_yandex_lifecycle, s.nats_subject_cleanup_requested, s.nats_consumer_cleanup_requested),
            (s.nats_stream_seo_maintenance, s.nats_subject_pages_merge, s.nats_consumer_pages_merge),
            (
                s.nats_stream_seo_maintenance,
                s.nats_subject_pages_merge_finalized,
                s.nats_consumer_pages_merge_finalized,
            ),
        ]
