import asyncio
import logging
from collections.abc import Mapping
from typing import Protocol

from nats.aio.msg import Msg
from nats.errors import TimeoutError
from nats.js import JetStreamContext

from clients.nats.handlers.parse_run import PermanentCommandError
from settings import NatsSettings

logger = logging.getLogger(__name__)


class ParseRunHandlerProtocol(Protocol):
    async def handle(self, *, payload: bytes, headers: Mapping[str, str] | None, subject: str) -> None: ...


class NatsConsumerClientProtocol(Protocol):
    @property
    def jetstream(self) -> JetStreamContext: ...


class ParseRunConsumer:
    def __init__(
        self, *, client: NatsConsumerClientProtocol, settings: NatsSettings, handler: ParseRunHandlerProtocol
    ) -> None:
        self._client = client
        self._settings = settings
        self._handler = handler
        self._task: asyncio.Task[None] | None = None
        self._subscription: JetStreamContext.PullSubscription | None = None

    @property
    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self.is_running:
            return
        self._subscription = await self._client.jetstream.pull_subscribe(
            subject=self._settings.nats_subject_parse_run,
            durable=self._settings.nats_consumer_parse_run,
            stream=self._settings.nats_stream_parse_commands,
        )
        self._task = asyncio.create_task(self._consume(), name="metrica-parse-run-consumer")

    async def wait(self) -> None:
        if self._task is not None:
            await self._task

    async def stop(self) -> None:
        try:
            if self._task is not None:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    logger.warning("NATS consumer stopped after failure")
        finally:
            self._task = None
            subscription, self._subscription = self._subscription, None
            if subscription is not None:
                try:
                    await subscription.unsubscribe()
                except Exception:
                    logger.warning("NATS subscription already unavailable")

    async def _consume(self) -> None:
        assert self._subscription is not None
        while True:
            try:
                messages = await self._subscription.fetch(
                    batch=self._settings.nats_consumer_fetch_batch_size,
                    timeout=self._settings.nats_consumer_fetch_timeout_seconds,
                )
            except TimeoutError:
                continue
            # Other fetch/ack failures restart the transport via the supervisor.
            for message in messages:
                await self._process_message(message)

    async def _process_message(self, message: Msg) -> None:
        try:
            await self._handler.handle(payload=message.data, headers=message.headers, subject=message.subject)
        except PermanentCommandError:
            await message.term()
        except Exception:
            logger.warning("NATS parse delivery failed; retry requested")
            await message.nak()
        else:
            await message.ack()
