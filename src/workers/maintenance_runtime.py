"""Independent maintenance transport; no Redis consumer performs NATS I/O."""

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from uuid import uuid4

from nats.js.errors import BadRequestError

from clients.nats.client import NatsJetstreamClient
from clients.nats.consumers.maintenance import MaintenanceConsumer
from clients.nats.maintenance_result_publisher import MaintenanceResultPublisher
from containers.application import create_parse_services
from utils.database import worker_session

logger = logging.getLogger(__name__)


async def relay_results(publisher: MaintenanceResultPublisher, *, interval: int) -> None:
    while True:
        async with worker_session() as session:
            graph = create_parse_services(session)
            graph.maintenance_result_publisher.override(publisher)
            await graph.maintenance_result_relay().relay_once(owner=uuid4())
        await asyncio.sleep(interval)


async def supervise_maintenance(
    client: NatsJetstreamClient,
    consumers: Sequence[MaintenanceConsumer],
    relay: Callable[[], Awaitable[None]],
) -> None:
    """Restart the entire group if any consumer/relay exits, including partial startup."""

    async def run_relay() -> None:
        await relay()

    backoff = 1.0
    while True:
        tasks: list[asyncio.Task[None]] = []
        try:
            await client.connect()
            await client.setup_maintenance()
            for consumer in consumers:
                await consumer.start()
            tasks = [asyncio.create_task(consumer.wait()) for consumer in consumers]
            tasks.append(asyncio.create_task(run_relay(), name="metrica-maintenance-result-relay"))
            backoff = 1.0
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
            logger.warning("Maintenance runtime exited; restarting")
        except asyncio.CancelledError:
            raise
        except BadRequestError:
            logger.error("Maintenance configuration rejected; supervisor stopped")
            return
        except Exception:
            logger.warning("Maintenance runtime unavailable; retry scheduled")
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.gather(*(consumer.stop() for consumer in consumers), return_exceptions=True)
            try:
                await client.close()
            except Exception:
                logger.warning("Maintenance transport already unavailable")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30.0)
