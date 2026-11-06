"""Disposable wakeups only. Beat recovers signals from PostgreSQL."""

import asyncio
import logging
from uuid import UUID, uuid4

from celery import shared_task  # type: ignore[import-untyped]

from containers.application import create_parse_services
from settings import settings
from utils.database import worker_session

logger = logging.getLogger(__name__)


async def _tick(tenant: UUID, *, merge: bool = False) -> str:
    if not settings.maintenance_runtime_enabled:
        return "disabled"
    async with worker_session() as session:
        graph = create_parse_services(session)
        handler = graph.merge_worker_service() if merge else graph.cleanup_worker_service()
        if handler is None:
            logger.warning("Merge handler unavailable; durable barrier retained")
            return "unavailable"
        return await handler.tick(tenant, owner=uuid4())


async def _dispatch_maintenance() -> None:
    if not settings.maintenance_runtime_enabled:
        return
    async with worker_session() as session:
        graph = create_parse_services(session)
        await graph.maintenance_dispatch_service().dispatch_ready(limit=settings.maintenance_dispatch_limit)


async def _replay_intents() -> None:
    if not settings.maintenance_runtime_enabled:
        return
    async with worker_session() as session:
        await create_parse_services(session).parse_intent_replay_service().replay_ready(limit=100)


@shared_task(name="metrica.cleanup_tick", queue="ya_metrica_cleanup")
def cleanup_tick(tenant: str) -> str:
    return asyncio.run(_tick(UUID(tenant)))


@shared_task(name="metrica.merge_tick", queue="ya_metrica_merge")
def merge_tick(tenant: str) -> str:
    return asyncio.run(_tick(UUID(tenant), merge=True))


@shared_task(name="metrica.dispatch_maintenance", queue="ya_metrica")
def dispatch_maintenance() -> None:
    asyncio.run(_dispatch_maintenance())


@shared_task(name="metrica.replay_intents", queue="ya_metrica")
def replay_intents() -> None:
    asyncio.run(_replay_intents())
