from collections.abc import Mapping

from dependency_injector import providers
from sqlalchemy.ext.asyncio import AsyncSession

from clients.nats.client import NatsJetstreamClient
from clients.nats.consumers.parse_run import ParseRunConsumer
from clients.nats.handlers.parse_run import ParseRunHandler
from containers.clients import ClientsContainer
from containers.repositories import RepositoriesContainer
from containers.services import ServicesContainer
from containers.settings import SettingsContainer
from utils.database import worker_session
from workers.app_factory import create_celery_app

settings_container = SettingsContainer()
clients_container = ClientsContainer(settings_container=settings_container)
repositories_container = RepositoriesContainer()
celery_app_provider = providers.Singleton(
    create_celery_app,
    settings=settings_container.celery_settings,
    interval=settings_container.app_settings.provided.parse_dispatch_interval_seconds,
)
services_container = ServicesContainer(
    settings_container=settings_container,
    repositories_container=repositories_container,
    clients_container=clients_container,
    celery_app=celery_app_provider,
)


def create_parse_services(session: AsyncSession) -> ServicesContainer:
    """Fresh graph, without mutating process-global session providers."""
    repositories = RepositoriesContainer(session=providers.Object(session))
    return ServicesContainer(
        settings_container=settings_container,
        clients_container=clients_container,
        repositories_container=repositories,
        celery_app=celery_app_provider,
    )


def create_page_stats_services(session: AsyncSession) -> ServicesContainer:
    """Request-local read graph; unrelated clients and workers stay lazy."""
    repositories = RepositoriesContainer(session=providers.Object(session))
    return ServicesContainer(repositories_container=repositories)


def create_search_query_stats_services(session: AsyncSession) -> ServicesContainer:
    """Request-local query read graph; no workers, external clients or commits."""
    repositories = RepositoriesContainer(session=providers.Object(session))
    return ServicesContainer(repositories_container=repositories)


class DeliveryParseRunHandler:
    """DI boundary: each delivery owns a fresh session and service graph."""

    async def handle(self, *, payload: bytes, headers: Mapping[str, str] | None, subject: str) -> None:
        async with worker_session() as session:
            graph = create_parse_services(session)
            handler = ParseRunHandler(service=graph.parse_enqueue_service())
            await handler.handle(payload=payload, headers=headers, subject=subject)


nats_client_provider = providers.Singleton(NatsJetstreamClient, settings=settings_container.nats_settings)
parse_run_handler_provider = providers.Factory(DeliveryParseRunHandler)
parse_run_consumer_provider = providers.Singleton(
    ParseRunConsumer,
    client=nats_client_provider,
    settings=settings_container.nats_settings,
    handler=parse_run_handler_provider,
)
