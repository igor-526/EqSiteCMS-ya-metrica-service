from dependency_injector import providers
from sqlalchemy.ext.asyncio import AsyncSession

from containers.clients import ClientsContainer
from containers.repositories import RepositoriesContainer
from containers.services import ServicesContainer
from containers.settings import SettingsContainer
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
