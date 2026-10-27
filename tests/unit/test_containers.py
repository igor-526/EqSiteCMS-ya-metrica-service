from unittest.mock import AsyncMock

from dependency_injector import providers

from clients import YandexMetricaClient
from containers import clients_container, repositories_container, services_container, settings_container
from containers.clients import ClientsContainer
from containers.repositories import RepositoriesContainer
from containers.services import ServicesContainer
from containers.settings import SettingsContainer
from core.services import EncryptionService, YandexMetricaService
from depends import utils as dependencies
from settings import Settings


def test_settings_provider_is_singleton():
    container = SettingsContainer()
    first = container.app_settings()
    assert isinstance(first, Settings)
    assert container.app_settings() is first
    container.app_settings.reset()
    assert container.app_settings() is not first


def test_application_container_wires_factories():
    assert services_container.settings_container.app_settings() is settings_container.app_settings()
    assert services_container.repositories_container.session.overridden[-1] is repositories_container.session
    assert (
        services_container.clients_container.yandex_metrica_client.overridden[-1]
        is clients_container.yandex_metrica_client
    )
    first = services_container.yandex_metrica_service()
    second = services_container.yandex_metrica_service()
    assert isinstance(first, YandexMetricaService)
    assert first is not second
    assert first._yandex_metrica_client is not second._yandex_metrica_client
    assert isinstance(first._yandex_metrica_client, YandexMetricaClient)
    assert isinstance(first._encryption_service, EncryptionService)
    token = first._encryption_service.encrypt(data="token")
    assert second._encryption_service.decrypt(data=token) == "token"


def test_container_settings_override_controls_encryption_secret():
    settings = SettingsContainer()
    clients = ClientsContainer(settings_container=settings)
    repositories = RepositoriesContainer()
    services = ServicesContainer(
        settings_container=settings, clients_container=clients, repositories_container=repositories
    )
    custom_settings = Settings(_env_file=None, ENCRYPTION_SERVICE_KEY="isolated-secret")
    original = settings.app_settings()
    with settings.app_settings.override(providers.Object(custom_settings)):
        encrypted = services.encryption_service().encrypt(data="token")
        assert EncryptionService("isolated-secret").decrypt(data=encrypted) == "token"
    assert settings.app_settings() is original
    assert not settings.app_settings.overridden


async def test_context_local_session_resource_initializes_and_shuts_down(monkeypatch):
    context = AsyncMock()
    session = object()
    context.__aenter__.return_value = session
    monkeypatch.setattr(dependencies, "AsyncSessionLocal", lambda: context)
    container = RepositoriesContainer()
    assert await container.session() is session
    assert await container.session() is session
    context.__aenter__.assert_awaited_once_with()
    await container.shutdown_resources()
    context.__aexit__.assert_awaited_once_with(None, None, None)


def test_client_override_restores_real_factory():
    fake = object()
    with clients_container.yandex_metrica_client.override(fake):
        assert services_container.yandex_metrica_service()._yandex_metrica_client is fake
    assert not clients_container.yandex_metrica_client.overridden
    assert isinstance(services_container.yandex_metrica_service()._yandex_metrica_client, YandexMetricaClient)


def test_parse_graph_shares_one_session_without_global_override():
    from unittest.mock import MagicMock

    from containers.application import celery_app_provider, create_parse_services
    from core.entities.metrica_parse import ParseKind

    session = MagicMock()
    other_session = MagicMock()
    original_overrides = repositories_container.session.overridden
    with celery_app_provider.override(MagicMock()):
        services = create_parse_services(session)
        runner = services.task_runner()
        enqueue = services.parse_enqueue_service()
        dispatch = services.parse_dispatch_service()
        assert runner._transaction is runner._ingestion._transaction is session
        assert enqueue._transaction is dispatch._transaction is session
        assert runner._tasks.session is runner._ingestion._tasks.session is session
        assert enqueue._runs.session is session
        for kind in ParseKind:
            assert runner._stats[kind].session is runner._ingestion._stats[kind].session is session
            assert enqueue._stats[kind].session is session
        assert create_parse_services(other_session).task_runner()._transaction is other_session
        assert services.task_runner()._transaction is session
        assert runner._lease_seconds == settings_container.app_settings().task_lease_seconds
        assert runner._ingestion._limit == settings_container.app_settings().ya_metrica_stat_page_limit
        assert enqueue._max_attempts == settings_container.app_settings().task_max_attempts
        assert enqueue._resolver._utc_offset_hours == settings_container.app_settings().ya_metrica_utc_offset_hours
    assert repositories_container.session.overridden == original_overrides


def test_celery_di_singleton_and_cli_entrypoint(monkeypatch):
    import importlib

    from containers.application import celery_app_provider
    from settings import CelerySettings

    custom = CelerySettings(_env_file=None, REDIS_PASSWORD="test-only")
    celery_app_provider.reset()
    with settings_container.celery_settings.override(custom):
        first = celery_app_provider()
        try:
            assert celery_app_provider() is first
            module = importlib.import_module("workers.celery_app")
            assert module.celery_app is first
            assert services_container.parse_dispatcher()._app is first
        finally:
            first.close()
            celery_app_provider.reset()


def test_page_stats_graph_is_request_scoped_and_lazy():
    from unittest.mock import MagicMock

    from containers.application import create_page_stats_services
    from core.services.page_stats_query_service import PageStatsQueryService
    from repositories.page_stats_query_repository import PageStatsQueryRepository

    session, other_session = MagicMock(), MagicMock()
    original = repositories_container.session.overridden
    graph = create_page_stats_services(session)
    first, second = graph.page_stats_query_service(), graph.page_stats_query_service()
    assert isinstance(first, PageStatsQueryService)
    assert isinstance(first._repository, PageStatsQueryRepository)
    assert first is not second
    assert first._repository is not second._repository
    assert first._repository.session is second._repository.session is session
    assert create_page_stats_services(other_session).page_stats_query_service()._repository.session is other_session
    assert repositories_container.session.overridden == original
