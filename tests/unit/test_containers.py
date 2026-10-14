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
