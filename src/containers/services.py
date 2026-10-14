from dependency_injector import containers, providers

from core.services import EncryptionService, YandexMetricaService


class ServicesContainer(containers.DeclarativeContainer):
    settings_container = providers.DependenciesContainer()
    repositories_container = providers.DependenciesContainer()
    clients_container = providers.DependenciesContainer()

    encryption_service = providers.Factory(
        EncryptionService, secret_key=settings_container.app_settings.provided.encryption_service_key
    )
    yandex_metrica_service = providers.Factory(
        YandexMetricaService,
        encryption_service=encryption_service,
        yandex_metrica_client=clients_container.yandex_metrica_client,
    )
