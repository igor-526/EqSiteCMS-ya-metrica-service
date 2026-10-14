from dependency_injector import containers, providers

from clients import YandexMetricaClient


class ClientsContainer(containers.DeclarativeContainer):
    settings_container = providers.DependenciesContainer()

    yandex_metrica_client = providers.Factory(
        YandexMetricaClient,
    )
