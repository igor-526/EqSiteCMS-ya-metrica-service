from dependency_injector import containers, providers

from clients import SeoServiceClient, YandexMetricaClient, YandexMetricaStatClient, YandexServiceClient


class ClientsContainer(containers.DeclarativeContainer):
    settings_container = providers.DependenciesContainer()

    yandex_metrica_client = providers.Factory(
        YandexMetricaClient,
    )

    stat_client = providers.Factory(
        YandexMetricaStatClient, timeout=settings_container.app_settings.provided.ya_metrica_stat_timeout_seconds
    )
    yandex_service_client = providers.Factory(
        YandexServiceClient, base_url=settings_container.app_settings.provided.yandex_service_url
    )
    seo_service_client = providers.Factory(
        SeoServiceClient, base_url=settings_container.app_settings.provided.seo_service_url
    )
