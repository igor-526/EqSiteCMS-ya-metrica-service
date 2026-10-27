from dependency_injector import containers, providers

from settings import CelerySettings, NatsSettings, Settings


class SettingsContainer(containers.DeclarativeContainer):
    nats_settings = providers.Singleton(NatsSettings)
    celery_settings = providers.Singleton(CelerySettings)
    app_settings = providers.Singleton(Settings)
