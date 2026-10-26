from dependency_injector import containers, providers

from settings import CelerySettings, Settings


class SettingsContainer(containers.DeclarativeContainer):
    celery_settings = providers.Singleton(CelerySettings)
    app_settings = providers.Singleton(Settings)
