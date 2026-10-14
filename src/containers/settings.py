from dependency_injector import containers, providers

from settings import Settings


class SettingsContainer(containers.DeclarativeContainer):
    app_settings = providers.Singleton(Settings)
