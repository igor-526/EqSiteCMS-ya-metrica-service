from containers.clients import ClientsContainer
from containers.repositories import RepositoriesContainer
from containers.services import ServicesContainer
from containers.settings import SettingsContainer

settings_container = SettingsContainer()
clients_container = ClientsContainer(settings_container=settings_container)
repositories_container = RepositoriesContainer()
services_container = ServicesContainer(
    settings_container=settings_container,
    repositories_container=repositories_container,
    clients_container=clients_container,
)
