from dependency_injector import containers, providers

from depends.utils import get_session


class RepositoriesContainer(containers.DeclarativeContainer):
    session = providers.ContextLocalResource(get_session)
