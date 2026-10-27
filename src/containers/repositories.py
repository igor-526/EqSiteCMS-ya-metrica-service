from dependency_injector import containers, providers

from core.entities.metrica_parse import ParseKind
from depends.utils import get_session
from repositories.metrica_parse_run_repository import ParseRunRepository
from repositories.metrica_parse_task_repository import ParseTaskRepository
from repositories.metrica_stats_repository import StatsRepository
from repositories.page_stats_query_repository import PageStatsQueryRepository


class RepositoriesContainer(containers.DeclarativeContainer):
    session = providers.ContextLocalResource(get_session)

    run_repository = providers.Factory(ParseRunRepository, session=session)
    task_repository = providers.Factory(ParseTaskRepository, session=session)
    pages_repository = providers.Factory(StatsRepository, session=session, kind=ParseKind.PAGES)
    search_queries_repository = providers.Factory(StatsRepository, session=session, kind=ParseKind.SEARCH_QUERIES)

    page_stats_query_repository = providers.Factory(PageStatsQueryRepository, session=session)
