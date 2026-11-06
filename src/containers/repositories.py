from dependency_injector import containers, providers

from core.entities.metrica_parse import ParseKind
from depends.utils import get_session
from repositories.metrica_cleanup_repository import CleanupBatchRepository
from repositories.metrica_cleanup_worker_repository import CleanupWorkerRepository
from repositories.metrica_maintenance_delivery_repository import MaintenanceDeliveryRepository
from repositories.metrica_maintenance_dispatch_repository import MaintenanceDispatchRepository
from repositories.metrica_merge_worker_repository import MergeWorkerRepository
from repositories.metrica_parse_run_repository import ParseRunRepository
from repositories.metrica_parse_task_repository import ParseTaskRepository
from repositories.metrica_pending_parse_repository import PendingParseIntentRepository
from repositories.metrica_result_outbox_repository import MaintenanceResultOutboxRepository
from repositories.metrica_stats_repository import StatsRepository
from repositories.page_stats_query_repository import PageStatsQueryRepository
from repositories.search_query_stats_query_repository import SearchQueryStatsQueryRepository


class RepositoriesContainer(containers.DeclarativeContainer):
    session = providers.ContextLocalResource(get_session)

    maintenance_storage_repository = providers.Factory(MaintenanceDeliveryRepository, session=session)

    maintenance_result_outbox_repository = providers.Factory(MaintenanceResultOutboxRepository, session=session)

    cleanup_batch_repository = providers.Factory(CleanupBatchRepository, session=session)

    cleanup_worker_repository = providers.Factory(CleanupWorkerRepository, session=session)

    merge_worker_repository = providers.Factory(MergeWorkerRepository, session=session)

    maintenance_dispatch_repository = providers.Factory(MaintenanceDispatchRepository, session=session)

    intent_repository = providers.Factory(PendingParseIntentRepository, session=session)
    run_repository = providers.Factory(ParseRunRepository, session=session)
    task_repository = providers.Factory(ParseTaskRepository, session=session)
    pages_repository = providers.Factory(StatsRepository, session=session, kind=ParseKind.PAGES)
    search_queries_repository = providers.Factory(StatsRepository, session=session, kind=ParseKind.SEARCH_QUERIES)

    page_stats_query_repository = providers.Factory(PageStatsQueryRepository, session=session)

    search_query_stats_query_repository = providers.Factory(SearchQueryStatsQueryRepository, session=session)
