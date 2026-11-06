from typing import Any

from dependency_injector import containers, providers

from core.services import (
    DateRangeResolver,
    EncryptionService,
    ParseDispatchService,
    ParseEnqueueService,
    ParseJobFactory,
    RetryPolicy,
    StatIngestionService,
    TaskDecomposer,
    TaskRunner,
    YandexMetricaService,
)
from core.services.maintenance_delivery_service import MaintenanceDeliveryService
from core.services.maintenance_dispatch_service import MaintenanceDispatchService
from core.services.maintenance_result_relay import MaintenanceResultRelay
from core.services.metrica_cleanup_service import CleanupBatchService, CleanupFinalizerService, CleanupWorkerService
from core.services.metrica_merge_service import MergeWorkerService
from core.services.page_stats_query_service import PageStatsQueryService
from core.services.parse_intent_replay_service import ParseIntentReplayService
from core.services.search_query_stats_query_service import SearchQueryStatsQueryService
from workers.dispatcher import CeleryMaintenanceDispatcher, CeleryParseDispatcher


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

    celery_app: providers.Dependency[Any] = providers.Dependency()
    merge_worker_service = providers.Factory(
        MergeWorkerService,
        repository=repositories_container.merge_worker_repository,
        transaction=repositories_container.session,
        lease_seconds=settings_container.app_settings.provided.maintenance_lease_seconds,
    )
    maintenance_dispatcher = providers.Factory(CeleryMaintenanceDispatcher, celery_app=celery_app)
    maintenance_dispatch_service = providers.Factory(
        MaintenanceDispatchService,
        repository=repositories_container.maintenance_dispatch_repository,
        transaction=repositories_container.session,
        dispatcher=maintenance_dispatcher,
        merge_available=providers.Callable(lambda handler: handler is not None, merge_worker_service),
    )
    parse_dispatcher = providers.Factory(CeleryParseDispatcher, celery_app=celery_app)
    date_resolver = providers.Factory(
        DateRangeResolver, utc_offset_hours=settings_container.app_settings.provided.ya_metrica_utc_offset_hours
    )
    decomposer = providers.Factory(TaskDecomposer)
    retry_policy = providers.Factory(
        RetryPolicy, base_seconds=settings_container.app_settings.provided.task_retry_base_seconds
    )
    parse_job_factory = providers.Factory(
        ParseJobFactory, yandex_client=clients_container.yandex_service_client, encryption=encryption_service
    )
    stat_ingestion_service = providers.Factory(
        StatIngestionService,
        stat_client=clients_container.stat_client,
        seo_client=clients_container.seo_service_client,
        task_repository=repositories_container.task_repository,
        pages_repository=repositories_container.pages_repository,
        search_queries_repository=repositories_container.search_queries_repository,
        transaction=repositories_container.session,
        page_limit=settings_container.app_settings.provided.ya_metrica_stat_page_limit,
        resolve_batch_size=settings_container.app_settings.provided.seo_resolve_batch_size,
        lease_seconds=settings_container.app_settings.provided.task_lease_seconds,
    )
    parse_dispatch_service = providers.Factory(
        ParseDispatchService,
        task_repository=repositories_container.task_repository,
        transaction=repositories_container.session,
        yandex_client=clients_container.yandex_service_client,
    )
    task_runner = providers.Factory(
        TaskRunner,
        dispatch_service=parse_dispatch_service,
        task_repository=repositories_container.task_repository,
        pages_repository=repositories_container.pages_repository,
        search_queries_repository=repositories_container.search_queries_repository,
        transaction=repositories_container.session,
        job_factory=parse_job_factory,
        ingestion=stat_ingestion_service,
        retry_policy=retry_policy,
        lease_seconds=settings_container.app_settings.provided.task_lease_seconds,
    )
    parse_enqueue_service = providers.Factory(
        ParseEnqueueService,
        yandex_client=clients_container.yandex_service_client,
        transaction=repositories_container.session,
        intent_repository=repositories_container.intent_repository,
        pages_repository=repositories_container.pages_repository,
        search_queries_repository=repositories_container.search_queries_repository,
        date_resolver=date_resolver,
        decomposer=decomposer,
        dispatcher=parse_dispatcher,
        max_attempts=settings_container.app_settings.provided.task_max_attempts,
    )

    maintenance_delivery_service = providers.Factory(
        MaintenanceDeliveryService,
        repository=repositories_container.maintenance_storage_repository,
        transaction=repositories_container.session,
    )

    maintenance_result_publisher: providers.Dependency[Any] = providers.Dependency()
    maintenance_result_relay = providers.Factory(
        MaintenanceResultRelay,
        repository=repositories_container.maintenance_result_outbox_repository,
        publisher=maintenance_result_publisher,
        transaction=repositories_container.session,
        lease_seconds=settings_container.app_settings.provided.maintenance_lease_seconds,
    )
    cleanup_finalizer_service = providers.Factory(
        CleanupFinalizerService,
        repository=repositories_container.cleanup_batch_repository,
        transaction=repositories_container.session,
    )

    cleanup_batch_service = providers.Factory(
        CleanupBatchService,
        repository=repositories_container.cleanup_batch_repository,
        transaction=repositories_container.session,
        batch_size=settings_container.app_settings.provided.cleanup_batch_size,
        lease_seconds=settings_container.app_settings.provided.maintenance_lease_seconds,
    )

    cleanup_worker_service = providers.Factory(
        CleanupWorkerService,
        repository=repositories_container.cleanup_worker_repository,
        transaction=repositories_container.session,
        batch_service=cleanup_batch_service,
        finalizer=cleanup_finalizer_service,
        lease_seconds=settings_container.app_settings.provided.maintenance_lease_seconds,
    )

    parse_intent_replay_service = providers.Factory(
        ParseIntentReplayService,
        intent_repository=repositories_container.intent_repository,
        enqueue_service=parse_enqueue_service,
        transaction=repositories_container.session,
    )

    page_stats_query_service = providers.Factory(
        PageStatsQueryService, repository=repositories_container.page_stats_query_repository
    )

    search_query_stats_query_service = providers.Factory(
        SearchQueryStatsQueryService, repository=repositories_container.search_query_stats_query_repository
    )
