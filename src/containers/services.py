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
from core.services.page_stats_query_service import PageStatsQueryService
from core.services.search_query_stats_query_service import SearchQueryStatsQueryService
from workers.dispatcher import CeleryParseDispatcher


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
    task_runner = providers.Factory(
        TaskRunner,
        task_repository=repositories_container.task_repository,
        pages_repository=repositories_container.pages_repository,
        search_queries_repository=repositories_container.search_queries_repository,
        transaction=repositories_container.session,
        job_factory=parse_job_factory,
        ingestion=stat_ingestion_service,
        retry_policy=retry_policy,
        lease_seconds=settings_container.app_settings.provided.task_lease_seconds,
    )
    parse_dispatch_service = providers.Factory(
        ParseDispatchService,
        task_repository=repositories_container.task_repository,
        transaction=repositories_container.session,
    )
    parse_enqueue_service = providers.Factory(
        ParseEnqueueService,
        yandex_client=clients_container.yandex_service_client,
        transaction=repositories_container.session,
        run_repository=repositories_container.run_repository,
        pages_repository=repositories_container.pages_repository,
        search_queries_repository=repositories_container.search_queries_repository,
        date_resolver=date_resolver,
        decomposer=decomposer,
        dispatcher=parse_dispatcher,
        max_attempts=settings_container.app_settings.provided.task_max_attempts,
    )

    page_stats_query_service = providers.Factory(
        PageStatsQueryService, repository=repositories_container.page_stats_query_repository
    )

    search_query_stats_query_service = providers.Factory(
        SearchQueryStatsQueryService, repository=repositories_container.search_query_stats_query_repository
    )
