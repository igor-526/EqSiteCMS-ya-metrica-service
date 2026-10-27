import logging
from contextlib import AbstractAsyncContextManager
from typing import Protocol
from uuid import UUID

from core.entities.metrica_parse import ParseKind, ParseRun, ParseTask
from core.exceptions import PeerServiceException
from core.exceptions.parse import (
    MetricaBindingMissingError,
    ParseAlreadyActiveError,
    ParseJobAlreadyExistsError,
    YandexIntegrationUnavailableError,
)
from core.protocols.clients.yandex_service import YandexServiceClientProtocol
from core.protocols.parse_dispatcher import ParseDispatcherProtocol
from core.protocols.repositories.metrica_parse import ParseRunRepositoryProtocol
from core.protocols.repositories.metrica_stats import StatsRepositoryProtocol
from core.schemas.parse import ParseEnqueueInDto, ParseEnqueueOutDto, ParseTaskOutDto
from core.services.parse_date_resolver import DateRangeResolver
from core.services.parse_decomposer import TaskDecomposer

logger = logging.getLogger(__name__)


class ParseTransactionProtocol(Protocol):
    """Injected session; repositories must use this same transaction."""

    def begin(self) -> AbstractAsyncContextManager[object]: ...


class ParseEnqueueService:
    def __init__(
        self,
        *,
        yandex_client: YandexServiceClientProtocol,
        transaction: ParseTransactionProtocol,
        run_repository: ParseRunRepositoryProtocol,
        pages_repository: StatsRepositoryProtocol,
        search_queries_repository: StatsRepositoryProtocol,
        date_resolver: DateRangeResolver,
        decomposer: TaskDecomposer,
        dispatcher: ParseDispatcherProtocol,
        max_attempts: int,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self._yandex_client = yandex_client
        self._transaction = transaction
        self._runs = run_repository
        self._stats = {ParseKind.PAGES: pages_repository, ParseKind.SEARCH_QUERIES: search_queries_repository}
        self._resolver = date_resolver
        self._decomposer = decomposer
        self._dispatcher = dispatcher
        self._max_attempts = max_attempts

    async def enqueue(self, *, data: ParseEnqueueInDto, job_id: UUID | None = None) -> ParseEnqueueOutDto:
        try:
            binding = await self._yandex_client.get_metrica_binding(equestrian_id=data.equestrian_id)
        except PeerServiceException:
            raise YandexIntegrationUnavailableError() from None
        counter = binding.counter
        if counter is None:
            raise MetricaBindingMissingError()
        if counter.equestrian_id != data.equestrian_id:
            raise YandexIntegrationUnavailableError()
        async with self._transaction.begin():
            await self._runs.lock_equestrian(data.equestrian_id)
            if job_id is not None and await self._runs.exists_job(job_id):
                raise ParseJobAlreadyExistsError()
            if await self._runs.has_active_tasks(data.equestrian_id):
                raise ParseAlreadyActiveError()
            last_dates = {}
            if data.date_from is None and data.date_to is None:
                for kind, repository in self._stats.items():
                    last_dates[kind] = await repository.get_last_stat_date(data.equestrian_id)
            resolved = self._resolver.resolve(date_from=data.date_from, date_to=data.date_to, last_dates=last_dates)
            run = ParseRun(
                equestrian_id=data.equestrian_id,
                job_id=job_id,
                date_mode=resolved.date_mode,
                clamped=resolved.clamped,
            )
            windows = self._decomposer.decompose(resolved.ranges)
            tasks = [
                ParseTask(
                    run_id=run.id,
                    equestrian_id=data.equestrian_id,
                    counter_id=counter.counter_id,
                    yandex_account_id=counter.yandex_account_id,
                    max_attempts=self._max_attempts,
                    **window.model_dump(),
                )
                for window in windows
            ]
            if not await self._runs.create_run_with_tasks(run, tasks):
                raise ParseAlreadyActiveError()
            result = ParseEnqueueOutDto(
                run_id=run.id,
                equestrian_id=data.equestrian_id,
                **resolved.model_dump(),
                tasks=[ParseTaskOutDto.model_validate(task.model_dump()) for task in tasks],
            )
        if resolved.clamped:
            logger.warning(
                "Parse dates clamped: equestrian_id=%s original=%s..%s resolved=%s",
                data.equestrian_id,
                data.date_from,
                data.date_to,
                resolved.ranges,
            )
        logger.info("Parse enqueued: run_id=%s equestrian_id=%s tasks=%s", run.id, data.equestrian_id, len(tasks))
        try:
            await self._dispatcher.dispatch_account(counter.yandex_account_id)
        except Exception:
            # Do not render broker exception text/tracebacks: they may contain credentials.
            logger.warning("Parse dispatch failed after commit: run_id=%s", run.id)
        return result
