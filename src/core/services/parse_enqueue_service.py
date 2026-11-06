import logging
from contextlib import AbstractAsyncContextManager
from typing import Protocol
from uuid import UUID, uuid4

from core.entities.metrica_maintenance import IntentAdmission, PendingParseIntent
from core.entities.metrica_parse import ParseKind, ParseRun, ParseTask
from core.exceptions import PeerServiceException
from core.exceptions.parse import (
    MetricaBindingMissingError,
    ParseAlreadyActiveError,
    ParseJobAlreadyExistsError,
    ParseRangeError,
    YandexIntegrationUnavailableError,
)
from core.protocols.clients.yandex_service import YandexServiceClientProtocol
from core.protocols.parse_dispatcher import ParseDispatcherProtocol
from core.protocols.repositories.metrica_maintenance import PendingParseIntentProtocol
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
        intent_repository: PendingParseIntentProtocol,
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
        self._intents = intent_repository
        self._stats = {ParseKind.PAGES: pages_repository, ParseKind.SEARCH_QUERIES: search_queries_repository}
        self._resolver = date_resolver
        self._decomposer = decomposer
        self._dispatcher = dispatcher
        self._max_attempts = max_attempts

    async def enqueue(self, *, data: ParseEnqueueInDto, job_id: UUID | None = None) -> ParseEnqueueOutDto | None:
        """None is a committed durable wait/terminal outcome, safe to ACK.

        Calls without job_id get a fresh identity; stored legacy NULL ids stay unchanged.
        """
        try:
            intent = PendingParseIntent(
                job_id=job_id or uuid4(),
                equestrian_id=data.equestrian_id,
                date_mode="auto" if data.date_from is None and data.date_to is None else "explicit",
                date_from=data.date_from,
                date_to=data.date_to,
            )
        except ValueError:
            raise ParseRangeError() from None
        return await self._admit(intent, replay=False)

    async def replay_intent(self, intent: PendingParseIntent) -> ParseEnqueueOutDto | None:
        """Recheck saved identity, never re-enqueue it or reuse an admission snapshot.

        Callable after maintenance finalization and from periodic recovery. Concurrent
        calls serialize in prepare/consume; terminal/admitted intents remain terminal.
        """
        return await self._admit(intent, replay=True)

    async def _admit(self, intent: PendingParseIntent, *, replay: bool) -> ParseEnqueueOutDto | None:
        data = ParseEnqueueInDto(
            equestrian_id=intent.equestrian_id, date_from=intent.date_from, date_to=intent.date_to
        )
        try:
            binding = await self._yandex_client.get_metrica_binding(equestrian_id=data.equestrian_id)
        except PeerServiceException:
            raise YandexIntegrationUnavailableError() from None
        counter = binding.counter
        if counter is not None and counter.equestrian_id != data.equestrian_id:
            raise YandexIntegrationUnavailableError()
        lifecycle = binding.lifecycle()
        missing_binding = False
        result = None
        async with self._transaction.begin():
            if not replay:
                decision = await self._intents.enqueue_intent(intent, lifecycle=lifecycle)
                if decision == IntentAdmission.DUPLICATE:
                    raise ParseJobAlreadyExistsError()
                if decision == IntentAdmission.ACTIVE_RUN:
                    raise ParseAlreadyActiveError()
            decision = await self._intents.prepare_admission(intent, lifecycle=lifecycle)
            if decision == IntentAdmission.MISSING_BINDING:
                missing_binding = True
            elif decision != IntentAdmission.READY:
                return None  # commits durable wait before ACK, also with counter=null
            if not missing_binding:
                assert counter is not None
                last_dates = {}
                if data.date_from is None and data.date_to is None:
                    for kind, repository in self._stats.items():
                        last_dates[kind] = await repository.get_last_stat_date(data.equestrian_id)
                resolved = self._resolver.resolve(
                    date_from=data.date_from, date_to=data.date_to, last_dates=last_dates
                )
                run = ParseRun(
                    equestrian_id=data.equestrian_id,
                    job_id=intent.job_id,
                    binding_generation=lifecycle.binding_generation,
                    observed_cleanup_requested_seq=lifecycle.cleanup_requested_seq,
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
                        binding_generation=lifecycle.binding_generation,
                        observed_cleanup_requested_seq=lifecycle.cleanup_requested_seq,
                        max_attempts=self._max_attempts,
                        **window.model_dump(),
                    )
                    for window in windows
                ]
                decision = await self._intents.consume_intent(intent, lifecycle=lifecycle, run=run, tasks=tasks)
                if decision != IntentAdmission.ADMITTED:
                    return None
                result = ParseEnqueueOutDto(
                    run_id=run.id,
                    equestrian_id=data.equestrian_id,
                    **resolved.model_dump(),
                    tasks=[ParseTaskOutDto.model_validate(task.model_dump()) for task in tasks],
                )
        if missing_binding:
            # Preserve terminal missing-binding state before the consumer ACKs it.
            raise MetricaBindingMissingError()
        assert result is not None and counter is not None
        if result.clamped:
            logger.warning(
                "Parse dates clamped: equestrian_id=%s original=%s..%s resolved=%s",
                data.equestrian_id,
                data.date_from,
                data.date_to,
                result.ranges,
            )
        logger.info(
            "Parse enqueued: run_id=%s equestrian_id=%s tasks=%s", result.run_id, data.equestrian_id, len(result.tasks)
        )
        try:
            await self._dispatcher.dispatch_account(counter.yandex_account_id)
        except Exception:
            # Do not render broker exception text/tracebacks: they may contain credentials.
            logger.warning("Parse dispatch failed after commit: run_id=%s", result.run_id)
        return result
