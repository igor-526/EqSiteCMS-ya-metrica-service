import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from time import monotonic
from uuid import UUID

from core.entities.metrica_parse import ParseErrorCode, ParseKind
from core.protocols.repositories.metrica_parse import LeaseLostError, ParseTaskRepositoryProtocol
from core.protocols.repositories.metrica_stats import StatsRepositoryProtocol
from core.services.parse_enqueue_service import ParseTransactionProtocol
from core.services.parse_job_factory import ParseJobFactory
from core.services.parse_retry_policy import ParseExecutionError, RetryPolicy
from core.services.stat_ingestion_service import StatIngestionService

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunOutcome:
    has_more: bool = False


class TaskRunner:
    """Per invocation/session, never a singleton with shared mutable session state."""

    def __init__(
        self,
        *,
        task_repository: ParseTaskRepositoryProtocol,
        pages_repository: StatsRepositoryProtocol,
        search_queries_repository: StatsRepositoryProtocol,
        transaction: ParseTransactionProtocol,
        job_factory: ParseJobFactory,
        ingestion: StatIngestionService,
        retry_policy: RetryPolicy,
        lease_seconds: int = 900,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        self._tasks = task_repository
        self._stats = {ParseKind.PAGES: pages_repository, ParseKind.SEARCH_QUERIES: search_queries_repository}
        self._transaction = transaction
        self._factory = job_factory
        self._ingestion = ingestion
        self._retry = retry_policy
        self._lease_seconds = lease_seconds
        self._clock = clock or (lambda: datetime.now(UTC))

    async def run_next_for_account(self, account_id: UUID, *, locked_by: str) -> RunOutcome:
        async with self._transaction.begin():
            task = await self._tasks.claim(account_id, locked_by=locked_by, lease_seconds=self._lease_seconds)
        if task is None:
            return RunOutcome()
        started = monotonic()
        logger.info(
            "Parse start: task_id=%s run_id=%s equestrian_id=%s kind=%s window=%s..%s attempt=%s",
            task.id,
            task.run_id,
            task.equestrian_id,
            task.kind,
            task.date_from,
            task.date_to,
            task.attempts + 1,
        )
        deleted = 0
        status = "failed"
        try:
            job = await self._factory.create(task=task)
            await self._ingestion.ingest(job=job, locked_by=locked_by)
            try:
                async with self._transaction.begin():
                    deleted = await self._stats[task.kind].reconcile_window(
                        equestrian_id=task.equestrian_id,
                        date_from=task.date_from,
                        date_to=task.date_to,
                        parse_task_id=task.id,
                    )
                    await self._tasks.mark_succeeded(task.id, locked_by=locked_by, rows_deleted=deleted)
            except LeaseLostError:
                raise
            except Exception:
                raise ParseExecutionError(ParseErrorCode.STORAGE_ERROR) from None
            status = "succeeded"
        except LeaseLostError:
            # A stale worker MUST NOT change task state, reconcile, or fail queued work.
            logger.warning("Parse lease lost: task_id=%s", task.id)
            return RunOutcome()
        except Exception as error:
            decision = self._retry.classify(error)
            status = "queued" if decision.retryable and task.attempts + 1 < task.max_attempts else "failed"
            logger.warning(
                "Parse error: task_id=%s error_code=%s error_status=%s",
                task.id,
                decision.error_code,
                decision.error_status,
            )
            try:
                async with self._transaction.begin():
                    if status == "queued":
                        delay = timedelta(seconds=self._retry.delay_seconds(task.attempts + 1))
                        next_attempt_at = self._clock() + delay
                        await self._tasks.schedule_retry(
                            task.id,
                            next_attempt_at=next_attempt_at,
                            locked_by=locked_by,
                            error_code=decision.error_code,
                            error_status=decision.error_status,
                        )
                    else:
                        await self._tasks.mark_failed(
                            task.id,
                            locked_by=locked_by,
                            error_code=decision.error_code,
                            error_status=decision.error_status,
                        )
                        if decision.error_code == ParseErrorCode.CREDENTIALS_INVALID:
                            await self._tasks.fail_queued_by_account(
                                account_id, error_code=decision.error_code, error_status=decision.error_status
                            )
            except LeaseLostError:
                logger.warning("Parse lease lost: task_id=%s", task.id)
                return RunOutcome()
        logger.info(
            "Parse finish: task_id=%s status=%s pages=%s fetched=%s inserted=%s updated=%s skipped=%s "
            "deleted=%s duration=%.3f",
            task.id,
            status,
            task.pages_fetched,
            task.rows_fetched,
            task.rows_inserted,
            task.rows_updated,
            task.rows_skipped,
            deleted if status == "succeeded" else 0,
            monotonic() - started,
        )
        async with self._transaction.begin():
            ready = await self._tasks.list_dispatchable_accounts()
        return RunOutcome(has_more=account_id in ready)
