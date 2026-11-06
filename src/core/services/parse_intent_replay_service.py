"""Bounded recovery of durable parse commands; no maintenance runtime activation."""

import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from core.exceptions.parse import MetricaBindingMissingError
from core.protocols.repositories.metrica_maintenance import PendingParseIntentProtocol
from core.services.parse_enqueue_service import ParseEnqueueService, ParseTransactionProtocol

logger = logging.getLogger(__name__)


class ParseIntentReplayService:
    def __init__(
        self,
        *,
        intent_repository: PendingParseIntentProtocol,
        enqueue_service: ParseEnqueueService,
        transaction: ParseTransactionProtocol,
        retry_seconds: int = 60,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if retry_seconds < 1:
            raise ValueError("retry_seconds must be positive")
        self._intents = intent_repository
        self._enqueue = enqueue_service
        self._transaction = transaction
        self._retry_seconds = retry_seconds
        self._clock = clock or (lambda: datetime.now(UTC))

    async def replay_ready(self, *, limit: int = 100) -> int:
        """Return admitted count, not acceptance evidence or number of discovered jobs.

        Discovery commits before peer I/O. Each admission owns a separate transaction.
        Defer even gated candidates so one waiting tenant cannot starve other tenants.
        Concurrent discovery is safe: prepare/consume rechecks pending state under locks.
        Storage failures during discovery/defer propagate for periodic recovery retry.
        """
        if not 1 <= limit <= 1000:
            raise ValueError("Invalid intent batch limit")
        async with self._transaction.begin():
            intents = await self._intents.ready_intents(limit=limit)
        admitted = 0
        for intent in intents:
            try:
                result = await self._enqueue.replay_intent(intent)
                admitted += result is not None
            except MetricaBindingMissingError:
                # Admission committed the terminal missing_binding state.
                continue
            except Exception:
                # Never log peer/storage exception text, credentials or command body.
                logger.warning("Parse intent replay deferred: job_id=%s", intent.job_id)
            # A crash here leaves the committed intent/run intact. Terminal rows are
            # protected by the repository's pending-only update; no resurrection.
            async with self._transaction.begin():
                await self._intents.defer_intent(
                    intent, next_attempt_at=self._clock() + timedelta(seconds=self._retry_seconds)
                )
        return admitted
