import logging

logger = logging.getLogger(__name__)


class NatsConnectionErrorPolicy:
    """One safe monitoring event per prolonged outage, reset only on recovery."""

    def __init__(self, *, service_name: str, report_after_attempts: int) -> None:
        self._service_name = service_name
        self._report_after_attempts = report_after_attempts
        self.reset()

    def reset(self) -> None:
        self.consecutive_failures = 0
        self.escalated = False

    async def on_error(self, error: Exception) -> None:
        self.consecutive_failures += 1
        level = logging.WARNING
        if self.consecutive_failures > self._report_after_attempts and not self.escalated:
            self.escalated = True
            level = logging.ERROR
        # SDK exception text/tracebacks may carry URLs and credentials.
        logger.log(level, "NATS unavailable: service=%s attempts=%s", self._service_name, self.consecutive_failures)

    async def on_disconnected(self) -> None:
        logger.warning("NATS disconnected: service=%s", self._service_name)

    async def on_reconnected(self) -> None:
        logger.info("NATS reconnected: service=%s", self._service_name)
        self.reset()

    async def on_closed(self) -> None:
        logger.info("NATS closed: service=%s", self._service_name)
