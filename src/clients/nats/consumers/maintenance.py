import logging

from nats.aio.msg import Msg

from clients.nats.consumers.parse_run import ParseRunConsumer
from clients.nats.handlers.parse_run import PermanentCommandError

logger = logging.getLogger(__name__)


class MaintenanceConsumer(ParseRunConsumer):
    """Same supervised pull lifecycle, but maintenance never caps valid retries."""

    async def _process_message(self, message: Msg) -> None:
        try:
            if message.subject != self._subject:
                raise PermanentCommandError("Unexpected route")
            await self._handler.handle(payload=message.data, headers=message.headers, subject=message.subject)
        except PermanentCommandError:
            await message.term()
        except Exception:
            # Broker delivery count survives process restarts; no payload/error text.
            attempts = message.metadata.num_delivered
            delay = 2 ** min(max(attempts - 1, 0), 6)
            logger.log(
                logging.ERROR if attempts >= 5 else logging.WARNING,
                "Maintenance storage retry: consumer=%s attempt=%s",
                self._durable,
                attempts,
            )
            await message.nak(delay=min(delay, 60))
        else:
            await message.ack()
