import logging
from collections.abc import Mapping

from pydantic import ValidationError

from core.exceptions.parse import (
    MetricaBindingMissingError,
    ParseAlreadyActiveError,
    ParseJobAlreadyExistsError,
    ParseRangeError,
)
from core.schemas.messaging.parse_command import PARSE_RUN_SUBJECT, ExplicitWindow, ParseCommand
from core.schemas.parse import ParseEnqueueInDto
from core.services.parse_enqueue_service import ParseEnqueueService

logger = logging.getLogger(__name__)


class PermanentCommandError(Exception):
    """Invalid command: the consumer must term, not retry, this delivery."""


class ParseRunHandler:
    """Return means ack; permanent errors mean term; other errors mean nak.

    The injected service/session graph belongs to this delivery, not the consumer.
    Headers are transport metadata only and never select a tenant.
    """

    def __init__(self, *, service: ParseEnqueueService) -> None:
        self._service = service

    async def handle(
        self,
        *,
        payload: bytes,
        headers: Mapping[str, str] | None = None,
        subject: str = PARSE_RUN_SUBJECT,
    ) -> None:
        try:
            command = ParseCommand.model_validate_json(payload)
        except ValidationError:
            logger.warning("Parse command rejected: invalid_schema")
            raise PermanentCommandError("Invalid parse command") from None
        if subject != f"commands.parse.{command.provider}.run":
            logger.warning("Parse command rejected: subject_mismatch")
            raise PermanentCommandError("Invalid parse command subject") from None

        data = ParseEnqueueInDto(equestrian_id=command.equestrian_id)
        if isinstance(command.window, ExplicitWindow):
            data.date_from = command.window.date_from
            data.date_to = command.window.date_to
        outcome = "enqueued"
        level = logging.INFO
        try:
            await self._service.enqueue(data=data, job_id=command.job_id)
        except ParseJobAlreadyExistsError:
            outcome = "duplicate"
        except ParseAlreadyActiveError:
            outcome = "already_active"
        except MetricaBindingMissingError:
            outcome, level = "binding_missing", logging.WARNING
        except ParseRangeError:
            outcome, level = "invalid_range", logging.WARNING
        except Exception:
            # No exception text, traceback, payload, headers, or peer/account data.
            logger.warning(
                "Parse command retry: job_id=%s equestrian_id=%s",
                command.job_id,
                command.equestrian_id,
            )
            raise
        logger.log(
            level,
            "Parse command ack: outcome=%s job_id=%s equestrian_id=%s",
            outcome,
            command.job_id,
            command.equestrian_id,
        )
