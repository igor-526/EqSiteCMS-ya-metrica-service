"""UT-MP-02: return=ack, PermanentCommandError=term, other errors=nak."""

import json
import logging
from datetime import date
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from clients.nats.handlers.parse_run import ParseRunHandler, PermanentCommandError
from core.exceptions.parse import (
    MetricaBindingMissingError,
    ParseAlreadyActiveError,
    ParseJobAlreadyExistsError,
    ParseRangeError,
    YandexIntegrationUnavailableError,
)
from core.schemas.messaging import PARSE_RUN_SUBJECT, ParseCommand
from core.schemas.parse import ParseEnqueueInDto
from tests.unit.test_parse_command_schema import command_payload


@pytest.mark.parametrize(
    "window,dates",
    [
        ({"mode": "auto"}, (None, None)),
        (
            {"mode": "explicit", "date_from": "2026-10-21", "date_to": "2026-10-25"},
            (date(2026, 10, 21), date(2026, 10, 25)),
        ),
    ],
)
async def test_handler_enqueues_command(window, dates, caplog):
    body = command_payload(window=window)
    command = ParseCommand.model_validate(body)
    service = AsyncMock()
    with caplog.at_level(logging.INFO):
        result = await ParseRunHandler(service=service).handle(
            payload=json.dumps(body).encode(),
            headers={"X-Equestrian-Id": "foreign", "Authorization": "Bearer SECRET"},
            subject=PARSE_RUN_SUBJECT,
        )
    assert result is None
    service.enqueue.assert_awaited_once_with(
        data=ParseEnqueueInDto(equestrian_id=command.equestrian_id, date_from=dates[0], date_to=dates[1]),
        job_id=command.job_id,
    )
    assert "outcome=enqueued" in caplog.text
    assert "SECRET" not in caplog.text


@pytest.mark.parametrize(
    "error,outcome,level",
    [
        (ParseJobAlreadyExistsError(), "duplicate", logging.INFO),
        (ParseAlreadyActiveError(), "already_active", logging.INFO),
        (MetricaBindingMissingError(), "binding_missing", logging.WARNING),
        (ParseRangeError(), "invalid_range", logging.WARNING),
    ],
)
async def test_ack_domain_outcomes(error, outcome, level, caplog):
    service = AsyncMock()
    service.enqueue.side_effect = error
    with caplog.at_level(logging.INFO):
        assert await ParseRunHandler(service=service).handle(payload=json.dumps(command_payload()).encode()) is None
    assert f"outcome={outcome}" in caplog.text
    assert caplog.records[-1].levelno == level
    service.enqueue.assert_awaited_once()


@pytest.mark.parametrize(
    "error", [YandexIntegrationUnavailableError(), SQLAlchemyError("SECRET"), RuntimeError("SECRET")]
)
async def test_retry_propagates_without_secrets(error, caplog):
    service = AsyncMock()
    service.enqueue.side_effect = error
    with pytest.raises(type(error)) as raised:
        await ParseRunHandler(service=service).handle(payload=json.dumps(command_payload()).encode())
    assert raised.value is error
    assert "retry" in caplog.text
    assert "SECRET" not in caplog.text
    assert not any(record.exc_info for record in caplog.records)


@pytest.mark.parametrize(
    "payload,subject",
    [
        (b"not json SECRET", PARSE_RUN_SUBJECT),
        (json.dumps(command_payload(schema_version=2)).encode(), PARSE_RUN_SUBJECT),
        (json.dumps(command_payload(provider="demo")).encode(), PARSE_RUN_SUBJECT),
        (json.dumps(command_payload(params={"token": "SECRET"})).encode(), PARSE_RUN_SUBJECT),
        (json.dumps(command_payload()).encode(), "commands.parse.demo.run"),
        (json.dumps(command_payload()).encode(), "SECRET"),
    ],
)
async def test_term_invalid_command_without_enqueue(payload, subject, caplog):
    service = AsyncMock()
    with pytest.raises(PermanentCommandError) as raised:
        await ParseRunHandler(service=service).handle(payload=payload, headers={"token": "SECRET"}, subject=subject)
    service.enqueue.assert_not_awaited()
    assert "rejected" in caplog.text
    assert "SECRET" not in caplog.text + str(raised.value)
    assert not any(record.exc_info for record in caplog.records)
