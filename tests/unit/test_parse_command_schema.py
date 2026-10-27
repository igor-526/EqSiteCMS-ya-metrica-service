"""UT-MP-01: wire envelope, strict params and windows."""

import json
from uuid import NAMESPACE_DNS, uuid4, uuid5

import pytest
from pydantic import ValidationError

from core.schemas.messaging import NAMESPACE_PARSE_COMMANDS, ParseCommand


def command_payload(**changes):
    return {
        "schema_version": 1,
        "occurred_at": "2026-10-28T00:00:03Z",
        "job_id": str(uuid4()),
        "provider": "ya_metrica",
        "equestrian_id": str(uuid4()),
        "trigger": "schedule",
        "window": {"mode": "auto"},
        "params": {},
        **changes,
    }


@pytest.mark.parametrize("trigger", ["schedule", "manual"])
@pytest.mark.parametrize(
    "window",
    [{"mode": "auto"}, {"mode": "explicit", "date_from": "2026-10-21", "date_to": "2026-10-25"}],
)
def test_valid_envelope(trigger, window):
    body = command_payload(trigger=trigger, window=window)
    assert ParseCommand.model_validate_json(json.dumps(body)).model_dump(mode="json") == body
    assert NAMESPACE_PARSE_COMMANDS == uuid5(NAMESPACE_DNS, "parse-commands.eqcms")


@pytest.mark.parametrize(
    "changes",
    [
        {"schema_version": 2},
        {"provider": "demo"},
        {"access_token": "SECRET"},
        {"job_id": "bad"},
        {"equestrian_id": "bad"},
        {"occurred_at": "2026-10-28T00:00:03"},
        {"trigger": "unknown"},
        {"window": {"mode": "unknown"}},
        {"window": {"mode": "auto", "date_from": "2026-10-21"}},
        {"window": {"mode": "explicit", "date_from": "2026-10-21"}},
        {"window": {"mode": "explicit", "date_from": "bad", "date_to": "2026-10-25"}},
        {"window": {"mode": "explicit", "date_from": "2026-10-21", "date_to": "2026-10-25", "extra": 1}},
        {"params": {"account_id": "SECRET"}},
        {"params": []},
    ],
)
def test_invalid_envelope(changes):
    with pytest.raises(ValidationError):
        ParseCommand.model_validate_json(json.dumps(command_payload(**changes)))


@pytest.mark.parametrize("payload", [b"not json SECRET", b"\xff", b"[]", b"null"])
def test_invalid_json(payload):
    with pytest.raises(ValidationError):
        ParseCommand.model_validate_json(payload)


@pytest.mark.parametrize(
    "field", ["schema_version", "occurred_at", "job_id", "provider", "equestrian_id", "trigger", "window", "params"]
)
def test_required_fields(field):
    body = command_payload()
    del body[field]
    with pytest.raises(ValidationError):
        ParseCommand.model_validate(body)


def test_reversed_explicit_dates_are_domain_outcome_not_poison_message():
    command = ParseCommand.model_validate(
        command_payload(window={"mode": "explicit", "date_from": "2026-10-25", "date_to": "2026-10-21"})
    )
    assert command.window.mode == "explicit"
