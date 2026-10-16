from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from core.entities.base import Entity, TimestampMixin
from core.exceptions import AlreadyExistsError, AppError, ClientError, YandexClientException
from core.schemas.api import YandexMetricaCounterOutDto, YandexMetricaCounterResponseOutDto
from core.schemas.base import Schema


def test_counter_schema_loads_attributes_and_serializes_response():
    counter = YandexMetricaCounterOutDto.model_validate(SimpleNamespace(counter_id="42", site="example.com"))
    response = YandexMetricaCounterResponseOutDto(total="1", items=[counter])

    assert response.model_dump() == {"total": 1, "items": [{"counter_id": 42, "site": "example.com"}]}


@pytest.mark.parametrize(
    "payload", [{}, {"counter_id": 1}, {"counter_id": "bad", "site": "example"}, {"counter_id": 1, "site": None}]
)
def test_counter_schema_rejects_missing_or_invalid_fields(payload):
    with pytest.raises(ValidationError):
        YandexMetricaCounterOutDto.model_validate(payload)


def test_response_items_default_is_independent():
    first = YandexMetricaCounterResponseOutDto(total=1)
    second = YandexMetricaCounterResponseOutDto(total=0)
    first.items.append(YandexMetricaCounterOutDto(counter_id=1, site="example"))

    assert second.items == []


def test_base_schema_accepts_attribute_objects():
    class Example(Schema):
        name: str

    assert Example.model_validate(SimpleNamespace(name="example")).name == "example"


def test_entity_generates_unique_uuid_and_accepts_explicit_id():
    first, second = Entity(), Entity()
    identifier = uuid4()

    assert isinstance(first.id, UUID)
    assert first.id != second.id
    assert Entity(id=str(identifier)).id == identifier


def test_timestamps_default_to_current_utc_and_allow_explicit_values():
    before = datetime.now(UTC)
    timestamp = TimestampMixin()
    after = datetime.now(UTC)

    assert before <= timestamp.created_at <= after
    assert timestamp.created_at.tzinfo == UTC
    assert timestamp.updated_at is None
    assert TimestampMixin(created_at=before, updated_at=after).updated_at == after


@pytest.mark.parametrize(("error_class", "status"), [(AppError, 500), (ClientError, 400), (AlreadyExistsError, 409)])
def test_application_exceptions_preserve_message_and_status(error_class, status):
    error = error_class("failure")

    assert str(error) == error.message == "failure"
    assert error.args == ("failure",)
    assert error.status_code == status
    assert isinstance(error, AppError)


@pytest.mark.parametrize("status", [403, None])
@pytest.mark.parametrize("details", [(None, None), ("SECRET_MARKER", None), (None, "SECRET_MARKER")])
def test_yandex_exception_legacy_arguments_are_safe(status, details):
    exception = YandexClientException(status, *details)

    assert exception.status == status
    assert str(exception) == exception.message == exception.args[0]
    assert f"Status code: {status}" in exception.message
    assert "SECRET_MARKER" not in str(exception)


@pytest.mark.parametrize("category", ["timeout", "transport", "http", "response", "SECRET_MARKER"])
def test_yandex_exception_category_is_allowlisted(category):
    exception = YandexClientException(None, category=category)

    assert exception.category == ("client" if category == "SECRET_MARKER" else category)
    assert "SECRET_MARKER" not in exception.message
