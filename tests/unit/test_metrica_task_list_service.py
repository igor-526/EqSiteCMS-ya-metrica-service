"""DTO and Protocol use-case contract, independent of storage."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from pydantic import ValidationError

from core.entities.metrica_task_list import (
    MetricaTaskListItem,
    MetricaTaskListPage,
    MetricaTaskStatus,
    MetricaTaskType,
)
from core.protocols.repositories.metrica_task_list import MetricaTaskListRepositoryProtocol
from core.schemas.api.metrica_tasks import MetricaTaskListInDto, MetricaTaskListOutDto, MetricaTaskOutDto
from core.services.metrica_task_list_service import MetricaTaskListService

SAMPLE = {
    "id": "parsing:example",
    "type": "parsing",
    "status": "queued",
    "is_active": True,
    "created_at": "2026-10-20T00:00:00Z",
}


@pytest.mark.parametrize("field", list(SAMPLE))
def test_all_item_fields_required(field):
    with pytest.raises(ValidationError):
        MetricaTaskOutDto.model_validate({k: v for k, v in SAMPLE.items() if k != field})


@pytest.mark.parametrize(
    "field,value",
    [
        ("id", 1),
        ("type", "cleanup"),
        ("status", "awaiting_seo"),
        ("is_active", "true"),
        ("created_at", "2026-10-20"),
        ("created_at", "2026-10-20T00:00:00"),
        ("counter_id", 123),
    ],
)
def test_invalid_item_contract(field, value):
    with pytest.raises(ValidationError):
        MetricaTaskOutDto.model_validate({**SAMPLE, field: value})


@pytest.mark.parametrize(
    "payload", [{"items": []}, {"total": 0}, {"items": [], "total": -1}, {"items": [], "total": "0"}]
)
def test_invalid_page_contract(payload):
    with pytest.raises(ValidationError):
        MetricaTaskListOutDto.model_validate(payload)


@pytest.mark.parametrize(
    "field,value",
    [
        ("limit", 0),
        ("limit", 101),
        ("limit", True),
        ("limit", "20"),
        ("offset", -1),
        ("offset", 1.0),
        ("offset", False),
    ],
)
def test_query_strict_integers(field, value):
    with pytest.raises(ValidationError):
        MetricaTaskListInDto.model_validate({"equestrian_id": UUID(int=1), field: value})


@pytest.mark.parametrize("task_type", list(MetricaTaskType))
@pytest.mark.parametrize("status", list(MetricaTaskStatus))
async def test_use_case_safe_projection_and_no_mutation(task_type, status):
    repository = AsyncMock(spec=MetricaTaskListRepositoryProtocol)
    row = MetricaTaskListItem(
        f"{task_type}:example",
        task_type,
        status,
        status in (MetricaTaskStatus.QUEUED, MetricaTaskStatus.RUNNING),
        datetime(2026, 10, 20, tzinfo=UTC),
    )
    page = MetricaTaskListPage([row], 10)
    repository.list_tasks.return_value = page
    data = MetricaTaskListInDto(equestrian_id=UUID(int=1), limit=1, offset=2)
    result = await MetricaTaskListService(repository).list_tasks(data=data)
    assert result.model_dump(mode="json") == {
        "items": [
            {**SAMPLE, "id": row.id, "type": task_type.value, "status": status.value, "is_active": row.is_active}
        ],
        "total": 10,
    }
    repository.list_tasks.assert_awaited_once_with(equestrian_id=data.equestrian_id, limit=1, offset=2)
    assert len(repository.mock_calls) == 1
    assert page.items == [row] and page.total == 10


async def test_use_case_propagates_failure_without_retry():
    repository = AsyncMock(spec=MetricaTaskListRepositoryProtocol)
    error = RuntimeError("storage failure")
    repository.list_tasks.side_effect = error
    with pytest.raises(RuntimeError) as caught:
        await MetricaTaskListService(repository).list_tasks(data=MetricaTaskListInDto(equestrian_id=UUID(int=1)))
    assert caught.value is error
    assert repository.list_tasks.await_count == 1
