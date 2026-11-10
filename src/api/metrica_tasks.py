"""Trusted-network read: selector, then query, then request-local storage."""

import re
from collections.abc import AsyncIterator
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

from containers.application import create_metrica_task_list_service
from core.schemas.api.metrica_tasks import MetricaTaskListInDto, MetricaTaskListOutDto
from core.services.metrica_task_list_service import MetricaTaskListService
from utils.database import worker_session

router = APIRouter(prefix="/tasks")


class InvalidMetricaTaskSelector(HTTPException):
    """Dedicated 401 mapping; existing peer routes retain their error policy."""


def get_metrica_task_selector(request: Request) -> UUID:
    try:
        return UUID(request.headers.get("X-Equestrian-Id", ""))
    except ValueError:
        raise InvalidMetricaTaskSelector(status_code=401) from None


def get_metrica_task_query(
    request: Request, equestrian_id: Annotated[UUID, Depends(get_metrica_task_selector)]
) -> MetricaTaskListInDto:
    values = {}
    for name, default in (("limit", "20"), ("offset", "0")):
        raw = request.query_params.get(name, default)
        # Do not accept floats, exponent notation, whitespace or Python int coercions.
        if not re.fullmatch(r"-?[0-9]+", raw):
            raise HTTPException(status_code=400)
        try:
            values[name] = int(raw)
        except ValueError:
            raise HTTPException(status_code=400) from None
    try:
        return MetricaTaskListInDto(equestrian_id=equestrian_id, **values)
    except ValidationError as exc:
        raise RequestValidationError(exc.errors()) from None


async def get_metrica_task_list_service(
    _data: Annotated[MetricaTaskListInDto, Depends(get_metrica_task_query)],
) -> AsyncIterator[MetricaTaskListService]:
    """Open a fresh read session only after both validation stages passed."""
    async with worker_session() as session:
        yield create_metrica_task_list_service(session)


@router.get("", response_model=MetricaTaskListOutDto)
async def list_tasks(
    data: Annotated[MetricaTaskListInDto, Depends(get_metrica_task_query)],
    service: Annotated[MetricaTaskListService, Depends(get_metrica_task_list_service)],
) -> MetricaTaskListOutDto:
    return await service.list_tasks(data=data)
