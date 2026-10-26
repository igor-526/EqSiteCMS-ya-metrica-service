from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import APIRouter, Depends

from containers.application import create_parse_services
from core.schemas.api.parse import ParseRunInDto, ParseRunOutDto
from core.schemas.parse import ParseEnqueueInDto
from core.services import ParseEnqueueService
from utils.database import worker_session

router = APIRouter()


async def get_parse_enqueue_service() -> AsyncIterator[ParseEnqueueService]:
    """One session per request, no outer transaction around enqueue/dispatch."""
    async with worker_session() as session:
        yield create_parse_services(session).parse_enqueue_service()


@router.post("/stats/parse", response_model=ParseRunOutDto)
async def enqueue_parse(
    data: ParseRunInDto,
    service: Annotated[ParseEnqueueService, Depends(get_parse_enqueue_service)],
) -> ParseRunOutDto:
    result = await service.enqueue(data=ParseEnqueueInDto.model_validate(data.model_dump()))
    return ParseRunOutDto.model_validate(result.model_dump())
