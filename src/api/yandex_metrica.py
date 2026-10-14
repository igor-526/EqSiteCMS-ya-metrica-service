from typing import Annotated

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Query

from containers.services import ServicesContainer
from core.schemas.api import YandexMetricaCounterResponseOutDto
from core.services import YandexMetricaService

router = APIRouter()


@router.get("/counters", response_model=YandexMetricaCounterResponseOutDto)
@inject
async def get_metrica_counters(
    service: Annotated[
        YandexMetricaService,
        Depends(Provide[ServicesContainer.yandex_metrica_service]),
    ],
    access_token: Annotated[str, Query(description="Access token для авторизации в Яндекс")],
    search: Annotated[str | None, Query(description="Поиск по счётчикам")] = None,
    limit: Annotated[int, Query(description="Кол-во записей")] = 20,
    offset: Annotated[int, Query(description="Смещение")] = 0,
) -> YandexMetricaCounterResponseOutDto:
    return await service.get_counters(
        access_token=access_token,
        search=search,
        limit=limit,
        offset=offset,
    )
