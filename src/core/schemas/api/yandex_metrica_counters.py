from pydantic import Field

from core.schemas.baseschema import BaseSchema


class YandexMetricaCounterOutDto(BaseSchema):
    """DTO выдачи счётчика"""

    counter_id: int = Field(default=..., description="ID счётчика Яндекс")
    site: str = Field(default=..., description="Сайт счётчика Яндекс")


class YandexMetricaCounterResponseOutDto(BaseSchema):
    """DTO выдачи счётчиков"""

    total: int = Field(default=..., description="Общее кол-во счётчиков")
    items: list[YandexMetricaCounterOutDto] = Field(default_factory=list, description="Счётчики Яндекс.Метрика")
