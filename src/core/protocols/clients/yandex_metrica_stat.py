from datetime import date
from typing import Protocol

from core.entities.metrica_parse import ParseKind
from core.schemas.clients.yandex_metrica_stat import StatPage


class YandexMetricaStatClientProtocol(Protocol):
    async def get_stat_page(
        self,
        *,
        counter_id: str,
        access_token: str,
        kind: ParseKind,
        date_from: date,
        date_to: date,
        limit: int = 100000,
        offset: int = 1,
    ) -> StatPage: ...
