from typing import Protocol
from uuid import UUID

from core.schemas.clients.yandex_service import EncAccessToken, MetricaBinding


class YandexServiceClientProtocol(Protocol):
    async def get_metrica_binding(self, *, equestrian_id: UUID) -> MetricaBinding: ...

    async def get_enc_access_token(self, *, account_id: UUID) -> EncAccessToken: ...
