from functools import partial
from uuid import UUID

from clients.base import BaseApiClient
from core.exceptions import PeerServiceException
from core.schemas.clients.yandex_service import EncAccessToken, MetricaBinding


class YandexServiceClient(BaseApiClient):
    def __init__(self, base_url: str, timeout: float = 10.0) -> None:
        super().__init__(base_url, timeout, partial(PeerServiceException, "yandex"))

    async def get_metrica_binding(self, *, equestrian_id: UUID) -> MetricaBinding:
        response = await self.make_request(url=f"api/connections/metrica/{equestrian_id}")
        try:
            return MetricaBinding.model_validate(response)
        except TypeError, ValueError:
            raise PeerServiceException("yandex", 200, "response") from None

    async def get_enc_access_token(self, *, account_id: UUID) -> EncAccessToken:
        response = await self.make_request(url=f"api/servicing/access-token/{account_id}")
        try:
            return EncAccessToken.model_validate(response)
        except TypeError, ValueError:
            raise PeerServiceException("yandex", 200, "response") from None
