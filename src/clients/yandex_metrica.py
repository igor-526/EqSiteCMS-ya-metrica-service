from clients.base import BaseApiClient
from core.exceptions import YandexClientException
from core.schemas.clients.yandex_metrica import validate_counters_response


class YandexMetricaClient(BaseApiClient):
    def __init__(self):
        super().__init__("https://api-metrika.yandex.net")

    async def get_counters(
        self, *, access_token: str, search: str | None = None, limit: int = 20, offset: int = 0
    ) -> dict:
        try:
            response = await self.make_request(
                method="GET",
                url="management/v1/counters",
                params={
                    "per_page": limit,
                    "offset": offset + 1,
                    "search_string": search,
                },
                headers={"Authorization": f"OAuth {access_token}"},
            )
            return validate_counters_response(response)
        except YandexClientException:
            raise
        except Exception as ex:
            raise YandexClientException(status=None, category="response") from ex
