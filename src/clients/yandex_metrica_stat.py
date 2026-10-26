from datetime import date

from clients.base import BaseApiClient
from core.entities.metrica_parse import ParseKind
from core.exceptions import YandexClientException
from core.schemas.clients.yandex_metrica_stat import StatPage, validate_stat_response


class YandexMetricaStatClient(BaseApiClient):
    def __init__(self, timeout: float = 120.0, base_url: str = "https://api-metrika.yandex.net") -> None:
        # DI supplies YA_METRICA_STAT_TIMEOUT_SECONDS; no settings dependency in core.
        super().__init__(base_url, timeout)

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
    ) -> StatPage:
        if not 1 <= limit <= 100000 or offset < 1:
            raise ValueError("Invalid stat pagination")
        dimension = {
            ParseKind.PAGES: "ym:s:startURL",
            ParseKind.SEARCH_QUERIES: "ym:s:lastSearchPhrase",
        }[kind]
        dimensions = f"ym:s:date,{dimension}"
        response = await self.make_request(
            url="stat/v1/data",
            params={
                "ids": counter_id,
                "metrics": (
                    "ym:s:visits,ym:s:users,ym:s:pageviews,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds"
                ),
                "dimensions": dimensions,
                "date1": date_from.isoformat(),
                "date2": date_to.isoformat(),
                "limit": limit,
                "offset": offset,
                "sort": dimensions,
                "accuracy": "full",
            },
            headers={"Authorization": f"OAuth {access_token}"},
        )
        try:
            return validate_stat_response(response)
        except TypeError, ValueError:
            raise YandexClientException(status=200, category="response") from None
