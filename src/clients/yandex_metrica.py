from collections.abc import Mapping
from typing import Any

import httpx

from core.exceptions import YandexClientException


class YandexMetricaClient:
    base_url: str

    def __init__(self):
        self.base_url = "https://api-metrika.yandex.net"

    async def make_request(
        self,
        *,
        method: str = "GET",
        url: str,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        data: Mapping[str, Any] | None = None,
        json: Any | None = None,
        auth: httpx.Auth | None = None,
        request_timeout: float | httpx.Timeout | None = 10.0,
    ) -> dict:
        async with httpx.AsyncClient() as client:
            response = await client.request(
                method=method.upper(),
                url=f"{self.base_url}/{url}",
                params=params,
                headers=headers,
                data=data,
                json=json,
                auth=auth,
                timeout=request_timeout,
            )
        response_data = response.json()
        if not isinstance(response_data, dict):
            raise YandexClientException(status=response.status_code, error="Empty response")
        if not 200 <= response.status_code < 300:
            print(response_data)
            raise YandexClientException(
                status=response.status_code,
                error=response_data.get("error", None),
                error_description=response_data.get("error_description", None),
            )
        return response_data

    async def get_counters(
        self, *, access_token: str, search: str | None = None, limit: int = 20, offset: int = 0
    ) -> Any:
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
        return response
