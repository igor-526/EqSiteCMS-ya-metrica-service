from collections.abc import Mapping
from typing import Any

import httpx

from core.exceptions import YandexClientException
from core.schemas.clients.yandex_metrica import validate_counters_response


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
        status = None
        try:
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
                status = response.status_code
                # Check status before decoding: HTML/empty error bodies are still HTTP failures.
                response.raise_for_status()
                response_data = response.json()
                if not isinstance(response_data, dict):
                    raise ValueError("Expected a JSON object")
            return response_data
        except YandexClientException:
            raise
        except httpx.HTTPStatusError as ex:
            raise YandexClientException(status=status, category="http") from ex
        except httpx.TimeoutException as ex:
            raise YandexClientException(status=status, category="timeout") from ex
        except httpx.TransportError as ex:
            raise YandexClientException(status=status, category="transport") from ex
        except (TypeError, ValueError) as ex:
            raise YandexClientException(status=status, category="response") from ex
        except Exception as ex:
            # The boundary includes construction, context entry and close, not just request().
            raise YandexClientException(status=status) from ex

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
