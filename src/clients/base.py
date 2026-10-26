from collections.abc import Callable, Mapping
from typing import Any, cast

import httpx

from core.exceptions import PeerServiceException, YandexClientException


class BaseApiClient:
    """HTTP boundary: no upstream bodies, credentials or query values in errors."""

    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        exception_factory: Callable[..., Exception] = YandexClientException,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.exception_factory = exception_factory

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
        request_timeout: float | httpx.Timeout | None | object = ...,
    ) -> dict:
        status = None
        try:
            async with httpx.AsyncClient() as client:
                response = await client.request(
                    method=method.upper(),
                    url=f"{self.base_url}/{url.lstrip('/')}",
                    params=params,
                    headers=headers,
                    data=data,
                    json=json,
                    auth=auth,
                    timeout=cast(
                        float | httpx.Timeout | None, self.timeout if request_timeout is ... else request_timeout
                    ),
                )
                status = response.status_code
                # Check status before decoding: HTML/empty error bodies are still HTTP failures.
                response.raise_for_status()
                response_data = response.json()
                if not isinstance(response_data, dict):
                    raise ValueError("Expected a JSON object")
            return response_data
        except YandexClientException, PeerServiceException:
            raise
        except httpx.HTTPStatusError as ex:
            raise self.exception_factory(status=status, category="http") from ex
        except httpx.TimeoutException as ex:
            raise self.exception_factory(status=status, category="timeout") from ex
        except httpx.TransportError as ex:
            raise self.exception_factory(status=status, category="transport") from ex
        except (TypeError, ValueError) as ex:
            raise self.exception_factory(status=status, category="response") from ex
        except Exception as ex:
            # The boundary includes construction, context entry and close, not just request().
            raise self.exception_factory(status=status) from ex
