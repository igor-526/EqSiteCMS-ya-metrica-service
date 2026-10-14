from typing import Any, Protocol


class YandexMetricaClientProtocol(Protocol):
    async def get_counters(
        self, *, access_token: str, search: str | None = None, limit: int = 20, offset: int = 0
    ) -> Any: ...
