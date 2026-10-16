from typing import Any, Protocol


class YandexMetricaClientProtocol(Protocol):
    async def get_counters(
        self, *, access_token: str, search: str | None = None, limit: int = 20, offset: int = 0
    ) -> Any:
        """Вернуть исходный dict с пригодными rows/counters, включая пустой список.

        Числовые строки сохраняются; отдельные невалидные элементы пропускает
        сервис, без пересчёта rows или изменения семантики пустого результата.

        Raises:
            YandexClientException: Отказ HTTP/lifecycle, JSON или всей top-level
                структуры. Отмена распространяется без обёртки.
        """
        ...
