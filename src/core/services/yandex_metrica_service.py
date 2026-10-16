import logging

from pydantic import ValidationError

from core.exceptions import ClientError, YandexClientException
from core.protocols.clients import YandexMetricaClientProtocol
from core.schemas.api import YandexMetricaCounterOutDto, YandexMetricaCounterResponseOutDto
from core.services.encryption_service import EncryptionService

logger = logging.getLogger("Yandex Metrica Service")


class YandexMetricaService:
    def __init__(
        self, yandex_metrica_client: YandexMetricaClientProtocol, encryption_service: EncryptionService
    ) -> None:
        self._yandex_metrica_client = yandex_metrica_client
        self._encryption_service = encryption_service

    async def get_counters(
        self, *, access_token: str, search: str | None = None, limit: int = 20, offset: int = 0
    ) -> YandexMetricaCounterResponseOutDto:
        if limit < 1:
            limit = 1
        elif limit > 100:
            limit = 100
        if offset < 0:
            offset = 0
        try:
            decrypted_access_token = self._encryption_service.decrypt(data=access_token)
        except Exception as ex:
            logger.error("Access token decryption failed")
            raise ClientError("Ошибка расшифровки токена") from ex
        try:
            raw_response = await self._yandex_metrica_client.get_counters(
                access_token=decrypted_access_token,
                search=search,
                limit=limit,
                offset=offset,
            )
        except YandexClientException as ex:
            raise ClientError("Ошибка получения счётчиков Яндекс.Метрики") from ex
        try:
            total = int(raw_response["rows"])
        except (KeyError, TypeError, ValueError) as ex:
            logger.error("Invalid Yandex.Metrica response total")
            raise ClientError("Яндекс выдал некорректный ответ") from ex
        counters: list[YandexMetricaCounterOutDto] = []
        raw_response_counters = raw_response.get("counters")
        if not isinstance(raw_response_counters, list):
            logger.error("Invalid Yandex.Metrica counters list")
            raise ClientError("Яндекс выдал некорректный ответ")
        for ya_counter in raw_response_counters:
            if not isinstance(ya_counter, dict):
                logger.warning("Invalid counter in Yandex.Metrica response")
                continue
            try:
                counters.append(
                    YandexMetricaCounterOutDto.model_validate(
                        {"counter_id": ya_counter.get("id"), "site": ya_counter.get("site")}
                    )
                )
            except ValidationError:
                logger.warning("Invalid counter in Yandex.Metrica response")
        return YandexMetricaCounterResponseOutDto(total=total, items=counters)
