import logging

from pydantic import ValidationError

from core.exceptions import ClientError
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
        except ValueError as ve:
            logger.error("Error while decrypting access token")
            raise ClientError("Ошибка расшифровки токена") from ve
        except Exception as ex:
            logger.error(f"Error: {ex}")
            raise ClientError(f"Ошибка: {ex}") from ex
        raw_response = await self._yandex_metrica_client.get_counters(
            access_token=decrypted_access_token,
            search=search,
            limit=limit,
            offset=offset,
        )
        total = raw_response.get("rows", None)
        try:
            total = int(total)
        except ValueError as ex:
            logging.error(f"Incorrent response from yandex_metrica: {raw_response}")
            raise ClientError("Яндекс выдал некорректный ответ") from ex
        counters: list[YandexMetricaCounterOutDto] = []
        raw_response_counters = raw_response.get("counters", None)
        if not isinstance(raw_response_counters, list):
            logging.error(f"Incorrent response from yandex_metrica: {raw_response}")
            raise ClientError("Яндекс выдал некорректный ответ")
        for ya_counter in raw_response_counters:
            try:
                counters.append(
                    YandexMetricaCounterOutDto(
                        counter_id=ya_counter.get("id", None), site=ya_counter.get("site", None)
                    )
                )
            except ValidationError:
                logging.error(f"Invalid counter from Yandex.Metrica response: {ya_counter}")
                continue
            except Exception:
                logging.error(f"Error while adding Yandex.Metrica counter from Yandex.Metrica response: {ya_counter}")
        return YandexMetricaCounterResponseOutDto(total=total, items=counters)
