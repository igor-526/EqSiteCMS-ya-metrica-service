from .base import ClientError


class ParseRangeError(ClientError):
    def __init__(self) -> None:
        super().__init__("Некорректный диапазон дат")


class MetricaBindingMissingError(ClientError):
    def __init__(self) -> None:
        super().__init__("Счётчик Яндекс.Метрики не привязан к конюшне")


class YandexIntegrationUnavailableError(ClientError):
    def __init__(self) -> None:
        super().__init__("Не удалось получить данные интеграции Яндекс")


class ParseAlreadyActiveError(ClientError):
    def __init__(self) -> None:
        super().__init__("Парсинг конюшни уже выполняется")
