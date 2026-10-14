class YandexClientException(Exception):
    def __init__(self, status: int, error: str | None = None, error_description: str | None = None) -> None:
        message = f"Yandex Client exception \nStatus code: {status}"
        if error:
            message += f"\nError: {error}"
        if error_description:
            message += f"\nError description: {error_description}"
        self.message = message
        super().__init__(message)
