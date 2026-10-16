class YandexClientException(Exception):
    """Безопасная ошибка интеграции; исходные данные доступны только через cause."""

    def __init__(
        self,
        status: int | None,
        error: str | None = None,
        error_description: str | None = None,
        *,
        category: str = "client",
    ) -> None:
        # Legacy arguments remain accepted, but upstream text is never rendered.
        self.status = status
        self.category = category if category in {"client", "timeout", "transport", "http", "response"} else "client"
        self.message = f"Yandex Client exception \nStatus code: {status}\nCategory: {self.category}"
        super().__init__(self.message)
