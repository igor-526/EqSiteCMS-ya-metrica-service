class PeerServiceException(Exception):
    """Safe peer error: never retain response bodies or request values."""

    def __init__(self, service: str, status: int | None, category: str = "client") -> None:
        self.service = service
        self.status = status
        self.category = category if category in {"client", "timeout", "transport", "http", "response"} else "client"
        super().__init__(f"Peer service: {service}; status: {status}; category: {self.category}")
