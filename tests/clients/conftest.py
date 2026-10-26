import httpx
import pytest


@pytest.fixture
def mock_transport(monkeypatch):
    original_client = httpx.AsyncClient
    captured = []

    def install(handler):
        def capture(request):
            captured.append(request)
            return handler(request)

        monkeypatch.setattr(
            "clients.base.httpx.AsyncClient",
            lambda: original_client(transport=httpx.MockTransport(capture)),
        )
        return captured

    return install
