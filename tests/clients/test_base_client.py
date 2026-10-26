from functools import partial

import httpx
import pytest

from clients.base import BaseApiClient
from core.exceptions import PeerServiceException


@pytest.mark.parametrize(
    ("outcome", "status", "category"),
    [
        (httpx.ReadTimeout("SECRET_MARKER"), None, "timeout"),
        (httpx.ConnectError("SECRET_MARKER"), None, "transport"),
        (httpx.Response(503, text="SECRET_MARKER"), 503, "http"),
        (httpx.Response(200, text="SECRET_MARKER"), 200, "response"),
        (httpx.Response(200, json=[]), 200, "response"),
        (RuntimeError("SECRET_MARKER"), None, "client"),
    ],
)
async def test_base_client_classifies_with_factory(mock_transport, outcome, status, category, caplog):
    def handler(request):
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    mock_transport(handler)
    client = BaseApiClient("https://peer.test/", 42, partial(PeerServiceException, "test"))
    with pytest.raises(PeerServiceException) as error:
        await client.make_request(url="/test", headers={"Authorization": "SECRET_MARKER"})
    assert (error.value.service, error.value.status, error.value.category) == ("test", status, category)
    assert "SECRET_MARKER" not in str(error.value)
    assert "SECRET_MARKER" not in caplog.text


async def test_base_client_uses_configured_timeout_and_normalizes_slashes(mock_transport):
    captured = mock_transport(lambda request: httpx.Response(200, json={}))
    assert await BaseApiClient("https://peer.test/", timeout=42).make_request(url="/test") == {}
    assert str(captured[0].url) == "https://peer.test/test"
    assert set(captured[0].extensions["timeout"].values()) == {42}
