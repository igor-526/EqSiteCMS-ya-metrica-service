from uuid import UUID

import httpx
import pytest

from clients.yandex_service import YandexServiceClient
from core.exceptions import PeerServiceException

TENANT = UUID("5f6496aa-4fe9-42da-a763-29f9bab22d73")
ACCOUNT = UUID("799c85a2-1f16-4477-9ef5-cb44ee4b81b2")


@pytest.mark.parametrize(
    "counter",
    [
        None,
        {
            "id": str(ACCOUNT),
            "equestrian_id": str(TENANT),
            "counter_id": "105850153",
            "yandex_account": {"id": str(ACCOUNT), "login": "unused"},
        },
    ],
)
async def test_binding_y7_actual_nested_account_contract(mock_transport, counter):
    captured = mock_transport(lambda request: httpx.Response(200, json={"counter": counter}))
    result = await YandexServiceClient("http://yandex.test/").get_metrica_binding(equestrian_id=TENANT)
    if counter is None:
        assert result.counter is None
    else:
        assert result.counter is not None
        assert result.counter.yandex_account_id == ACCOUNT
        assert result.counter.equestrian_id == TENANT
        assert result.counter.counter_id == "105850153"
    assert captured[0].url.path == f"/api/connections/metrica/{TENANT}"
    assert "authorization" not in captured[0].headers
    assert "cookie" not in captured[0].headers


async def test_y5_returns_ciphertext_without_decryption(mock_transport, caplog):
    captured = mock_transport(lambda request: httpx.Response(200, json={"enc_access_token": "CIPHER_MARKER"}))
    result = await YandexServiceClient("http://yandex.test").get_enc_access_token(account_id=ACCOUNT)
    assert result.enc_access_token == "CIPHER_MARKER"
    assert "CIPHER_MARKER" not in repr(result) + caplog.text
    assert captured[0].url.path == f"/api/servicing/access-token/{ACCOUNT}"
    assert captured[0].method == "GET"
    assert "authorization" not in captured[0].headers


@pytest.mark.parametrize(
    "method,kwargs,payload",
    [
        ("get_metrica_binding", {"equestrian_id": TENANT}, {}),
        ("get_metrica_binding", {"equestrian_id": TENANT}, {"counter": {}}),
        ("get_enc_access_token", {"account_id": ACCOUNT}, {"access_token": "TOKEN_MARKER"}),
        ("get_enc_access_token", {"account_id": ACCOUNT}, {"enc_access_token": ""}),
    ],
)
async def test_invalid_peer_response(mock_transport, method, kwargs, payload):
    mock_transport(lambda request: httpx.Response(200, json=payload))
    with pytest.raises(PeerServiceException) as error:
        await getattr(YandexServiceClient("http://yandex.test"), method)(**kwargs)
    assert (error.value.service, error.value.status, error.value.category) == ("yandex", 200, "response")
    assert "TOKEN_MARKER" not in str(error.value)


@pytest.mark.parametrize("status", [400, 401, 403, 429, 500, 503])
@pytest.mark.parametrize(
    "method,kwargs",
    [
        ("get_metrica_binding", {"equestrian_id": TENANT}),
        ("get_enc_access_token", {"account_id": ACCOUNT}),
    ],
)
async def test_peer_failure_preserves_status(mock_transport, status, method, kwargs, caplog):
    mock_transport(lambda request: httpx.Response(status, text="CIPHER_MARKER"))
    with pytest.raises(PeerServiceException) as error:
        await getattr(YandexServiceClient("http://yandex.test"), method)(**kwargs)
    assert (error.value.service, error.value.status, error.value.category) == ("yandex", status, "http")
    assert "CIPHER_MARKER" not in str(error.value) + caplog.text


@pytest.mark.parametrize("cause,category", [(httpx.ReadTimeout, "timeout"), (httpx.ConnectError, "transport")])
async def test_yandex_service_network_failure(mock_transport, cause, category):
    def handler(request):
        raise cause("CIPHER_MARKER")

    mock_transport(handler)
    with pytest.raises(PeerServiceException) as error:
        await YandexServiceClient("http://yandex.test").get_metrica_binding(equestrian_id=TENANT)
    assert (error.value.status, error.value.category) == (None, category)
