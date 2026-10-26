import asyncio
import json
from unittest.mock import AsyncMock

import httpx
import pytest

from clients.yandex_metrica import YandexMetricaClient
from core.exceptions import YandexClientException


@pytest.fixture
def transport_client(monkeypatch):
    original_client = httpx.AsyncClient

    def install(handler):
        client = original_client(transport=httpx.MockTransport(handler))
        monkeypatch.setattr("clients.base.httpx.AsyncClient", lambda: client)
        return client

    return install


@pytest.mark.parametrize("status", [200, 201, 299])
async def test_make_request_accepts_successful_dictionary_response(transport_client, status):
    captured = []

    def handler(request):
        captured.append(request)
        return httpx.Response(status, json={"rows": 0, "counters": []})

    client = transport_client(handler)
    result = await YandexMetricaClient().make_request(
        method="post",
        url="test",
        params={"query": "hello world"},
        headers={"X-Test": "value"},
        json={"payload": "тест"},
        auth=httpx.BasicAuth("user", "pass"),
        request_timeout=3.5,
    )

    assert result == {"rows": 0, "counters": []}
    assert client.is_closed
    request = captured[0]
    assert request.method == "POST"
    assert str(request.url.copy_with(query=None)) == "https://api-metrika.yandex.net/test"
    assert request.url.params["query"] == "hello world"
    assert request.headers["X-Test"] == "value"
    assert request.headers["Authorization"] == "Basic dXNlcjpwYXNz"
    assert request.content.decode() == '{"payload":"тест"}'
    assert request.extensions["timeout"] == dict.fromkeys(["connect", "read", "write", "pool"], 3.5)


async def test_make_request_forwards_form_data_and_disabled_timeout(transport_client):
    captured = []

    def handler(request):
        captured.append(request)
        return httpx.Response(200, json={})

    transport_client(handler)
    response = await YandexMetricaClient().make_request(
        method="patch", url="test", data={"field": "value"}, request_timeout=None
    )
    assert response == {}
    assert captured[0].content == b"field=value"
    assert captured[0].extensions["timeout"] == dict.fromkeys(["connect", "read", "write", "pool"])
    assert "Authorization" not in captured[0].headers


@pytest.mark.parametrize("status", [199, 300, 401, 403, 429, 500, 503])
@pytest.mark.parametrize("body", [b'{"error":"SECRET_MARKER"}', b"<html>SECRET_MARKER</html>", b""])
async def test_make_request_reports_safe_http_failure(transport_client, status, body, caplog, capsys):
    client = transport_client(lambda request: httpx.Response(status, content=body))

    with pytest.raises(YandexClientException) as error:
        await YandexMetricaClient().make_request(url="test")

    assert error.value.status == status
    assert error.value.category == "http"
    assert isinstance(error.value.__cause__, httpx.HTTPStatusError)
    assert client.is_closed
    assert "SECRET_MARKER" not in str(error.value)
    assert "SECRET_MARKER" not in caplog.text
    assert "SECRET_MARKER" not in str(capsys.readouterr())


async def test_make_request_handles_error_response_without_optional_details(transport_client):
    transport_client(lambda request: httpx.Response(400, json={}))

    with pytest.raises(YandexClientException, match="Status code: 400") as error:
        await YandexMetricaClient().make_request(url="test")

    assert "Error:" not in str(error.value)


@pytest.mark.parametrize("payload", [None, [], [1], "string", 42])
@pytest.mark.parametrize("status", [200, 500])
async def test_make_request_rejects_non_dictionary_json(transport_client, payload, status):
    transport_client(lambda request: httpx.Response(status, content=json.dumps(payload)))

    with pytest.raises(YandexClientException) as error:
        await YandexMetricaClient().make_request(url="test")

    assert f"Status code: {status}" in str(error.value)


@pytest.mark.parametrize("body", [b"invalid-json SECRET_MARKER", b""])
async def test_malformed_json_is_typed_failure(transport_client, body):
    client = transport_client(lambda request: httpx.Response(200, content=body))

    with pytest.raises(YandexClientException) as error:
        await YandexMetricaClient().make_request(url="test")

    assert error.value.status == 200
    assert isinstance(error.value.__cause__, ValueError)
    assert "SECRET_MARKER" not in error.value.message
    assert client.is_closed


@pytest.mark.parametrize(
    "error_class",
    [
        httpx.ConnectTimeout,
        httpx.ReadTimeout,
        httpx.WriteTimeout,
        httpx.PoolTimeout,
        httpx.ConnectError,
        httpx.ReadError,
        httpx.WriteError,
        httpx.RemoteProtocolError,
    ],
)
async def test_network_errors_are_typed_and_client_is_closed(transport_client, error_class, caplog, capsys):
    cause = error_class("network failed SECRET_MARKER")

    def handler(request):
        raise cause

    client = transport_client(handler)
    with pytest.raises(YandexClientException) as error:
        await YandexMetricaClient().make_request(url="test")

    assert error.value.__cause__ is cause
    assert error.value.status is None
    assert error.value.category == ("timeout" if issubclass(error_class, httpx.TimeoutException) else "transport")
    assert "SECRET_MARKER" not in error.value.message
    assert "SECRET_MARKER" not in caplog.text
    assert "SECRET_MARKER" not in str(capsys.readouterr())
    assert client.is_closed


@pytest.mark.parametrize(("search", "limit", "offset"), [(None, 20, 0), ("тест", 100, 25)])
async def test_get_counters_maps_pagination_and_authorization(monkeypatch, search, limit, offset):
    client = YandexMetricaClient()
    make_request = AsyncMock(return_value={"rows": 1, "counters": []})
    monkeypatch.setattr(client, "make_request", make_request)

    result = await client.get_counters(access_token="token", search=search, limit=limit, offset=offset)

    assert result is make_request.return_value
    make_request.assert_awaited_once_with(
        method="GET",
        url="management/v1/counters",
        params={"per_page": limit, "offset": offset + 1, "search_string": search},
        headers={"Authorization": "OAuth token"},
    )


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"counters": []},
        {"rows": None, "counters": []},
        {"rows": "bad SECRET_MARKER", "counters": []},
        {"rows": [], "counters": []},
        {"rows": 0},
        {"rows": 0, "counters": None},
        {"rows": 0, "counters": {}},
        {"rows": 0, "counters": "bad"},
    ],
)
async def test_get_counters_rejects_unusable_top_level(transport_client, payload):
    client = transport_client(lambda request: httpx.Response(200, json=payload))

    with pytest.raises(YandexClientException) as error:
        await YandexMetricaClient().get_counters(access_token="token")

    assert error.value.__cause__ is not None
    assert error.value.category == "response"
    assert "SECRET_MARKER" not in error.value.message
    assert client.is_closed


@pytest.mark.parametrize(
    "payload",
    [
        {"rows": 0, "counters": []},
        {"rows": "0", "counters": []},
        {"rows": "7", "counters": [{"id": "42", "site": "example.com"}, None, {}, {"id": None}]},
        {"rows": 4, "counters": [None, 1, "invalid", {"id": "bad", "site": None}]},
        {"rows": -1, "counters": []},
    ],
)
async def test_get_counters_preserves_rows_and_partial_items(transport_client, payload):
    captured = []

    def handler(request):
        captured.append(request)
        return httpx.Response(200, json=payload)

    client = transport_client(handler)
    result = await YandexMetricaClient().get_counters(access_token="token")

    assert result == payload
    assert client.is_closed
    assert captured[0].extensions["timeout"] == dict.fromkeys(["connect", "read", "write", "pool"], 10.0)
    assert captured[0].headers["Authorization"] == "OAuth token"


@pytest.mark.parametrize("phase", ["construct", "enter", "request", "close"])
@pytest.mark.parametrize("cause_type", [RuntimeError, httpx.ConnectError, httpx.ReadTimeout])
async def test_lifecycle_failures_are_wrapped(monkeypatch, phase, cause_type):
    cause = cause_type("SECRET_MARKER")
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.request.return_value = httpx.Response(200, json={}, request=httpx.Request("GET", "https://example.com"))
    if phase == "enter":
        client.__aenter__.side_effect = cause
    elif phase == "request":
        client.request.side_effect = cause
    elif phase == "close":
        client.__aexit__.side_effect = cause

    def create():
        if phase == "construct":
            raise cause
        return client

    monkeypatch.setattr("clients.base.httpx.AsyncClient", create)
    with pytest.raises(YandexClientException) as error:
        await YandexMetricaClient().make_request(url="test")

    assert error.value.__cause__ is cause
    assert error.value.status == (200 if phase == "close" else None)
    assert "SECRET_MARKER" not in error.value.message
    if phase in {"request", "close"}:
        client.__aexit__.assert_awaited_once()


@pytest.mark.parametrize("method", ["make_request", "get_counters"])
@pytest.mark.parametrize(
    "cause", [asyncio.CancelledError(), YandexClientException(429), KeyboardInterrupt(), SystemExit()]
)
async def test_cancellation_and_typed_failure_are_not_wrapped(transport_client, method, cause):
    def handler(request):
        raise cause

    client = transport_client(handler)
    kwargs = {"url": "test"} if method == "make_request" else {"access_token": "token"}
    with pytest.raises(type(cause)) as error:
        await getattr(YandexMetricaClient(), method)(**kwargs)

    assert error.value is cause
    assert client.is_closed


async def test_get_counters_wraps_unexpected_method_failure(monkeypatch):
    client = YandexMetricaClient()
    cause = RuntimeError("SECRET_MARKER")
    monkeypatch.setattr(client, "make_request", AsyncMock(side_effect=cause))

    with pytest.raises(YandexClientException) as error:
        await client.get_counters(access_token="token")

    assert error.value.__cause__ is cause
    assert "SECRET_MARKER" not in error.value.message
