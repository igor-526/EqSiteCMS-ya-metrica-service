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
        monkeypatch.setattr("clients.yandex_metrica.httpx.AsyncClient", lambda: client)
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


@pytest.mark.parametrize("status", [199, 300, 401, 403, 429, 500])
async def test_make_request_reports_yandex_error_details(transport_client, status):
    transport_client(lambda request: httpx.Response(status, json={"error": "denied", "error_description": "detail"}))

    with pytest.raises(YandexClientException) as error:
        await YandexMetricaClient().make_request(url="test")

    expected_message = f"Yandex Client exception \nStatus code: {status}\nError: denied\nError description: detail"
    assert str(error.value) == expected_message


async def test_make_request_handles_error_response_without_optional_details(transport_client):
    transport_client(lambda request: httpx.Response(400, json={}))

    with pytest.raises(YandexClientException, match="Status code: 400") as error:
        await YandexMetricaClient().make_request(url="test")

    assert "Error:" not in str(error.value)


@pytest.mark.parametrize("payload", [None, [], [1], "string", 42])
@pytest.mark.parametrize("status", [200, 500])
async def test_make_request_rejects_non_dictionary_json(transport_client, payload, status):
    transport_client(lambda request: httpx.Response(status, content=json.dumps(payload)))

    with pytest.raises(YandexClientException, match="Empty response") as error:
        await YandexMetricaClient().make_request(url="test")

    assert f"Status code: {status}" in str(error.value)


async def test_malformed_json_is_not_returned_as_success(transport_client):
    transport_client(lambda request: httpx.Response(200, content=b"invalid-json"))

    with pytest.raises(ValueError):
        await YandexMetricaClient().make_request(url="test")


@pytest.mark.parametrize("error_class", [httpx.ConnectError, httpx.ReadTimeout])
async def test_network_errors_propagate_and_client_is_closed(transport_client, error_class):
    cause = error_class("network failed")

    def handler(request):
        raise cause

    client = transport_client(handler)
    with pytest.raises(error_class) as error:
        await YandexMetricaClient().make_request(url="test")

    assert error.value is cause
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
