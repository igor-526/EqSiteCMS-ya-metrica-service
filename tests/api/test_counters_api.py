from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi.exceptions import ResponseValidationError
from fastapi.testclient import TestClient
from starlette.exceptions import HTTPException

import main
from clients.yandex_metrica import YandexMetricaClient
from containers import clients_container, services_container
from core.exceptions import AlreadyExistsError, AppError, ClientError
from core.schemas.api import YandexMetricaCounterResponseOutDto
from core.services import EncryptionService
from settings import settings


@pytest.fixture(params=[{}, {"Authorization": "Bearer foreign-user", "Cookie": "session=foreign"}])
def caller_headers(request):
    return request.param


@pytest.fixture
def client(monkeypatch, caller_headers):
    close = AsyncMock()
    monkeypatch.setattr(main, "close_database", close)
    with TestClient(main.app, headers=caller_headers, raise_server_exceptions=False) as client:
        yield client
    close.assert_awaited_once_with()


@pytest.fixture
def service():
    service = AsyncMock()
    service.get_counters.return_value = YandexMetricaCounterResponseOutDto(total=0)
    with services_container.yandex_metrica_service.override(service):
        yield service


@pytest.mark.parametrize(
    ("params", "expected"),
    [
        ({"access_token": "encrypted"}, {"access_token": "encrypted", "search": None, "limit": 20, "offset": 0}),
        (
            {"access_token": "encrypted", "search": "пример", "limit": "12", "offset": "4"},
            {"access_token": "encrypted", "search": "пример", "limit": 12, "offset": 4},
        ),
        (
            {"access_token": "", "search": "", "limit": "-5", "offset": "-1"},
            {"access_token": "", "search": "", "limit": -5, "offset": -1},
        ),
    ],
)
def test_route_passes_query_parameters(client, service, params, expected):
    response = client.get("/api/metrica/counters", params=params)
    assert response.status_code == 200
    assert response.json() == {"total": 0, "items": []}
    service.get_counters.assert_awaited_once_with(**expected)


@pytest.mark.parametrize(
    ("params", "field", "error_type"),
    [
        ({}, "access_token", "missing"),
        ({"access_token": "token", "limit": "invalid"}, "limit", "int_parsing"),
        ({"access_token": "token", "offset": "1.5"}, "offset", "int_parsing"),
    ],
)
def test_query_validation_returns_400(client, service, params, field, error_type):
    response = client.get("/api/metrica/counters", params=params)
    assert response.status_code == 400
    error = response.json()["detail"][0]
    assert error["loc"] == ["query", field]
    assert error["type"] == error_type
    service.get_counters.assert_not_awaited()


@pytest.mark.parametrize(
    "exception",
    [
        AppError("SECRET_MARKER"),
        ClientError("SECRET_MARKER"),
        AlreadyExistsError("SECRET_MARKER"),
        RuntimeError("SECRET_MARKER"),
        ResponseValidationError([{"input": "SECRET_MARKER"}]),
        HTTPException(401, "SECRET_MARKER"),
        HTTPException(403, "SECRET_MARKER"),
        HTTPException(500, "SECRET_MARKER", headers={"X-Secret": "SECRET_MARKER"}),
    ],
)
def test_errors_are_safe_400(client, service, exception, caplog, capsys):
    service.get_counters.side_effect = exception
    response = client.get("/api/metrica/counters", params={"access_token": "token"})
    assert response.status_code == 400
    assert response.json()["detail"]
    assert "SECRET_MARKER" not in response.text + str(response.headers) + caplog.text + capsys.readouterr().out
    assert "Traceback" not in response.text


@pytest.mark.parametrize(("limit", "offset", "expected_limit", "expected_offset"), [(0, -1, 1, 0), (200, 2, 100, 2)])
def test_real_service_chain_decrypts_clamps_and_serializes(client, limit, offset, expected_limit, expected_offset):
    fake_client = AsyncMock()
    fake_client.get_counters.return_value = {
        "rows": "2",
        "counters": [{"id": "42", "site": "example.org"}, {"id": None, "site": "invalid"}],
    }
    token = EncryptionService(settings.encryption_service_key).encrypt(data="oauth-secret")
    with clients_container.yandex_metrica_client.override(fake_client):
        response = client.get(
            "/api/metrica/counters", params={"access_token": token, "limit": limit, "offset": offset, "search": "site"}
        )
    assert response.status_code == 200
    assert response.json() == {"total": 2, "items": [{"counter_id": 42, "site": "example.org"}]}
    fake_client.get_counters.assert_awaited_once_with(
        access_token="oauth-secret", search="site", limit=expected_limit, offset=expected_offset
    )


def test_invalid_encrypted_token_returns_domain_error(client):
    fake_client = AsyncMock()
    with clients_container.yandex_metrica_client.override(fake_client):
        response = client.get("/api/metrica/counters", params={"access_token": "invalid"})
    assert response.status_code == 400
    assert response.json() == {"detail": "Ошибка расшифровки токена"}
    fake_client.get_counters.assert_not_awaited()


def test_lifespan_closes_database_on_body_error(monkeypatch):
    close = AsyncMock()
    monkeypatch.setattr(main, "close_database", close)
    with pytest.raises(RuntimeError, match="client failure"), TestClient(main.app):
        raise RuntimeError("client failure")
    close.assert_awaited_once_with()


@pytest.mark.parametrize("field", ["limit", "offset"])
def test_validation_does_not_reflect_input(client, service, field, caplog):
    response = client.get("/api/metrica/counters", params={"access_token": "token", field: "SECRET_MARKER"})
    assert response.status_code == 400
    assert "SECRET_MARKER" not in response.text
    assert all(set(error) == {"loc", "type"} for error in response.json()["detail"])
    service.get_counters.assert_not_awaited()


@pytest.mark.parametrize("mode", ["timeout", "transport", "http", "json", "null", "rows", "counters"])
def test_real_client_service_failure_chain(client, monkeypatch, mode, caplog, capsys):
    def handler(request):
        if mode == "timeout":
            raise httpx.ReadTimeout("SECRET_MARKER", request=request)
        if mode == "transport":
            raise httpx.ConnectError("SECRET_MARKER", request=request)
        if mode == "http":
            return httpx.Response(503, text="SECRET_MARKER")
        if mode == "json":
            return httpx.Response(200, text="SECRET_MARKER")
        payloads = {"null": None, "rows": {"rows": None, "counters": []}, "counters": {"rows": 1}}
        return httpx.Response(200, json=payloads[mode])

    upstream = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    monkeypatch.setattr("clients.base.httpx.AsyncClient", lambda: upstream)
    token = EncryptionService(settings.encryption_service_key).encrypt(data="SECRET_MARKER")
    with clients_container.yandex_metrica_client.override(YandexMetricaClient()):
        response = client.get("/api/metrica/counters", params={"access_token": token})
    assert response.status_code == 400
    assert response.json() == {"detail": "Ошибка получения счётчиков Яндекс.Метрики"}
    assert upstream.is_closed
    assert "SECRET_MARKER" not in response.text + caplog.text + capsys.readouterr().out


@pytest.mark.parametrize(
    ("rows", "counters", "items"),
    [
        (
            "6",
            [
                {"id": "7", "site": "valid.example"},
                None,
                "SECRET_MARKER",
                {"id": None, "site": "SECRET_MARKER"},
                {"id": 9, "site": None},
            ],
            [{"counter_id": 7, "site": "valid.example"}],
        ),
        ("12", [None, {"id": None, "site": "SECRET_MARKER"}], []),
        ("5", [], []),
        (0, [], []),
    ],
)
def test_real_client_service_partial_and_empty_200(client, monkeypatch, rows, counters, items, caplog, capsys):
    captured = []

    def handler(request):
        captured.append(request)
        return httpx.Response(200, json={"rows": rows, "counters": counters})

    upstream = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    monkeypatch.setattr("clients.base.httpx.AsyncClient", lambda: upstream)
    token = EncryptionService(settings.encryption_service_key).encrypt(data="oauth-token")
    with clients_container.yandex_metrica_client.override(YandexMetricaClient()):
        response = client.get("/api/metrica/counters", params={"access_token": token, "limit": 200, "offset": -1})
    assert response.status_code == 200
    assert response.json() == {"total": int(rows), "items": items}
    assert upstream.is_closed
    assert captured[0].url.params["per_page"] == "100"
    assert captured[0].url.params["offset"] == "1"
    assert "SECRET_MARKER" not in response.text + caplog.text + capsys.readouterr().out


def test_debug_setting_cannot_enable_tracebacks(client, service, monkeypatch):
    monkeypatch.setattr(settings, "debug", True)
    assert main.app.debug is False
    service.get_counters.side_effect = RuntimeError("SECRET_MARKER")
    response = client.get("/api/metrica/counters", params={"access_token": "token"})
    assert response.status_code == 400
    assert response.json() == {"detail": "Ошибка обработки запроса"}
