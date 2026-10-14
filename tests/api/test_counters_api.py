from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

import main
from containers import clients_container, services_container
from core.exceptions import AlreadyExistsError, AppError, ClientError
from core.schemas.api import YandexMetricaCounterResponseOutDto
from core.services import EncryptionService
from settings import settings


@pytest.fixture
def client(monkeypatch):
    close = AsyncMock()
    monkeypatch.setattr(main, "close_database", close)
    with TestClient(main.app) as client:
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


@pytest.mark.parametrize(("exception", "status"), [(AppError, 500), (ClientError, 400), (AlreadyExistsError, 409)])
def test_application_errors_preserve_status_and_message(client, service, exception, status):
    service.get_counters.side_effect = exception("Ошибка запроса")
    response = client.get("/api/metrica/counters", params={"access_token": "token"})
    assert response.status_code == status
    assert response.json() == {"detail": "Ошибка запроса"}


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
