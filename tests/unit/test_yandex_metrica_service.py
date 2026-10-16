from unittest.mock import AsyncMock, Mock

import pytest

from core.exceptions import ClientError, YandexClientException
from core.protocols.clients import YandexMetricaClientProtocol
from core.services.encryption_service import EncryptionService
from core.services.yandex_metrica_service import YandexMetricaService


@pytest.fixture
def dependencies():
    client = AsyncMock(spec=YandexMetricaClientProtocol)
    encryption = Mock(spec=EncryptionService)
    encryption.decrypt.return_value = "decrypted-token"
    client.get_counters.return_value = {"rows": "2", "counters": [{"id": 42, "site": "example.com"}]}
    return YandexMetricaService(client, encryption), client, encryption


@pytest.mark.parametrize(
    ("limit", "offset", "expected_limit", "expected_offset"),
    [(0, -1, 1, 0), (-100, -20, 1, 0), (101, 2, 100, 2), (1, 0, 1, 0), (100, 500, 100, 500), (20, 3, 20, 3)],
)
async def test_get_counters_clamps_pagination_and_forwards_search(
    dependencies, limit, offset, expected_limit, expected_offset
):
    service, client, encryption = dependencies

    result = await service.get_counters(access_token="encrypted-token", search="пример", limit=limit, offset=offset)

    encryption.decrypt.assert_called_once_with(data="encrypted-token")
    client.get_counters.assert_awaited_once_with(
        access_token="decrypted-token", search="пример", limit=expected_limit, offset=expected_offset
    )
    assert result.model_dump() == {"total": 2, "items": [{"counter_id": 42, "site": "example.com"}]}


async def test_get_counters_defaults_and_empty_page(dependencies):
    service, client, _ = dependencies
    client.get_counters.return_value = {"rows": 0, "counters": []}

    result = await service.get_counters(access_token="encrypted")

    assert result.total == 0
    assert result.items == []
    client.get_counters.assert_awaited_once_with(access_token="decrypted-token", search=None, limit=20, offset=0)


async def test_invalid_counters_are_skipped_without_losing_valid_entries(dependencies, caplog):
    service, client, _ = dependencies
    client.get_counters.return_value = {
        "rows": 6,
        "counters": [
            {"id": "7", "site": "first.example"},
            {"site": "missing-id.example"},
            {"id": 8, "site": None},
            None,
            "not a mapping",
            {"id": 9, "site": "last.example", "extra": "ignored"},
        ],
    }

    result = await service.get_counters(access_token="encrypted")

    assert result.model_dump() == {
        "total": 6,
        "items": [{"counter_id": 7, "site": "first.example"}, {"counter_id": 9, "site": "last.example"}],
    }
    assert "Invalid counter" in caplog.text


@pytest.mark.parametrize("rows", ["bad", "", "3.5"])
async def test_invalid_total_is_reported_as_client_error(dependencies, rows):
    service, client, _ = dependencies
    client.get_counters.return_value = {"rows": rows, "counters": []}

    with pytest.raises(ClientError, match="Яндекс выдал некорректный ответ") as error:
        await service.get_counters(access_token="encrypted")

    assert isinstance(error.value.__cause__, ValueError)


@pytest.mark.parametrize(
    "response",
    [
        pytest.param({"counters": []}, id="missing-rows"),
        pytest.param({"rows": None, "counters": []}, id="null-rows"),
        pytest.param({"rows": [], "counters": []}, id="list-rows"),
        pytest.param({"rows": {}, "counters": []}, id="dict-rows"),
    ],
)
async def test_non_numeric_total_should_be_reported_as_client_error(dependencies, response):
    service, client, _ = dependencies
    client.get_counters.return_value = response

    with pytest.raises(ClientError, match="Яндекс выдал некорректный ответ"):
        await service.get_counters(access_token="encrypted")


@pytest.mark.parametrize("counters", [None, {}, "bad", 1])
async def test_non_list_counters_are_reported_as_client_error(dependencies, counters):
    service, client, _ = dependencies
    client.get_counters.return_value = {"rows": 0, "counters": counters}

    with pytest.raises(ClientError, match="Яндекс выдал некорректный ответ"):
        await service.get_counters(access_token="encrypted")


@pytest.mark.parametrize(
    ("cause", "message"),
    [(ValueError("bad token"), "Ошибка расшифровки токена"), (RuntimeError("broken"), "Ошибка расшифровки токена")],
)
async def test_decryption_failure_does_not_call_yandex(dependencies, cause, message):
    service, client, encryption = dependencies
    encryption.decrypt.side_effect = cause

    with pytest.raises(ClientError, match=message) as error:
        await service.get_counters(access_token="encrypted")

    assert error.value.__cause__ is cause
    client.get_counters.assert_not_awaited()


@pytest.mark.parametrize("status", [None, 403, 503])
async def test_yandex_errors_are_mapped_with_cause(dependencies, status, caplog, capsys):
    service, client, _ = dependencies
    cause = YandexClientException(status, "SECRET_MARKER")
    client.get_counters.side_effect = cause

    with pytest.raises(ClientError, match="Ошибка получения счётчиков Яндекс.Метрики") as error:
        await service.get_counters(access_token="encrypted")

    assert error.value.__cause__ is cause
    assert "SECRET_MARKER" not in str(error.value) + caplog.text + capsys.readouterr().out
    client.get_counters.assert_awaited_once()


async def test_unrelated_client_programming_error_is_not_masked(dependencies):
    service, client, _ = dependencies
    cause = RuntimeError("internal failure")
    client.get_counters.side_effect = cause
    with pytest.raises(RuntimeError) as error:
        await service.get_counters(access_token="encrypted")
    assert error.value is cause


@pytest.mark.parametrize("counters", [[], [None, {"id": None, "site": "SECRET_MARKER"}]])
async def test_empty_after_skip_keeps_upstream_total(dependencies, counters, caplog):
    service, client, _ = dependencies
    client.get_counters.return_value = {"rows": "12", "counters": counters}
    result = await service.get_counters(access_token="encrypted")
    assert result.model_dump() == {"total": 12, "items": []}
    assert "SECRET_MARKER" not in caplog.text


@pytest.mark.parametrize("cause", [ValueError("SECRET_MARKER"), RuntimeError("SECRET_MARKER")])
async def test_decrypt_failure_is_safe(dependencies, cause, caplog):
    service, client, encryption = dependencies
    encryption.decrypt.side_effect = cause
    with pytest.raises(ClientError) as error:
        await service.get_counters(access_token="SECRET_MARKER")
    assert error.value.__cause__ is cause
    assert "SECRET_MARKER" not in str(error.value) + caplog.text
    client.get_counters.assert_not_awaited()
