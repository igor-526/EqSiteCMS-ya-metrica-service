import pytest

from core.entities.metrica_parse import ParseErrorCode
from core.exceptions.peer import PeerServiceException
from core.exceptions.yandex import YandexClientException
from core.services.parse_retry_policy import ParseExecutionError, RetryPolicy


@pytest.mark.parametrize(
    "status,category,code,retry",
    [
        (401, "http", "credentials_invalid", False),
        (403, "http", "credentials_invalid", False),
        (400, "http", "provider_query_error", False),
        (429, "http", "provider_rate_limited", True),
        (500, "http", "provider_unavailable", True),
        (503, "http", "provider_unavailable", True),
        (None, "timeout", "provider_unavailable", True),
        (None, "transport", "provider_unavailable", True),
        (200, "response", "provider_response_invalid", False),
        (404, "http", "provider_response_invalid", False),
    ],
)
def test_provider_mapping(status, category, code, retry):
    decision = RetryPolicy().classify(YandexClientException(status, category=category))
    assert (decision.error_code, decision.error_status, decision.retryable) == (code, status, retry)


@pytest.mark.parametrize("service", ["seo", "yandex"])
@pytest.mark.parametrize(
    "status,category,retry",
    [
        (None, "timeout", True),
        (None, "transport", True),
        (502, "http", True),
        (401, "http", False),
        (400, "http", False),
        (404, "http", False),
        (200, "response", False),
    ],
)
def test_peer_mapping(service, status, category, retry):
    decision = RetryPolicy().classify(PeerServiceException(service, status, category))
    expected = "yandex_service_unavailable" if service == "yandex" else "seo_unavailable" if retry else "seo_rejected"
    assert decision.error_code == expected
    assert decision.error_status == status
    assert decision.retryable == retry


@pytest.mark.parametrize("code", list(ParseErrorCode))
def test_explicit_codes(code):
    decision = RetryPolicy().classify(ParseExecutionError(code))
    assert decision.error_code == code
    assert decision.retryable == (
        code.value
        in {
            "yandex_service_unavailable",
            "provider_rate_limited",
            "provider_unavailable",
            "seo_unavailable",
            "storage_error",
            "lease_expired",
            "unexpected",
        }
    )


def test_backoff_cap_and_unknown():
    policy = RetryPolicy()
    assert [policy.delay_seconds(i) for i in range(1, 7)] == [60, 240, 960, 1800, 1800, 1800]
    assert policy.delay_seconds(10**9) == 1800
    assert policy.classify(RuntimeError("secret")).error_code == "unexpected"
    assert policy.classify(RuntimeError()).retryable


@pytest.mark.parametrize("kwargs", [{"base_seconds": 0}, {"cap_seconds": 0}])
def test_invalid_config(kwargs):
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)
