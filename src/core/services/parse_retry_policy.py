from dataclasses import dataclass

from core.entities.metrica_parse import ParseErrorCode
from core.exceptions.peer import PeerServiceException
from core.exceptions.yandex import YandexClientException


class ParseExecutionError(Exception):
    """Only safe machine-readable metadata may cross the execution boundary."""

    def __init__(self, code: ParseErrorCode, status: int | None = None) -> None:
        self.code = code
        self.status = status
        super().__init__(code.value)


@dataclass(frozen=True)
class RetryDecision:
    error_code: ParseErrorCode
    error_status: int | None
    retryable: bool


class RetryPolicy:
    def __init__(self, *, base_seconds: int = 60, cap_seconds: int = 1800) -> None:
        if base_seconds < 1 or cap_seconds < 1:
            raise ValueError("Retry delays must be positive")
        self._base = base_seconds
        self._cap = cap_seconds

    def delay_seconds(self, attempts: int) -> int:
        # Bounded arithmetic even for corrupted or arbitrarily large attempt counts.
        delay = min(self._base, self._cap)
        for _ in range(max(0, attempts - 1)):
            if delay >= self._cap:
                break
            delay = min(delay * 4, self._cap)
        return delay

    def classify(self, error: Exception) -> RetryDecision:
        code = ParseErrorCode.UNEXPECTED
        status = None
        if isinstance(error, ParseExecutionError):
            code, status = error.code, error.status
        elif isinstance(error, YandexClientException):
            status = error.status
            if status in (401, 403):
                code = ParseErrorCode.CREDENTIALS_INVALID
            elif status == 400:
                code = ParseErrorCode.PROVIDER_QUERY_ERROR
            elif status == 429:
                code = ParseErrorCode.PROVIDER_RATE_LIMITED
            elif error.category in {"timeout", "transport"} or (status is not None and status >= 500):
                code = ParseErrorCode.PROVIDER_UNAVAILABLE
            else:
                code = ParseErrorCode.PROVIDER_RESPONSE_INVALID
        elif isinstance(error, PeerServiceException):
            status = error.status
            transient = error.category in {"timeout", "transport"} or (status is not None and status >= 500)
            if error.service == "seo":
                code = ParseErrorCode.SEO_UNAVAILABLE if transient else ParseErrorCode.SEO_REJECTED
            else:
                code = ParseErrorCode.YANDEX_SERVICE_UNAVAILABLE
                return RetryDecision(code, status, transient)
        retryable = code in {
            ParseErrorCode.YANDEX_SERVICE_UNAVAILABLE,
            ParseErrorCode.PROVIDER_RATE_LIMITED,
            ParseErrorCode.PROVIDER_UNAVAILABLE,
            ParseErrorCode.SEO_UNAVAILABLE,
            ParseErrorCode.STORAGE_ERROR,
            ParseErrorCode.LEASE_EXPIRED,
            ParseErrorCode.UNEXPECTED,
        }
        return RetryDecision(code, status, retryable)
