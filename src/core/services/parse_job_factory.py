from dataclasses import dataclass, field
from typing import Protocol

from core.entities.metrica_parse import ParseErrorCode, ParseTask
from core.exceptions.peer import PeerServiceException
from core.protocols.clients.yandex_service import YandexServiceClientProtocol
from core.services.parse_retry_policy import ParseExecutionError


class TokenDecryptorProtocol(Protocol):
    def decrypt(self, *, data: str) -> str: ...


@dataclass(frozen=True)
class ParseJob:
    task: ParseTask
    access_token: str = field(repr=False)


class ParseJobFactory:
    def __init__(self, *, yandex_client: YandexServiceClientProtocol, encryption: TokenDecryptorProtocol) -> None:
        self._yandex = yandex_client
        self._encryption = encryption

    async def create(self, *, task: ParseTask) -> ParseJob:
        binding = await self._yandex.get_metrica_binding(equestrian_id=task.equestrian_id)
        counter = binding.counter
        if counter is None:
            raise ParseExecutionError(ParseErrorCode.BINDING_CHANGED)
        if any(
            (
                counter.equestrian_id != task.equestrian_id,
                counter.counter_id != task.counter_id,
                counter.yandex_account_id != task.yandex_account_id,
            )
        ):
            raise ParseExecutionError(ParseErrorCode.BINDING_CHANGED)
        try:
            encrypted = await self._yandex.get_enc_access_token(account_id=task.yandex_account_id)
        except PeerServiceException as error:
            if error.status == 400:
                raise ParseExecutionError(ParseErrorCode.CREDENTIALS_UNAVAILABLE, error.status) from None
            raise
        try:
            token = self._encryption.decrypt(data=encrypted.enc_access_token)
            if not token.strip():
                raise ValueError("Empty token")
        except ValueError, UnicodeError:
            raise ParseExecutionError(ParseErrorCode.CREDENTIALS_INVALID) from None
        return ParseJob(task=task, access_token=token)
