from typing import Protocol
from uuid import UUID


class ParseDispatcherProtocol(Protocol):
    async def dispatch_account(self, account_id: UUID) -> None: ...
