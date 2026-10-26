from uuid import UUID

from core.protocols.repositories.metrica_parse import ParseTaskRepositoryProtocol
from core.services.parse_enqueue_service import ParseTransactionProtocol


class ParseDispatchService:
    def __init__(self, *, task_repository: ParseTaskRepositoryProtocol, transaction: ParseTransactionProtocol) -> None:
        self._tasks = task_repository
        self._transaction = transaction

    async def collect_ready_accounts(self) -> list[UUID]:
        async with self._transaction.begin():
            await self._tasks.reap_expired_leases()
            return await self._tasks.list_dispatchable_accounts()
