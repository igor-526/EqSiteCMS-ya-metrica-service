import asyncio
from uuid import UUID

from celery import Celery  # type: ignore[import-untyped]


class CeleryParseDispatcher:
    def __init__(self, celery_app: Celery) -> None:
        self._app = celery_app

    async def dispatch_account(self, account_id: UUID) -> None:
        await asyncio.to_thread(
            self._app.send_task, "metrica.process_account", args=[str(account_id)], queue="ya_metrica"
        )
