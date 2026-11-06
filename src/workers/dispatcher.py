import asyncio
from uuid import UUID

from celery import Celery  # type: ignore[import-untyped]

from core.entities.metrica_maintenance import WorkKind


class CeleryParseDispatcher:
    def __init__(self, celery_app: Celery) -> None:
        self._app = celery_app

    async def dispatch_account(self, account_id: UUID) -> None:
        await asyncio.to_thread(
            self._app.send_task, "metrica.process_account", args=[str(account_id)], queue="ya_metrica"
        )


class CeleryMaintenanceDispatcher:
    def __init__(self, celery_app: Celery) -> None:
        self._app = celery_app

    async def dispatch_maintenance(self, tenant: UUID, *, kind: WorkKind) -> None:
        if kind not in {WorkKind.CLEANUP, WorkKind.MERGE}:
            raise ValueError("Invalid maintenance kind")
        await asyncio.to_thread(
            self._app.send_task, f"metrica.{kind}_tick", args=[str(tenant)], queue=f"ya_metrica_{kind}"
        )
