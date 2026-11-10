"""Tenant-scoped read-only history; pagination is validated by the caller."""

from typing import Protocol
from uuid import UUID

from core.entities.metrica_task_list import MetricaTaskListPage


class MetricaTaskListRepositoryProtocol(Protocol):
    async def list_tasks(self, *, equestrian_id: UUID, limit: int, offset: int) -> MetricaTaskListPage: ...
