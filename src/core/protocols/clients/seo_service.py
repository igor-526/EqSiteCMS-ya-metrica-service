from typing import Protocol
from uuid import UUID

from core.schemas.clients.seo_service import ResolvedItem


class SeoServiceClientProtocol(Protocol):
    async def resolve_pages(self, *, equestrian_id: UUID, items: list[str]) -> list[ResolvedItem]: ...

    async def resolve_search_queries(self, *, equestrian_id: UUID, items: list[str]) -> list[ResolvedItem]: ...
