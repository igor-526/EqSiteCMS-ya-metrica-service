from functools import partial
from uuid import UUID

from clients.base import BaseApiClient
from core.exceptions import PeerServiceException
from core.schemas.clients.seo_service import ResolvedItem, ResolveResponse


class SeoServiceClient(BaseApiClient):
    def __init__(self, base_url: str, timeout: float = 10.0) -> None:
        super().__init__(base_url, timeout, partial(PeerServiceException, "seo"))

    async def resolve_pages(self, *, equestrian_id: UUID, items: list[str]) -> list[ResolvedItem]:
        return await self._resolve(equestrian_id, items, "pages")

    async def resolve_search_queries(self, *, equestrian_id: UUID, items: list[str]) -> list[ResolvedItem]:
        return await self._resolve(equestrian_id, items, "search-queries")

    async def _resolve(self, equestrian_id: UUID, items: list[str], resource: str) -> list[ResolvedItem]:
        # Batching and 400 bisection belong to ingestion, not this HTTP adapter.
        if not 1 <= len(items) <= 1000:
            raise ValueError("Resolve batch must contain 1 to 1000 items")
        response = await self.make_request(
            method="POST",
            url=f"api/{resource}/resolve",
            headers={"X-Equestrian-Id": str(equestrian_id)},
            json={"items": items},
        )
        try:
            result = ResolveResponse.model_validate(response).items
            if [item.source for item in result] != items:
                raise ValueError("Resolve response does not match request order")
            return result
        except TypeError, ValueError:
            raise PeerServiceException("seo", 200, "response") from None
