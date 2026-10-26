from uuid import UUID

from pydantic import BaseModel, Field


class ResolvedItem(BaseModel):
    source: str = Field(repr=False)
    normalized: str = Field(repr=False)
    id: UUID


class ResolveResponse(BaseModel):
    items: list[ResolvedItem]
