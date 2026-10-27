"""Peer wire structure; semantic validation is performed by the query service."""

from datetime import date
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, Field, field_serializer

from core.entities.metrica_stats_query import Direction, Metric


class SearchQueryStatsInDto(BaseModel):
    equestrian_id: str | None = None
    metric: str | None = None


class SearchQueryStatsTableInDto(SearchQueryStatsInDto):
    bucket: list[str] = Field(default_factory=list)
    search_query_ids: list[str] = Field(default_factory=list)


class SearchQueryStatsRankingInDto(SearchQueryStatsInDto):
    date_from: str | None = None
    date_to: str | None = None
    direction: str | None = None


class SearchQueryStatsValueOutDto(BaseModel):
    search_query_id: UUID
    bucket: date
    value: int | Decimal

    @field_serializer("value", when_used="json")
    def serialize_value(self, value: int | Decimal) -> int | float:
        return float(round(value, 8)) if isinstance(value, Decimal) else value


class SearchQueryStatsTableOutDto(BaseModel):
    metric: Metric
    values: list[SearchQueryStatsValueOutDto]


class SearchQueryStatsRankingOutDto(BaseModel):
    metric: Metric
    direction: Direction
    search_query_ids: list[UUID]
