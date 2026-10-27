"""Peer query structure; periods are supplied by SEO, never resolved here."""

import re
from datetime import date
from decimal import Decimal
from typing import Annotated, Self
from uuid import UUID

from pydantic import BaseModel, BeforeValidator, Field, field_serializer, field_validator, model_validator

from core.entities.metrica_stats_query import Direction, Metric, StatsBucket


def parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        raise ValueError("Invalid date format")
    return date.fromisoformat(value)


def parse_bucket(value: str) -> StatsBucket:
    parts = value.split("..")
    if len(parts) != 2:
        raise ValueError("Invalid bucket format")
    start, end = map(parse_date, parts)
    if start > end:
        raise ValueError("Invalid bucket bounds")
    return StatsBucket(start, end)


class PageStatsTableInDto(BaseModel):
    equestrian_id: UUID
    metric: Metric
    bucket: list[str] = Field(min_length=1, max_length=31)
    page_ids: list[UUID] = Field(min_length=1, max_length=100)

    @field_validator("bucket")
    @classmethod
    def validate_buckets(cls, values: list[str]) -> list[str]:
        buckets = [parse_bucket(value) for value in values]
        if any(right.date_from <= left.date_to for left, right in zip(buckets, buckets[1:], strict=False)):
            raise ValueError("Buckets must be ordered and disjoint")
        return values

    @field_validator("page_ids", mode="before")
    @classmethod
    def deduplicate_pages(cls, values: list[str | UUID]) -> list[UUID]:
        return list(dict.fromkeys(UUID(str(value)) for value in values))

    @property
    def buckets(self) -> list[StatsBucket]:
        return [parse_bucket(value) for value in self.bucket]


class PageStatsRankingInDto(BaseModel):
    equestrian_id: UUID
    metric: Metric
    date_from: Annotated[date, BeforeValidator(parse_date)]
    date_to: Annotated[date, BeforeValidator(parse_date)]
    direction: Direction

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        if not 0 <= (self.date_to - self.date_from).days < 366:
            raise ValueError("Invalid ranking bounds")
        return self


class PageStatsValueOutDto(BaseModel):
    page_id: UUID
    bucket: date
    value: int | Decimal

    @field_serializer("value", when_used="json")
    def serialize_value(self, value: int | Decimal) -> int | float:
        return float(round(value, 8)) if isinstance(value, Decimal) else value


class PageStatsTableOutDto(BaseModel):
    metric: Metric
    values: list[PageStatsValueOutDto]


class PageStatsRankingOutDto(BaseModel):
    metric: Metric
    direction: Direction
    page_ids: list[UUID]
