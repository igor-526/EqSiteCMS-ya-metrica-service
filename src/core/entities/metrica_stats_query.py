"""Read-side types; calendar and request validation belong to the caller."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import StrEnum
from uuid import UUID


class Metric(StrEnum):
    VISITS = "visits"
    USERS = "users"
    PAGEVIEWS = "pageviews"
    BOUNCES = "bounces"
    VISIT_DURATION_SECONDS = "visit_duration_seconds"
    BOUNCE_RATE = "bounce_rate"
    PAGE_DEPTH = "page_depth"
    AVG_VISIT_DURATION_SECONDS = "avg_visit_duration_seconds"


class Direction(StrEnum):
    ASC = "asc"
    DESC = "desc"


@dataclass(frozen=True)
class StatsBucket:
    date_from: date
    date_to: date

    @property
    def key(self) -> date:
        return self.date_from


@dataclass(frozen=True)
class PageStatsValue:
    page_id: UUID
    bucket: date
    value: int | Decimal
