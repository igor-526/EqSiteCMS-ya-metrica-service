"""Query-only read value; shared metrics and buckets retain page semantics."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from uuid import UUID


@dataclass(frozen=True)
class SearchQueryStatsValue:
    search_query_id: UUID
    bucket: date
    value: int | Decimal
