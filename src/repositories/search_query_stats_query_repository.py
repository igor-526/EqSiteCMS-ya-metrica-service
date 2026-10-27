"""One bound SQL statement per query, independent of query/bucket counts."""

from collections.abc import Sequence
from datetime import date
from uuid import UUID

from sqlalchemy import Date, any_, bindparam, column, func, select
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities.metrica_stats_query import Direction, Metric, StatsBucket
from core.entities.search_query_stats_query import SearchQueryStatsValue
from models import metrica_search_query_stats

from ._stats_metric_expressions import ADDITIVE_METRICS, metric_expressions

_stats = metrica_search_query_stats.c
METRIC_EXPRESSIONS = metric_expressions(metrica_search_query_stats)


class SearchQueryStatsQueryRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def table(
        self, *, equestrian_id: UUID, metric: Metric, buckets: Sequence[StatsBucket], search_query_ids: Sequence[UUID]
    ) -> list[SearchQueryStatsValue]:
        bounds = (
            func.unnest(bindparam("froms", type_=ARRAY(Date)), bindparam("tos", type_=ARRAY(Date)))
            .table_valued(column("bucket_from", Date), column("bucket_to", Date))
            .render_derived(name="b")
        )
        stmt = (
            select(
                _stats.search_query_id, bounds.c.bucket_from.label("bucket"), METRIC_EXPRESSIONS[metric].label("value")
            )
            .select_from(
                metrica_search_query_stats.join(
                    bounds, _stats.stat_date.between(bounds.c.bucket_from, bounds.c.bucket_to)
                )
            )
            .where(
                _stats.equestrian_id == equestrian_id,
                _stats.search_query_id == any_(bindparam("search_query_ids", type_=ARRAY(PGUUID(as_uuid=True)))),
                _stats.stat_date.between(min(b.date_from for b in buckets), max(b.date_to for b in buckets)),
            )
            .group_by(_stats.search_query_id, bounds.c.bucket_from)
            .order_by(_stats.search_query_id, bounds.c.bucket_from)
        )
        rows = await self.session.execute(
            stmt,
            {
                "froms": [b.date_from for b in buckets],
                "tos": [b.date_to for b in buckets],
                "search_query_ids": list(search_query_ids),
            },
        )
        # PostgreSQL SUM(bigint) returns numeric; expose all additive metrics as integers.
        return [
            SearchQueryStatsValue(
                row.search_query_id, row.bucket, int(row.value) if metric in ADDITIVE_METRICS else row.value
            )
            for row in rows
        ]

    async def ranking(
        self, *, equestrian_id: UUID, metric: Metric, date_from: date, date_to: date, direction: Direction
    ) -> list[UUID]:
        value = METRIC_EXPRESSIONS[metric]
        ordering = {Direction.ASC: value.asc(), Direction.DESC: value.desc()}[direction]
        stmt = (
            select(_stats.search_query_id)
            .where(_stats.equestrian_id == equestrian_id, _stats.stat_date.between(date_from, date_to))
            .group_by(_stats.search_query_id)
            .order_by(ordering, _stats.search_query_id.asc())
        )
        return list((await self.session.scalars(stmt)).all())
