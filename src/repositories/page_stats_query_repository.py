"""One bound SQL statement per query, independent of page/bucket counts."""

from collections.abc import Sequence
from datetime import date
from uuid import UUID

from sqlalchemy import Date, any_, bindparam, column, func, select
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities.metrica_stats_query import Direction, Metric, PageStatsValue, StatsBucket
from models import metrica_page_stats

from ._stats_metric_expressions import ADDITIVE_METRICS, metric_expressions

_stats = metrica_page_stats.c
METRIC_EXPRESSIONS = metric_expressions(metrica_page_stats)


class PageStatsQueryRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def table(
        self, *, equestrian_id: UUID, metric: Metric, buckets: Sequence[StatsBucket], page_ids: Sequence[UUID]
    ) -> list[PageStatsValue]:
        bounds = (
            func.unnest(bindparam("froms", type_=ARRAY(Date)), bindparam("tos", type_=ARRAY(Date)))
            .table_valued(column("bucket_from", Date), column("bucket_to", Date))
            .render_derived(name="b")
        )
        stmt = (
            select(_stats.page_id, bounds.c.bucket_from.label("bucket"), METRIC_EXPRESSIONS[metric].label("value"))
            .select_from(
                metrica_page_stats.join(bounds, _stats.stat_date.between(bounds.c.bucket_from, bounds.c.bucket_to))
            )
            .where(
                _stats.equestrian_id == equestrian_id,
                _stats.page_id == any_(bindparam("page_ids", type_=ARRAY(PGUUID(as_uuid=True)))),
                _stats.stat_date.between(min(b.date_from for b in buckets), max(b.date_to for b in buckets)),
            )
            .group_by(_stats.page_id, bounds.c.bucket_from)
            .order_by(_stats.page_id, bounds.c.bucket_from)
        )
        rows = await self.session.execute(
            stmt,
            {"froms": [b.date_from for b in buckets], "tos": [b.date_to for b in buckets], "page_ids": list(page_ids)},
        )
        # PostgreSQL SUM(bigint) returns numeric; expose all additive metrics as integers.
        return [
            PageStatsValue(row.page_id, row.bucket, int(row.value) if metric in ADDITIVE_METRICS else row.value)
            for row in rows
        ]

    async def ranking(
        self, *, equestrian_id: UUID, metric: Metric, date_from: date, date_to: date, direction: Direction
    ) -> list[UUID]:
        value = METRIC_EXPRESSIONS[metric]
        ordering = {Direction.ASC: value.asc(), Direction.DESC: value.desc()}[direction]
        stmt = (
            select(_stats.page_id)
            .where(_stats.equestrian_id == equestrian_id, _stats.stat_date.between(date_from, date_to))
            .group_by(_stats.page_id)
            .order_by(ordering, _stats.page_id.asc())
        )
        return list((await self.session.scalars(stmt)).all())
