from collections.abc import Sequence
from datetime import date
from uuid import UUID

from sqlalchemy import case, delete, func, literal_column, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.dml import ReturningInsert

from core.entities import ParseKind, StatRow
from core.protocols.repositories.metrica_stats import UpsertCounts
from models import metrica_page_stats, metrica_search_query_stats

METRICS = ("visits", "users", "pageviews", "bounces", "visit_duration_seconds")


class StatsRepository:
    """No commit: a page's upsert and checkpoint must use the same session."""

    def __init__(self, session: AsyncSession, kind: ParseKind) -> None:
        self.session = session
        if kind == ParseKind.PAGES:
            self.table = metrica_page_stats
            self.entity_column = "page_id"
        elif kind == ParseKind.SEARCH_QUERIES:
            self.table = metrica_search_query_stats
            self.entity_column = "search_query_id"
        else:
            raise ValueError("Unknown parse kind")

    async def upsert(self, rows: Sequence[StatRow]) -> UpsertCounts:
        # API pages can contain 100000 rows; asyncpg permits only 32767 binds.
        # Chunk statements, never transactions: the whole page remains atomic.
        inserted = updated = 0
        for start in range(0, len(rows), 1000):
            counts = await self._upsert_batch(rows[start : start + 1000])
            inserted += counts.inserted
            updated += counts.updated
        return UpsertCounts(inserted, updated)

    async def _upsert_batch(self, rows: Sequence[StatRow]) -> UpsertCounts:
        values = []
        for row in rows:
            if row.parse_task_id is None:
                raise ValueError("Upsert requires a parse task")
            values.append(
                {
                    "id": row.id,
                    "equestrian_id": row.equestrian_id,
                    "stat_date": row.stat_date,
                    self.entity_column: row.entity_id,
                    "parse_task_id": row.parse_task_id,
                    **{name: getattr(row, name) for name in METRICS},
                }
            )
        stmt = insert(self.table).values(values)
        same_task = self.table.c.parse_task_id == stmt.excluded.parse_task_id
        upsert_stmt: ReturningInsert[tuple[bool]] = stmt.on_conflict_do_update(
            index_elements=["equestrian_id", "stat_date", self.entity_column],
            set_={
                **{
                    name: case((same_task, self.table.c[name] + stmt.excluded[name]), else_=stmt.excluded[name])
                    for name in METRICS
                },
                "parse_task_id": stmt.excluded.parse_task_id,
                "updated_at": func.now(),
            },
        ).returning(literal_column("xmax = 0").label("inserted"))
        flags = (await self.session.execute(upsert_stmt)).scalars().all()
        inserted = sum(bool(flag) for flag in flags)
        return UpsertCounts(inserted, len(flags) - inserted)

    async def reconcile_window(
        self, *, equestrian_id: UUID, date_from: date, date_to: date, parse_task_id: UUID
    ) -> int:
        stmt = (
            delete(self.table)
            .where(
                self.table.c.equestrian_id == equestrian_id,
                self.table.c.stat_date.between(date_from, date_to),
                self.table.c.parse_task_id.is_distinct_from(parse_task_id),
            )
            .returning(self.table.c.id)
        )
        return len((await self.session.execute(stmt)).all())

    async def get_last_stat_date(self, equestrian_id: UUID) -> date | None:
        return await self.session.scalar(
            select(func.max(self.table.c.stat_date)).where(self.table.c.equestrian_id == equestrian_id)
        )

    async def list_for_equestrian(self, *, equestrian_id: UUID, date_from: date, date_to: date) -> list[StatRow]:
        rows = (
            await self.session.execute(
                select(self.table)
                .where(
                    self.table.c.equestrian_id == equestrian_id,
                    self.table.c.stat_date.between(date_from, date_to),
                )
                .order_by(self.table.c.stat_date, self.table.c[self.entity_column])
            )
        ).mappings()
        return [StatRow.model_validate({**row, "entity_id": row[self.entity_column]}) for row in rows]
