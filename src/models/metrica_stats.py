from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    Computed,
    Date,
    ForeignKey,
    Integer,
    Numeric,
    Table,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID

from utils.basemodel import metadata, timestamp_columns, uuid_pk


def _stats_table(name: str, entity_column: str) -> Table:
    return Table(
        name,
        metadata,
        uuid_pk(),
        Column("equestrian_id", UUID(as_uuid=True), nullable=False),
        Column("stat_date", Date, nullable=False),
        Column(entity_column, UUID(as_uuid=True), nullable=False),
        *(Column(metric, Integer, nullable=False) for metric in ("visits", "users", "pageviews", "bounces")),
        Column("visit_duration_seconds", BigInteger, nullable=False),
        Column(
            "bounce_rate",
            Numeric(11, 8),
            Computed("CASE WHEN visits > 0 THEN ROUND(bounces * 100.0 / visits, 8) ELSE 0 END", persisted=True),
        ),
        Column(
            "page_depth",
            Numeric(14, 8),
            Computed("CASE WHEN visits > 0 THEN ROUND(pageviews::numeric / visits, 8) ELSE 0 END", persisted=True),
        ),
        Column(
            "avg_visit_duration_seconds",
            Numeric(14, 8),
            Computed(
                "CASE WHEN visits > 0 THEN ROUND(visit_duration_seconds::numeric / visits, 8) ELSE 0 END",
                persisted=True,
            ),
        ),
        Column("parse_task_id", UUID(as_uuid=True), ForeignKey("metrica_parse_tasks.id", ondelete="SET NULL")),
        *timestamp_columns(),
        CheckConstraint(
            "visits >= 0 AND users >= 0 AND pageviews >= 0 AND bounces >= 0 "
            "AND bounces <= visits AND visit_duration_seconds >= 0",
            name=f"ck_{name}_metrics",
        ),
        UniqueConstraint("equestrian_id", "stat_date", entity_column, name=f"ux_{name}_eq_date_entity"),
    )


metrica_page_stats = _stats_table("metrica_page_stats", "page_id")
metrica_search_query_stats = _stats_table("metrica_search_query_stats", "search_query_id")
