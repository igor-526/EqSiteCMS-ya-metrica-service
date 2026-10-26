from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID

from utils.basemodel import metadata, timestamp_columns, uuid_pk

metrica_parse_runs = Table(
    "metrica_parse_runs",
    metadata,
    uuid_pk(),
    Column("equestrian_id", UUID(as_uuid=True), nullable=False),
    Column("date_mode", String(8), nullable=False),
    Column("clamped", Boolean, nullable=False, server_default=text("false")),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    CheckConstraint("date_mode IN ('explicit', 'auto')", name="ck_metrica_parse_runs_date_mode"),
)

metrica_parse_tasks = Table(
    "metrica_parse_tasks",
    metadata,
    uuid_pk(),
    Column("run_id", UUID(as_uuid=True), ForeignKey("metrica_parse_runs.id", ondelete="CASCADE"), nullable=False),
    Column("equestrian_id", UUID(as_uuid=True), nullable=False),
    Column("counter_id", String(31), nullable=False),
    Column("yandex_account_id", UUID(as_uuid=True), nullable=False),
    Column("kind", String(14), nullable=False),
    Column("date_from", Date, nullable=False),
    Column("date_to", Date, nullable=False),
    Column("sequence", Integer, nullable=False),
    Column("status", String(9), nullable=False, server_default="queued"),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("max_attempts", Integer, nullable=False),
    Column("next_attempt_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("next_offset", Integer, nullable=False, server_default="1"),
    Column("locked_by", String(128)),
    Column("locked_until", DateTime(timezone=True)),
    Column("started_at", DateTime(timezone=True)),
    Column("finished_at", DateTime(timezone=True)),
    *(
        Column(name, Integer, nullable=False, server_default="0")
        for name in ("pages_fetched", "rows_fetched", "rows_inserted", "rows_updated", "rows_skipped", "rows_deleted")
    ),
    Column("sampled", Boolean, nullable=False, server_default=text("false")),
    Column("error_code", String(64)),
    Column("error_status", Integer),
    *timestamp_columns(),
    CheckConstraint("kind IN ('pages', 'search_queries')", name="ck_metrica_parse_tasks_kind"),
    CheckConstraint("status IN ('queued', 'running', 'succeeded', 'failed')", name="ck_metrica_parse_tasks_status"),
    CheckConstraint("date_from <= date_to AND date_to - date_from <= 4", name="ck_metrica_parse_tasks_window"),
    CheckConstraint("attempts >= 0 AND max_attempts > 0", name="ck_metrica_parse_tasks_attempts"),
    CheckConstraint("next_offset >= 1", name="ck_metrica_parse_tasks_offset"),
    CheckConstraint("sequence >= 0", name="ck_metrica_parse_tasks_sequence"),
    CheckConstraint(
        "pages_fetched >= 0 AND rows_fetched >= 0 AND rows_inserted >= 0 "
        "AND rows_updated >= 0 AND rows_skipped >= 0 AND rows_deleted >= 0",
        name="ck_metrica_parse_tasks_counters",
    ),
    UniqueConstraint("run_id", "sequence", name="ux_metrica_parse_tasks_run_sequence"),
)

Index(
    "ux_metrica_parse_tasks_running_account",
    metrica_parse_tasks.c.yandex_account_id,
    unique=True,
    postgresql_where=text("status = 'running'"),
)
Index(
    "ix_metrica_parse_tasks_claim",
    metrica_parse_tasks.c.yandex_account_id,
    metrica_parse_tasks.c.next_attempt_at,
    metrica_parse_tasks.c.created_at,
    metrica_parse_tasks.c.sequence,
    postgresql_where=text("status = 'queued'"),
)
Index(
    "ix_metrica_parse_tasks_active_eq",
    metrica_parse_tasks.c.equestrian_id,
    postgresql_where=text("status IN ('queued', 'running')"),
)
