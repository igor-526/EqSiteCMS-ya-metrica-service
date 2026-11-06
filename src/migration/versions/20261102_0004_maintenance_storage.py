"""Additive maintenance storage. Frozen schema; never imports application tables.

Generation zero identifies legacy runs/tasks pending operator reconciliation.
Downgrade is refused: it would erase durable barriers and delivery audit.
"""

import uuid

from alembic import op
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "20261102_0004"
down_revision = "20261028_0003"
branch_labels = None
depends_on = None

metadata = MetaData()
revision_metadata = metadata


def uuid_pk() -> Column:
    return Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)


def timestamp_columns() -> tuple[Column, Column]:
    return (
        Column("created_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
        Column("updated_at", DateTime(timezone=True), onupdate=func.now()),
    )


# Reference only; the existing run table is NOT created by this revision.
Table(
    "metrica_parse_runs",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("equestrian_id", UUID(as_uuid=True), nullable=False),
    UniqueConstraint("equestrian_id", "id", name="ux_metrica_parse_runs_tenant_id"),
)


def _lease_columns() -> tuple[Column, ...]:
    return (
        Column("lease_owner", UUID(as_uuid=True)),
        Column("lease_expires_at", DateTime(timezone=True)),
        Column("fencing_revision", BigInteger, nullable=False, server_default="0"),
    )


def _lease_checks(prefix: str) -> tuple[CheckConstraint, ...]:
    return (
        CheckConstraint("fencing_revision >= 0", name=f"ck_{prefix}_fencing"),
        CheckConstraint("(lease_owner IS NULL) = (lease_expires_at IS NULL)", name=f"ck_{prefix}_lease"),
    )


metrica_tenant_gates = Table(
    "metrica_tenant_gates",
    metadata,
    Column("equestrian_id", UUID(as_uuid=True), primary_key=True),
    Column("binding_generation", BigInteger, nullable=False, server_default="0"),
    Column("cleanup_requested_seq", BigInteger, nullable=False, server_default="0"),
    Column("cleanup_completed_seq", BigInteger, nullable=False, server_default="0"),
    *timestamp_columns(),
    CheckConstraint(
        "binding_generation >= 0 AND cleanup_requested_seq >= 0 "
        "AND cleanup_completed_seq >= 0 AND cleanup_completed_seq <= cleanup_requested_seq",
        name="ck_metrica_tenant_gates_lifecycle",
    ),
)

metrica_maintenance_operations = Table(
    "metrica_maintenance_operations",
    metadata,
    uuid_pk(),
    Column("equestrian_id", UUID(as_uuid=True), nullable=False),
    Column("kind", String(7), nullable=False),
    Column("status", String(24), nullable=False),
    Column("binding_generation", BigInteger),
    Column("cleanup_seq", BigInteger),
    Column("normalization_revision", BigInteger),
    Column("part_count", Integer),
    Column("cleanup_operation_id", UUID(as_uuid=True)),
    Column("result_message_id", UUID(as_uuid=True)),
    Column("checkpoint", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("next_attempt_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("error_code", String(64)),
    *_lease_columns(),
    *timestamp_columns(),
    UniqueConstraint("equestrian_id", "id", name="ux_metrica_maintenance_operations_tenant_id"),
    UniqueConstraint("equestrian_id", "cleanup_seq", name="ux_metrica_maintenance_cleanup_seq"),
    UniqueConstraint("equestrian_id", "normalization_revision", name="ux_metrica_maintenance_merge_revision"),
    ForeignKeyConstraint(
        ["equestrian_id"], ["metrica_tenant_gates.equestrian_id"], name="fk_metrica_maintenance_gate"
    ),
    ForeignKeyConstraint(
        ["equestrian_id", "cleanup_operation_id"],
        ["metrica_maintenance_operations.equestrian_id", "metrica_maintenance_operations.id"],
        name="fk_metrica_maintenance_superseding_cleanup",
    ),
    CheckConstraint(
        "(kind = 'cleanup' AND cleanup_seq > 0 AND binding_generation >= 0 "
        "AND cleanup_seq IS NOT NULL AND binding_generation IS NOT NULL AND normalization_revision IS NULL "
        "AND status IN ('pending', 'draining', 'deleting', 'completed', 'failed_blocked')) OR "
        "(kind = 'merge' AND normalization_revision > 0 AND normalization_revision IS NOT NULL "
        "AND cleanup_seq IS NULL AND binding_generation IS NULL "
        "AND status IN ('collecting', 'waiting', 'running', 'awaiting_seo', 'completed', "
        "'superseded_by_delete', 'failed_blocked'))",
        name="ck_metrica_maintenance_kind_state",
    ),
    CheckConstraint("part_count IS NULL OR part_count >= 0", name="ck_metrica_maintenance_part_count"),
    CheckConstraint("attempts >= 0", name="ck_metrica_maintenance_attempts"),
    *_lease_checks("metrica_maintenance"),
)
Index(
    "ix_metrica_maintenance_ready",
    metrica_maintenance_operations.c.status,
    metrica_maintenance_operations.c.next_attempt_at,
)
Index(
    "ix_metrica_maintenance_tenant_status",
    metrica_maintenance_operations.c.equestrian_id,
    metrica_maintenance_operations.c.status,
)

Index("ix_metrica_maintenance_lease", metrica_maintenance_operations.c.lease_expires_at)


metrica_maintenance_chunks = Table(
    "metrica_maintenance_chunks",
    metadata,
    Column("equestrian_id", UUID(as_uuid=True), nullable=False),
    Column("operation_id", UUID(as_uuid=True), primary_key=True),
    Column("part_index", Integer, primary_key=True),
    Column("payload_hash", String(64), nullable=False),
    Column("mappings", JSONB, nullable=False),
    Column("status", String(9), nullable=False, server_default="pending"),
    Column("checkpoint", JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    *timestamp_columns(),
    ForeignKeyConstraint(
        ["equestrian_id", "operation_id"],
        ["metrica_maintenance_operations.equestrian_id", "metrica_maintenance_operations.id"],
        name="fk_metrica_maintenance_chunks_operation",
    ),
    CheckConstraint("part_index >= 0", name="ck_metrica_maintenance_chunks_part"),
    CheckConstraint("payload_hash ~ '^[0-9a-f]{64}$'", name="ck_metrica_maintenance_chunks_hash"),
    CheckConstraint("status IN ('pending', 'running', 'completed')", name="ck_metrica_maintenance_chunks_status"),
    CheckConstraint(
        "jsonb_typeof(mappings) = 'array' AND jsonb_array_length(mappings) BETWEEN 1 AND 500",
        name="ck_metrica_maintenance_chunks_mappings",
    ),
)
Index(
    "ix_metrica_maintenance_chunks_ready",
    metrica_maintenance_chunks.c.operation_id,
    metrica_maintenance_chunks.c.status,
)

metrica_pending_parse_intents = Table(
    "metrica_pending_parse_intents",
    metadata,
    Column("job_id", UUID(as_uuid=True), primary_key=True),
    Column("equestrian_id", UUID(as_uuid=True), nullable=False),
    Column("payload_hash", String(64), nullable=False),
    Column("date_mode", String(8), nullable=False),
    Column("date_from", Date),
    Column("date_to", Date),
    Column("binding_generation", BigInteger, nullable=False),
    Column("observed_cleanup_requested_seq", BigInteger, nullable=False),
    Column("status", String(16), nullable=False, server_default="pending"),
    Column("run_id", UUID(as_uuid=True)),
    Column("error_code", String(64)),
    Column("next_attempt_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    *timestamp_columns(),
    ForeignKeyConstraint(
        ["equestrian_id"], ["metrica_tenant_gates.equestrian_id"], name="fk_metrica_pending_parse_gate"
    ),
    ForeignKeyConstraint(
        ["equestrian_id", "run_id"],
        ["metrica_parse_runs.equestrian_id", "metrica_parse_runs.id"],
        name="fk_metrica_pending_parse_run",
    ),
    CheckConstraint("payload_hash ~ '^[0-9a-f]{64}$'", name="ck_metrica_pending_parse_hash"),
    CheckConstraint(
        "binding_generation >= 0 AND observed_cleanup_requested_seq >= 0", name="ck_metrica_pending_parse_lifecycle"
    ),
    CheckConstraint(
        "(date_mode = 'auto' AND date_from IS NULL AND date_to IS NULL) OR "
        "(date_mode = 'explicit' AND date_from IS NOT NULL AND date_to IS NOT NULL AND date_from <= date_to)",
        name="ck_metrica_pending_parse_dates",
    ),
    CheckConstraint(
        "status IN ('pending', 'admitted', 'cancelled', 'missing_binding')", name="ck_metrica_pending_parse_status"
    ),
    CheckConstraint("(status = 'admitted') = (run_id IS NOT NULL)", name="ck_metrica_pending_parse_admitted"),
)
Index(
    "ix_metrica_pending_parse_ready",
    metrica_pending_parse_intents.c.status,
    metrica_pending_parse_intents.c.next_attempt_at,
)
Index(
    "ix_metrica_pending_parse_tenant_status",
    metrica_pending_parse_intents.c.equestrian_id,
    metrica_pending_parse_intents.c.status,
)

metrica_maintenance_inbox = Table(
    "metrica_maintenance_inbox",
    metadata,
    Column("consumer", String(128), primary_key=True),
    Column("message_id", UUID(as_uuid=True), primary_key=True),
    Column("equestrian_id", UUID(as_uuid=True), nullable=False),
    Column("operation_id", UUID(as_uuid=True), nullable=False),
    Column("payload_hash", String(64), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    ForeignKeyConstraint(
        ["equestrian_id", "operation_id"],
        ["metrica_maintenance_operations.equestrian_id", "metrica_maintenance_operations.id"],
        name="fk_metrica_maintenance_inbox_operation",
    ),
    CheckConstraint("payload_hash ~ '^[0-9a-f]{64}$'", name="ck_metrica_maintenance_inbox_hash"),
)

metrica_maintenance_outbox = Table(
    "metrica_maintenance_outbox",
    metadata,
    Column("message_id", UUID(as_uuid=True), primary_key=True),
    Column("equestrian_id", UUID(as_uuid=True), nullable=False),
    Column("operation_id", UUID(as_uuid=True), nullable=False),
    Column("subject", String(128), nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("payload_hash", String(64), nullable=False),
    Column("status", String(9), nullable=False, server_default="pending"),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("next_attempt_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
    Column("published_at", DateTime(timezone=True)),
    Column("error_code", String(64)),
    *_lease_columns(),
    *timestamp_columns(),
    ForeignKeyConstraint(
        ["equestrian_id", "operation_id"],
        ["metrica_maintenance_operations.equestrian_id", "metrica_maintenance_operations.id"],
        name="fk_metrica_maintenance_outbox_operation",
    ),
    CheckConstraint("status IN ('pending', 'published')", name="ck_metrica_maintenance_outbox_status"),
    CheckConstraint("attempts >= 0", name="ck_metrica_maintenance_outbox_attempts"),
    CheckConstraint("payload_hash ~ '^[0-9a-f]{64}$'", name="ck_metrica_maintenance_outbox_hash"),
    *_lease_checks("metrica_maintenance_outbox"),
)
Index(
    "ix_metrica_maintenance_outbox_ready",
    metrica_maintenance_outbox.c.status,
    metrica_maintenance_outbox.c.next_attempt_at,
)


Index(
    "ix_metrica_maintenance_outbox_tenant_status",
    metrica_maintenance_outbox.c.equestrian_id,
    metrica_maintenance_outbox.c.status,
)
Index("ix_metrica_maintenance_outbox_lease", metrica_maintenance_outbox.c.lease_expires_at)


NEW_TABLE_NAMES = (
    "metrica_tenant_gates",
    "metrica_maintenance_operations",
    "metrica_maintenance_chunks",
    "metrica_pending_parse_intents",
    "metrica_maintenance_inbox",
    "metrica_maintenance_outbox",
)


def upgrade() -> None:
    # Constant defaults preserve every legacy row and do not invent a peer snapshot.
    for table in ("metrica_parse_runs", "metrica_parse_tasks"):
        for name in ("binding_generation", "observed_cleanup_requested_seq"):
            op.add_column(table, Column(name, BigInteger, nullable=False, server_default="0"))
        expression = "binding_generation >= 0 AND observed_cleanup_requested_seq >= 0"
        if table == "metrica_parse_tasks":
            op.add_column(table, Column("fencing_revision", BigInteger, nullable=False, server_default="0"))
            expression += " AND fencing_revision >= 0"
        op.create_check_constraint(f"ck_{table}_lifecycle", table, expression)
    op.drop_constraint("ck_metrica_parse_tasks_status", "metrica_parse_tasks", type_="check")
    op.create_check_constraint(
        "ck_metrica_parse_tasks_status",
        "metrica_parse_tasks",
        "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
    )
    op.create_unique_constraint("ux_metrica_parse_runs_tenant_id", "metrica_parse_runs", ["equestrian_id", "id"])
    for new_table in metadata.sorted_tables:
        if new_table.name in NEW_TABLE_NAMES:
            new_table.create(op.get_bind())
    for name in ("metrica_page_stats", "metrica_search_query_stats"):
        op.create_index(f"ix_{name}_tenant_pk", name, ["equestrian_id", "id"])


def downgrade() -> None:
    raise RuntimeError("Maintenance ledgers must be retained; use backup/forward-fix, not destructive downgrade")
