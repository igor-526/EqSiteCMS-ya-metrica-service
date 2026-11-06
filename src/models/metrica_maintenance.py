"""Durable tenant arbitration storage; no broker or worker state is authoritative."""

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    Date,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Table,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from utils.basemodel import metadata, timestamp_columns, uuid_pk


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
