"""M-1 schema contract only; runtime UT-D-04/05/06 and UT-N-04 follow in repositories."""

from io import StringIO
from pathlib import Path

import pytest
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from pydantic import ValidationError
from sqlalchemy import CheckConstraint, ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, CreateTable

import models
from core.entities import ParseRun, TaskStatus
from utils.basemodel import metadata

CONFIG_PATH = Path(__file__).parents[2] / "src" / "alembic.ini"
SCRIPT = ScriptDirectory.from_config(Config(str(CONFIG_PATH)))
REVISION = SCRIPT.get_revision("20261102_0004").module
NEW_TABLES = REVISION.NEW_TABLE_NAMES
DIALECT = postgresql.dialect()


def test_maintenance_revision_is_frozen_and_matches_metadata():
    assert SCRIPT.get_heads() == ["20261102_0004"]
    assert REVISION.down_revision == "20261028_0003"
    for name in NEW_TABLES:
        frozen, live = REVISION.revision_metadata.tables[name], metadata.tables[name]
        assert frozen is not live
        assert str(CreateTable(frozen).compile(dialect=DIALECT)) == str(CreateTable(live).compile(dialect=DIALECT))
        assert {str(CreateIndex(index).compile(dialect=DIALECT)) for index in frozen.indexes} == {
            str(CreateIndex(index).compile(dialect=DIALECT)) for index in live.indexes
        }


def test_maintenance_upgrade_is_additive_and_refuses_destructive_downgrade():
    output = StringIO()
    context = MigrationContext.configure(dialect_name="postgresql", opts={"as_sql": True, "output_buffer": output})
    with Operations.context(context):
        REVISION.upgrade()
        with pytest.raises(RuntimeError, match="must be retained"):
            REVISION.downgrade()
    sql = output.getvalue()
    for name in NEW_TABLES:
        assert f"CREATE TABLE {name}" in sql
    assert "'cancelled'" in sql
    assert "ADD COLUMN binding_generation BIGINT DEFAULT '0' NOT NULL" in sql
    assert "ADD COLUMN observed_cleanup_requested_seq BIGINT DEFAULT '0' NOT NULL" in sql
    assert "ADD COLUMN fencing_revision BIGINT DEFAULT '0' NOT NULL" in sql
    assert "DROP TABLE" not in sql and "DELETE FROM" not in sql and "UPDATE metrica" not in sql
    for name in ("metrica_page_stats", "metrica_search_query_stats"):
        assert f"CREATE INDEX ix_{name}_tenant_pk ON {name} (equestrian_id, id)" in sql


def _checks(table):
    return " ".join(
        str(constraint.sqltext) for constraint in table.constraints if isinstance(constraint, CheckConstraint)
    )


@pytest.mark.parametrize("name", ["metrica_parse_runs", "metrica_parse_tasks"])
def test_maintenance_legacy_parse_backfill_preserves_audit(name):
    table = metadata.tables[name]
    for field in ("binding_generation", "observed_cleanup_requested_seq"):
        assert not table.c[field].nullable
        assert table.c[field].server_default.arg == "0"
        assert f"{field} >= 0" in _checks(table)
    if name.endswith("tasks"):
        assert "'cancelled'" in _checks(table)
        assert table.c.status.type.length >= len(TaskStatus.CANCELLED)
        assert table.c.fencing_revision.server_default.arg == "0"


def test_maintenance_parse_entity_roundtrip_and_lifecycle_validation():
    from uuid import uuid4

    run = ParseRun(equestrian_id=uuid4(), date_mode="auto")
    assert run.binding_generation == run.observed_cleanup_requested_seq == 0
    assert TaskStatus("cancelled") == TaskStatus.CANCELLED
    for field in ("binding_generation", "observed_cleanup_requested_seq"):
        with pytest.raises(ValidationError):
            ParseRun(**{**run.model_dump(), field: -1})


@pytest.mark.parametrize(
    "name",
    [
        "metrica_maintenance_chunks",
        "metrica_maintenance_inbox",
        "metrica_maintenance_outbox",
        "metrica_pending_parse_intents",
    ],
)
def test_maintenance_references_are_tenant_qualified(name):
    table = metadata.tables[name]
    links = [constraint for constraint in table.constraints if isinstance(constraint, ForeignKeyConstraint)]
    assert links
    for link in links:
        assert "equestrian_id" in link.column_keys
        assert all(element.ondelete is None for element in link.elements), "No cascading loss of durable audit"


def test_maintenance_dedup_keys_and_ready_indexes():
    assert list(models.metrica_maintenance_chunks.primary_key.columns.keys()) == ["operation_id", "part_index"]
    assert list(models.metrica_maintenance_inbox.primary_key.columns.keys()) == ["consumer", "message_id"]
    assert list(models.metrica_pending_parse_intents.primary_key.columns.keys()) == ["job_id"]
    operation = models.metrica_maintenance_operations
    unique_keys = {
        tuple(constraint.columns.keys())
        for constraint in operation.constraints
        if isinstance(constraint, UniqueConstraint)
    }
    assert ("equestrian_id", "cleanup_seq") in unique_keys
    assert ("equestrian_id", "normalization_revision") in unique_keys
    for table in (operation, models.metrica_pending_parse_intents, models.metrica_maintenance_outbox):
        assert any(tuple(index.columns.keys()) == ("status", "next_attempt_at") for index in table.indexes)
    for table in (operation, models.metrica_maintenance_outbox):
        assert "fencing_revision >= 0" in _checks(table)
        assert "(lease_owner IS NULL) = (lease_expires_at IS NULL)" in _checks(table)


def test_maintenance_auto_intent_has_no_resolved_window_or_credentials():
    table = models.metrica_pending_parse_intents
    assert "date_mode = 'auto' AND date_from IS NULL AND date_to IS NULL" in _checks(table)
    assert not {"counter_id", "yandex_account_id", "token", "credentials", "tasks"}.intersection(table.c.keys())
    assert "cleanup_completed_seq <= cleanup_requested_seq" in _checks(models.metrica_tenant_gates)
    for table in (
        models.metrica_maintenance_chunks,
        models.metrica_maintenance_inbox,
        models.metrica_maintenance_outbox,
    ):
        assert "payload_hash ~ '^[0-9a-f]{64}$'" in _checks(table)
