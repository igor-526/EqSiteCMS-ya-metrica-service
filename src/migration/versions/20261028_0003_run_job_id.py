"""Idempotency key for scheduled parse commands; legacy runs remain NULL."""

from alembic import op
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import UUID

revision = "20261028_0003"
down_revision = "20261026_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("metrica_parse_runs", Column("job_id", UUID(as_uuid=True), nullable=True))
    op.create_index("ux_metrica_parse_runs_job_id", "metrica_parse_runs", ["job_id"], unique=True)


def downgrade() -> None:
    op.drop_index("ux_metrica_parse_runs_job_id", table_name="metrica_parse_runs")
    op.drop_column("metrica_parse_runs", "job_id")
