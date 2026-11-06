"""Real PG arbitration prerequisites for IT-D-02/IT-N-02 (not batch acceptance)."""

import asyncio
import os
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities.metrica_maintenance import MaintenanceLeaseLostError, WorkKind
from models import metrica_maintenance_operations as operations
from models import metrica_tenant_gates as gates
from repositories.metrica_maintenance_repository import MaintenanceArbitrationRepository

pytestmark = pytest.mark.infrastructure


@pytest.fixture(autouse=True)
def require_maintenance_test_database():
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.fail("BLOCKED: dedicated TEST_DATABASE_URL required; skips are not acceptance")


async def seed(engine, *, kind="merge", status="waiting", seq=1):
    tenant, operation = uuid4(), uuid4()
    values = {"normalization_revision": seq} if kind == "merge" else {"cleanup_seq": seq, "binding_generation": 1}
    async with engine.begin() as conn:
        await conn.execute(gates.insert().values(equestrian_id=tenant))
        await conn.execute(
            operations.insert().values(
                id=operation,
                equestrian_id=tenant,
                kind=kind,
                status=status,
                **values,
            )
        )
    return tenant, operation


async def test_gate_other_tenant_not_blocked_but_same_tenant_serialized(pg_engine):
    a, b = uuid4(), uuid4()
    async with AsyncSession(pg_engine) as first, AsyncSession(pg_engine) as second:
        await MaintenanceArbitrationRepository(first).lock_scope([a])
        await asyncio.wait_for(MaintenanceArbitrationRepository(second).lock_scope([b]), timeout=2)
        await second.rollback()
        await second.execute(text("SET LOCAL lock_timeout = '100ms'"))
        from sqlalchemy.exc import DBAPIError

        with pytest.raises(DBAPIError) as failure:
            await MaintenanceArbitrationRepository(second).lock_scope([a])
        assert getattr(failure.value.orig, "sqlstate", None) == "55P03"
        await second.rollback()
        await first.rollback()


async def test_maintenance_claims_unique_and_same_owner_revision_still_fenced(pg_engine):
    tenant, operation = await seed(pg_engine)
    owner = uuid4()
    async with AsyncSession(pg_engine) as first, AsyncSession(pg_engine) as second:
        repo = MaintenanceArbitrationRepository(first)
        old = await repo.claim(tenant, owner=owner, lease_seconds=30)
        assert old is not None and old.revision == 1
        await first.commit()
        next_repo = MaintenanceArbitrationRepository(second)
        assert await next_repo.claim(tenant, owner=uuid4(), lease_seconds=30) is None
        await second.rollback()
        await first.execute(
            operations.update()
            .where(operations.c.id == operation)
            .values(
                lease_expires_at=func.clock_timestamp() - timedelta(seconds=1),
            )
        )
        await first.commit()
        new = await next_repo.claim(tenant, owner=owner, lease_seconds=30)
        assert new is not None and new.revision == 2
        await second.commit()
        with pytest.raises(MaintenanceLeaseLostError):
            await repo.checkpoint(old, checkpoint={"bad": True}, lease_seconds=30)
        await first.rollback()
        await next_repo.checkpoint(new, checkpoint={"cursor": 1}, lease_seconds=30)
        await second.commit()
        assert (await first.execute(select(operations.c.checkpoint))).scalar_one() == {"cursor": 1}


async def test_maintenance_cleanup_preempts_merge_and_checkpoint_effect_rolls_back(pg_engine):
    tenant, operation = await seed(pg_engine)
    async with AsyncSession(pg_engine) as session:
        repo = MaintenanceArbitrationRepository(session)
        lease = await repo.claim(tenant, owner=uuid4(), lease_seconds=30)
        assert lease is not None
        await session.commit()
        # Authoritative watermark blocks even before cleanup command delivery.
        await session.execute(gates.update().where(gates.c.equestrian_id == tenant).values(cleanup_requested_seq=1))
        await session.commit()
        assert await repo.selected_kind(tenant) == WorkKind.CLEANUP
        assert await repo.claim(tenant, owner=uuid4(), lease_seconds=30) is None
        await session.execute(operations.update().where(operations.c.id == operation).values(checkpoint={"bad": 1}))
        with pytest.raises(MaintenanceLeaseLostError):
            await repo.checkpoint(lease, checkpoint={"bad": 2}, lease_seconds=30)
        await session.rollback()
        assert (await session.execute(select(operations.c.checkpoint))).scalar_one() == {}


@pytest.mark.parametrize("status", ["failed_blocked", "collecting", "awaiting_seo", "superseded_by_delete"])
async def test_maintenance_blocking_head_never_releases_parse(pg_engine, status):
    tenant, _ = await seed(pg_engine, status=status)
    async with AsyncSession(pg_engine) as session:
        repo = MaintenanceArbitrationRepository(session)
        assert await repo.selected_kind(tenant) == WorkKind.MERGE
        assert await repo.claim(tenant, owner=uuid4(), lease_seconds=30) is None


async def test_maintenance_cleanup_sequence_not_bypassed_when_first_delayed(pg_engine):
    tenant, first = await seed(pg_engine, kind="cleanup", status="pending")
    async with AsyncSession(pg_engine) as session:
        await session.execute(
            operations.update()
            .where(operations.c.id == first)
            .values(
                next_attempt_at=func.clock_timestamp() + timedelta(minutes=1),
            )
        )
        await session.execute(
            operations.insert().values(
                id=uuid4(),
                equestrian_id=tenant,
                kind="cleanup",
                status="pending",
                cleanup_seq=2,
                binding_generation=2,
            )
        )
        repo = MaintenanceArbitrationRepository(session)
        assert await repo.claim(tenant, owner=uuid4(), lease_seconds=30) is None
