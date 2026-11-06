"""Real-PG finalizer/outbox crash recovery. Missing URL is blocked, not skip."""

import os
from datetime import date, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities.metrica_maintenance import MaintenanceLeaseLostError, WorkKind
from core.services.metrica_cleanup_service import CleanupFinalizerService
from models import metrica_maintenance_operations as operations
from models import metrica_maintenance_outbox as outbox
from models import metrica_page_stats as pages
from models import metrica_tenant_gates as gates
from repositories.metrica_cleanup_repository import CleanupBatchRepository
from repositories.metrica_result_outbox_repository import MaintenanceResultOutboxRepository

pytestmark = pytest.mark.infrastructure


@pytest.fixture(autouse=True)
def require_result_database():
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.fail("BLOCKED: dedicated TEST_DATABASE_URL required; skip is not acceptance")


async def seed(session):
    tenant, operation = uuid4(), uuid4()
    repo = CleanupBatchRepository(session)
    await repo.save_operation(tenant, operation, kind=WorkKind.CLEANUP, cleanup_seq=1, binding_generation=1)
    lease = await repo.claim(tenant, owner=uuid4(), lease_seconds=60)
    assert lease is not None
    await session.commit()
    return tenant, operation, lease


async def test_finalizer_rolls_back_outbox_terminal_and_prefix_on_crash(pg_engine):
    class CrashingRepository(CleanupBatchRepository):
        async def finish_work(self, *args, **kwargs):
            await super().finish_work(*args, **kwargs)
            raise RuntimeError("crash before commit")

    async with AsyncSession(pg_engine) as session:
        tenant, operation, lease = await seed(session)
        service = CleanupFinalizerService(repository=CrashingRepository(session), transaction=session)
        with pytest.raises(RuntimeError):
            await service.finalize(lease)
        assert await session.scalar(select(func.count()).select_from(outbox)) == 0
        assert await session.scalar(select(operations.c.status).where(operations.c.id == operation)) == "deleting"
        assert await session.scalar(select(gates.c.cleanup_completed_seq).where(gates.c.equestrian_id == tenant)) == 0
        await session.rollback()
        assert await CleanupFinalizerService(repository=CleanupBatchRepository(session), transaction=session).finalize(
            lease
        )


async def test_finalizer_rechecks_stats_instead_of_trusting_exhausted_checkpoint(pg_engine):
    async with AsyncSession(pg_engine) as session:
        tenant, operation, lease = await seed(session)
        await session.execute(
            operations.update().where(operations.c.id == operation).values(checkpoint={"table": "exhausted"})
        )
        await session.execute(
            pages.insert().values(
                id=uuid4(),
                equestrian_id=tenant,
                page_id=uuid4(),
                stat_date=date(2026, 1, 1),
                visits=1,
                users=1,
                pageviews=1,
                bounces=0,
                visit_duration_seconds=1,
            )
        )
        await session.commit()
        assert not await CleanupFinalizerService(
            repository=CleanupBatchRepository(session), transaction=session
        ).finalize(lease)
        assert await session.scalar(select(func.count()).select_from(outbox)) == 0


async def test_outbox_restart_after_claim_or_lost_puback_reuses_payload_and_fences_old_owner(pg_engine):
    async with AsyncSession(pg_engine) as session:
        tenant, operation, lease = await seed(session)
        assert await CleanupFinalizerService(repository=CleanupBatchRepository(session), transaction=session).finalize(
            lease
        )
        async with session.begin():
            first = await MaintenanceResultOutboxRepository(session).claim_result(owner=uuid4(), lease_seconds=60)
        assert first is not None
        # Crash after claim, publish timeout OR PubAck followed by process death:
        # all leave the same pending envelope available after lease expiry.
        await session.execute(
            outbox.update()
            .where(outbox.c.message_id == first.message_id)
            .values(lease_expires_at=func.clock_timestamp() - timedelta(seconds=1))
        )
        await session.commit()
    async with AsyncSession(pg_engine) as session:
        repo = MaintenanceResultOutboxRepository(session)
        async with session.begin():
            second = await repo.claim_result(owner=uuid4(), lease_seconds=60)
        assert second is not None
        assert (second.message_id, second.payload) == (first.message_id, first.payload)
        assert second.revision > first.revision
        with pytest.raises(MaintenanceLeaseLostError):
            async with session.begin():
                await repo.mark_published(first)
        async with session.begin():
            await repo.mark_published(second)
        async with session.begin():
            assert await repo.claim_result(owner=uuid4(), lease_seconds=60) is None
        assert (
            await session.scalar(select(outbox.c.status).where(outbox.c.message_id == first.message_id)) == "published"
        )
        assert await session.scalar(select(gates.c.cleanup_completed_seq).where(gates.c.equestrian_id == tenant)) == 1
        assert await session.scalar(select(operations.c.status).where(operations.c.id == operation)) == "completed"
