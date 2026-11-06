"""M-3d durable rediscovery fixtures; collection is NOT PG acceptance."""

import os
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities.metrica_maintenance import WorkKind
from models import metrica_maintenance_operations as operations
from repositories.metrica_maintenance_dispatch_repository import MaintenanceDispatchRepository
from repositories.metrica_maintenance_storage_repository import MaintenanceStorageRepository

pytestmark = pytest.mark.infrastructure


@pytest.fixture(autouse=True)
def require_dispatch_database():
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.fail("BLOCKED: dedicated TEST_DATABASE_URL required; skip is not acceptance")


async def test_lost_signal_rediscovery_live_expired_lease_and_blocked_prefix(pg_engine):
    tenant, operation, next_operation = uuid4(), uuid4(), uuid4()
    async with AsyncSession(pg_engine) as session:
        storage = MaintenanceStorageRepository(session)
        discovery = MaintenanceDispatchRepository(session)
        async with session.begin():
            await storage.save_operation(tenant, operation, kind=WorkKind.CLEANUP, cleanup_seq=1, binding_generation=1)
            await storage.save_operation(
                tenant, next_operation, kind=WorkKind.CLEANUP, cleanup_seq=2, binding_generation=2
            )
        for _ in range(2):
            async with session.begin():
                assert await discovery.ready_tenants(kind=WorkKind.CLEANUP, limit=100) == [tenant]
        async with session.begin():
            assert await storage.claim(tenant, owner=uuid4(), lease_seconds=60) is not None
        async with session.begin():
            assert await discovery.ready_tenants(kind=WorkKind.CLEANUP, limit=100) == []
            await session.execute(
                update(operations)
                .where(operations.c.id == operation)
                .values(lease_expires_at=func.clock_timestamp() - timedelta(seconds=1))
            )
        async with session.begin():
            assert await discovery.ready_tenants(kind=WorkKind.CLEANUP, limit=100) == [tenant]
            await session.execute(
                update(operations).where(operations.c.id == operation).values(status="failed_blocked")
            )
        async with session.begin():
            assert await discovery.ready_tenants(kind=WorkKind.CLEANUP, limit=100) == []


async def test_merge_discovery_does_not_bypass_collecting_head_or_cleanup(pg_engine):
    tenant, first, second = uuid4(), uuid4(), uuid4()
    async with AsyncSession(pg_engine) as session:
        storage = MaintenanceStorageRepository(session)
        discovery = MaintenanceDispatchRepository(session)
        async with session.begin():
            await storage.save_operation(tenant, first, kind=WorkKind.MERGE, normalization_revision=1)
            await storage.save_operation(tenant, second, kind=WorkKind.MERGE, normalization_revision=2)
            await storage.seal(tenant, second, part_count=0)
        async with session.begin():
            assert await discovery.ready_tenants(kind=WorkKind.MERGE, limit=100) == []
            await storage.seal(tenant, first, part_count=0)
        async with session.begin():
            assert await discovery.ready_tenants(kind=WorkKind.MERGE, limit=1) == [tenant]
            await storage.save_operation(tenant, uuid4(), kind=WorkKind.CLEANUP, cleanup_seq=1, binding_generation=1)
        async with session.begin():
            assert await discovery.ready_tenants(kind=WorkKind.MERGE, limit=100) == []


async def test_bounded_waiting_heads_do_not_starve_other_tenants(pg_engine):
    tenants = [uuid4(), uuid4(), uuid4()]
    async with AsyncSession(pg_engine) as session:
        storage = MaintenanceStorageRepository(session)
        discovery = MaintenanceDispatchRepository(session)
        async with session.begin():
            for tenant in tenants:
                await storage.save_operation(
                    tenant, uuid4(), kind=WorkKind.CLEANUP, cleanup_seq=1, binding_generation=1
                )
        discovered = []
        for _ in tenants:
            async with session.begin():
                discovered.extend(await discovery.ready_tenants(kind=WorkKind.CLEANUP, limit=1))
        assert set(discovered) == set(tenants)
