"""Real PostgreSQL storage prerequisites, not batch IT-D-02/IT-N-02 acceptance."""

import os
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities.metrica_maintenance import MaintenanceConflictError, MaintenanceLeaseLostError, WorkKind
from models import metrica_maintenance_chunks as chunks
from models import metrica_maintenance_inbox as inbox
from models import metrica_maintenance_operations as operations
from models import metrica_tenant_gates as gates
from repositories.metrica_maintenance_storage_repository import MaintenanceStorageRepository

pytestmark = pytest.mark.infrastructure


@pytest.fixture(autouse=True)
def require_maintenance_test_database():
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.fail("BLOCKED: dedicated TEST_DATABASE_URL required; skips are not acceptance")


def mappings():
    return [{"source_page_id": str(uuid4()), "target_page_id": str(uuid4())}]


async def setup_merge(session):
    tenant, operation = uuid4(), uuid4()
    repo = MaintenanceStorageRepository(session)
    assert await repo.save_operation(tenant, operation, kind=WorkKind.MERGE, normalization_revision=1)
    return repo, tenant, operation


async def test_maintenance_seal_before_parts_duplicate_and_payload_conflict(pg_session):
    repo, tenant, operation = await setup_merge(pg_session)
    assert await repo.seal(tenant, operation, part_count=2)
    body = mappings()
    assert await repo.save_chunk(tenant, operation, part_index=1, mappings=body)
    assert not await repo.save_chunk(tenant, operation, part_index=1, mappings=body)
    assert (await pg_session.scalar(select(operations.c.status))) == "collecting"
    assert await repo.save_chunk(tenant, operation, part_index=0, mappings=mappings())
    assert (await pg_session.scalar(select(operations.c.status))) == "waiting"
    await pg_session.commit()
    with pytest.raises(MaintenanceConflictError):
        await repo.save_chunk(tenant, operation, part_index=1, mappings=mappings())
    await pg_session.rollback()
    assert await pg_session.scalar(select(func.count()).select_from(chunks)) == 2
    with pytest.raises(MaintenanceConflictError):
        await repo.seal(tenant, operation, part_count=3)


async def test_maintenance_inbox_and_operation_rollback_together(pg_session):
    repo, tenant, operation = await setup_merge(pg_session)
    message = uuid4()
    assert await repo.save_inbox(tenant, operation, consumer="merge", message_id=message, payload={"a": 1})
    await pg_session.rollback()  # crash before ACK/commit
    assert await pg_session.scalar(select(func.count()).select_from(inbox)) == 0
    assert await pg_session.scalar(select(func.count()).select_from(operations)) == 0
    assert await repo.save_operation(tenant, operation, kind=WorkKind.MERGE, normalization_revision=1)
    assert await repo.save_inbox(tenant, operation, consumer="merge", message_id=message, payload={"a": 1})
    await pg_session.commit()  # crash after commit, broker redelivers
    assert not await repo.save_inbox(tenant, operation, consumer="merge", message_id=message, payload={"a": 1})
    with pytest.raises(MaintenanceConflictError):
        await repo.save_inbox(tenant, operation, consumer="merge", message_id=message, payload={"a": 2})


async def test_maintenance_identity_conflicts_do_not_rebind_foreign_tenant(pg_session):
    repo, tenant, operation = await setup_merge(pg_session)
    await pg_session.commit()
    assert not await repo.save_operation(tenant, operation, kind=WorkKind.MERGE, normalization_revision=1)
    with pytest.raises(MaintenanceConflictError):
        await repo.save_operation(uuid4(), operation, kind=WorkKind.MERGE, normalization_revision=1)
    await pg_session.rollback()
    with pytest.raises(MaintenanceConflictError):
        await repo.save_operation(tenant, uuid4(), kind=WorkKind.MERGE, normalization_revision=1)
    await pg_session.rollback()
    assert await pg_session.scalar(select(func.count()).select_from(gates)) == 1


async def test_maintenance_chunk_effect_checkpoint_and_finalize_correlated(pg_session):
    repo, tenant, operation = await setup_merge(pg_session)
    await repo.save_chunk(tenant, operation, part_index=0, mappings=mappings())
    await repo.seal(tenant, operation, part_count=1)
    lease = await repo.claim(tenant, owner=uuid4(), lease_seconds=30)
    assert lease is not None
    await repo.checkpoint_chunk(lease, part_index=0, checkpoint={"cursor": 1}, completed=True, lease_seconds=30)
    message = uuid4()
    await repo.finish_work(lease, status="awaiting_seo", result_message_id=message)
    await pg_session.commit()
    with pytest.raises(MaintenanceLeaseLostError):
        await repo.checkpoint(lease, checkpoint={}, lease_seconds=30)
    await pg_session.rollback()
    with pytest.raises(MaintenanceConflictError):
        await repo.finalize(tenant, operation, normalization_revision=1, result_message_id=uuid4())
    await pg_session.rollback()
    assert await repo.finalize(tenant, operation, normalization_revision=1, result_message_id=message)
    assert not await repo.finalize(tenant, operation, normalization_revision=1, result_message_id=message)


async def test_maintenance_supersession_late_parts_never_reopen_operation(pg_session):
    repo, tenant, operation = await setup_merge(pg_session)
    lease = None
    await repo.seal(tenant, operation, part_count=0)
    lease = await repo.claim(tenant, owner=uuid4(), lease_seconds=30)
    assert lease is not None
    cleanup = uuid4()
    await repo.save_operation(tenant, cleanup, kind=WorkKind.CLEANUP, cleanup_seq=1, binding_generation=1)
    assert await repo.supersede(tenant, operation, cleanup_operation=cleanup)
    message = uuid4()
    assert await repo.record_superseded_result(tenant, operation, result_message_id=message)
    assert not await repo.record_superseded_result(tenant, operation, result_message_id=message)
    assert not await repo.seal(tenant, operation, part_count=0)
    with pytest.raises(MaintenanceLeaseLostError):
        await repo.checkpoint(lease, checkpoint={}, lease_seconds=30)
    await pg_session.commit()
    with pytest.raises(MaintenanceConflictError):
        await repo.finalize(tenant, operation, normalization_revision=1, result_message_id=message)
    await pg_session.rollback()
    cleanup_lease = await repo.claim(tenant, owner=uuid4(), lease_seconds=30)
    assert cleanup_lease is not None
    await repo.finish_work(cleanup_lease, status="completed", result_message_id=uuid4())
    assert await repo.finalize(tenant, operation, normalization_revision=1, result_message_id=message)
    assert await pg_session.scalar(select(gates.c.cleanup_completed_seq)) == 1


async def test_maintenance_checkpoint_lost_lease_rolls_back_chunk_effect(pg_engine):
    async with AsyncSession(pg_engine) as session:
        repo, tenant, operation = await setup_merge(session)
        await repo.save_chunk(tenant, operation, part_index=0, mappings=mappings())
        await repo.seal(tenant, operation, part_count=1)
        lease = await repo.claim(tenant, owner=uuid4(), lease_seconds=30)
        assert lease is not None
        await session.commit()
        await session.execute(operations.update().where(operations.c.id == operation).values(fencing_revision=99))
        await session.commit()
        await session.execute(chunks.update().values(checkpoint={"uncommitted_effect": True}))
        with pytest.raises(MaintenanceLeaseLostError):
            await repo.checkpoint_chunk(lease, part_index=0, checkpoint={}, completed=True, lease_seconds=30)
        await session.rollback()
        assert await session.scalar(select(chunks.c.checkpoint)) == {}
