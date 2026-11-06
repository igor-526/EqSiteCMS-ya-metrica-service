"""IT-N-02 cross-flow on Alembic head; collection is not PostgreSQL acceptance."""

import os
from datetime import date
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities.metrica_maintenance import MaintenanceConflictError, WorkKind
from core.entities.metrica_parse import ParseKind
from core.entities.metrica_stats import StatRow
from core.services.metrica_cleanup_service import CleanupBatchService, CleanupFinalizerService, CleanupWorkerService
from core.services.metrica_merge_service import MergeWorkerService
from models import metrica_maintenance_chunks as chunks
from models import metrica_maintenance_operations as operations
from models import metrica_maintenance_outbox as outbox
from models import metrica_parse_runs as runs
from models import metrica_parse_tasks as tasks
from models import metrica_search_query_stats as queries
from repositories.metrica_cleanup_worker_repository import CleanupWorkerRepository
from repositories.metrica_maintenance_delivery_repository import MaintenanceDeliveryRepository
from repositories.metrica_merge_worker_repository import MergeWorkerRepository
from repositories.metrica_stats_repository import StatsRepository
from tests.integration.test_merge_batch_postgres import list_pages, seed
from tests.integration.test_schema_postgres import _migrate

pytestmark = pytest.mark.infrastructure


@pytest.fixture(autouse=True)
def require_cross_flow_database():
    if not os.environ.get("TEST_DATABASE_URL"):
        pytest.fail("BLOCKED: dedicated TEST_DATABASE_URL required; collection is not acceptance")


@pytest.fixture
async def migrated_engine(pg_schema_engine):
    # These scenarios run against actual migrations, not just metadata.create_all.
    async with pg_schema_engine.begin() as connection:
        await connection.run_sync(lambda conn: _migrate(conn, "head"))
    return pg_schema_engine


async def prepared(session, **kwargs):
    values = await seed(session, **kwargs)
    async with session.begin():
        await MergeWorkerRepository(session).yield_merge(values[-1])
    return values


async def drain(service, tenant, *, terminal):
    for _ in range(20):
        result = await service.tick(tenant, owner=uuid4())
        if result == terminal:
            return
        assert result == "progress"
    pytest.fail("bounded worker did not reach expected terminal barrier")


async def test_worker_finalized_redelivery_then_fresh_parse_reconcile(migrated_engine):
    async with AsyncSession(migrated_engine) as session:
        tenant, foreign, operation, _, target, _ = await prepared(session, part_count=2)
        repo = MergeWorkerRepository(session)
        service = MergeWorkerService(repository=repo, transaction=session, batch_size=2)
        await drain(service, tenant, terminal="awaiting_seo")
        async with session.begin():
            before = await list_pages(session, tenant)
            assert before[0]["visits"] == 13 and all(row["parse_task_id"] is None for row in before)
            assert await repo.selected_kind(tenant) == WorkKind.MERGE
            result_id = await session.scalar(
                select(operations.c.result_message_id).where(operations.c.id == operation)
            )
            assert result_id is not None
        # Repeated worker signal cannot execute the already moved sources again.
        assert await service.tick(tenant, owner=uuid4()) == "waiting"
        with pytest.raises(MaintenanceConflictError):
            async with session.begin():
                await repo.finalize(tenant, operation, normalization_revision=1, result_message_id=uuid4())
        async with session.begin():
            assert before == await list_pages(session, tenant)
            assert await repo.finalize(tenant, operation, normalization_revision=1, result_message_id=result_id)
            assert not await repo.finalize(tenant, operation, normalization_revision=1, result_message_id=result_id)
            assert await repo.selected_kind(tenant) == WorkKind.PARSE
            assert await session.scalar(select(func.count()).select_from(outbox)) == 1
            # New run/task after correlated finalized; no bypass of worker barrier.
            run, task = uuid4(), uuid4()
            await session.execute(runs.insert().values(id=run, equestrian_id=tenant, date_mode="explicit"))
            await session.execute(
                tasks.insert().values(
                    id=task,
                    run_id=run,
                    equestrian_id=tenant,
                    counter_id="123",
                    yandex_account_id=uuid4(),
                    kind="pages",
                    date_from=date(2026, 1, 1),
                    date_to=date(2026, 1, 4),
                    sequence=0,
                    max_attempts=3,
                )
            )
            stats = StatsRepository(session, ParseKind.PAGES)
            row = StatRow(
                equestrian_id=tenant,
                entity_id=target,
                stat_date=date(2026, 1, 1),
                parse_task_id=task,
                visits=4,
                users=2,
                pageviews=12,
                bounces=1,
                visit_duration_seconds=40,
            )
            await stats.upsert([row])
            await stats.upsert([row])
            assert (
                await stats.reconcile_window(
                    equestrian_id=tenant, date_from=date(2026, 1, 1), date_to=date(2026, 1, 4), parse_task_id=task
                )
                == 3  # noqa: W503
            )
        rows = await list_pages(session, tenant)
        assert len(rows) == 2  # fifth date outside requested reconcile window survives
        assert (rows[0]["visits"], rows[0]["users"], rows[0]["pageviews"]) == (8, 4, 24)
        assert rows[1]["stat_date"] == date(2026, 1, 5)
        assert len(await list_pages(session, foreign)) == 1
        assert await session.scalar(select(func.count()).select_from(queries)) == 1


async def test_worker_overflow_rolls_back_then_durably_blocks(migrated_engine):
    async with AsyncSession(migrated_engine) as session:
        tenant, _, operation, _, _, _ = await prepared(session, visits=2**31 - 1)
        async with session.begin():
            before = await list_pages(session, tenant)
        repo = MergeWorkerRepository(session)
        service = MergeWorkerService(repository=repo, transaction=session, batch_size=2)
        assert await service.tick(tenant, owner=uuid4()) == "failed_blocked"
        assert await service.tick(tenant, owner=uuid4()) == "waiting"
        assert before == await list_pages(session, tenant)
        state = (await session.execute(select(operations).where(operations.c.id == operation))).mappings().one()
        assert state["status"] == "failed_blocked" and state["error_code"] == "merge_aggregate_overflow"
        assert await session.scalar(select(chunks.c.checkpoint).where(chunks.c.operation_id == operation)) == {}
        assert await session.scalar(select(func.count()).select_from(outbox)) == 0


async def test_cleanup_after_committed_merge_batch_and_late_part_never_resums(migrated_engine):
    async with AsyncSession(migrated_engine) as session:
        tenant, foreign, operation, source, target, _ = await prepared(session)
        repo = MergeWorkerRepository(session)
        service = MergeWorkerService(repository=repo, transaction=session, batch_size=2)
        assert await service.tick(tenant, owner=uuid4()) == "progress"
        cleanup = uuid4()
        async with session.begin():
            delivery = MaintenanceDeliveryRepository(session)
            await delivery.save_operation(tenant, cleanup, kind=WorkKind.CLEANUP, cleanup_seq=1, binding_generation=1)
            # Duplicate part keeps content identity and cannot restart summing.
            original = await session.scalar(select(chunks.c.mappings).where(chunks.c.operation_id == operation))
            assert original is not None
            assert not await delivery.save_chunk(
                tenant,
                operation,
                part_index=0,
                mappings=[
                    {"source_page_id": str(source), "target_page_id": str(target)},
                    # Read the immutable original second mapping, not a new UUID.
                    original[1],
                ],
            )
            assert await repo.selected_kind(tenant) == WorkKind.CLEANUP
            result_id = await session.scalar(
                select(operations.c.result_message_id).where(operations.c.id == operation)
            )
            assert result_id is not None
        with pytest.raises(MaintenanceConflictError):
            async with session.begin():
                await repo.finalize(tenant, operation, normalization_revision=1, result_message_id=result_id)
        assert await service.tick(tenant, owner=uuid4()) == "waiting"
        cleanup_repo = CleanupWorkerRepository(session)
        cleanup_service = CleanupWorkerService(
            repository=cleanup_repo,
            transaction=session,
            batch_service=CleanupBatchService(repository=cleanup_repo, transaction=session, batch_size=2),
            finalizer=CleanupFinalizerService(repository=cleanup_repo, transaction=session),
        )
        await drain(cleanup_service, tenant, terminal="completed")
        async with session.begin():
            assert not await list_pages(session, tenant)
            assert len(await list_pages(session, foreign)) == 1
            assert await session.scalar(select(func.count()).select_from(queries)) == 0
            assert await repo.selected_kind(tenant) == WorkKind.MERGE
            assert await repo.finalize(tenant, operation, normalization_revision=1, result_message_id=result_id)
            assert await repo.selected_kind(tenant) == WorkKind.PARSE
            assert await session.scalar(select(func.count()).select_from(outbox)) == 2
