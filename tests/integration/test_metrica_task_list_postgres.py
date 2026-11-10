"""PG-01..04: exact permit-runner nodes. No application DB or skip fallback."""

import asyncio
from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import event, func, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities.metrica_maintenance import WorkKind
from core.services.metrica_cleanup_service import CleanupBatchService, CleanupFinalizerService, CleanupWorkerService
from models import metrica_maintenance_operations as operations
from models import metrica_page_stats as pages
from models import metrica_parse_runs as runs
from models import metrica_parse_tasks as tasks
from models import metrica_pending_parse_intents as intents
from models import metrica_search_query_stats as queries
from models import metrica_tenant_gates as gates
from repositories.metrica_cleanup_worker_repository import CleanupWorkerRepository
from repositories.metrica_maintenance_delivery_repository import MaintenanceDeliveryRepository
from repositories.metrica_task_list_repository import MetricaTaskListRepository, _statement

pytestmark = pytest.mark.infrastructure
STAMP = datetime(2026, 11, 1, 12, tzinfo=UTC)


async def gate(session, tenant):
    await session.execute(insert(gates).values(equestrian_id=tenant).on_conflict_do_nothing())


async def run(session, tenant, statuses, *, job_id=None, created_at=STAMP):
    run_id = uuid4()
    await session.execute(
        runs.insert().values(
            id=run_id,
            equestrian_id=tenant,
            job_id=job_id,
            date_mode="auto",
            created_at=created_at,
        )
    )
    for index, status in enumerate(statuses):
        await session.execute(
            tasks.insert().values(
                id=uuid4(),
                run_id=run_id,
                equestrian_id=tenant,
                yandex_account_id=uuid4(),
                counter_id="123",
                kind="pages" if index % 2 == 0 else "search_queries",
                date_from=date(2026, 11, 1),
                date_to=date(2026, 11, 1),
                sequence=index,
                max_attempts=3,
                status=status,
            )
        )
    return run_id


async def intent(session, tenant, *, job_id=None, status="pending", run_id=None, created_at=STAMP):
    await gate(session, tenant)
    job_id = job_id or uuid4()
    await session.execute(
        intents.insert().values(
            job_id=job_id,
            equestrian_id=tenant,
            payload_hash="a" * 64,
            date_mode="auto",
            binding_generation=0,
            observed_cleanup_requested_seq=0,
            status=status,
            run_id=run_id,
            created_at=created_at,
        )
    )
    return job_id


async def operation(session, tenant, kind, status, *, number=1, created_at=STAMP):
    await gate(session, tenant)
    operation_id = uuid4()
    await session.execute(
        operations.insert().values(
            id=operation_id,
            equestrian_id=tenant,
            kind=kind,
            status=status,
            created_at=created_at,
            **(
                {"cleanup_seq": number, "binding_generation": 0}
                if kind == "cleanup"
                else {"normalization_revision": number}
            ),
        )
    )
    return operation_id


async def read(session, tenant, limit=100, offset=0):
    return await MetricaTaskListRepository(session).list_tasks(equestrian_id=tenant, limit=limit, offset=offset)


async def test_aggregate_identity(pg_engine, sql_statements):
    tenant, other, job = uuid4(), uuid4(), uuid4()
    async with AsyncSession(pg_engine) as session:
        await intent(session, tenant, job_id=job)
        await session.commit()
        pending = await read(session, tenant)
        assert pending.total == 1
        original = pending.items[0]
        assert original.id == f"parsing:{job}" and original.status == "queued"
        # Admission changes the persisted source, not logical identity/time.
        run_id = await run(
            session, tenant, ["succeeded"] * 7 + ["queued"], job_id=job, created_at=STAMP + timedelta(days=1)
        )
        await session.execute(update(intents).where(intents.c.job_id == job).values(status="admitted", run_id=run_id))
        legacy_job = uuid4()
        await run(session, tenant, ["succeeded"], job_id=legacy_job)
        null_run = await run(session, tenant, [])
        cancelled = await intent(session, tenant, status="cancelled")
        missing = await intent(session, tenant, status="missing_binding")
        await run(session, other, ["running"])
        await intent(session, other)
        await operation(session, other, "merge", "running")
        # Tasks carry a tenant too: a foreign-tenant child must not influence A.
        await session.execute(
            tasks.insert().values(
                id=uuid4(),
                run_id=run_id,
                equestrian_id=other,
                yandex_account_id=uuid4(),
                counter_id="999",
                kind="pages",
                date_from=date(2026, 11, 1),
                date_to=date(2026, 11, 1),
                sequence=99,
                max_attempts=3,
                status="running",
            )
        )
        await session.commit()
    async with AsyncSession(pg_engine) as observer:
        sql_statements.clear()
        page = await read(observer, tenant)
        assert len(sql_statements) == 1
        assert len(page.items) == page.total == 5
        rows = {row.id: row for row in page.items}
        assert rows[f"parsing:{job}"] == original
        assert rows[f"parsing:{legacy_job}"].status == "succeeded"
        assert rows[f"parsing-run:{null_run}"].status == "failed"
        assert rows[f"parsing:{cancelled}"].status == "cancelled"
        assert rows[f"parsing:{missing}"].status == "failed"
        # Inspect the real PG plan on synthetic data; do not impose a brittle
        # index choice on this tiny fixture or invent a production latency SLA.
        sql = str(_statement(tenant, 20, 0).compile(dialect=pg_engine.dialect, compile_kwargs={"literal_binds": True}))
        plan = await observer.scalar(text("EXPLAIN (FORMAT JSON) " + sql))
        assert plan[0]["Plan"]

        def node_types(node):
            return [node["Node Type"], *(kind for child in node.get("Plans", []) for kind in node_types(child))]

        assert any("Aggregate" in kind for kind in node_types(plan[0]["Plan"]))


async def test_status_order_pagination(pg_engine):
    tenant = uuid4()
    expected = {}
    async with AsyncSession(pg_engine) as session:
        parse_cases = [
            (["failed", "running", "queued"], "running"),
            (["failed", "queued"], "queued"),
            (["failed", "cancelled", "succeeded"], "failed"),
            (["cancelled", "succeeded"], "cancelled"),
            (["succeeded"] * 8, "succeeded"),
            ([], "failed"),
            (["queued"], "queued"),
        ]
        for statuses, status in parse_cases:
            run_id = await run(session, tenant, statuses)
            expected[f"parsing-run:{run_id}"] = status
        maintenance = {
            "merge": {
                "collecting": "queued",
                "waiting": "queued",
                "running": "running",
                "awaiting_seo": "running",
                "completed": "succeeded",
                "superseded_by_delete": "cancelled",
                "failed_blocked": "failed",
            },
            "cleanup": {
                "pending": "queued",
                "draining": "running",
                "deleting": "running",
                "completed": "succeeded",
                "failed_blocked": "failed",
            },
        }
        for kind, native in maintenance.items():
            for number, (status, mapped) in enumerate(native.items(), 1):
                op_id = await operation(session, tenant, kind, status, number=number)
                expected[f"{'delete' if kind == 'cleanup' else 'merge'}:{op_id}"] = mapped
        # Older active must outrank a newer terminal, across all branches.
        oldest = await run(session, tenant, ["queued"], created_at=STAMP - timedelta(days=10))
        newest = await operation(
            session, tenant, "merge", "completed", number=99, created_at=STAMP + timedelta(days=10)
        )
        expected[f"parsing-run:{oldest}"] = "queued"
        expected[f"merge:{newest}"] = "succeeded"
        await session.commit()
    async with AsyncSession(pg_engine) as session:
        full = await read(session, tenant)
        assert full.total == len(expected)
        assert {row.id: row.status for row in full.items} == expected
        assert all(row.is_active == (row.status in {"queued", "running"}) for row in full.items)
        keys = [(row.is_active, row.created_at, row.id) for row in full.items]
        assert keys == sorted(keys, reverse=True)
        collected = []
        for offset in range(0, full.total, 3):
            page = await read(session, tenant, 3, offset)
            assert page.total == full.total
            collected.extend(page.items)
        assert collected == full.items and len({row.id for row in collected}) == full.total
        assert (await read(session, tenant, 1)).items == full.items[:1]
        past = await read(session, tenant, 100, full.total + 1)
        assert past.items == [] and past.total == full.total
        empty = await read(session, uuid4())
        assert empty.items == [] and empty.total == 0


async def test_consistent_read_snapshot(pg_engine):
    tenant = uuid4()
    async with AsyncSession(pg_engine) as seed:
        run_id = await run(seed, tenant, ["queued"])
        await seed.commit()
    snapshot_taken, writer_done = asyncio.Event(), asyncio.Event()

    class PausedReader(AsyncSession):
        async def execute(self, statement, *args, **kwargs):
            result = await super().execute(statement, *args, **kwargs)
            if getattr(statement, "is_select", False):
                snapshot_taken.set()
                await asyncio.wait_for(writer_done.wait(), timeout=10)
            return result

    async def writer():
        await asyncio.wait_for(snapshot_taken.wait(), timeout=10)
        async with AsyncSession(pg_engine) as session:
            await session.execute(update(tasks).where(tasks.c.run_id == run_id).values(status="succeeded"))
            await run(session, tenant, ["running"])
            await session.commit()
        writer_done.set()

    statements = []

    def capture(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    async def reader():
        async with PausedReader(pg_engine) as session:
            await session.execute(text("SET TRANSACTION READ ONLY"))
            connection = await session.connection()
            event.listen(connection.sync_connection, "before_cursor_execute", capture)
            try:
                page = await read(session, tenant)
                assert page.total == len(page.items) == 1
                assert page.items[0].status == "queued"
                assert len(statements) == 1 and statements[0].startswith("WITH ")
                assert "FOR UPDATE" not in statements[0]
            finally:
                event.remove(connection.sync_connection, "before_cursor_execute", capture)

    await asyncio.wait_for(asyncio.gather(reader(), writer()), timeout=20)
    async with AsyncSession(pg_engine) as observer:
        page = await read(observer, tenant)
        assert page.total == 2
        assert {row.status for row in page.items} == {"succeeded", "running"}
        assert await observer.scalar(select(func.count()).select_from(tasks)) == 2


async def test_history_survives_cleanup(pg_engine):
    tenant, other, cleanup_id = uuid4(), uuid4(), uuid4()
    async with AsyncSession(pg_engine) as session:
        parse_id = await run(session, tenant, ["succeeded"], job_id=uuid4())
        merge_id = await operation(session, tenant, "merge", "completed")
        for table, column in ((pages, "page_id"), (queries, "search_query_id")):
            for owner in (tenant, other):
                await session.execute(
                    table.insert().values(
                        id=uuid4(),
                        equestrian_id=owner,
                        stat_date=date(2026, 11, 1),
                        **{column: uuid4()},
                        visits=1,
                        users=1,
                        pageviews=1,
                        bounces=0,
                        visit_duration_seconds=1,
                    )
                )
        await MaintenanceDeliveryRepository(session).save_operation(
            tenant,
            cleanup_id,
            kind=WorkKind.CLEANUP,
            cleanup_seq=1,
            binding_generation=1,
        )
        await session.commit()
        before = await read(session, tenant)
        await session.rollback()
        repo = CleanupWorkerRepository(session)
        worker = CleanupWorkerService(
            repository=repo,
            transaction=session,
            batch_service=CleanupBatchService(repository=repo, transaction=session, batch_size=100),
            finalizer=CleanupFinalizerService(repository=repo, transaction=session),
        )
        for _ in range(8):
            if await worker.tick(tenant, owner=uuid4()) == "completed":
                break
        else:
            pytest.fail("Cleanup failed to finish within bounded ticks")
    # Fresh session proves persisted effects/history, not an in-memory/mock result.
    async with AsyncSession(pg_engine) as observer:
        after = await read(observer, tenant)
        assert after.total == before.total == 3
        assert {row.id for row in after.items} == {row.id for row in before.items}
        assert {row.type for row in after.items} == {"parsing", "merge", "delete"}
        assert all(row.status == "succeeded" and not row.is_active for row in after.items)
        assert any(row.id == f"merge:{merge_id}" for row in after.items)
        assert await observer.scalar(select(runs.c.id).where(runs.c.id == parse_id)) == parse_id
        for table in (pages, queries):
            tenant_count = await observer.scalar(
                select(func.count()).select_from(table).where(table.c.equestrian_id == tenant)
            )
            other_count = await observer.scalar(
                select(func.count()).select_from(table).where(table.c.equestrian_id == other)
            )
            assert tenant_count == 0
            assert other_count == 1
