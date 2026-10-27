"""IT-MS-08..14: claims, advisory locks, leases and atomic page checkpoints."""

import asyncio
from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities import DateMode, ParseErrorCode, ParseKind, ParseRun, ParseTask, StatRow, TaskStatus
from core.protocols.repositories import LeaseLostError
from models import metrica_page_stats, metrica_parse_runs, metrica_parse_tasks
from repositories import ParseRunRepository, ParseTaskRepository, StatsRepository

DAY = date(2026, 10, 23)


def run_tasks(*, eq=None, account=None, count=2, max_attempts=3):
    run = ParseRun(equestrian_id=eq or uuid4(), date_mode=DateMode.EXPLICIT)
    account = account or uuid4()
    tasks = [
        ParseTask(
            run_id=run.id,
            equestrian_id=run.equestrian_id,
            yandex_account_id=account,
            counter_id="123",
            kind=ParseKind.PAGES,
            date_from=DAY,
            date_to=DAY,
            sequence=i,
            max_attempts=max_attempts,
        )
        for i in range(count)
    ]
    return run, tasks


async def enqueue(session, **kwargs):
    run, tasks = run_tasks(**kwargs)
    assert await ParseRunRepository(session).create_run_with_tasks(run, tasks)
    return run, tasks


async def task_row(session, task_id):
    return (
        (await session.execute(select(metrica_parse_tasks).where(metrica_parse_tasks.c.id == task_id)))
        .mappings()
        .one()
    )


async def test_it_ms_08_order_and_dispatch(pg_session):
    account = uuid4()
    older, tasks = await enqueue(pg_session, account=account, count=3)
    _, newer = await enqueue(pg_session, account=account)
    _, foreign = await enqueue(pg_session)
    repo = ParseTaskRepository(pg_session)
    await pg_session.execute(
        update(metrica_parse_tasks)
        .where(metrica_parse_tasks.c.id == tasks[0].id)
        .values(next_attempt_at=datetime.now(UTC) + timedelta(hours=1))
    )
    assert set(await repo.list_dispatchable_accounts()) == {account, foreign[0].yandex_account_id}
    task = await repo.claim(account, locked_by="worker", lease_seconds=60)
    assert task is not None and task.id == tasks[1].id and task.run_id == older.id
    assert task.status == TaskStatus.RUNNING and task.attempts == 0 and task.started_at is not None
    assert task.locked_until > datetime.now(UTC)
    assert await repo.claim(account, locked_by="duplicate", lease_seconds=60) is None
    assert await repo.list_dispatchable_accounts() == [foreign[0].yandex_account_id]
    await repo.mark_succeeded(task.id, locked_by="worker", rows_deleted=2)
    assert (await task_row(pg_session, task.id))["rows_deleted"] == 2
    next_task = await repo.claim(account, locked_by="worker", lease_seconds=60)
    assert next_task.id == tasks[2].id
    await repo.mark_succeeded(next_task.id, locked_by="worker")
    next_task = await repo.claim(account, locked_by="worker", lease_seconds=60)
    assert next_task.id == newer[0].id


@pytest.mark.parametrize("same_account", [True, False])
async def test_it_ms_09_parallel_claim(pg_engine, same_account):
    async with AsyncSession(pg_engine) as setup:
        _, tasks = await enqueue(setup)
        account = tasks[0].yandex_account_id
        other = account
        if not same_account:
            _, others = await enqueue(setup)
            other = others[0].yandex_account_id
        await setup.commit()
    claimed, second_started = asyncio.Event(), asyncio.Event()

    async def first_claim():
        async with AsyncSession(pg_engine) as session:
            result = await ParseTaskRepository(session).claim(account, locked_by="first", lease_seconds=60)
            claimed.set()
            await second_started.wait()
            # Let second claim observe uncommitted row locks and race the unique index.
            await asyncio.sleep(0.1)
            await session.commit()
            return result

    async def second_claim():
        await claimed.wait()
        async with AsyncSession(pg_engine) as session:
            second_started.set()
            result = await ParseTaskRepository(session).claim(other, locked_by="second", lease_seconds=60)
            # A unique-index loser must leave the outer transaction usable.
            assert await session.scalar(select(func.count()).select_from(metrica_parse_runs))
            await session.commit()
            return result

    results = await asyncio.wait_for(asyncio.gather(first_claim(), second_claim()), timeout=10)
    assert sum(result is not None for result in results) == (1 if same_account else 2)
    async with AsyncSession(pg_engine) as session:
        assert await session.scalar(
            select(func.count()).select_from(metrica_parse_tasks).where(metrica_parse_tasks.c.status == "running")
        ) == (1 if same_account else 2)


async def test_it_ms_10_running_unique(pg_session):
    _, tasks = await enqueue(pg_session)
    repo = ParseTaskRepository(pg_session)
    await repo.claim(tasks[0].yandex_account_id, locked_by="worker", lease_seconds=60)
    with pytest.raises(IntegrityError) as exc:
        async with pg_session.begin_nested():
            await pg_session.execute(
                update(metrica_parse_tasks).where(metrica_parse_tasks.c.id == tasks[1].id).values(status="running")
            )
    assert exc.value.orig.sqlstate == "23505"


@pytest.mark.parametrize("max_attempts,expected", [(3, "queued"), (1, "failed")])
async def test_it_ms_11_reaper(pg_session, max_attempts, expected):
    _, tasks = await enqueue(pg_session, max_attempts=max_attempts)
    repo = ParseTaskRepository(pg_session)
    task = await repo.claim(tasks[0].yandex_account_id, locked_by="old", lease_seconds=60)
    await repo.save_checkpoint(task.id, locked_by="old", next_offset=101, lease_seconds=120, rows_fetched=100)
    await pg_session.execute(
        update(metrica_parse_tasks)
        .where(metrica_parse_tasks.c.id == task.id)
        .values(locked_until=datetime.now(UTC) - timedelta(seconds=1))
    )
    assert await repo.reap_expired_leases() == 1
    assert await repo.reap_expired_leases() == 0
    row = await task_row(pg_session, task.id)
    assert row["status"] == expected and row["attempts"] == 1
    assert row["error_code"] == "lease_expired" and row["error_status"] is None
    assert row["next_offset"] == 101 and row["rows_fetched"] == 100
    assert row["locked_by"] is None and row["locked_until"] is None
    assert (row["finished_at"] is not None) == (expected == "failed")
    with pytest.raises(LeaseLostError):
        await repo.renew_lease(task.id, locked_by="old", lease_seconds=60)


async def test_it_ms_12_page_checkpoint_atomicity(pg_engine):
    async with AsyncSession(pg_engine) as session:
        run, tasks = await enqueue(session)
        repo = ParseTaskRepository(session)
        task = await repo.claim(tasks[0].yandex_account_id, locked_by="worker", lease_seconds=60)
        await session.commit()
        row = StatRow(
            equestrian_id=run.equestrian_id,
            entity_id=uuid4(),
            parse_task_id=task.id,
            stat_date=DAY,
            visits=3,
            users=3,
            pageviews=6,
            bounces=1,
            visit_duration_seconds=30,
        )
        with pytest.raises(RuntimeError, match="page failed"):
            async with session.begin():
                await StatsRepository(session, ParseKind.PAGES).upsert([row])
                await repo.save_checkpoint(
                    task.id,
                    locked_by="worker",
                    next_offset=101,
                    lease_seconds=120,
                    rows_fetched=100,
                    rows_inserted=1,
                    rows_skipped=99,
                    sampled=True,
                )
                raise RuntimeError("page failed")
        assert await session.scalar(select(func.count()).select_from(metrica_page_stats)) == 0
        assert (await task_row(session, task.id))["next_offset"] == 1
        await session.rollback()
        async with session.begin():
            await StatsRepository(session, ParseKind.PAGES).upsert([row])
            await repo.save_checkpoint(
                task.id,
                locked_by="worker",
                next_offset=101,
                lease_seconds=120,
                rows_fetched=100,
                rows_inserted=1,
                rows_skipped=99,
                sampled=True,
            )
        saved = await task_row(session, task.id)
        assert saved["next_offset"] == 101 and saved["sampled"]
        assert (saved["pages_fetched"], saved["rows_fetched"], saved["rows_inserted"], saved["rows_skipped"]) == (
            1,
            100,
            1,
            99,
        )
        assert await session.scalar(select(func.count()).select_from(metrica_page_stats)) == 1


async def test_it_ms_13_concurrent_enqueue(pg_engine):
    eq = uuid4()
    first_locked, second_started = asyncio.Event(), asyncio.Event()

    async def first():
        async with AsyncSession(pg_engine) as session:
            repo = ParseRunRepository(session)
            assert not await repo.has_active_tasks(eq)
            first_locked.set()
            await second_started.wait()
            await asyncio.sleep(0.1)
            run, tasks = run_tasks(eq=eq)
            result = await repo.create_run_with_tasks(run, tasks)
            await session.commit()
            return result

    async def second():
        await first_locked.wait()
        async with AsyncSession(pg_engine) as session:
            second_started.set()
            run, tasks = run_tasks(eq=eq)
            result = await ParseRunRepository(session).create_run_with_tasks(run, tasks)
            await session.commit()
            return result

    assert await asyncio.wait_for(asyncio.gather(first(), second()), timeout=10) == [True, False]
    async with AsyncSession(pg_engine) as session:
        assert await session.scalar(select(func.count()).select_from(metrica_parse_runs)) == 1
        assert await session.scalar(select(func.count()).select_from(metrica_parse_tasks)) == 2


async def test_it_ms_14_fail_fast(pg_session):
    _, tasks = await enqueue(pg_session, count=7)
    _, foreign = await enqueue(pg_session)
    repo = ParseTaskRepository(pg_session)
    task = await repo.claim(tasks[0].yandex_account_id, locked_by="worker", lease_seconds=60)
    await repo.mark_failed(
        task.id, locked_by="worker", error_code=ParseErrorCode.CREDENTIALS_INVALID, error_status=401
    )
    assert await repo.fail_queued_by_account(task.yandex_account_id, error_status=401) == 6
    assert await repo.fail_queued_by_account(task.yandex_account_id) == 0
    assert (await task_row(pg_session, foreign[0].id))["status"] == "queued"
    for item in tasks:
        row = await task_row(pg_session, item.id)
        assert row["status"] == "failed" and row["error_code"] == "credentials_invalid"
        assert row["finished_at"] is not None and row["error_status"] == 401


async def test_retry_checkpoint_and_lease_fencing(pg_session):
    _, tasks = await enqueue(pg_session, max_attempts=2)
    repo = ParseTaskRepository(pg_session)
    task = await repo.claim(tasks[0].yandex_account_id, locked_by="one", lease_seconds=60)
    with pytest.raises(LeaseLostError):
        await repo.save_checkpoint(task.id, locked_by="stale", next_offset=11, lease_seconds=60)
    await repo.renew_lease(task.id, locked_by="one", lease_seconds=120)
    await repo.save_checkpoint(
        task.id, locked_by="one", next_offset=11, lease_seconds=60, rows_updated=1, sampled=True
    )
    await repo.schedule_retry(
        task.id,
        locked_by="one",
        next_attempt_at=datetime.now(UTC) - timedelta(seconds=1),
        error_code=ParseErrorCode.PROVIDER_UNAVAILABLE,
        error_status=503,
    )
    row = await task_row(pg_session, task.id)
    assert row["attempts"] == 1 and row["status"] == "queued" and row["next_offset"] == 11
    resumed = await repo.claim(task.yandex_account_id, locked_by="two", lease_seconds=60)
    assert resumed.id == task.id and resumed.next_offset == 11 and resumed.started_at == task.started_at
    with pytest.raises(LeaseLostError):
        await repo.mark_succeeded(task.id, locked_by="one")
    await repo.save_checkpoint(task.id, locked_by="two", next_offset=21, lease_seconds=60, sampled=False)
    assert (await task_row(pg_session, task.id))["sampled"]
    await repo.schedule_retry(
        task.id,
        locked_by="two",
        next_attempt_at=datetime.now(UTC) + timedelta(hours=1),
        error_code=ParseErrorCode.PROVIDER_RATE_LIMITED,
        error_status=429,
    )
    row = await task_row(pg_session, task.id)
    assert row["attempts"] == 2 and row["status"] == "failed" and row["finished_at"] is not None


@pytest.mark.infrastructure
async def test_it_mp_01_migration_preserves_legacy_rows(pg_schema_engine):
    from pathlib import Path

    from alembic.autogenerate import compare_metadata
    from alembic.config import Config
    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from alembic.script import ScriptDirectory
    from sqlalchemy import inspect, text

    from utils.basemodel import metadata

    script = ScriptDirectory.from_config(Config(str(Path(__file__).parents[2] / "src" / "alembic.ini")))

    def migrate(connection, target, *, downgrade=False):
        def steps(revision, context):
            return script._downgrade_revs(target, revision) if downgrade else script._upgrade_revs(target, revision)

        context = MigrationContext.configure(connection, opts={"fn": steps})
        with Operations.context(context), context.begin_transaction():
            context.run_migrations()

    legacy, tasks = run_tasks()
    async with pg_schema_engine.begin() as connection:
        await connection.run_sync(lambda conn: migrate(conn, "20261026_0002"))
        # Use the frozen revision, whose run table deliberately has no job_id.
        frozen = script.get_revision("20261026_0002").module
        await connection.execute(frozen.metrica_parse_runs.insert().values(legacy.model_dump(exclude={"job_id"})))
        for task in tasks:
            await connection.execute(frozen.metrica_parse_tasks.insert().values(task.model_dump(exclude_none=True)))
        await connection.execute(
            frozen.metrica_page_stats.insert().values(
                id=uuid4(),
                equestrian_id=legacy.equestrian_id,
                stat_date=DAY,
                page_id=uuid4(),
                visits=3,
                users=2,
                pageviews=4,
                bounces=1,
                visit_duration_seconds=30,
                parse_task_id=tasks[0].id,
            )
        )
        task_rows = (await connection.execute(select(frozen.metrica_parse_tasks))).mappings().all()
        stat_rows = (await connection.execute(select(metrica_page_stats))).mappings().all()
        await connection.run_sync(lambda conn: migrate(conn, "head"))
        assert await connection.scalar(text("SELECT version_num FROM alembic_version")) == "20261028_0003"
        runs = (await connection.execute(select(metrica_parse_runs))).mappings().all()
        assert len(runs) == 1 and runs[0]["job_id"] is None
        assert ParseRun.model_validate(runs[0]).id == legacy.id
        assert (await connection.execute(select(metrica_parse_tasks))).mappings().all() == task_rows
        assert (await connection.execute(select(metrica_page_stats))).mappings().all() == stat_rows
        assert (
            await connection.run_sync(lambda conn: compare_metadata(MigrationContext.configure(conn), metadata)) == []
        )
        for _ in range(2):
            extra, _ = run_tasks()
            await connection.execute(metrica_parse_runs.insert().values(extra.model_dump()))
        await connection.run_sync(lambda conn: migrate(conn, "20261026_0002", downgrade=True))
        columns = await connection.run_sync(lambda conn: inspect(conn).get_columns("metrica_parse_runs"))
        assert "job_id" not in {column["name"] for column in columns}
        assert await connection.scalar(select(func.count()).select_from(frozen.metrica_parse_runs)) == 3
        assert (await connection.execute(select(metrica_parse_tasks))).mappings().all() == task_rows
        assert (await connection.execute(select(metrica_page_stats))).mappings().all() == stat_rows
        await connection.run_sync(lambda conn: migrate(conn, "head"))
        assert await connection.scalar(select(func.count()).select_from(metrica_parse_runs)) == 3


@pytest.mark.infrastructure
async def test_it_mp_01_duplicate_and_active_are_distinct(pg_session):
    from core.exceptions.parse import ParseJobAlreadyExistsError

    repository = ParseRunRepository(pg_session)
    job_id = uuid4()
    assert not await repository.exists_job(job_id)
    first, tasks = run_tasks()
    first.job_id = job_id
    assert await repository.create_run_with_tasks(first, tasks)
    assert await repository.exists_job(job_id)
    # Duplicate wins over the active-run outcome for the same equestrian.
    repeat, repeated_tasks = run_tasks(eq=first.equestrian_id)
    repeat.job_id = job_id
    with pytest.raises(ParseJobAlreadyExistsError):
        await repository.create_run_with_tasks(repeat, repeated_tasks)
    repeat.job_id = uuid4()
    assert await repository.create_run_with_tasks(repeat, repeated_tasks) is False
    # Key remains effective after the original run has finished, even across equestrians.
    await pg_session.execute(update(metrica_parse_tasks).values(status="succeeded"))
    repeat, repeated_tasks = run_tasks()
    repeat.job_id = job_id
    with pytest.raises(ParseJobAlreadyExistsError):
        await repository.create_run_with_tasks(repeat, repeated_tasks)
    assert await pg_session.scalar(select(func.count()).select_from(metrica_parse_runs)) == 1
    assert await pg_session.scalar(select(func.count()).select_from(metrica_parse_tasks)) == len(tasks)
    # NULL keys remain repeatable for independent legacy runs.
    await enqueue(pg_session)
    await enqueue(pg_session)


@pytest.mark.infrastructure
@pytest.mark.parametrize("same_equestrian", [True, False])
async def test_it_mp_01_concurrent_job_conflict(pg_engine, same_equestrian):
    from core.exceptions.parse import ParseJobAlreadyExistsError

    job_id, eq = uuid4(), uuid4()
    inserted, second_started = asyncio.Event(), asyncio.Event()

    async def first():
        async with AsyncSession(pg_engine) as session:
            run, tasks = run_tasks(eq=eq)
            run.job_id = job_id
            assert await ParseRunRepository(session).create_run_with_tasks(run, tasks)
            inserted.set()
            await second_started.wait()
            await asyncio.sleep(0.1)
            await session.commit()

    async def second():
        await inserted.wait()
        async with AsyncSession(pg_engine) as session:
            run, tasks = run_tasks(eq=eq if same_equestrian else uuid4())
            run.job_id = job_id
            second_started.set()
            with pytest.raises(ParseJobAlreadyExistsError):
                await ParseRunRepository(session).create_run_with_tasks(run, tasks)
            # ON CONFLICT must not leave the session in a failed transaction.
            assert await session.scalar(select(func.count()).select_from(metrica_parse_runs)) == 1
            await session.commit()

    await asyncio.wait_for(asyncio.gather(first(), second()), timeout=10)
    async with AsyncSession(pg_engine) as session:
        assert await session.scalar(select(func.count()).select_from(metrica_parse_runs)) == 1
        assert await session.scalar(select(func.count()).select_from(metrica_parse_tasks)) == 2
