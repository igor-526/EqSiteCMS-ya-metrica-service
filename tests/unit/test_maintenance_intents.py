"""Admission UT-D-04/05/06; SQL-shape assertions are not real PG acceptance."""

from datetime import UTC, date, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.dialects import postgresql

from core.entities import DateMode, ParseKind, ParseRun, ParseTask
from core.entities.metrica_maintenance import (
    BindingLifecycle,
    IntentAdmission,
    MaintenanceConflictError,
    PendingParseIntent,
    WorkKind,
)
from core.protocols.repositories.metrica_maintenance import PendingParseIntentProtocol
from repositories.metrica_pending_parse_repository import PendingParseIntentRepository


def command():
    return PendingParseIntent(uuid4(), uuid4(), "auto")


def snapshot(*, requested=0, completed=0, generation=1, bound=True):
    return BindingLifecycle(generation, requested, completed, uuid4() if bound else None, "42" if bound else None)


def resolved(intent, lifecycle):
    run = ParseRun(
        job_id=intent.job_id,
        equestrian_id=intent.equestrian_id,
        date_mode=DateMode(intent.date_mode),
        binding_generation=lifecycle.binding_generation,
        observed_cleanup_requested_seq=lifecycle.cleanup_requested_seq,
    )
    tasks = [
        ParseTask(
            run_id=run.id,
            equestrian_id=run.equestrian_id,
            binding_generation=run.binding_generation,
            observed_cleanup_requested_seq=run.observed_cleanup_requested_seq,
            yandex_account_id=lifecycle.account_id,
            counter_id=lifecycle.counter_id,
            kind=ParseKind.PAGES,
            date_from=date(2026, 1, 1),
            date_to=date(2026, 1, 1),
            sequence=0,
            max_attempts=3,
        )
    ]
    return run, tasks


def repo_fixture() -> tuple[Any, AsyncMock]:
    # Methods are deliberately replaced by AsyncMock in this test double.
    session = AsyncMock()
    repo: Any = PendingParseIntentRepository(session)
    repo.lock_scope = AsyncMock()
    repo._intent = AsyncMock(return_value=None)
    repo.exists_job = AsyncMock(return_value=False)
    repo._observe = AsyncMock(return_value={"binding_generation": 1, "cleanup_requested_seq": 0})
    repo.selected_kind = AsyncMock(return_value=WorkKind.PARSE)
    repo.has_active_tasks = AsyncMock(return_value=False)
    return repo, session


def test_maintenance_intent_hash_stable_and_identity_includes_tenant_and_dates():
    intent = command()
    assert intent.payload_hash == PendingParseIntent(intent.job_id, intent.equestrian_id, "auto").payload_hash
    assert intent.payload_hash != PendingParseIntent(intent.job_id, uuid4(), "auto").payload_hash
    assert (
        intent.payload_hash
        != PendingParseIntent(  # noqa: W503
            intent.job_id, intent.equestrian_id, "explicit", date(2026, 1, 1), date(2026, 1, 2)
        ).payload_hash
    )
    assert len(intent.payload_hash) == 64


@pytest.mark.parametrize(
    ("mode", "start", "end"),
    [
        ("auto", date.today(), None),
        ("explicit", None, None),
        ("explicit", date(2026, 2, 1), date(2026, 1, 1)),
        ("unknown", None, None),
    ],
)
def test_maintenance_intent_invalid_dates(mode, start, end):
    with pytest.raises(ValueError):
        PendingParseIntent(uuid4(), uuid4(), mode, start, end)


@pytest.mark.parametrize("values", [(-1, 0, 0), (1, 1, 2), (1, -1, 0), (1, 0, 0, uuid4(), None)])
def test_maintenance_lifecycle_invalid(values):
    with pytest.raises(ValueError):
        BindingLifecycle(*values)


@pytest.mark.parametrize("kind", [WorkKind.CLEANUP, WorkKind.MERGE])
async def test_maintenance_enqueue_saves_auto_without_expansion_even_with_active_run(kind):
    repo, session = repo_fixture()
    intent, lifecycle = command(), snapshot(requested=1)
    repo.selected_kind.return_value = kind
    repo._observe.return_value["cleanup_requested_seq"] = 1
    session.scalar.return_value = intent.job_id
    contract: PendingParseIntentProtocol = repo
    assert await contract.enqueue_intent(intent, lifecycle=lifecycle) == IntentAdmission.DEFERRED
    params = session.scalar.call_args.args[0].compile().params
    assert params["date_mode"] == "auto" and params["date_from"] is None and params["date_to"] is None
    assert params["observed_cleanup_requested_seq"] == 1
    assert "counter_id" not in params and "account_id" not in params
    repo.has_active_tasks.assert_not_called()
    session.commit.assert_not_called()


async def test_maintenance_enqueue_outside_gate_preserves_active_rejection_without_insert():
    repo, session = repo_fixture()
    repo.has_active_tasks.return_value = True
    assert await repo.enqueue_intent(command(), lifecycle=snapshot()) == IntentAdmission.ACTIVE_RUN
    session.scalar.assert_not_called()


async def test_maintenance_redelivery_preserves_original_observation_without_overwrite():
    repo, session = repo_fixture()
    repo._intent.return_value = {"status": "pending"}
    assert await repo.enqueue_intent(command(), lifecycle=snapshot(requested=2)) == IntentAdmission.DUPLICATE
    repo._observe.assert_not_called()
    session.scalar.assert_not_called()


@pytest.mark.parametrize("foreign", [True, False])
async def test_maintenance_intent_conflict_checks_tenant_and_hash(foreign):
    repo, session = repo_fixture()
    del repo._intent  # exercise actual immutable identity check
    intent = command()
    result = MagicMock()
    result.mappings.return_value.one_or_none.return_value = {
        "equestrian_id": uuid4() if foreign else intent.equestrian_id,
        "payload_hash": intent.payload_hash if foreign else "0" * 64,
    }
    session.execute.return_value = result
    with pytest.raises(MaintenanceConflictError):
        await repo._intent(intent)


@pytest.mark.parametrize(
    ("status", "observed", "requested", "active", "bound", "expected"),
    [
        ("admitted", 0, 0, False, True, IntentAdmission.DUPLICATE),
        ("cancelled", 0, 0, False, True, IntentAdmission.DUPLICATE),
        ("pending", 0, 1, False, True, IntentAdmission.CANCELLED),
        ("pending", 1, 1, False, True, IntentAdmission.DEFERRED),
        ("pending", 0, 0, True, True, IntentAdmission.DEFERRED),
        ("pending", 0, 0, False, False, IntentAdmission.MISSING_BINDING),
        ("pending", 0, 0, False, True, IntentAdmission.READY),
    ],
)
async def test_maintenance_prepare_state_machine(status, observed, requested, active, bound, expected):
    repo, session = repo_fixture()
    lifecycle, intent = snapshot(requested=requested, bound=bound), command()
    repo._intent.return_value = {"status": status, "observed_cleanup_requested_seq": observed}
    repo._observe.return_value["cleanup_requested_seq"] = requested
    repo.has_active_tasks.return_value = active
    session.scalar.return_value = intent.job_id
    assert await repo.prepare_admission(intent, lifecycle=lifecycle) == expected
    repo.lock_scope.assert_awaited_once_with(
        [intent.equestrian_id], [lifecycle.account_id] if lifecycle.account_id else []
    )
    session.commit.assert_not_called()


@pytest.mark.parametrize("kind", [WorkKind.CLEANUP, WorkKind.MERGE])
async def test_maintenance_local_barrier_blocks_completed_authoritative_snapshot(kind):
    repo, _ = repo_fixture()
    repo._intent.return_value = {"status": "pending", "observed_cleanup_requested_seq": 0}
    repo.selected_kind.return_value = kind
    assert await repo.prepare_admission(command(), lifecycle=snapshot()) == IntentAdmission.DEFERRED


async def test_maintenance_stale_generation_snapshot_fails_closed():
    repo, _ = repo_fixture()
    repo._intent.return_value = {"status": "pending", "observed_cleanup_requested_seq": 0}
    repo._observe.return_value["binding_generation"] = 2
    assert await repo.prepare_admission(command(), lifecycle=snapshot()) == IntentAdmission.DEFERRED


async def test_maintenance_consume_rechecks_before_creating_run_and_is_atomic():
    repo, session = repo_fixture()
    intent, lifecycle = command(), snapshot()
    run, tasks = resolved(intent, lifecycle)
    repo.prepare_admission = AsyncMock(return_value=IntentAdmission.READY)
    repo.create_run_with_tasks = AsyncMock(return_value=True)
    session.scalar.return_value = intent.job_id
    assert await repo.consume_intent(intent, lifecycle=lifecycle, run=run, tasks=tasks) == IntentAdmission.ADMITTED
    repo.create_run_with_tasks.assert_awaited_once_with(run, tasks)
    stmt = session.scalar.call_args.args[0]
    assert stmt.compile().params["run_id"] == run.id
    assert stmt.compile().params["status"] == "admitted"
    session.commit.assert_not_called()


@pytest.mark.parametrize("field", ["job", "generation", "account", "counter", "sequence"])
async def test_maintenance_consume_fences_mismatching_snapshot(field):
    repo, _ = repo_fixture()
    intent, lifecycle = command(), snapshot()
    run, tasks = resolved(intent, lifecycle)
    repo.prepare_admission = AsyncMock(return_value=IntentAdmission.READY)
    repo.create_run_with_tasks = AsyncMock()
    if field == "job":
        run.job_id = uuid4()
    elif field == "generation":
        tasks[0].binding_generation = 0
    elif field == "sequence":
        run.observed_cleanup_requested_seq = 1
    elif field == "account":
        tasks[0].yandex_account_id = uuid4()
    else:
        tasks[0].counter_id = "other"
    with pytest.raises(MaintenanceConflictError):
        await repo.consume_intent(intent, lifecycle=lifecycle, run=run, tasks=tasks)
    repo.create_run_with_tasks.assert_not_called()


async def test_maintenance_consume_preempted_does_not_create_or_expand():
    repo, session = repo_fixture()
    intent, lifecycle = command(), snapshot()
    run, tasks = resolved(intent, lifecycle)
    repo.prepare_admission = AsyncMock(return_value=IntentAdmission.DEFERRED)
    repo.create_run_with_tasks = AsyncMock()
    assert await repo.consume_intent(intent, lifecycle=lifecycle, run=run, tasks=tasks) == IntentAdmission.DEFERRED
    repo.create_run_with_tasks.assert_not_called()
    session.scalar.assert_not_called()


async def test_maintenance_cleanup_cancels_only_older_observations_for_tenant():
    repo, session = repo_fixture()
    tenant = uuid4()
    await repo.cancel_before_cleanup(tenant, cleanup_seq=2)
    query = str(
        session.execute.call_args.args[0].compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
    )
    assert str(tenant) in query and "observed_cleanup_requested_seq < 2" in query
    assert "status = 'pending'" in query
    assert "binding_generation" not in query


async def test_maintenance_retry_discovery_is_bounded_and_terminal_rows_not_reopened():
    repo, session = repo_fixture()
    result = MagicMock()
    result.mappings.return_value = []
    session.execute.return_value = result
    assert await repo.ready_intents(limit=10) == []
    query = str(session.execute.call_args.args[0].compile(dialect=postgresql.dialect()))
    assert "LIMIT" in query and "next_attempt_at" in query and "clock_timestamp()" in query
    with pytest.raises(ValueError):
        await repo.ready_intents(limit=1001)
    repo._intent.return_value = {"status": "admitted"}
    await repo.defer_intent(command(), next_attempt_at=datetime.now(UTC))
    assert "status" in str(session.execute.call_args.args[0].whereclause)
    with pytest.raises(ValueError):
        await repo.defer_intent(command(), next_attempt_at=datetime(2026, 1, 1))


@pytest.mark.parametrize("created", [False, True])
async def test_maintenance_consumption_cas_loss_requires_full_rollback(created):
    repo, session = repo_fixture()
    intent, lifecycle = command(), snapshot()
    run, tasks = resolved(intent, lifecycle)
    repo.prepare_admission = AsyncMock(return_value=IntentAdmission.READY)
    repo.create_run_with_tasks = AsyncMock(return_value=created)
    session.scalar.return_value = None
    if created:
        with pytest.raises(MaintenanceConflictError, match="roll back"):
            await repo.consume_intent(intent, lifecycle=lifecycle, run=run, tasks=tasks)
    else:
        assert await repo.consume_intent(intent, lifecycle=lifecycle, run=run, tasks=tasks) == IntentAdmission.DEFERRED
        session.scalar.assert_not_called()
    session.commit.assert_not_called()


async def test_maintenance_lifecycle_observation_cannot_regress_gate_or_fabricate_completion():
    repo, session = repo_fixture()
    del repo._observe
    result = MagicMock()
    session.execute.return_value = result
    await repo._observe(uuid4(), snapshot(requested=2, completed=2))
    query = str(session.execute.call_args.args[0].compile(dialect=postgresql.dialect()))
    conflict = query.split("DO UPDATE SET", 1)[1].split("RETURNING", 1)[0]
    assert "greatest" in conflict and "binding_generation" in conflict and "cleanup_requested_seq" in conflict
    assert "cleanup_completed_seq" not in conflict


async def test_maintenance_unique_insert_race_rechecks_command_identity():
    repo, session = repo_fixture()
    repo._intent.side_effect = [None, MaintenanceConflictError("Parse command identity conflict")]
    session.scalar.return_value = None
    with pytest.raises(MaintenanceConflictError):
        await repo.enqueue_intent(command(), lifecycle=snapshot())
    assert repo._intent.await_count == 2


async def test_maintenance_delayed_retry_not_ready_and_missing_intent_is_not_admitted():
    repo, session = repo_fixture()
    repo._intent.return_value = {"status": "pending", "observed_cleanup_requested_seq": 0}
    session.scalar.return_value = None
    assert await repo.prepare_admission(command(), lifecycle=snapshot()) == IntentAdmission.DEFERRED
    repo._intent.return_value = None
    with pytest.raises(MaintenanceConflictError, match="Unknown"):
        await repo.prepare_admission(command(), lifecycle=snapshot())
