import logging
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from core.entities.metrica_parse import ParseErrorCode, ParseKind, ParseTask
from core.exceptions.yandex import YandexClientException
from core.protocols.repositories.metrica_parse import LeaseLostError
from core.services.parse_job_factory import ParseJob
from core.services.parse_retry_policy import ParseExecutionError, RetryPolicy
from core.services.parse_task_runner import TaskRunner


@pytest.fixture
def env():
    events = []
    task = ParseTask(
        run_id=uuid4(),
        equestrian_id=uuid4(),
        counter_id="123",
        yandex_account_id=uuid4(),
        kind=ParseKind.PAGES,
        date_from=date(2026, 10, 21),
        date_to=date(2026, 10, 25),
        sequence=1,
        max_attempts=3,
        locked_by="worker",
    )
    now = datetime(2026, 10, 26, tzinfo=UTC)

    @asynccontextmanager
    async def begin():
        events.append("begin")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    def action(name, result=None):
        async def call(*args, **kwargs):
            events.append(name)
            return result

        return AsyncMock(side_effect=call)

    tasks = SimpleNamespace(
        claim=action("claim", task),
        mark_succeeded=action("succeeded"),
        mark_failed=action("failed"),
        schedule_retry=action("retry"),
        fail_queued_by_account=action("fail-account", 6),
        list_dispatchable_accounts=action("ready", [task.yandex_account_id]),
    )
    pages = SimpleNamespace(reconcile_window=action("reconcile", 2))
    search = SimpleNamespace(reconcile_window=action("search-reconcile", 3))
    job = ParseJob(task=task, access_token="TOKEN_SECRET")
    factory = SimpleNamespace(create=action("job", job))

    async def ingest(**kwargs):
        events.append("ingest")
        task.pages_fetched += 4
        task.rows_fetched += 40
        task.rows_inserted += 30
        task.rows_updated += 5
        task.rows_skipped += 5

    ingestion = SimpleNamespace(ingest=AsyncMock(side_effect=ingest))
    runner = TaskRunner(
        task_repository=tasks,
        pages_repository=pages,
        search_queries_repository=search,
        transaction=SimpleNamespace(begin=begin),
        job_factory=factory,
        ingestion=ingestion,
        retry_policy=RetryPolicy(),
        lease_seconds=600,
        clock=lambda: now,
    )
    return SimpleNamespace(**locals())


async def run(env):
    return await env.runner.run_next_for_account(env.task.yandex_account_id, locked_by="worker")


async def test_success_commits_claim_before_job_and_reconciles_atomically(env, caplog):
    with caplog.at_level(logging.INFO):
        result = await run(env)
    assert result.has_more
    assert env.events == [
        "begin",
        "claim",
        "commit",
        "job",
        "ingest",
        "begin",
        "reconcile",
        "succeeded",
        "commit",
        "begin",
        "ready",
        "commit",
    ]
    env.tasks.claim.assert_awaited_once_with(env.task.yandex_account_id, locked_by="worker", lease_seconds=600)
    env.ingestion.ingest.assert_awaited_once_with(job=env.job, locked_by="worker")
    env.pages.reconcile_window.assert_awaited_once_with(
        equestrian_id=env.task.equestrian_id,
        date_from=env.task.date_from,
        date_to=env.task.date_to,
        parse_task_id=env.task.id,
    )
    env.tasks.mark_succeeded.assert_awaited_once_with(env.task.id, locked_by="worker", rows_deleted=2)
    for marker in ["Parse start", "Parse finish", "inserted=30", "updated=5", "skipped=5", "deleted=2", "pages=4"]:
        assert marker in caplog.text
    assert "TOKEN_SECRET" not in caplog.text


async def test_search_uses_kind_repository(env):
    env.task.kind = ParseKind.SEARCH_QUERIES
    await run(env)
    env.pages.reconcile_window.assert_not_awaited()
    env.search.reconcile_window.assert_awaited_once()


async def test_no_claim_quiet_exit(env):
    env.tasks.claim.side_effect = None
    env.tasks.claim.return_value = None
    assert not (await run(env)).has_more
    env.factory.create.assert_not_awaited()
    env.tasks.list_dispatchable_accounts.assert_not_awaited()


@pytest.mark.parametrize("attempts,delay", [(0, 60), (1, 240)])
async def test_retry_attempt_count_backoff_and_no_reconciliation(env, attempts, delay):
    env.task.attempts = attempts
    env.ingestion.ingest.side_effect = YandexClientException(503, category="http")
    await run(env)
    env.tasks.schedule_retry.assert_awaited_once_with(
        env.task.id,
        locked_by="worker",
        error_code=ParseErrorCode.PROVIDER_UNAVAILABLE,
        error_status=503,
        next_attempt_at=env.now + timedelta(seconds=delay),
    )
    env.pages.reconcile_window.assert_not_awaited()
    env.tasks.mark_failed.assert_not_awaited()
    env.tasks.fail_queued_by_account.assert_not_awaited()


async def test_exhausted_attempt_fails(env):
    env.task.attempts = 2
    env.ingestion.ingest.side_effect = YandexClientException(429, category="http")
    await run(env)
    env.tasks.mark_failed.assert_awaited_once_with(
        env.task.id,
        locked_by="worker",
        error_code=ParseErrorCode.PROVIDER_RATE_LIMITED,
        error_status=429,
    )
    env.tasks.schedule_retry.assert_not_awaited()


@pytest.mark.parametrize("phase", ["factory", "ingest"])
async def test_credentials_invalid_fail_fast_and_logs_safe(env, caplog, phase):
    marker = "TOKEN_SECRET CIPHERTEXT_SECRET PHRASE_SECRET https://secret.example"
    if phase == "factory":
        error = ParseExecutionError(ParseErrorCode.CREDENTIALS_INVALID)
        error.__cause__ = ValueError(marker)
        env.factory.create.side_effect = error
    else:
        error = YandexClientException(403, marker, marker, category="http")
        error.__cause__ = RuntimeError(marker)
        env.ingestion.ingest.side_effect = error
    with caplog.at_level(logging.INFO):
        await run(env)
    assert "credentials_invalid" in caplog.text
    for secret in ["TOKEN_SECRET", "CIPHERTEXT_SECRET", "PHRASE_SECRET", "https://secret.example", "Traceback"]:
        assert secret not in caplog.text
    assert env.events.index("failed") < env.events.index("fail-account") < env.events.index("ready")
    env.tasks.fail_queued_by_account.assert_awaited_once_with(
        env.task.yandex_account_id,
        error_code=ParseErrorCode.CREDENTIALS_INVALID,
        error_status=None if phase == "factory" else 403,
    )
    env.pages.reconcile_window.assert_not_awaited()


@pytest.mark.parametrize(
    "code",
    [
        ParseErrorCode.BINDING_CHANGED,
        ParseErrorCode.CREDENTIALS_UNAVAILABLE,
        ParseErrorCode.PROVIDER_QUERY_ERROR,
        ParseErrorCode.PROVIDER_RESPONSE_INVALID,
        ParseErrorCode.SEO_REJECTED,
    ],
)
async def test_permanent_failure_does_not_fail_account(env, code):
    env.factory.create.side_effect = ParseExecutionError(code)
    await run(env)
    env.tasks.mark_failed.assert_awaited_once()
    env.tasks.schedule_retry.assert_not_awaited()
    env.tasks.fail_queued_by_account.assert_not_awaited()
    env.ingestion.ingest.assert_not_awaited()


@pytest.mark.parametrize("phase", ["ingest", "reconcile", "succeeded", "failed", "retry"])
async def test_stale_worker_never_transitions_or_dispatches(env, phase):
    if phase == "ingest":
        env.ingestion.ingest.side_effect = LeaseLostError()
    elif phase == "reconcile":
        env.pages.reconcile_window.side_effect = LeaseLostError()
    elif phase == "succeeded":
        env.tasks.mark_succeeded.side_effect = LeaseLostError()
    else:
        env.ingestion.ingest.side_effect = YandexClientException(403 if phase == "failed" else 503)
        getattr(env.tasks, "mark_failed" if phase == "failed" else "schedule_retry").side_effect = LeaseLostError()
    assert not (await run(env)).has_more
    env.tasks.fail_queued_by_account.assert_not_awaited()
    env.tasks.list_dispatchable_accounts.assert_not_awaited()
    if phase not in {"failed", "retry"}:
        env.tasks.mark_failed.assert_not_awaited()
        env.tasks.schedule_retry.assert_not_awaited()
    if phase != "ingest":
        assert env.events[-1] == "rollback"


async def test_reconcile_storage_failure_rolls_back_and_retries(env):
    env.tasks.mark_succeeded.side_effect = RuntimeError("database secret")
    await run(env)
    assert env.events.index("reconcile") < env.events.index("rollback") < env.events.index("retry")
    assert env.tasks.schedule_retry.call_args.kwargs["error_code"] == "storage_error"


async def test_unknown_failure_retry_without_secret_logging(env, caplog):
    env.ingestion.ingest.side_effect = RuntimeError("TOKEN_SECRET CIPHERTEXT_SECRET PHRASE_SECRET")
    with caplog.at_level(logging.INFO):
        await run(env)
    assert "unexpected" in caplog.text and "status=queued" in caplog.text
    assert "SECRET" not in caplog.text


async def test_no_more_ready_work(env):
    env.tasks.list_dispatchable_accounts.side_effect = None
    env.tasks.list_dispatchable_accounts.return_value = [uuid4()]
    assert not (await run(env)).has_more


async def test_claim_storage_failure_propagates_before_provider(env):
    env.tasks.claim.side_effect = RuntimeError()
    with pytest.raises(RuntimeError):
        await run(env)
    assert env.events[-1] == "rollback"
    env.factory.create.assert_not_awaited()


async def test_error_transition_storage_failure_propagates(env):
    env.ingestion.ingest.side_effect = YandexClientException(503)
    env.tasks.schedule_retry.side_effect = RuntimeError()
    with pytest.raises(RuntimeError):
        await run(env)
    env.tasks.list_dispatchable_accounts.assert_not_awaited()


async def test_error_logs_include_counts_from_already_committed_pages(env, caplog):
    async def ingest(**kwargs):
        env.task.pages_fetched = 1
        env.task.rows_fetched = 10
        env.task.rows_inserted = 8
        env.task.rows_skipped = 2
        raise YandexClientException(503)

    env.ingestion.ingest.side_effect = ingest
    with caplog.at_level(logging.INFO):
        await run(env)
    for marker in ["status=queued", "pages=1", "fetched=10", "inserted=8", "skipped=2", "deleted=0"]:
        assert marker in caplog.text
