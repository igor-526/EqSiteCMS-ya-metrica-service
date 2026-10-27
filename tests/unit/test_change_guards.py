"""Boundary regressions for metrica-stats-parsing uncovered by the final suite."""

from contextlib import asynccontextmanager
from datetime import date
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError

from core.entities import DateMode, ParseKind, ParseRun, StatRow
from core.services import ParseEnqueueService, StatIngestionService, TaskRunner
from repositories.metrica_parse_run_repository import ParseRunRepository
from repositories.metrica_parse_task_repository import ParseTaskRepository
from repositories.metrica_stats_repository import StatsRepository


def test_enqueue_rejects_nonpositive_attempt_limit():
    with pytest.raises(ValueError, match="max_attempts"):
        ParseEnqueueService(
            yandex_client=Mock(),
            transaction=Mock(),
            run_repository=Mock(),
            pages_repository=Mock(),
            search_queries_repository=Mock(),
            date_resolver=Mock(),
            decomposer=Mock(),
            dispatcher=Mock(),
            max_attempts=0,
        )


def test_runner_rejects_nonpositive_lease():
    with pytest.raises(ValueError, match="lease_seconds"):
        TaskRunner(
            task_repository=Mock(),
            pages_repository=Mock(),
            search_queries_repository=Mock(),
            transaction=Mock(),
            job_factory=Mock(),
            ingestion=Mock(),
            retry_policy=Mock(),
            lease_seconds=0,
        )


@pytest.mark.parametrize("limits", [{"page_limit": 0}, {"resolve_batch_size": 1001}, {"lease_seconds": 0}])
def test_ingestion_rejects_invalid_limits(limits):
    with pytest.raises(ValueError, match="Invalid ingestion limits"):
        StatIngestionService(
            stat_client=Mock(),
            seo_client=Mock(),
            task_repository=Mock(),
            pages_repository=Mock(),
            search_queries_repository=Mock(),
            transaction=Mock(),
            **limits,
        )


async def test_run_repository_rejects_empty_tasks_without_sql():
    session = AsyncMock()
    repo = ParseRunRepository(session)
    with pytest.raises(ValueError, match="matching queued tasks"):
        await repo.create_run_with_tasks(ParseRun(equestrian_id=uuid4(), date_mode=DateMode.AUTO), [])
    session.execute.assert_not_awaited()


def test_stats_repository_rejects_unknown_kind():
    with pytest.raises(ValueError, match="Unknown parse kind"):
        StatsRepository(Mock(), Mock())


async def test_stats_repository_rejects_row_without_task():
    session = AsyncMock()
    repo = StatsRepository(session, ParseKind.PAGES)
    row = StatRow(
        equestrian_id=uuid4(),
        entity_id=uuid4(),
        stat_date=date(2026, 10, 21),
        visits=0,
        users=0,
        pageviews=0,
        bounces=0,
        visit_duration_seconds=0,
    )
    with pytest.raises(ValueError, match="requires a parse task"):
        await repo.upsert([row])
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("lease,owner", [(0, "worker"), (1, ""), (1, "w" * 129)])
async def test_claim_rejects_invalid_lease_or_owner(lease, owner):
    session = AsyncMock()
    with pytest.raises(ValueError, match="Invalid lease"):
        await ParseTaskRepository(session).claim(uuid4(), locked_by=owner, lease_seconds=lease)
    session.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "method,kwargs",
    [
        ("renew_lease", {"lease_seconds": 0}),
        ("save_checkpoint", {"next_offset": 0, "lease_seconds": 1}),
        ("save_checkpoint", {"next_offset": 1, "lease_seconds": 0}),
        ("save_checkpoint", {"next_offset": 1, "lease_seconds": 1, "rows_fetched": -1}),
        ("mark_succeeded", {"rows_deleted": -1}),
    ],
)
async def test_task_repository_rejects_invalid_update(method, kwargs):
    session = AsyncMock()
    with pytest.raises(ValueError, match="Invalid"):
        await getattr(ParseTaskRepository(session), method)(uuid4(), locked_by="worker", **kwargs)
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("state,message", [("23503", "foreign key"), ("23505", "another_unique_index")])
async def test_claim_does_not_swallow_unrelated_integrity_failures(state, message):
    class DatabaseFailure(Exception):
        sqlstate = state

    failure = IntegrityError(None, None, DatabaseFailure(message))
    session = Mock(execute=AsyncMock(side_effect=failure))

    @asynccontextmanager
    async def nested():
        yield

    session.begin_nested = nested
    with pytest.raises(IntegrityError) as raised:
        await ParseTaskRepository(session).claim(uuid4(), locked_by="worker", lease_seconds=60)
    assert raised.value is failure


@pytest.mark.parametrize("response", [None, [], "invalid"])
def test_counter_response_validator_rejects_non_object(response):
    from core.schemas.clients.yandex_metrica import validate_counters_response

    with pytest.raises(ValueError, match="Expected a counters object"):
        validate_counters_response(response)


def test_http_parse_trigger_and_external_dto_removed():
    from importlib.util import find_spec

    import main

    assert find_spec("api.parse") is None
    assert find_spec("core.schemas.api.parse") is None
    assert "/api/metrica/stats/parse" not in main.app.openapi()["paths"]
    assert "/health" in main.app.openapi()["paths"]
