from contextlib import asynccontextmanager
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from core.entities.metrica_parse import ParseKind, ParseTask
from core.exceptions.peer import PeerServiceException
from core.protocols.repositories.metrica_parse import LeaseLostError
from core.protocols.repositories.metrica_stats import UpsertCounts
from core.schemas.clients.seo_service import ResolvedItem
from core.schemas.clients.yandex_metrica_stat import StatPage
from core.services.parse_job_factory import ParseJob
from core.services.parse_retry_policy import ParseExecutionError, RetryPolicy
from core.services.stat_ingestion_service import StatIngestionService, parse_row


def raw(source="URL_SECRET", metrics=None, day="2026-10-21"):
    return {
        "dimensions": [{"name": day}, {"name": source}],
        "metrics": metrics if metrics is not None else [30.0, 30.0, 59.0, 16.66666667, 1.96666667, 114.86666667],
    }


@pytest.fixture
def env():
    events, writes = [], []
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
    )
    entity = uuid4()

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

    async def upsert(rows):
        events.append("upsert")
        writes.append(rows)
        return UpsertCounts(len(rows), 0)

    async def checkpoint(*args, **kwargs):
        events.append("checkpoint")

    async def resolve(**kwargs):
        return [ResolvedItem(source=s, normalized="hidden", id=entity) for s in kwargs["items"]]

    pages = SimpleNamespace(upsert=AsyncMock(side_effect=upsert))
    search = SimpleNamespace(upsert=AsyncMock(side_effect=upsert))
    tasks = SimpleNamespace(save_checkpoint=AsyncMock(side_effect=checkpoint))
    client = SimpleNamespace(get_stat_page=AsyncMock(return_value=StatPage(data=[raw()], total_rows=1)))
    seo = SimpleNamespace(
        resolve_pages=AsyncMock(side_effect=resolve), resolve_search_queries=AsyncMock(side_effect=resolve)
    )
    service = StatIngestionService(
        stat_client=client,
        seo_client=seo,
        task_repository=tasks,
        pages_repository=pages,
        search_queries_repository=search,
        transaction=SimpleNamespace(begin=begin),
        page_limit=10,
        lease_seconds=600,
    )
    job = ParseJob(task=task, access_token="TOKEN_SECRET")
    return SimpleNamespace(**locals())


async def test_additive_metrics_and_atomic_checkpoint(env):
    counts = await env.service.ingest(job=env.job, locked_by="worker")
    row = env.writes[0][0]
    assert (row.visits, row.users, row.pageviews, row.bounces, row.visit_duration_seconds) == (30, 30, 59, 5, 3446)
    assert row.parse_task_id == env.task.id
    assert row.equestrian_id == env.task.equestrian_id
    assert row.entity_id == env.entity
    assert counts.inserted == 1 and counts.pages == 1 and counts.skipped == 0
    assert env.events == ["begin", "upsert", "checkpoint", "commit"]
    env.tasks.save_checkpoint.assert_awaited_once_with(
        env.task.id,
        locked_by="worker",
        next_offset=2,
        lease_seconds=600,
        rows_fetched=1,
        rows_inserted=1,
        rows_updated=0,
        rows_skipped=0,
        sampled=False,
    )


@pytest.mark.parametrize(
    "start,total,sizes,offsets",
    [
        (1, 35, [10, 10, 10, 5], [1, 11, 21, 31]),
        (1, 0, [0], [1]),
        (1, 100, [0], [1]),
        (21, 35, [10, 5], [21, 31]),
    ],
)
async def test_pagination_resume_and_empty(env, start, total, sizes, offsets):
    env.task.next_offset = start
    env.client.get_stat_page.side_effect = [StatPage(data=[raw()] * size, total_rows=total) for size in sizes]
    counts = await env.service.ingest(job=env.job, locked_by="worker")
    assert [call.kwargs["offset"] for call in env.client.get_stat_page.call_args_list] == offsets
    assert counts.pages == len(sizes) and counts.fetched == sum(sizes)
    assert env.events.count("commit") == len(sizes)
    for call in env.client.get_stat_page.call_args_list:
        assert call.kwargs["access_token"] == "TOKEN_SECRET"
        assert call.kwargs["limit"] == 10


@pytest.mark.parametrize(
    "bad",
    [
        None,
        [],
        {},
        {"dimensions": None, "metrics": []},
        raw(None),
        raw("  "),
        raw(metrics=[1, 2, 3, 4, 5]),
        raw(metrics=[1.5, 1, 1, 0, 1, 1]),
        raw(metrics=[1, 0.5, 1, 0, 1, 1]),
        raw(metrics=[1, 1, 1.1, 0, 1, 1]),
        raw(metrics=[True, 1, 1, 0, 1, 1]),
        raw(metrics=[1, 1, 1, float("nan"), 1, 1]),
        raw(metrics=[1, 1, 1, 101, 1, 1]),
        raw(metrics=[-1, 1, 1, 0, 1, 1]),
        raw(metrics=[1, 1, 1, 0, 1, float("inf")]),
        raw(day="invalid"),
        raw(day="2026-10-26"),
    ],
)
async def test_invalid_rows_skip_without_resolve(env, bad):
    env.client.get_stat_page.return_value = StatPage(data=[bad], total_rows=1)
    counts = await env.service.ingest(job=env.job, locked_by="worker")
    assert counts.skipped == 1 and counts.inserted == 0
    env.seo.resolve_pages.assert_not_awaited()


@pytest.mark.parametrize(
    "page",
    [None, {}, {"data": {}, "total_rows": 1}, {"data": [], "total_rows": True}, {"data": [], "total_rows": 1.5}],
)
async def test_bad_envelope(env, page):
    env.client.get_stat_page.return_value = page
    with pytest.raises(ParseExecutionError, match="provider_response_invalid"):
        await env.service.ingest(job=env.job, locked_by="worker")
    assert env.events == []


async def test_batching_unique_values_and_merging_across_batches(env):
    env.service._limit = 100000
    env.client.get_stat_page.return_value = StatPage(
        data=[raw(str(i)) for i in range(2500)] + [raw("0")], total_rows=2501
    )
    counts = await env.service.ingest(job=env.job, locked_by="worker")
    assert [len(c.kwargs["items"]) for c in env.seo.resolve_pages.call_args_list] == [1000, 1000, 500]
    assert all(c.kwargs["equestrian_id"] == env.task.equestrian_id for c in env.seo.resolve_pages.call_args_list)
    assert counts.inserted == 1
    assert env.writes[0][0].visits == 2501 * 30
    assert env.writes[0][0].bounces == 2501 * 5


async def test_weighted_aggregation_per_date(env):
    env.client.get_stat_page.return_value = StatPage(
        data=[
            raw("a", [3, 2, 6, 0, 2, 100]),
            raw("b", [1, 1, 1, 100, 1, 20]),
            raw("c", [99, 1, 99, 0, 1, 100], day="2026-10-22"),
            raw("d", [1, 1, 1, 100, 1, 20], day="2026-10-22"),
        ],
        total_rows=4,
    )
    await env.service.ingest(job=env.job, locked_by="worker")
    first, second = env.writes[0]
    assert (first.visits, first.pageviews, first.bounces, first.visit_duration_seconds) == (4, 7, 1, 320)
    assert second.visits == 100 and second.bounces == 1


async def test_bisection_and_skip_counts_each_source_occurrence(env):
    calls = []

    async def resolve(**kwargs):
        items = kwargs["items"]
        calls.append(items)
        if "bad" in items:
            raise PeerServiceException("seo", 400, "http")
        return [ResolvedItem(source=s, normalized=s, id=env.entity) for s in items]

    env.seo.resolve_pages.side_effect = resolve
    env.client.get_stat_page.return_value = StatPage(data=[raw(s) for s in ["a", "bad", "c", "bad"]], total_rows=4)
    counts = await env.service.ingest(job=env.job, locked_by="worker")
    assert counts.skipped == 2 and env.writes[0][0].visits == 60
    assert calls == [["a", "bad", "c"], ["a"], ["bad", "c"], ["bad"], ["c"]]


@pytest.mark.parametrize(
    "status,category,code",
    [(503, "http", "seo_unavailable"), (None, "timeout", "seo_unavailable"), (401, "http", "seo_rejected")],
)
async def test_seo_failure_before_transaction(env, status, category, code):
    env.seo.resolve_pages.side_effect = PeerServiceException("seo", status, category)
    with pytest.raises(PeerServiceException) as exc:
        await env.service.ingest(job=env.job, locked_by="worker")
    assert RetryPolicy().classify(exc.value).error_code == code
    assert env.events == []


async def test_search_resolution_and_sampling_warning(env, caplog):
    env.task.kind = ParseKind.SEARCH_QUERIES
    env.client.get_stat_page.return_value = StatPage(data=[raw()], total_rows=1, total_rows_rounded=True)
    counts = await env.service.ingest(job=env.job, locked_by="worker")
    env.seo.resolve_search_queries.assert_awaited_once()
    env.pages.upsert.assert_not_awaited()
    assert counts.sampled and "sampled" in caplog.text
    assert "URL_SECRET" not in caplog.text
    assert env.tasks.save_checkpoint.call_args.kwargs["sampled"]


@pytest.mark.parametrize("where", ["upsert", "checkpoint", "lease", "commit"])
async def test_storage_and_fencing_roll_back_page(env, where):
    if where == "upsert":
        env.pages.upsert.side_effect = RuntimeError("database secret")
    elif where in {"checkpoint", "lease"}:
        env.tasks.save_checkpoint.side_effect = LeaseLostError() if where == "lease" else RuntimeError()
    else:

        @asynccontextmanager
        async def fail_commit():
            try:
                yield
                raise RuntimeError("commit secret")
            except Exception:
                env.events.append("rollback")
                raise

        env.service._transaction = SimpleNamespace(begin=fail_commit)
    expected = LeaseLostError if where == "lease" else ParseExecutionError
    with pytest.raises(expected):
        await env.service.ingest(job=env.job, locked_by="worker")
    assert env.events[-1] == "rollback"
    assert env.task.next_offset == 1 and env.task.pages_fetched == 0


async def test_second_page_failure_keeps_committed_checkpoint(env):
    env.client.get_stat_page.side_effect = [
        StatPage(data=[raw()] * 10, total_rows=20),
        PeerServiceException("seo", 503, "http"),
    ]
    with pytest.raises(PeerServiceException):
        await env.service.ingest(job=env.job, locked_by="worker")
    assert env.events == ["begin", "upsert", "checkpoint", "commit"]
    assert env.tasks.save_checkpoint.call_args.kwargs["next_offset"] == 11
    assert env.task.next_offset == 11 and env.task.rows_fetched == 10


def test_zero_metrics(env):
    assert parse_row(raw(metrics=[0, 0, 0, 0, 0, 0]), env.task).values == (0, 0, 0, 0, 0)


async def test_cancelled_page_rolls_back_without_advancing(env):
    from asyncio import CancelledError

    env.tasks.save_checkpoint.side_effect = CancelledError()
    with pytest.raises(CancelledError):
        await env.service.ingest(job=env.job, locked_by="worker")
    assert env.events[-1] == "rollback"
    assert env.task.next_offset == 1 and env.task.rows_fetched == 0


async def test_resolve_bad_length_rejected_before_page_transaction(env):
    env.seo.resolve_pages.side_effect = None
    env.seo.resolve_pages.return_value = []
    with pytest.raises(ParseExecutionError, match="seo_rejected"):
        await env.service.ingest(job=env.job, locked_by="worker")
    assert env.events == []
