import logging
import math
from dataclasses import dataclass
from datetime import date
from typing import Any
from uuid import UUID

from pydantic import ValidationError

from core.entities.metrica_parse import ParseErrorCode, ParseKind, ParseTask
from core.entities.metrica_stats import StatRow
from core.exceptions.peer import PeerServiceException
from core.protocols.clients.seo_service import SeoServiceClientProtocol
from core.protocols.clients.yandex_metrica_stat import YandexMetricaStatClientProtocol
from core.protocols.repositories.metrica_parse import LeaseLostError, ParseTaskRepositoryProtocol
from core.protocols.repositories.metrica_stats import StatsRepositoryProtocol
from core.schemas.clients.yandex_metrica_stat import StatPage
from core.services.parse_enqueue_service import ParseTransactionProtocol
from core.services.parse_job_factory import ParseJob
from core.services.parse_retry_policy import ParseExecutionError

logger = logging.getLogger(__name__)
METRICS = ("visits", "users", "pageviews", "bounces", "visit_duration_seconds")


@dataclass
class IngestionCounts:
    pages: int = 0
    fetched: int = 0
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    sampled: bool = False


@dataclass(frozen=True)
class ParsedRow:
    stat_date: date
    source: str
    values: tuple[int, int, int, int, int]


def parse_row(raw: Any, task: ParseTask) -> ParsedRow | None:
    """Malformed rows are skipped without retaining or rendering their contents."""
    try:
        dimensions, metrics = raw["dimensions"], raw["metrics"]
        if not isinstance(dimensions, list) or len(dimensions) != 2:
            return None
        source = dimensions[1]["name"]
        if not isinstance(source, str) or not source.strip():
            return None
        stat_date = date.fromisoformat(dimensions[0]["name"])
        if not task.date_from <= stat_date <= task.date_to:
            return None
        if not isinstance(metrics, list) or len(metrics) != 6:
            return None
        if any(type(value) not in (int, float) or not math.isfinite(value) or value < 0 for value in metrics):
            return None
        if any(int(value) != value for value in metrics[:3]) or metrics[3] > 100:
            return None
        visits, users, pageviews = map(int, metrics[:3])
        bounces = round(metrics[3] * visits / 100)
        duration = round(metrics[5] * visits)
        return ParsedRow(stat_date, source, (visits, users, pageviews, bounces, duration))
    except TypeError, ValueError, KeyError, IndexError, OverflowError:
        return None


class StatIngestionService:
    """All repositories and transaction MUST share one session; begin() commits/rolls back.

    No implicit outer transaction may surround ingest(): each API page commits alone.
    """

    def __init__(
        self,
        *,
        stat_client: YandexMetricaStatClientProtocol,
        seo_client: SeoServiceClientProtocol,
        task_repository: ParseTaskRepositoryProtocol,
        pages_repository: StatsRepositoryProtocol,
        search_queries_repository: StatsRepositoryProtocol,
        transaction: ParseTransactionProtocol,
        page_limit: int = 100000,
        resolve_batch_size: int = 1000,
        lease_seconds: int = 900,
    ) -> None:
        if not 1 <= page_limit <= 100000 or not 1 <= resolve_batch_size <= 1000 or lease_seconds < 1:
            raise ValueError("Invalid ingestion limits")
        self._client = stat_client
        self._seo = seo_client
        self._tasks = task_repository
        self._stats = {ParseKind.PAGES: pages_repository, ParseKind.SEARCH_QUERIES: search_queries_repository}
        self._transaction = transaction
        self._limit = page_limit
        self._batch_size = resolve_batch_size
        self._lease_seconds = lease_seconds

    async def _resolve(self, task: ParseTask, values: list[str]) -> dict[str, UUID]:
        resolve = self._seo.resolve_pages if task.kind == ParseKind.PAGES else self._seo.resolve_search_queries
        try:
            result = await resolve(equestrian_id=task.equestrian_id, items=values)
        except PeerServiceException as error:
            if error.status != 400:
                raise
            if len(values) == 1:
                return {}
            middle = len(values) // 2
            return {**await self._resolve(task, values[:middle]), **await self._resolve(task, values[middle:])}
        if len(result) != len(values):
            raise ParseExecutionError(ParseErrorCode.SEO_REJECTED)
        return {value: item.id for value, item in zip(values, result, strict=True)}

    async def _prepare(self, task: ParseTask, page: StatPage) -> tuple[list[StatRow], int]:
        parsed = [row for raw in page.data if (row := parse_row(raw, task)) is not None]
        skipped = len(page.data) - len(parsed)
        unique = list(dict.fromkeys(row.source for row in parsed))
        resolved: dict[str, UUID] = {}
        for start in range(0, len(unique), self._batch_size):
            resolved.update(await self._resolve(task, unique[start : start + self._batch_size]))
        aggregated: dict[tuple[date, UUID], StatRow] = {}
        for row in parsed:
            entity_id = resolved.get(row.source)
            if entity_id is None:
                skipped += 1
                continue
            key = row.stat_date, entity_id
            if key in aggregated:
                existing = aggregated[key]
                for name, value in zip(METRICS, row.values, strict=True):
                    setattr(existing, name, getattr(existing, name) + value)
            else:
                aggregated[key] = StatRow(
                    equestrian_id=task.equestrian_id,
                    stat_date=row.stat_date,
                    entity_id=entity_id,
                    parse_task_id=task.id,
                    visits=row.values[0],
                    users=row.values[1],
                    pageviews=row.values[2],
                    bounces=row.values[3],
                    visit_duration_seconds=row.values[4],
                )
        return list(aggregated.values()), skipped

    async def ingest(self, *, job: ParseJob, locked_by: str) -> IngestionCounts:
        task = job.task
        offset = task.next_offset
        counts = IngestionCounts()
        while True:
            raw_page = await self._client.get_stat_page(
                counter_id=task.counter_id,
                access_token=job.access_token,
                kind=task.kind,
                date_from=task.date_from,
                date_to=task.date_to,
                limit=self._limit,
                offset=offset,
            )
            try:
                page = StatPage.model_validate(raw_page)
            except ValidationError:
                raise ParseExecutionError(ParseErrorCode.PROVIDER_RESPONSE_INVALID) from None
            rows, skipped = await self._prepare(task, page)
            sampled = page.sampled or page.total_rows_rounded
            next_offset = offset + len(page.data)
            try:
                async with self._transaction.begin():
                    saved = await self._stats[task.kind].upsert(rows)
                    # save_checkpoint fences ownership and renews lease in the SAME transaction.
                    await self._tasks.save_checkpoint(
                        task.id,
                        locked_by=locked_by,
                        next_offset=next_offset,
                        lease_seconds=self._lease_seconds,
                        rows_fetched=len(page.data),
                        rows_inserted=saved.inserted,
                        rows_updated=saved.updated,
                        rows_skipped=skipped,
                        sampled=sampled,
                    )
            except LeaseLostError:
                raise
            except Exception:
                raise ParseExecutionError(ParseErrorCode.STORAGE_ERROR) from None
            task.next_offset = next_offset
            task.pages_fetched += 1
            task.rows_fetched += len(page.data)
            task.rows_inserted += saved.inserted
            task.rows_updated += saved.updated
            task.rows_skipped += skipped
            task.sampled |= sampled
            counts.pages += 1
            counts.fetched += len(page.data)
            counts.inserted += saved.inserted
            counts.updated += saved.updated
            counts.skipped += skipped
            counts.sampled |= sampled
            if sampled:
                logger.warning("Parse sampled: task_id=%s", task.id)
            if not page.data or next_offset > page.total_rows:
                return counts
            offset = next_offset
