"""One SELECT snapshot, no locks, writes, commits, peers or Python aggregation."""

from uuid import UUID

from sqlalchemy import String, case, cast, exists, func, literal, select, true, union_all
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities.metrica_task_list import (
    MetricaTaskListItem,
    MetricaTaskListPage,
    MetricaTaskStatus,
    MetricaTaskType,
)
from models import (
    metrica_maintenance_operations,
    metrica_parse_runs,
    metrica_parse_tasks,
    metrica_pending_parse_intents,
)


def _parse_status(counts):
    # Active work wins over terminal failures; zero children is never success.
    return case(
        (counts.running > 0, "running"),
        (counts.queued > 0, "queued"),
        (counts.failed > 0, "failed"),
        (counts.cancelled > 0, "cancelled"),
        (counts.total > 0, "succeeded"),
        else_="failed",
    )


def _maintenance_status(status):
    return case(
        {
            "collecting": "queued",
            "waiting": "queued",
            "pending": "queued",
            "running": "running",
            "awaiting_seo": "running",
            "draining": "running",
            "deleting": "running",
            "completed": "succeeded",
            "superseded_by_delete": "cancelled",
            "failed_blocked": "failed",
        },
        value=status,
        else_="failed",
    )


def _statement(equestrian_id: UUID, limit: int, offset: int):
    tasks, runs = metrica_parse_tasks.c, metrica_parse_runs.c
    intents, operations = metrica_pending_parse_intents.c, metrica_maintenance_operations.c
    counts = (
        select(
            tasks.run_id,
            func.count().label("total"),
            *(func.count().filter(tasks.status == s).label(s) for s in ("running", "queued", "failed", "cancelled")),
        )
        .where(tasks.equestrian_id == equestrian_id)
        .group_by(tasks.run_id)
        .cte("task_counts")
    )
    run_status = _parse_status(counts.c)
    run_projection = (
        select(runs.id, runs.job_id, runs.created_at, run_status.label("status"))
        .select_from(metrica_parse_runs.outerjoin(counts, counts.c.run_id == runs.id))
        .where(runs.equestrian_id == equestrian_id)
        .cte("tenant_runs")
    )
    r = run_projection.c
    intent_projection = (
        select(
            (literal("parsing:") + cast(intents.job_id, String)).label("id"),
            literal("parsing").label("type"),
            case(
                (intents.status == "admitted", func.coalesce(r.status, "failed")),
                (intents.status == "pending", "queued"),
                (intents.status == "cancelled", "cancelled"),
                else_="failed",
            ).label("status"),
            intents.created_at,
        )
        .select_from(metrica_pending_parse_intents.outerjoin(run_projection, intents.run_id == r.id))
        .where(intents.equestrian_id == equestrian_id)
    )
    legacy_projection = select(
        case(
            (r.job_id.is_not(None), literal("parsing:") + cast(r.job_id, String)),
            else_=literal("parsing-run:") + cast(r.id, String),
        ).label("id"),
        literal("parsing").label("type"),
        r.status,
        r.created_at,
    ).where(~exists().where(intents.equestrian_id == equestrian_id, intents.run_id == r.id))
    operation_type = case((operations.kind == "cleanup", "delete"), else_="merge")
    maintenance_projection = select(
        (operation_type + literal(":") + cast(operations.id, String)).label("id"),
        operation_type.label("type"),
        _maintenance_status(operations.status).label("status"),
        operations.created_at,
    ).where(operations.equestrian_id == equestrian_id)
    logical = union_all(intent_projection, legacy_projection, maintenance_projection).cte("logical_tasks")
    history = select(logical, logical.c.status.in_(("queued", "running")).label("is_active")).cte("history")
    order = (history.c.is_active.desc(), history.c.created_at.desc(), history.c.id.desc())
    page = select(history).order_by(*order).limit(limit).offset(offset).cte("page")
    total = select(func.count().label("total")).select_from(history).cte("total")
    # Count always yields one row: LEFT JOIN preserves total even for an empty page.
    return (
        select(total.c.total, page)
        .select_from(total.outerjoin(page, true()))
        .order_by(page.c.is_active.desc(), page.c.created_at.desc(), page.c.id.desc())
    )


class MetricaTaskListRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def list_tasks(self, *, equestrian_id: UUID, limit: int, offset: int) -> MetricaTaskListPage:
        if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or offset < 0:
            raise ValueError("Invalid task list pagination")
        with self.session.no_autoflush:
            rows = (await self.session.execute(_statement(equestrian_id, limit, offset))).mappings().all()
        return MetricaTaskListPage(
            items=[
                MetricaTaskListItem(
                    id=row["id"],
                    type=MetricaTaskType(row["type"]),
                    status=MetricaTaskStatus(row["status"]),
                    is_active=row["is_active"],
                    created_at=row["created_at"],
                )
                for row in rows
                if row["id"] is not None
            ],
            total=rows[0]["total"],
        )
