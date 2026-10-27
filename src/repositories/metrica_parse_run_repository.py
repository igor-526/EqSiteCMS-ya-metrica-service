from collections.abc import Sequence
from uuid import UUID

from sqlalchemy import exists, insert, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.entities import ParseRun, ParseTask, TaskStatus
from core.exceptions.parse import ParseJobAlreadyExistsError
from models import metrica_parse_runs, metrica_parse_tasks


class ParseRunRepository:
    """Advisory lock lasts until caller commit, including auto-date resolution.

    Call has_active_tasks before reading auto dates; create_run_with_tasks repeats
    the check under the same reentrant lock and returns False for an active run.
    """

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def lock_equestrian(self, equestrian_id: UUID) -> None:
        await self.session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
            {"key": f"metrica-parse:{equestrian_id}"},
        )

    async def has_active_tasks(self, equestrian_id: UUID) -> bool:
        await self.lock_equestrian(equestrian_id)
        return bool(
            await self.session.scalar(
                select(
                    exists().where(
                        metrica_parse_tasks.c.equestrian_id == equestrian_id,
                        metrica_parse_tasks.c.status.in_([TaskStatus.QUEUED, TaskStatus.RUNNING]),
                    )
                )
            )
        )

    async def exists_job(self, job_id: UUID) -> bool:
        return bool(await self.session.scalar(select(exists().where(metrica_parse_runs.c.job_id == job_id))))

    async def create_run_with_tasks(self, run: ParseRun, tasks: Sequence[ParseTask]) -> bool:
        if not tasks or any(
            task.run_id != run.id or task.equestrian_id != run.equestrian_id or task.status != TaskStatus.QUEUED
            for task in tasks
        ):
            raise ValueError("Run requires matching queued tasks")
        await self.lock_equestrian(run.equestrian_id)
        if run.job_id is not None and await self.exists_job(run.job_id):
            raise ParseJobAlreadyExistsError()
        if await self.has_active_tasks(run.equestrian_id):
            return False
        # The unique index also arbitrates concurrent commands for different equestrians.
        # DO NOTHING keeps the caller transaction usable and never swallows other constraints.
        inserted = await self.session.scalar(
            pg_insert(metrica_parse_runs)
            .values(run.model_dump())
            .on_conflict_do_nothing(index_elements=[metrica_parse_runs.c.job_id])
            .returning(metrica_parse_runs.c.id)
        )
        if inserted is None:
            raise ParseJobAlreadyExistsError()
        values = []
        for task in tasks:
            data = task.model_dump(exclude_none=True)
            values.append(data)
        # Individual inserts allow server defaults for optional task timestamps.
        for data in values:
            await self.session.execute(insert(metrica_parse_tasks).values(data))
        return True
