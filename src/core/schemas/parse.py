from datetime import date
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from core.entities.metrica_parse import DateMode, ParseKind, TaskStatus


class ParseEnqueueInDto(BaseModel):
    model_config = ConfigDict(extra="forbid")

    equestrian_id: UUID
    date_from: date | None = None
    date_to: date | None = None


class DateRange(BaseModel):
    date_from: date
    date_to: date


class ResolvedDateRanges(BaseModel):
    date_mode: DateMode
    clamped: bool = False
    ranges: dict[ParseKind, DateRange]


class ParseWindow(DateRange):
    kind: ParseKind
    sequence: int


class ParseTaskOutDto(ParseWindow):
    id: UUID
    status: TaskStatus = TaskStatus.QUEUED


class ParseEnqueueOutDto(ResolvedDateRanges):
    run_id: UUID
    equestrian_id: UUID
    tasks: list[ParseTaskOutDto]
