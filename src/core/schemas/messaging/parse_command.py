from datetime import date
from typing import Annotated, Literal
from uuid import NAMESPACE_DNS, UUID, uuid5

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

NAMESPACE_PARSE_COMMANDS = uuid5(NAMESPACE_DNS, "parse-commands.eqcms")
PARSE_RUN_SUBJECT = "commands.parse.ya_metrica.run"


class AutoWindow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: Literal["auto"]


class ExplicitWindow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: Literal["explicit"]
    date_from: date
    date_to: date


class MetricaParams(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ParseCommand(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1]
    occurred_at: AwareDatetime
    job_id: UUID
    provider: Literal["ya_metrica"]
    equestrian_id: UUID
    trigger: Literal["schedule", "manual"]
    window: Annotated[AutoWindow | ExplicitWindow, Field(discriminator="mode")]
    params: MetricaParams
