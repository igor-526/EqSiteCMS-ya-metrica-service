from datetime import date
from uuid import UUID

from pydantic import Field, model_validator

from .base import Entity, TimestampMixin


class StatRow(Entity, TimestampMixin):
    """A normalized row; entity_id is the SEO page/search-query UUID, never its text.

    Rates are derived by storage; users is summed only as an upper estimate.
    """

    equestrian_id: UUID
    stat_date: date
    entity_id: UUID
    visits: int = Field(ge=0)
    users: int = Field(ge=0)
    pageviews: int = Field(ge=0)
    bounces: int = Field(ge=0)
    visit_duration_seconds: int = Field(ge=0)
    parse_task_id: UUID | None = None

    @model_validator(mode="after")
    def validate_bounces(self) -> StatRow:
        if self.bounces > self.visits:
            raise ValueError("Bounces must not exceed visits")
        return self
