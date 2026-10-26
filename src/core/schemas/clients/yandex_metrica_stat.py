from typing import Any

from pydantic import BaseModel, Field, StrictBool, StrictInt


class StatPage(BaseModel):
    # Row validation belongs to ingestion: even non-object rows must survive here.
    data: list[Any] = Field(repr=False)
    total_rows: StrictInt = Field(ge=0)
    sampled: StrictBool = False
    total_rows_rounded: StrictBool = False


def validate_stat_response(response: Any) -> StatPage:
    return StatPage.model_validate(response)
