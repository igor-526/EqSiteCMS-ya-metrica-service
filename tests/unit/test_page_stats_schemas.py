"""UT-MP-01: query constraints and numeric JSON contract."""

from datetime import date, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from pydantic import ValidationError

from core.entities.metrica_stats_query import StatsBucket
from core.schemas.api.page_stats import PageStatsRankingInDto, PageStatsTableInDto, PageStatsValueOutDto

EQ, PAGE = UUID(int=1), UUID(int=2)
TABLE = dict(equestrian_id=EQ, metric="visits", bucket=["2099-10-08..2099-10-09"], page_ids=[PAGE])
RANKING = dict(equestrian_id=EQ, metric="visits", date_from="2099-10-08", date_to="2099-10-09", direction="desc")


@pytest.mark.parametrize(
    "change",
    [
        {"bucket": []},
        {"bucket": ["2026-01-01..2026-01-01"] * 32},
        {"bucket": ["2026-10-09..2026-10-08"]},
        {"bucket": ["2026-10-08..2026-10-09", "2026-10-09..2026-10-10"]},
        {"bucket": ["2026-10-10..2026-10-11", "2026-10-08..2026-10-09"]},
        {"bucket": ["2026-10-08"]},
        {"bucket": ["2026-1-01..2026-01-02"]},
        {"bucket": ["2026-02-30..2026-03-01"]},
        {"bucket": ["2026-10-08..2026-10-09..2026-10-10"]},
        {"page_ids": []},
        {"page_ids": [UUID(int=n) for n in range(101)]},
        {"page_ids": ["bad"]},
        {"metric": "sessions"},
        {"equestrian_id": "bad"},
    ],
)
def test_table_invalid(change):
    with pytest.raises(ValidationError):
        PageStatsTableInDto.model_validate(TABLE | change)


def test_table_preserves_buckets_deduplicates_normalized_ids():
    data = PageStatsTableInDto.model_validate(TABLE | {"page_ids": [PAGE, str(PAGE), PAGE.hex]})
    assert data.page_ids == [PAGE]
    assert data.buckets == [StatsBucket(date(2099, 10, 8), date(2099, 10, 9))]
    # Duplicate UUIDs do not consume the 100-distinct-page limit.
    assert PageStatsTableInDto.model_validate(TABLE | {"page_ids": [PAGE] * 101}).page_ids == [PAGE]


def test_maximum_table_sizes_and_disjoint_gaps():
    start = date(2099, 1, 1)
    buckets = [f"{start + timedelta(days=i * 2)}..{start + timedelta(days=i * 2)}" for i in range(31)]
    data = PageStatsTableInDto.model_validate(
        TABLE | {"bucket": buckets, "page_ids": [UUID(int=i) for i in range(100)]}
    )
    assert len(data.buckets) == 31
    assert len(data.page_ids) == 100


@pytest.mark.parametrize(
    "change",
    [
        {"date_from": "2099-10-10"},
        {"date_from": "2098-10-08"},
        {"date_to": "2099-1-01"},
        {"date_from": "2099-02-29"},
        {"direction": "up"},
        {"metric": "sessions"},
    ],
)
def test_ranking_invalid(change):
    with pytest.raises(ValidationError):
        PageStatsRankingInDto.model_validate(RANKING | change)


@pytest.mark.parametrize("missing", ["date_from", "date_to", "direction", "metric", "equestrian_id"])
def test_ranking_required(missing):
    with pytest.raises(ValidationError):
        PageStatsRankingInDto.model_validate({k: v for k, v in RANKING.items() if k != missing})


@pytest.mark.parametrize("days", [1, 366])
def test_ranking_inclusive_limit_and_date_objects(days):
    start = date(2099, 1, 1)
    data = PageStatsRankingInDto.model_validate(
        RANKING | {"date_from": start, "date_to": start + timedelta(days=days - 1)}
    )
    assert data.date_from == start
    assert (data.date_to - data.date_from).days + 1 == days


@pytest.mark.parametrize("value, expected", [(12, 12), (Decimal("114.86666667"), 114.86666667)])
def test_numeric_json(value, expected):
    data = PageStatsValueOutDto(page_id=PAGE, bucket=date(2026, 10, 19), value=value).model_dump(mode="json")
    assert data == {"page_id": str(PAGE), "bucket": "2026-10-19", "value": expected}
    assert type(data["value"]) is type(expected)
