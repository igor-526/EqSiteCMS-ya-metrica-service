from datetime import UTC, date, datetime

import pytest

from core.entities.metrica_parse import DateMode, ParseKind
from core.exceptions import ParseRangeError
from core.services.parse_date_resolver import DateRangeResolver

TODAY = date(2026, 10, 26)
LOWER = date(2024, 10, 26)


def resolver(now=datetime(2026, 10, 26, 12, tzinfo=UTC), offset=3):
    return DateRangeResolver(utc_offset_hours=offset, clock=lambda: now)


@pytest.mark.parametrize(
    "start,end,expected_start,expected_end,clamped",
    [
        (date(2026, 10, 20), date(2026, 10, 31), date(2026, 10, 20), TODAY, True),
        (date(2023, 1, 1), date(2024, 11, 3), LOWER, date(2024, 11, 3), True),
        (LOWER, TODAY, LOWER, TODAY, False),
        (TODAY, TODAY, TODAY, TODAY, False),
        (date(2020, 1, 1), date(2030, 1, 1), LOWER, TODAY, True),
    ],
)
def test_explicit(start, end, expected_start, expected_end, clamped):
    result = resolver().resolve(date_from=start, date_to=end)
    assert result.date_mode == DateMode.EXPLICIT
    assert result.clamped is clamped
    assert set(result.ranges) == set(ParseKind)
    for span in result.ranges.values():
        assert (span.date_from, span.date_to) == (expected_start, expected_end)


@pytest.mark.parametrize(
    "start,end",
    [
        (TODAY, None),
        (None, TODAY),
        (TODAY, LOWER),
        (date(2026, 10, 27), date(2026, 10, 31)),
        (date(2020, 1, 1), date(2023, 12, 31)),
    ],
)
def test_invalid_range(start, end):
    with pytest.raises(ParseRangeError, match="Некорректный диапазон дат"):
        resolver().resolve(date_from=start, date_to=end)


def test_auto_independent_inclusive():
    result = resolver().resolve(
        last_dates={
            ParseKind.PAGES: date(2026, 10, 25),
            ParseKind.SEARCH_QUERIES: date(2026, 10, 20),
        }
    )
    assert result.date_mode == DateMode.AUTO
    assert not result.clamped
    assert result.ranges[ParseKind.PAGES].date_from == date(2026, 10, 25)
    assert result.ranges[ParseKind.SEARCH_QUERIES].date_from == date(2026, 10, 20)
    assert all(span.date_to == TODAY for span in result.ranges.values())


@pytest.mark.parametrize("last", [None, date(2023, 1, 1), LOWER, TODAY])
def test_auto_lower_bound(last):
    result = resolver().resolve(last_dates={ParseKind.PAGES: last})
    assert result.ranges[ParseKind.PAGES].date_from == (TODAY if last == TODAY else LOWER)
    assert result.ranges[ParseKind.SEARCH_QUERIES].date_from == LOWER


@pytest.mark.parametrize("offset,expected", [(3, date(2026, 10, 27)), (-12, TODAY), (14, date(2026, 10, 27))])
def test_utc_offset(offset, expected):
    result = resolver(datetime(2026, 10, 26, 22, 30, tzinfo=UTC), offset).resolve()
    assert result.ranges[ParseKind.PAGES].date_to == expected


def test_negative_offset_previous_day():
    result = resolver(datetime(2026, 10, 26, 1, tzinfo=UTC), -12).resolve()
    assert result.ranges[ParseKind.PAGES].date_to == date(2026, 10, 25)


def test_leap_day():
    result = resolver(datetime(2024, 2, 29, 12, tzinfo=UTC)).resolve()
    assert result.ranges[ParseKind.PAGES].date_from == date(2022, 2, 28)


@pytest.mark.parametrize("offset", [-13, 15, 3.5, True])
def test_invalid_offset(offset):
    with pytest.raises(ValueError):
        DateRangeResolver(utc_offset_hours=offset)


def test_future_auto_date_rejected():
    with pytest.raises(ParseRangeError):
        resolver().resolve(last_dates={ParseKind.PAGES: date(2026, 10, 27)})
