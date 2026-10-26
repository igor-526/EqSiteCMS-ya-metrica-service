from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime, timedelta

from core.entities.metrica_parse import DateMode, ParseKind
from core.exceptions.parse import ParseRangeError
from core.schemas.parse import DateRange, ResolvedDateRanges


class DateRangeResolver:
    def __init__(self, *, utc_offset_hours: int = 3, clock: Callable[[], datetime] | None = None) -> None:
        if type(utc_offset_hours) is not int or not -12 <= utc_offset_hours <= 14:
            raise ValueError("Invalid UTC offset")
        self._utc_offset_hours = utc_offset_hours
        self._clock = clock or (lambda: datetime.now(UTC))

    def resolve(
        self,
        *,
        date_from: date | None = None,
        date_to: date | None = None,
        last_dates: Mapping[ParseKind, date | None] | None = None,
    ) -> ResolvedDateRanges:
        today = (self._clock().astimezone(UTC) + timedelta(hours=self._utc_offset_hours)).date()
        try:
            lower = today.replace(year=today.year - 2)
        except ValueError:  # February 29, two years earlier is not a leap year.
            lower = today.replace(year=today.year - 2, day=28)
        if (date_from is None) != (date_to is None):
            raise ParseRangeError()
        if date_from is not None and date_to is not None:
            start, end = max(date_from, lower), min(date_to, today)
            if start > end:
                raise ParseRangeError()
            return ResolvedDateRanges(
                date_mode=DateMode.EXPLICIT,
                clamped=(start, end) != (date_from, date_to),
                ranges={kind: DateRange(date_from=start, date_to=end) for kind in ParseKind},
            )
        ranges: dict[ParseKind, DateRange] = {}
        for kind in ParseKind:
            last = (last_dates or {}).get(kind)
            start = max(last, lower) if last is not None else lower
            if start > today:
                raise ParseRangeError()
            ranges[kind] = DateRange(date_from=start, date_to=today)
        return ResolvedDateRanges(date_mode=DateMode.AUTO, ranges=ranges)
