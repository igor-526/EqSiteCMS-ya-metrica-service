from datetime import date, timedelta

import pytest

from core.entities.metrica_parse import ParseKind
from core.exceptions import ParseRangeError
from core.schemas.parse import DateRange
from core.services.parse_decomposer import TaskDecomposer


@pytest.mark.parametrize("pages_days,search_days", [(20, 20), (7, 7), (1, 1), (731, 731), (1, 20), (20, 1), (7, 11)])
def test_windows_cover_each_range_and_interleave(pages_days, search_days):
    ranges = {
        ParseKind.PAGES: DateRange(
            date_from=date(2026, 1, 1), date_to=date(2026, 1, 1) + timedelta(days=pages_days - 1)
        ),
        ParseKind.SEARCH_QUERIES: DateRange(
            date_from=date(2025, 1, 1), date_to=date(2025, 1, 1) + timedelta(days=search_days - 1)
        ),
    }
    windows = TaskDecomposer().decompose(ranges)
    assert [w.sequence for w in windows] == list(range(1, len(windows) + 1))
    for kind, span in ranges.items():
        actual = []
        for w in windows:
            if w.kind == kind:
                assert 0 <= (w.date_to - w.date_from).days <= 4
                actual.extend(w.date_from + timedelta(days=i) for i in range((w.date_to - w.date_from).days + 1))
        assert actual == [span.date_from + timedelta(days=i) for i in range((span.date_to - span.date_from).days + 1)]
    paired_count = min((pages_days + 4) // 5, (search_days + 4) // 5)
    assert [w.kind for w in windows[: paired_count * 2]] == [ParseKind.PAGES, ParseKind.SEARCH_QUERIES] * paired_count
    longer = ParseKind.PAGES if pages_days > search_days else ParseKind.SEARCH_QUERIES
    assert all(w.kind == longer for w in windows[paired_count * 2 :])


def test_invalid_range():
    span = DateRange(date_from=date(2026, 1, 2), date_to=date(2026, 1, 1))
    with pytest.raises(ParseRangeError):
        TaskDecomposer().decompose({kind: span for kind in ParseKind})
