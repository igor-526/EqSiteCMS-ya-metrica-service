from collections.abc import Mapping
from datetime import timedelta
from itertools import zip_longest

from core.entities.metrica_parse import ParseKind
from core.exceptions.parse import ParseRangeError
from core.schemas.parse import DateRange, ParseWindow


class TaskDecomposer:
    def decompose(self, ranges: Mapping[ParseKind, DateRange]) -> list[ParseWindow]:
        by_kind: list[list[ParseWindow]] = []
        for kind in ParseKind:
            span = ranges[kind]
            if span.date_from > span.date_to:
                raise ParseRangeError()
            windows: list[ParseWindow] = []
            start = span.date_from
            while start <= span.date_to:
                end = min(start + timedelta(days=4), span.date_to)
                windows.append(ParseWindow(kind=kind, date_from=start, date_to=end, sequence=0))
                start = end + timedelta(days=1)
            by_kind.append(windows)
        result: list[ParseWindow] = []
        for pair in zip_longest(*by_kind):
            for window in pair:
                if window is not None:
                    result.append(window.model_copy(update={"sequence": len(result) + 1}))
        return result
