"""Shared base-metric semantics for ingestion and historical page moves.

Users is additive (not cross-page distinct). Ratios are never additive inputs:
PostgreSQL generated columns derive them from the five stored sums.
"""

from collections.abc import Sequence

BASE_METRICS = ("visits", "users", "pageviews", "bounces", "visit_duration_seconds")
MetricValues = tuple[int, int, int, int, int]


def add_metrics(left: MetricValues, right: MetricValues) -> MetricValues:
    """Pure, lossless addition; storage overflow must fail, never saturate."""
    return (
        left[0] + right[0],
        left[1] + right[1],
        left[2] + right[2],
        left[3] + right[3],
        left[4] + right[4],
    )


def provider_metrics(metrics: Sequence[int | float]) -> MetricValues:
    """Convert validated provider rates/averages to additive integer totals.

    Keep provider rounding identical to ingestion; do not round merged totals.
    The provider's page-depth field is intentionally ignored.
    """
    visits, users, pageviews = map(int, metrics[:3])
    return visits, users, pageviews, round(metrics[3] * visits / 100), round(metrics[5] * visits)
