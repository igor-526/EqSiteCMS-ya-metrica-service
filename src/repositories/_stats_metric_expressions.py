"""SQL metric whitelist for trusted model tables, never request identifiers."""

from sqlalchemy import Numeric, Table, case, cast, func
from sqlalchemy.sql.elements import ColumnElement

from core.entities.metrica_stats_query import Metric

ADDITIVE_METRICS = (Metric.VISITS, Metric.USERS, Metric.PAGEVIEWS, Metric.BOUNCES, Metric.VISIT_DURATION_SECONDS)


def metric_expressions(table: Table) -> dict[Metric, ColumnElement]:
    stats = table.c
    visits = func.sum(stats.visits)
    expressions: dict[Metric, ColumnElement] = {metric: func.sum(stats[metric.value]) for metric in ADDITIVE_METRICS}
    for metric, numerator in (
        (Metric.BOUNCE_RATE, func.sum(stats.bounces) * 100),
        (Metric.PAGE_DEPTH, func.sum(stats.pageviews)),
        (Metric.AVG_VISIT_DURATION_SECONDS, func.sum(stats.visit_duration_seconds)),
    ):
        expressions[metric] = case((visits > 0, func.round(cast(numerator, Numeric) / visits, 8)), else_=0)
    return expressions
