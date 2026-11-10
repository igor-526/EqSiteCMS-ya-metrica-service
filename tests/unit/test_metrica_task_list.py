"""UT-M-01: CASE truth table offline; not a substitute for real PG acceptance."""

from dataclasses import fields
from datetime import UTC, datetime
from itertools import product
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, literal, select
from sqlalchemy.dialects import postgresql

from core.entities.metrica_task_list import (
    MetricaTaskListItem,
    MetricaTaskListPage,
    MetricaTaskStatus,
    MetricaTaskType,
)
from repositories.metrica_task_list_repository import (
    MetricaTaskListRepository,
    _maintenance_status,
    _parse_status,
    _statement,
)


def test_safe_contract():
    assert {f.name for f in fields(MetricaTaskListItem)} == {"id", "type", "status", "is_active", "created_at"}
    assert {f.name for f in fields(MetricaTaskListPage)} == {"items", "total"}
    assert set(MetricaTaskType) == {"parsing", "merge", "delete"}
    assert set(MetricaTaskStatus) == {"queued", "running", "succeeded", "failed", "cancelled"}
    row = MetricaTaskListItem(
        "parsing:job", MetricaTaskType.PARSING, MetricaTaskStatus.QUEUED, True, datetime.now(UTC)
    )
    with pytest.raises(AttributeError):
        row.status = MetricaTaskStatus.FAILED


def test_parse_case_all_combinations():
    # Evaluate the actual production CASE, including failed+queued retry and zero children.
    engine = create_engine("sqlite://")
    try:
        with engine.connect() as connection:
            for values in product((0, 1, 2), repeat=5):
                running, queued, failed, cancelled, succeeded = values
                counts = SimpleNamespace(
                    running=literal(running),
                    queued=literal(queued),
                    failed=literal(failed),
                    cancelled=literal(cancelled),
                    total=literal(sum(values)),
                )
                expected = next(
                    (
                        name
                        for n, name in zip(
                            values, ("running", "queued", "failed", "cancelled", "succeeded"), strict=True
                        )
                        if n
                    ),
                    "failed",
                )
                assert connection.scalar(select(_parse_status(counts))) == expected
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("native", "expected"),
    [
        ("collecting", "queued"),
        ("waiting", "queued"),
        ("pending", "queued"),
        ("running", "running"),
        ("awaiting_seo", "running"),
        ("draining", "running"),
        ("deleting", "running"),
        ("completed", "succeeded"),
        ("superseded_by_delete", "cancelled"),
        ("failed_blocked", "failed"),
    ],
)
def test_native_maintenance_case(native, expected):
    engine = create_engine("sqlite://")
    try:
        with engine.connect() as connection:
            assert connection.scalar(select(_maintenance_status(literal(native)))) == expected
    finally:
        engine.dispose()


def test_single_bound_read_statement():
    tenant = uuid4()
    compiled = _statement(tenant, 20, 999).compile(dialect=postgresql.dialect())
    sql = str(compiled).upper()
    assert sql.count("UNION ALL") == 2
    assert "NOT (EXISTS" in sql
    assert "LEFT OUTER JOIN PAGE ON TRUE" in sql
    assert "ORDER BY PAGE.IS_ACTIVE DESC, PAGE.CREATED_AT DESC, PAGE.ID DESC" in sql
    for forbidden in ("FOR UPDATE", "INSERT INTO", "DELETE FROM", "UPDATE ", "COMMIT"):
        assert forbidden not in sql
    assert tenant in compiled.params.values()
    assert str(tenant) not in sql


@pytest.mark.parametrize(("limit", "offset"), [(0, 0), (101, 0), (True, 0), (1, -1), (1, False), (1.5, 0)])
async def test_invalid_page_never_touches_session(limit, offset):
    session = MagicMock()
    with pytest.raises(ValueError, match="pagination"):
        await MetricaTaskListRepository(session).list_tasks(equestrian_id=uuid4(), limit=limit, offset=offset)
    assert not session.mock_calls


@pytest.mark.parametrize("empty", [False, True])
async def test_page_decoding_and_out_of_range_total(empty):
    from unittest.mock import AsyncMock

    row = {
        "id": None if empty else "merge:operation",
        "type": None if empty else "merge",
        "status": None if empty else "running",
        "is_active": None if empty else True,
        "created_at": None if empty else datetime.now(UTC),
        "total": 7,
    }
    session = MagicMock()
    result = MagicMock()
    result.mappings.return_value.all.return_value = [row]
    session.execute = AsyncMock(return_value=result)
    page = await MetricaTaskListRepository(session).list_tasks(
        equestrian_id=uuid4(), limit=20, offset=999 if empty else 0
    )
    assert page.total == 7
    assert len(page.items) == (0 if empty else 1)
    if not empty:
        assert page.items[0].type is MetricaTaskType.MERGE
        assert page.items[0].status is MetricaTaskStatus.RUNNING
        assert page.items[0].is_active is True
    session.execute.assert_awaited_once()
    session.commit.assert_not_called()
    session.flush.assert_not_called()
    session.rollback.assert_not_called()
