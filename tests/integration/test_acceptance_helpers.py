"""INF-01 service adoption; PG-02 owned-schema isolation.

Run exact nodes via scripts/qa/infra/run.py with a fresh manifest/permit.
Even refusal cases require a valid runner context first. Synthetic partial-lifecycle
cases test the helper contract, not real rollback/topology acceptance. No public DDL.
"""

import importlib
import json
import os
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

pytestmark = pytest.mark.infrastructure


@pytest.mark.parametrize("case", ["missing-permit", "wrong-target", "public", "expired", "not-runner-owned"])
async def test_fixture_refuses_unsafe_context_before_connection(qa_infra_context, monkeypatch, case):
    infra = importlib.import_module("infra")
    contracts = importlib.import_module("infra.contracts")
    lifecycle = importlib.import_module("infra.lifecycle")
    manifest = deepcopy(qa_infra_context["manifest"])
    permit = contracts.load(os.environ["QA_INFRA_PERMIT"])
    sentinel = "synthetic-secret-never-export"
    connected = []

    def forbidden(*args, **kwargs):
        connected.append(True)
        raise AssertionError("connection must not happen")

    monkeypatch.setattr(lifecycle.Postgres, "connect", forbidden)
    if case == "missing-permit":
        monkeypatch.setenv("QA_INFRA_PERMIT", "")
    else:
        if case == "wrong-target":
            permit["target"]["port"] += 1
        elif case == "public":
            manifest["resource"]["schema"] = "public"
            permit["resource"] = manifest["resource"]
        elif case == "expired":
            permit["expiresAt"] = "2000-01-01T00:00:00+00:00"
        else:
            monkeypatch.setenv("QA_INFRA_ACTIVE", "not-owned")
        original = contracts.load
        paths = {"synthetic-manifest": manifest, "synthetic-permit": permit}
        monkeypatch.setattr(infra, "load", lambda path: paths[path] if path in paths else original(path))
        monkeypatch.setenv("QA_INFRA_MANIFEST", "synthetic-manifest")
        monkeypatch.setenv("QA_INFRA_PERMIT", "synthetic-permit")
    monkeypatch.setenv("TEST_DATABASE_URL", f"postgresql://{sentinel}@application.invalid/app")
    with pytest.raises(contracts.GuardError) as failure:
        infra.load_context("pg")
    assert str(failure.value) in {
        "missing-or-invalid-json",
        "wrong-scope",
        "unsafe-schema",
        "expired-permit",
        "not-runner-owned",
    }
    assert sentinel not in str(failure.value) and not connected


@pytest.mark.parametrize("phase", ["probe", "delete", "create"])
async def test_partial_setup_cleanup_and_safe_diagnostics(qa_infra_context, phase):
    """Partial setup uses a synthetic adapter, never a second real root lifecycle."""
    contracts = importlib.import_module("infra.contracts")
    lifecycle = importlib.import_module("infra.lifecycle")
    manifest = deepcopy(qa_infra_context["manifest"])
    permit = contracts.load(os.environ["QA_INFRA_PERMIT"])
    directory = (
        Path(os.environ["QA_TEST_SUPPORT_ROOT"]).parents[1]
        / ".qa/harden-agent-verification-pipeline/METRICA-1/fixture-cases"  # noqa: W503
        / uuid4().hex  # noqa: W503
    )
    calls = []
    sentinel = "synthetic-driver-secret"

    class PartialAdapter:
        def __init__(self, selected, raw):
            assert selected["resource"] == manifest["resource"]

        async def connect(self):
            calls.append("connect")

        async def create(self):
            calls.append("create")
            if phase == "create":
                raise RuntimeError(sentinel)

        async def probe(self):
            calls.append("probe")
            if phase == "probe":
                raise RuntimeError(sentinel)

        async def delete(self):
            calls.append("delete")
            if phase == "delete":
                raise RuntimeError(sentinel)

        async def close(self):
            calls.append("close")

    with pytest.raises((RuntimeError, contracts.GuardError)):
        async with lifecycle.owned(manifest, permit, adapter_factory=PartialAdapter, directory=directory):
            assert phase == "delete"
    journal_text = (directory / "resources.json").read_text()
    journal = json.loads(journal_text)
    assert sentinel not in journal_text and qa_infra_context["url"] not in journal_text
    assert calls[-1] == "close"
    assert journal["resource"] == manifest["resource"]
    if phase == "create":
        assert "delete" not in calls and journal["state"] == "residual-uncertain"
        assert journal["confirmed"] is False
    else:
        assert calls.count("delete") == 1 and journal["confirmed"] is True
        assert journal["state"] == ("removed" if phase == "probe" else "residual")
        assert journal["cleanup"] == ("passed" if phase == "probe" else "failed")


async def test_owned_schema_metadata_has_no_public_fallback(pg_engine, qa_infra_context):
    """No public DDL is authorized: inspect search_path and provoke missing-table error."""
    schema = qa_infra_context["resource"]["schema"]
    assert qa_infra_context["connect_args"] == {"server_settings": {"search_path": schema}}
    assert qa_infra_context["version_table_schema"] == schema
    async with pg_engine.connect() as connection:
        assert await connection.scalar(text("SHOW search_path")) == schema
        assert await connection.scalar(text("SELECT current_schema()")) == schema
        await connection.execute(text('CREATE TABLE "qa_isolation_probe" (value integer)'))
        await connection.execute(text('DROP TABLE "qa_isolation_probe"'))
        with pytest.raises(DBAPIError):
            await connection.execute(text('SELECT * FROM "qa_isolation_probe"'))
        await connection.rollback()


def test_summary_rejects_skip_empty_partial_and_teardown_failure(qa_infra_context):
    gate = importlib.import_module("infra.pytest_gate")
    node = "synthetic-node"
    good = {"setup": "passed", "call": "passed", "teardown": "passed"}
    assert gate.complete([node], {node: good})
    assert not gate.complete([], {})
    assert not gate.complete([node], {})
    for phase in good:
        for outcome in ("skipped", "failed", "xfail"):
            assert not gate.complete([node], {node: {**good, phase: outcome}})
    assert not gate.complete([node], {node: {"setup": "passed", "call": "passed"}})
