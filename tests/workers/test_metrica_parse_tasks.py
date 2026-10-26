from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from celery.exceptions import Retry  # type: ignore[import-untyped]
from sqlalchemy.exc import OperationalError

from core.services.parse_task_runner import RunOutcome
from workers.tasks import metrica_parse as tasks


@pytest.fixture
def graph(monkeypatch):
    sessions = []

    @asynccontextmanager
    async def session_context():
        session = object()
        sessions.append(session)
        try:
            yield session
        finally:
            sessions.remove(session)

    runner = SimpleNamespace(run_next_for_account=AsyncMock(return_value=RunOutcome()))
    collector = SimpleNamespace(collect_ready_accounts=AsyncMock(return_value=[]))
    dispatcher = SimpleNamespace(dispatch_account=AsyncMock())
    services = SimpleNamespace(task_runner=lambda: runner, parse_dispatch_service=lambda: collector)
    factory = MagicMock(return_value=services)
    monkeypatch.setattr(tasks, "worker_session", session_context)
    monkeypatch.setattr(tasks, "create_parse_services", factory)
    monkeypatch.setattr(tasks, "services_container", SimpleNamespace(parse_dispatcher=lambda: dispatcher))
    return runner, collector, dispatcher, sessions, factory


@pytest.mark.parametrize("has_more", [False, True])
def test_account_runs_and_redispatches_after_session_close(graph, has_more, caplog):
    runner, _, dispatcher, sessions, factory = graph
    runner.run_next_for_account.return_value = RunOutcome(has_more=has_more)
    account = uuid4()

    async def send(account_id):
        assert account_id == account
        assert not sessions

    dispatcher.dispatch_account.side_effect = send
    tasks.process_account.run(str(account))
    runner.run_next_for_account.assert_awaited_once()
    assert runner.run_next_for_account.await_args.args == (account,)
    assert runner.run_next_for_account.await_args.kwargs["locked_by"]
    assert dispatcher.dispatch_account.await_count == int(has_more)
    factory.assert_called_once()
    assert not sessions


def test_locked_by_unique_even_when_celery_redelivers(graph):
    runner, *_ = graph
    account = str(uuid4())
    tasks.process_account.run(account)
    tasks.process_account.run(account)
    owners = [call.kwargs["locked_by"] for call in runner.run_next_for_account.await_args_list]
    assert len(set(owners)) == 2


def test_dispatch_collects_and_sends_only_after_transaction_session_closes(graph):
    _, collector, dispatcher, sessions, _ = graph
    accounts = [uuid4(), uuid4()]
    collector.collect_ready_accounts.return_value = accounts

    async def send(account):
        assert not sessions
        assert account in accounts

    dispatcher.dispatch_account.side_effect = send
    tasks.dispatch_ready.run()
    assert [c.args[0] for c in dispatcher.dispatch_account.await_args_list] == accounts


@pytest.mark.parametrize("task_name", ["process_account", "dispatch_ready"])
@pytest.mark.parametrize("error", [ConnectionError("offline"), OperationalError("claim", {}, Exception())])
def test_infrastructure_autoretry(graph, monkeypatch, task_name, error):
    runner, collector, _, sessions, _ = graph
    target = runner.run_next_for_account if task_name == "process_account" else collector.collect_ready_accounts
    target.side_effect = error
    task = getattr(tasks, task_name)
    retry = MagicMock(side_effect=Retry())
    monkeypatch.setattr(task, "retry", retry)
    with pytest.raises(Retry):
        task.run(*([str(uuid4())] if task_name == "process_account" else []))
    retry.assert_called_once()
    assert not sessions


def test_domain_error_not_autoretried(graph, monkeypatch):
    runner, *_ = graph
    runner.run_next_for_account.side_effect = ValueError("domain")
    retry = MagicMock()
    monkeypatch.setattr(tasks.process_account, "retry", retry)
    with pytest.raises(ValueError, match="domain"):
        tasks.process_account.run(str(uuid4()))
    retry.assert_not_called()
