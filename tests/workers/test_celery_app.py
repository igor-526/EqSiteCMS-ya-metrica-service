from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from settings import CelerySettings
from workers.app_factory import create_celery_app
from workers.dispatcher import CeleryParseDispatcher


def test_app_configuration_and_discovery():
    settings = CelerySettings(_env_file=None, REDIS_PASSWORD="test-only")
    app = create_celery_app(settings, 17)
    try:
        conf = app.conf
        assert app.main == "ya-metrica-service"
        assert conf.task_default_queue == "ya_metrica"
        assert [q.name for q in conf.task_queues] == ["ya_metrica"]
        assert conf.task_serializer == conf.result_serializer == "json"
        assert conf.accept_content == ["json"]
        assert conf.task_acks_late and conf.task_reject_on_worker_lost and conf.task_ignore_result
        assert conf.worker_prefetch_multiplier == 1
        assert conf.broker_transport_options == {"visibility_timeout": 3600}
        assert conf.beat_schedule == {"metrica-dispatch-ready": {"task": "metrica.dispatch_ready", "schedule": 17}}
        app.loader.import_default_modules()
        for name in ["metrica.process_account", "metrica.dispatch_ready"]:
            assert name in app.tasks
            assert app.tasks[name].max_retries == 3
            assert app.tasks[name].retry_backoff is True
    finally:
        app.close()


async def test_dispatcher_only_sends_account_id():
    app = MagicMock()
    account = uuid4()
    await CeleryParseDispatcher(app).dispatch_account(account)
    app.send_task.assert_called_once_with("metrica.process_account", args=[str(account)], queue="ya_metrica")


async def test_dispatcher_propagates_broker_failure():
    app = MagicMock()
    app.send_task.side_effect = ConnectionError("broker unavailable")
    with pytest.raises(ConnectionError):
        await CeleryParseDispatcher(app).dispatch_account(uuid4())
