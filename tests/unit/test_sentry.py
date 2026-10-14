from unittest.mock import Mock

from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

from settings import Settings
from utils import configure_sentry


def test_disabled_sentry_does_not_initialize(monkeypatch):
    init = Mock()
    monkeypatch.setattr(configure_sentry.sentry_sdk, "init", init)
    monkeypatch.setattr(configure_sentry, "settings", Settings(_env_file=None, SENTRY_ENABLED=False))
    configure_sentry.configure_sentry()
    init.assert_not_called()


def test_enabled_sentry_passes_settings_and_privacy_options(monkeypatch):
    init = Mock()
    monkeypatch.setattr(configure_sentry.sentry_sdk, "init", init)
    monkeypatch.setattr(
        configure_sentry,
        "settings",
        Settings(
            _env_file=None,
            SENTRY_ENABLED=True,
            SENTRY_DSN="https://key@example.org/1",
            SENTRY_ENVIRONMENT="staging",
            SENTRY_RELEASE="v2",
            SENTRY_TRACES_SAMPLE_RATE=0.3,
        ),
    )
    configure_sentry.configure_sentry()
    init.assert_called_once()
    kwargs = init.call_args.kwargs
    integrations = kwargs.pop("integrations")
    assert kwargs == {
        "dsn": "https://key@example.org/1",
        "environment": "staging",
        "release": "v2",
        "traces_sample_rate": 0.3,
        "send_default_pii": False,
    }
    assert [type(item) for item in integrations] == [FastApiIntegration, SqlalchemyIntegration]
