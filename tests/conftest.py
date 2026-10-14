"""Deterministic test configuration: never read developer dotenv files."""

import os
from unittest.mock import patch

import pytest
from pydantic_settings.sources import DotEnvSettingsSource

TEST_ENV = {
    "ENVIRONMENT": "test",
    "DEBUG": "false",
    "APP_TITLE": "Yandex Metrica Test",
    "SENTRY_ENABLED": "false",
    "SENTRY_DSN": "",
    "SENTRY_ENVIRONMENT": "test",
    "SENTRY_TRACES_SAMPLE_RATE": "0",
    "SENTRY_RELEASE": "test-release",
    "POSTGRES_USER": "test",
    "POSTGRES_PASSWORD": "test",
    "POSTGRES_HOST": "database.invalid",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "test",
    "ENCRYPTION_SERVICE_KEY": "test-only-encryption-secret",
}
os.environ.update(TEST_ENV)
_dotenv_patch = patch.object(DotEnvSettingsSource, "_read_env_files", return_value={})
_dotenv_patch.start()


def pytest_sessionfinish(session, exitstatus):
    _dotenv_patch.stop()


@pytest.fixture
def clean_settings_env(monkeypatch):
    for name in TEST_ENV:
        monkeypatch.delenv(name, raising=False)
