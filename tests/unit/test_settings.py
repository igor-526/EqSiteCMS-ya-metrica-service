import pytest
from pydantic import ValidationError

from settings import Settings


@pytest.fixture(autouse=True)
def isolated_environment(clean_settings_env):
    pass


def test_defaults_and_database_url():
    settings = Settings(_env_file=None, ENCRYPTION_SERVICE_KEY="test")
    assert settings.environment == "development"
    assert settings.debug is True
    assert settings.app_title == "FastAPI Template"
    assert settings.sentry_enabled is False
    assert settings.sentry_release is None
    assert settings.database_url == "postgresql+asyncpg://app:app@localhost:5432/app"
    assert settings.database_url is settings.database_url


def test_environment_aliases_are_loaded(monkeypatch):
    values = {
        "ENVIRONMENT": "production",
        "DEBUG": "false",
        "APP_TITLE": "Analytics",
        "SENTRY_ENABLED": "true",
        "SENTRY_DSN": "https://public@example.org/1",
        "SENTRY_ENVIRONMENT": "production",
        "SENTRY_TRACES_SAMPLE_RATE": "0.5",
        "SENTRY_RELEASE": "v1",
        "POSTGRES_USER": "reader",
        "POSTGRES_PASSWORD": "password",
        "POSTGRES_HOST": "postgres.invalid",
        "POSTGRES_PORT": "5433",
        "POSTGRES_DB": "analytics",
        "ENCRYPTION_SERVICE_KEY": "test-secret",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    settings = Settings(_env_file=None)
    assert settings.environment == settings.sentry_environment == "production"
    assert settings.debug is False
    assert settings.app_title == "Analytics"
    assert settings.sentry_enabled is True
    assert settings.sentry_dsn == values["SENTRY_DSN"]
    assert settings.sentry_release == "v1"
    assert settings.sentry_traces_sample_rate == 0.5
    assert settings.encryption_service_key == "test-secret"
    assert settings.database_url == "postgresql+asyncpg://reader:password@postgres.invalid:5433/analytics"


@pytest.mark.parametrize("key", [{}, {"ENCRYPTION_SERVICE_KEY": ""}])
def test_encryption_secret_is_required(key):
    with pytest.raises(ValidationError, match="ENCRYPTION_SERVICE_KEY is required"):
        Settings(_env_file=None, **key)


def test_enabled_sentry_requires_dsn():
    with pytest.raises(ValidationError, match="SENTRY_DSN is required"):
        Settings(_env_file=None, ENCRYPTION_SERVICE_KEY="test", SENTRY_ENABLED=True)


@pytest.mark.parametrize("rate", [-0.01, 1.01])
def test_trace_sample_rate_bounds(rate):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, ENCRYPTION_SERVICE_KEY="test", SENTRY_TRACES_SAMPLE_RATE=rate)


@pytest.mark.parametrize("rate", [0.0, 1.0])
def test_trace_sample_rate_accepts_boundary(rate):
    settings = Settings(_env_file=None, ENCRYPTION_SERVICE_KEY="test", SENTRY_TRACES_SAMPLE_RATE=rate)
    assert settings.sentry_traces_sample_rate == rate


def test_explicit_values_override_environment_and_ignore_extra(monkeypatch):
    monkeypatch.setenv("APP_TITLE", "environment title")
    settings = Settings(_env_file=None, ENCRYPTION_SERVICE_KEY="test", APP_TITLE="explicit title", UNKNOWN="ignored")
    assert settings.app_title == "explicit title"
