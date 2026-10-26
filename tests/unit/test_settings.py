import pytest
from pydantic import ValidationError

from settings import CelerySettings, Settings


@pytest.fixture(autouse=True)
def isolated_environment(clean_settings_env, monkeypatch):
    for model in (Settings, CelerySettings):
        for field in model.model_fields.values():
            monkeypatch.delenv(field.alias, raising=False)


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


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("YA_METRICA_UTC_OFFSET_HOURS", -13),
        ("YA_METRICA_UTC_OFFSET_HOURS", 15),
        ("YA_METRICA_UTC_OFFSET_HOURS", "3.5"),
        ("YA_METRICA_STAT_PAGE_LIMIT", 0),
        ("YA_METRICA_STAT_PAGE_LIMIT", 100001),
        ("SEO_RESOLVE_BATCH_SIZE", 0),
        ("SEO_RESOLVE_BATCH_SIZE", 1001),
        ("YA_METRICA_STAT_TIMEOUT_SECONDS", 0),
        ("PARSE_DISPATCH_INTERVAL_SECONDS", 0),
        ("TASK_MAX_ATTEMPTS", 0),
        ("TASK_RETRY_BASE_SECONDS", -1),
        ("TASK_LEASE_SECONDS", 0),
    ],
)
def test_ut_ms_15_invalid_limits(name, value):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, ENCRYPTION_SERVICE_KEY="test", **{name: value})


@pytest.mark.parametrize("offset", [-12, 3, 14])
@pytest.mark.parametrize("limit,batch", [(1, 1), (100000, 1000)])
def test_ut_ms_15_boundaries(offset, limit, batch):
    settings = Settings(
        _env_file=None,
        ENCRYPTION_SERVICE_KEY="test",
        YA_METRICA_UTC_OFFSET_HOURS=offset,
        YA_METRICA_STAT_PAGE_LIMIT=limit,
        SEO_RESOLVE_BATCH_SIZE=batch,
    )
    assert settings.ya_metrica_utc_offset_hours == offset
    assert settings.ya_metrica_stat_page_limit == limit
    assert settings.seo_resolve_batch_size == batch
    assert settings.task_lease_seconds == 600
    assert settings.task_retry_base_seconds == 60
    assert settings.task_max_attempts == 3
    assert settings.parse_dispatch_interval_seconds == 30
    assert settings.ya_metrica_stat_timeout_seconds == 120
    assert settings.yandex_service_url == "http://eqsitecms-yandex-service:8000"
    assert settings.seo_service_url == "http://eqsitecms-seo-service:8000"


def test_ut_ms_15_celery_defaults_and_escaped_service_password():
    from urllib.parse import unquote, urlsplit

    from settings import CelerySettings

    config = CelerySettings(_env_file=None, REDIS_PASSWORD="test:/@ ?#")
    assert config.celery_app_main == "ya-metrica-service"
    for value, database in ((config.celery_app_broker, "/5"), (config.celery_app_backend, "/6")):
        url = urlsplit(value)
        assert url.path == database
        assert url.hostname == "eqsitecms-redis"
        assert unquote(url.password) == "test:/@ ?#"
    assert "test:/@ ?#" not in repr(config)
    assert "redis://" not in repr(config)


@pytest.mark.parametrize(
    "name,url",
    [
        ("CELERY_APP_BROKER", "redis://:test@localhost:6379/3"),
        ("CELERY_APP_BACKEND", "redis://:test@localhost:6379/5"),
        ("CELERY_APP_BROKER", "http://:test@localhost:6379/5"),
        ("CELERY_APP_BROKER", "redis://:test@localhost:bad/5"),
        ("CELERY_APP_BROKER", "redis://:test@:6379/5"),
        ("CELERY_APP_BROKER", "redis://:test@localhost:6379/5?db=3"),
    ],
)
def test_ut_ms_15_celery_invalid_urls(name, url):
    from settings import CelerySettings

    with pytest.raises(ValidationError):
        CelerySettings(_env_file=None, REDIS_PASSWORD="test", **{name: url})


def test_celery_credentials_required_and_environment_supported(monkeypatch):
    from settings import CelerySettings

    with pytest.raises(ValidationError, match="Redis password is required"):
        CelerySettings(_env_file=None)
    monkeypatch.setenv("REDIS_PASSWORD", "local-test-password")
    monkeypatch.setenv("CELERY_APP_MAIN", "test-worker")
    config = CelerySettings(_env_file=None)
    assert config.celery_app_main == "test-worker"
    assert ":local-test-password@" in config.celery_app_broker
    config = CelerySettings(
        _env_file=None,
        CELERY_APP_BROKER="rediss://acl:explicit@localhost:6379/5",
        CELERY_APP_BACKEND="rediss://acl:explicit@localhost:6379/6",
    )
    assert config.celery_app_broker == "rediss://acl:explicit@localhost:6379/5"


def test_celery_validation_errors_hide_credentials():
    with pytest.raises(ValidationError) as error:
        CelerySettings(
            _env_file=None,
            REDIS_PASSWORD="marker-do-not-log",
            CELERY_APP_BROKER="redis://:marker-do-not-log@localhost:6379/3",
        )
    assert "marker-do-not-log" not in str(error.value)
    assert "redis://" not in str(error.value)
