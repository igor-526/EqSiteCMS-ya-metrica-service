from functools import cached_property
from urllib.parse import quote, urlsplit, urlunsplit

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    environment: str = Field(default="development", alias="ENVIRONMENT")
    debug: bool = Field(default=True, alias="DEBUG")
    app_title: str = Field(default="FastAPI Template", alias="APP_TITLE")

    sentry_enabled: bool = Field(default=False, alias="SENTRY_ENABLED")
    sentry_dsn: str = Field(default="", alias="SENTRY_DSN")
    sentry_environment: str = Field(default="development", alias="SENTRY_ENVIRONMENT")
    sentry_traces_sample_rate: float = Field(default=0.0, alias="SENTRY_TRACES_SAMPLE_RATE", ge=0.0, le=1.0)
    sentry_release: str | None = Field(default=None, alias="SENTRY_RELEASE")

    postgres_user: str = Field(default="app", alias="POSTGRES_USER")
    postgres_password: str = Field(default="app", alias="POSTGRES_PASSWORD")
    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=5432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="app", alias="POSTGRES_DB")

    encryption_service_key: str = Field(default="", alias="ENCRYPTION_SERVICE_KEY")

    yandex_service_url: str = Field(default="http://eqsitecms-yandex-service:8000", alias="YANDEX_SERVICE_URL")
    seo_service_url: str = Field(default="http://eqsitecms-seo-service:8000", alias="SEO_SERVICE_URL")
    ya_metrica_utc_offset_hours: int = Field(default=3, alias="YA_METRICA_UTC_OFFSET_HOURS", ge=-12, le=14)
    ya_metrica_stat_page_limit: int = Field(default=100000, alias="YA_METRICA_STAT_PAGE_LIMIT", ge=1, le=100000)
    ya_metrica_stat_timeout_seconds: int = Field(default=120, alias="YA_METRICA_STAT_TIMEOUT_SECONDS", gt=0)
    seo_resolve_batch_size: int = Field(default=1000, alias="SEO_RESOLVE_BATCH_SIZE", ge=1, le=1000)
    parse_dispatch_interval_seconds: int = Field(default=30, alias="PARSE_DISPATCH_INTERVAL_SECONDS", gt=0)
    task_max_attempts: int = Field(default=3, alias="TASK_MAX_ATTEMPTS", gt=0)
    task_retry_base_seconds: int = Field(default=60, alias="TASK_RETRY_BASE_SECONDS", gt=0)
    task_lease_seconds: int = Field(default=600, alias="TASK_LEASE_SECONDS", gt=0)

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @model_validator(mode="after")
    def validate_production_secrets(self) -> Settings:
        if self.sentry_enabled and not self.sentry_dsn:
            raise ValueError("SENTRY_DSN is required when SENTRY_ENABLED=true")
        if self.encryption_service_key == "":
            raise ValueError("ENCRYPTION_SERVICE_KEY is required")
        return self

    @cached_property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


class CelerySettings(BaseSettings):
    """Only the service environment/dotenv supplies Redis credentials."""

    celery_app_main: str = Field(default="ya-metrica-service", alias="CELERY_APP_MAIN", min_length=1)
    celery_app_broker: str = Field(default="redis://eqsitecms-redis:6379/5", alias="CELERY_APP_BROKER", repr=False)
    celery_app_backend: str = Field(default="redis://eqsitecms-redis:6379/6", alias="CELERY_APP_BACKEND", repr=False)
    redis_password: SecretStr = Field(default=SecretStr(""), alias="REDIS_PASSWORD", repr=False)

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", hide_input_in_errors=True, populate_by_name=True
    )

    @model_validator(mode="after")
    def validate_redis_urls(self) -> CelerySettings:
        for name, database in (("celery_app_broker", "5"), ("celery_app_backend", "6")):
            try:
                url = urlsplit(getattr(self, name))
                valid = all(
                    (
                        url.scheme in {"redis", "rediss"},
                        bool(url.hostname),
                        url.port is not None,
                        url.path == f"/{database}",
                        not url.query,
                        not url.fragment,
                    )
                )
            except ValueError:
                valid = False
            if not valid:
                raise ValueError(f"{name.upper()} must be a Redis URL for database {database}")
            if not url.password:
                password = self.redis_password.get_secret_value()
                if not password:
                    raise ValueError("Redis password is required in the service environment/.env")
                # Preserve a configured ACL username and escape reserved password characters.
                host = url.netloc.rsplit("@", 1)[-1]
                username = quote(url.username or "", safe="")
                netloc = f"{username}:{quote(password, safe='')}@{host}"
                setattr(self, name, urlunsplit(url._replace(netloc=netloc)))
        return self


settings = Settings()
