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
    cleanup_batch_size: int = Field(default=1000, alias="CLEANUP_BATCH_SIZE", ge=1, le=10000)
    maintenance_runtime_enabled: bool = Field(default=False, alias="MAINTENANCE_RUNTIME_ENABLED")
    maintenance_dispatch_limit: int = Field(default=100, alias="MAINTENANCE_DISPATCH_LIMIT", ge=1, le=1000)
    maintenance_relay_interval_seconds: int = Field(default=1, alias="MAINTENANCE_RELAY_INTERVAL_SECONDS", gt=0)
    maintenance_lease_seconds: int = Field(default=60, alias="MAINTENANCE_LEASE_SECONDS", gt=0)

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


class NatsSettings(BaseSettings):
    nats_servers_raw: str = Field(default="nats://eqsitecms-nats:4222", alias="NATS_SERVERS", repr=False)
    nats_stream_parse_commands: str = Field(default="PARSE_COMMANDS", alias="NATS_STREAM_PARSE_COMMANDS", min_length=1)
    nats_subject_parse_run: str = Field(
        default="commands.parse.ya_metrica.run", alias="NATS_SUBJECT_PARSE_RUN", min_length=1
    )
    nats_consumer_parse_run: str = Field(default="ya-metrica-parse-run", alias="NATS_CONSUMER_PARSE_RUN", min_length=1)
    nats_stream_yandex_lifecycle: str = Field(
        default="YANDEX_LIFECYCLE", alias="NATS_STREAM_YANDEX_LIFECYCLE", min_length=1
    )
    nats_stream_seo_maintenance: str = Field(
        default="SEO_MAINTENANCE", alias="NATS_STREAM_SEO_MAINTENANCE", min_length=1
    )
    nats_stream_maintenance_results: str = Field(
        default="METRICA_MAINTENANCE_RESULTS", alias="NATS_STREAM_MAINTENANCE_RESULTS", min_length=1
    )
    nats_subject_cleanup_requested: str = Field(
        default="events.yandex.metrica.cleanup.requested", alias="NATS_SUBJECT_CLEANUP_REQUESTED", min_length=1
    )
    nats_subject_pages_merge: str = Field(
        default="commands.seo.pages.merge", alias="NATS_SUBJECT_PAGES_MERGE", min_length=1
    )
    nats_subject_pages_merge_finalized: str = Field(
        default="events.seo.pages.merge.finalized", alias="NATS_SUBJECT_PAGES_MERGE_FINALIZED", min_length=1
    )
    nats_subject_cleanup_completed: str = Field(
        default="events.metrica.cleanup.completed", alias="NATS_SUBJECT_CLEANUP_COMPLETED", min_length=1
    )
    nats_subject_pages_merge_completed: str = Field(
        default="events.metrica.pages.merge.completed", alias="NATS_SUBJECT_PAGES_MERGE_COMPLETED", min_length=1
    )
    nats_consumer_cleanup_requested: str = Field(
        default="ya-metrica-cleanup-requested", alias="NATS_CONSUMER_CLEANUP_REQUESTED", min_length=1
    )
    nats_consumer_pages_merge: str = Field(
        default="ya-metrica-pages-merge", alias="NATS_CONSUMER_PAGES_MERGE", min_length=1
    )
    nats_consumer_pages_merge_finalized: str = Field(
        default="ya-metrica-pages-merge-finalized", alias="NATS_CONSUMER_PAGES_MERGE_FINALIZED", min_length=1
    )
    nats_consumer_ack_wait_seconds: float = Field(default=30, alias="NATS_CONSUMER_ACK_WAIT_SECONDS", gt=0)
    nats_consumer_max_deliver: int = Field(default=5, alias="NATS_CONSUMER_MAX_DELIVER", ge=1)
    nats_consumer_fetch_batch_size: int = Field(default=1, alias="NATS_CONSUMER_FETCH_BATCH_SIZE", ge=1)
    nats_consumer_fetch_timeout_seconds: float = Field(default=5, alias="NATS_CONSUMER_FETCH_TIMEOUT_SECONDS", gt=0)
    nats_setup_max_attempts: int = Field(default=10, alias="NATS_SETUP_MAX_ATTEMPTS", ge=1)
    nats_setup_backoff_seconds: float = Field(default=2, alias="NATS_SETUP_BACKOFF_SECONDS", ge=0)
    nats_error_report_after_attempts: int = Field(default=3, alias="NATS_ERROR_REPORT_AFTER_ATTEMPTS", ge=1)

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", hide_input_in_errors=True, populate_by_name=True
    )

    @property
    def nats_servers(self) -> list[str]:
        return [server.strip() for server in self.nats_servers_raw.split(",") if server.strip()]

    @model_validator(mode="after")
    def validate_servers(self) -> NatsSettings:
        if not self.nats_servers:
            raise ValueError("NATS_SERVERS must not be empty")
        for server in self.nats_servers:
            try:
                url = urlsplit(server)
                valid = url.scheme in {"nats", "tls", "ws", "wss"} and bool(url.hostname)
                _ = url.port
            except ValueError:
                valid = False
            if not valid:
                raise ValueError("NATS_SERVERS must contain valid NATS URLs")
        return self


settings = Settings()
