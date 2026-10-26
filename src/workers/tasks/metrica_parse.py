import asyncio
import logging
from time import monotonic
from uuid import UUID, uuid4

from celery import shared_task  # type: ignore[import-untyped]
from kombu.exceptions import OperationalError as BrokerOperationalError  # type: ignore[import-untyped]
from sqlalchemy.exc import OperationalError

from containers.application import create_parse_services, services_container
from utils.database import worker_session

logger = logging.getLogger(__name__)
INFRASTRUCTURE_ERRORS = (OperationalError, ConnectionError, BrokerOperationalError)


async def _process_account(account_id: UUID, locked_by: str) -> None:
    async with worker_session() as session:
        services = create_parse_services(session)
        outcome = await services.task_runner().run_next_for_account(account_id, locked_by=locked_by)
    if outcome.has_more:
        await services_container.parse_dispatcher().dispatch_account(account_id)


async def _dispatch_ready() -> None:
    async with worker_session() as session:
        services = create_parse_services(session)
        accounts = await services.parse_dispatch_service().collect_ready_accounts()
    dispatcher = services_container.parse_dispatcher()
    for account_id in accounts:
        await dispatcher.dispatch_account(account_id)


@shared_task(
    bind=True, name="metrica.process_account", autoretry_for=INFRASTRUCTURE_ERRORS, max_retries=3, retry_backoff=True
)
def process_account(self, account_id: str) -> None:
    started = monotonic()
    asyncio.run(_process_account(UUID(account_id), locked_by=str(uuid4())))
    logger.info(
        "Celery finish: task_id=%s task=%s status=completed duration=%.3f",
        self.request.id,
        self.name,
        monotonic() - started,
    )


@shared_task(
    bind=True, name="metrica.dispatch_ready", autoretry_for=INFRASTRUCTURE_ERRORS, max_retries=3, retry_backoff=True
)
def dispatch_ready(self) -> None:
    started = monotonic()
    asyncio.run(_dispatch_ready())
    logger.info(
        "Celery finish: task_id=%s task=%s status=completed duration=%.3f",
        self.request.id,
        self.name,
        monotonic() - started,
    )
