from celery import Celery  # type: ignore[import-untyped]
from kombu import Queue  # type: ignore[import-untyped]

from settings import CelerySettings


def create_celery_app(settings: CelerySettings, interval: int) -> Celery:
    app = Celery(settings.celery_app_main, broker=settings.celery_app_broker, backend=settings.celery_app_backend)
    app.conf.update(
        task_queues=(Queue("ya_metrica"),),
        task_default_queue="ya_metrica",
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        task_expires=3600,
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        worker_prefetch_multiplier=1,
        task_ignore_result=True,
        broker_transport_options={"visibility_timeout": 3600},
        beat_schedule={"metrica-dispatch-ready": {"task": "metrica.dispatch_ready", "schedule": interval}},
    )
    app.autodiscover_tasks(["workers.tasks"], related_name=None)
    return app
