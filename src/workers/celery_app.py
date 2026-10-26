from containers.application import celery_app_provider

# CLI entry point; DI owns the singleton.
celery_app = celery_app_provider()
