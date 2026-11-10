from .metrica_tasks import router as metrica_tasks_router
from .page_stats import router as page_stats_router
from .yandex_metrica import router as yandex_metrica_router

__all__ = [
    "yandex_metrica_router",
    "page_stats_router",
    "metrica_tasks_router",
]
