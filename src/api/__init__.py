from .page_stats import router as page_stats_router
from .yandex_metrica import router as yandex_metrica_router

__all__ = [
    "yandex_metrica_router",
    "page_stats_router",
]
