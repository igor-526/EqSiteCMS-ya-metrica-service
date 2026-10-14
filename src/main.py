from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from api import yandex_metrica, yandex_metrica_router
from containers import services_container
from core.exceptions import AppError
from settings import settings
from utils.configure_sentry import configure_sentry
from utils.database import close_database

configure_sentry()


@asynccontextmanager
async def lifespan(_: FastAPI):
    yield
    await close_database()


services_container.wire(
    modules=[yandex_metrica],
)


app = FastAPI(title=settings.app_title, debug=settings.debug, lifespan=lifespan)
app.state.container = services_container

router = APIRouter(prefix="/api")
router.include_router(yandex_metrica_router, prefix="/metrica", tags=["Yandex Metrica"])
app.include_router(router)


@app.get("/health", tags=["Health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.exception_handler(AppError)
async def app_error_handler(_: Request, exc: AppError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.message})


@app.exception_handler(RequestValidationError)
async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"detail": exc.errors()})
