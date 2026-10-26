import logging
from contextlib import asynccontextmanager

from fastapi import APIRouter, FastAPI, Request
from fastapi.exceptions import RequestValidationError, ResponseValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from api import yandex_metrica, yandex_metrica_router
from api.parse import router as parse_router
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


class SafeUnexpectedErrorMiddleware:
    """Handle HTTP failures after dependency unwinding, before server rethrow."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        response_started = False

        async def track_response(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, receive, track_response)
        except Exception as exc:
            # Cancellation/BaseException propagate; never replace an existing response.
            if response_started:
                raise
            response = await unexpected_error_handler(Request(scope), exc)
            await response(scope, receive, send)


# Starlette debug mode bypasses the generic handler and exposes traceback/locals.
# This peer must keep safe error responses even when settings.debug is enabled.
app = FastAPI(title=settings.app_title, debug=False, lifespan=lifespan, redirect_slashes=False)
app.state.container = services_container
app.add_middleware(SafeUnexpectedErrorMiddleware)

router = APIRouter(prefix="/api")
router.include_router(yandex_metrica_router, prefix="/metrica", tags=["Yandex Metrica"])
router.include_router(parse_router, prefix="/metrica", tags=["Yandex Metrica"])
app.include_router(router)


@app.get("/health", tags=["Health"])
async def health() -> dict[str, str]:
    return {"status": "ok"}


logger = logging.getLogger(__name__)
SAFE_DETAILS = frozenset(
    {
        "Ошибка расшифровки токена",
        "Ошибка получения счётчиков Яндекс.Метрики",
        "Яндекс выдал некорректный ответ",
        "Некорректный диапазон дат",
        "Счётчик Яндекс.Метрики не привязан к конюшне",
        "Парсинг конюшни уже выполняется",
        "Не удалось получить данные интеграции Яндекс",
    }
)


@app.exception_handler(AppError)
async def app_error_handler(_: Request, exc: AppError) -> JSONResponse:
    logger.warning("Request failed: application")
    detail = exc.message if exc.message in SAFE_DETAILS else "Ошибка обработки запроса"
    return JSONResponse(status_code=400, content={"detail": detail})


@app.exception_handler(RequestValidationError)
async def validation_error_handler(_: Request, exc: RequestValidationError) -> JSONResponse:
    # Never reflect input, ctx, or validator messages (which may contain secrets).
    detail = [{"loc": error["loc"], "type": error["type"]} for error in exc.errors()]
    return JSONResponse(status_code=400, content={"detail": detail})


@app.exception_handler(HTTPException)
async def http_error_handler(_: Request, exc: HTTPException) -> JSONResponse:
    logger.warning("Request failed: http status=%s", exc.status_code)
    return JSONResponse(status_code=400, content={"detail": "Ошибка HTTP-запроса"})


@app.exception_handler(ResponseValidationError)
async def unexpected_error_handler(_: Request, exc: Exception) -> JSONResponse:
    # No raw exception, request URL, body, or chained traceback in application logs.
    logger.error("Request failed: unexpected")
    return JSONResponse(status_code=400, content={"detail": "Ошибка обработки запроса"})
