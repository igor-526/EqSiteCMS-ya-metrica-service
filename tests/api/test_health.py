import asyncio
import io
import logging
from unittest.mock import AsyncMock, Mock

import h11
import pytest
from fastapi.testclient import TestClient
from uvicorn.protocols.http.h11_impl import RequestResponseCycle

from main import SafeUnexpectedErrorMiddleware, app
from settings import Settings


@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer caller", "Cookie": "session=foreign"}])
@pytest.mark.parametrize("path", ["/health", "/docs", "/redoc", "/openapi.json", "/docs/oauth2-redirect"])
def test_health_and_docs(path, headers) -> None:
    response = TestClient(app).get(path, headers=headers)
    assert response.status_code == 200
    if path == "/health":
        assert response.json() == {"status": "ok"}


@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer caller", "Cookie": "session=foreign"}])
@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("post", "/api/auth/register"),
        ("post", "/health"),
        ("get", "/health/"),
        ("get", "/api/metrica/counters/"),
        ("delete", "/api/metrica/counters"),
    ],
)
def test_routing_rejections(method, path, headers) -> None:
    response = TestClient(app).request(method, path, headers=headers, follow_redirects=False)
    assert response.status_code == 400
    assert "location" not in response.headers


def test_template_runtime_has_no_cors_configuration_or_headers() -> None:
    assert not hasattr(Settings(), "cors_origins")
    response = TestClient(app).get("/health", headers={"Origin": "https://frontend.example"})
    assert "access-control-allow-origin" not in response.headers
    assert "access-control-allow-credentials" not in response.headers


@pytest.fixture
def http_scope():
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/health",
        "raw_path": b"/health",
        "query_string": b"",
        "root_path": "",
        "headers": [],
        "server": ("test", 80),
        "client": ("test", 1),
    }


@pytest.fixture
def health_route(monkeypatch):
    # Restore both the route endpoint and lazily constructed middleware stack.
    monkeypatch.setattr(app, "middleware_stack", app.middleware_stack)
    return next(route for route in app.routes if getattr(route, "path", None) == "/health")


@pytest.mark.asyncio
async def test_unexpected_error_is_safe_at_uvicorn_boundary(http_scope, health_route, monkeypatch, caplog):
    secret = "M_SYNTHETIC_EXCEPTION_SECRET"
    cause_secret = "M_SYNTHETIC_CAUSE_SECRET"
    unwound = False

    async def fail(scope, receive, send):
        nonlocal unwound
        try:
            try:
                raise ValueError(cause_secret)
            except ValueError as cause:
                raise RuntimeError(secret) from cause
        finally:
            # Models transaction/dependency cleanup before the boundary handles failure.
            unwound = True

    monkeypatch.setattr(health_route, "app", fail)
    stream = io.StringIO()
    server_logger = logging.getLogger("test.metrica.uvicorn")
    monkeypatch.setattr(server_logger, "handlers", [logging.StreamHandler(stream)])
    monkeypatch.setattr(server_logger, "propagate", False)
    monkeypatch.setattr(server_logger, "disabled", False)
    monkeypatch.setattr(server_logger, "level", logging.ERROR)
    transport = Mock()
    transport.is_closing.return_value = False

    def write_after_unwind(data):
        assert unwound

    transport.write.side_effect = write_after_unwind
    flow = Mock(write_paused=False)
    cycle = RequestResponseCycle(
        scope=http_scope,
        conn=h11.Connection(h11.SERVER),
        transport=transport,
        flow=flow,
        logger=server_logger,
        access_logger=server_logger,
        access_log=False,
        default_headers=[],
        message_event=asyncio.Event(),
        on_response=lambda: None,
    )
    with caplog.at_level(logging.ERROR, logger="main"):
        await cycle.run_asgi(app)

    wire = b"".join(call.args[0] for call in transport.write.call_args_list)
    client = h11.Connection(h11.CLIENT)
    client.receive_data(wire)
    response = client.next_event()
    assert isinstance(response, h11.Response)
    assert response.status_code == 400
    assert cycle.response_complete
    assert "Ошибка обработки запроса".encode() in wire
    for marker in (secret, cause_secret):
        assert marker.encode() not in wire
        assert marker not in stream.getvalue()
        assert marker not in caplog.text
    assert "Traceback" not in stream.getvalue()
    assert "Exception in ASGI application" not in stream.getvalue()
    assert "Request failed: unexpected" in caplog.text
    assert all(record.exc_info is None for record in caplog.records)
    assert app.debug is False
    assert app.router.redirect_slashes is False


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [asyncio.CancelledError(), BaseException("not an ordinary error")])
async def test_boundary_preserves_base_exceptions(failure, http_scope, health_route, monkeypatch):
    async def fail(scope, receive, send):
        raise failure

    monkeypatch.setattr(health_route, "app", fail)
    send = AsyncMock()
    with pytest.raises(type(failure)) as raised:
        await app(http_scope, AsyncMock(), send)
    assert raised.value is failure
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_boundary_does_not_replace_started_response(http_scope, health_route, monkeypatch):
    failure = RuntimeError("stream failed")

    async def fail(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"partial", "more_body": True})
        raise failure

    monkeypatch.setattr(health_route, "app", fail)
    send = AsyncMock()
    with pytest.raises(RuntimeError) as raised:
        await app(http_scope, AsyncMock(), send)
    assert raised.value is failure
    assert [call.args[0] for call in send.await_args_list] == [
        {"type": "http.response.start", "status": 200, "headers": []},
        {"type": "http.response.body", "body": b"partial", "more_body": True},
    ]


@pytest.mark.asyncio
async def test_boundary_does_not_normalize_lifespan_errors():
    failure = RuntimeError("startup failed")
    inner = AsyncMock(side_effect=failure)
    send = AsyncMock()
    with pytest.raises(RuntimeError) as raised:
        await SafeUnexpectedErrorMiddleware(inner)({"type": "lifespan"}, AsyncMock(), send)
    assert raised.value is failure
    send.assert_not_awaited()
