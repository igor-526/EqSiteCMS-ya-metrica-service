"""UT-MP-08 / access matrix: deleted M2 is rejected for every caller."""

from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

import main
from core.services.parse_enqueue_service import ParseEnqueueService

PATH = "/api/metrica/stats/parse"


@pytest.mark.parametrize("headers", [{}, {"Cookie": "session=foreign"}, {"Authorization": "Bearer foreign"}])
@pytest.mark.parametrize("body", [{"equestrian_id": str(uuid4())}, {"token": "SECRET"}, None])
def test_removed_route_rejected_without_operation(headers, body, monkeypatch, caplog):
    enqueue = AsyncMock()
    monkeypatch.setattr(ParseEnqueueService, "enqueue", enqueue)
    monkeypatch.setattr(main, "close_database", AsyncMock())
    graph = Mock(side_effect=AssertionError("No parse graph for removed route"))
    monkeypatch.setattr("containers.application.create_parse_services", graph)
    with TestClient(main.app, raise_server_exceptions=False) as client:
        response = client.post(PATH, headers=headers, json=body)
    assert response.status_code == 400
    assert response.json() == {"detail": "Ошибка HTTP-запроса"}
    assert "SECRET" not in response.text + caplog.text
    enqueue.assert_not_awaited()
    graph.assert_not_called()
    assert PATH not in main.app.openapi()["paths"]
