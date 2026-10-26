import json
from uuid import UUID

import httpx
import pytest

from clients.seo_service import SeoServiceClient
from core.exceptions import PeerServiceException

TENANT = UUID("5f6496aa-4fe9-42da-a763-29f9bab22d73")
ENTITY = UUID("799c85a2-1f16-4477-9ef5-cb44ee4b81b2")


@pytest.mark.parametrize("method,path", [("resolve_pages", "pages"), ("resolve_search_queries", "search-queries")])
@pytest.mark.parametrize("size", [1, 1000])
async def test_resolve_contract_keeps_source_order_and_duplicates(mock_transport, method, path, size):
    items = ["PHRASE_MARKER"] * size
    captured = mock_transport(
        lambda request: httpx.Response(
            200, json={"items": [{"source": item, "normalized": "normalized", "id": str(ENTITY)} for item in items]}
        )
    )
    result = await getattr(SeoServiceClient("http://seo.test"), method)(equestrian_id=TENANT, items=items)
    assert [item.source for item in result] == items
    assert all(item.id == ENTITY for item in result)
    assert "PHRASE_MARKER" not in repr(result)
    request = captured[0]
    assert request.method == "POST"
    assert request.url.path == f"/api/{path}/resolve"
    assert request.headers["X-Equestrian-Id"] == str(TENANT)
    assert "authorization" not in request.headers
    assert json.loads(request.content) == {"items": items}


@pytest.mark.parametrize("size", [0, 1001])
async def test_resolve_batch_limit_before_request(mock_transport, size):
    captured = mock_transport(lambda request: pytest.fail("unexpected HTTP call"))
    with pytest.raises(ValueError):
        await SeoServiceClient("http://seo.test").resolve_pages(equestrian_id=TENANT, items=["a"] * size)
    assert not captured


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"items": []},
        {"items": None},
        {"items": [{}]},
        {"items": [{"source": "a", "normalized": "a", "id": "bad"}]},
        {"items": [{"source": "wrong", "normalized": "a", "id": str(ENTITY)}]},
    ],
)
async def test_invalid_resolve_envelope_or_correspondence(mock_transport, payload):
    mock_transport(lambda request: httpx.Response(200, json=payload))
    with pytest.raises(PeerServiceException) as error:
        await SeoServiceClient("http://seo.test").resolve_pages(equestrian_id=TENANT, items=["a"])
    assert (error.value.service, error.value.status, error.value.category) == ("seo", 200, "response")


@pytest.mark.parametrize("status", [400, 401, 403, 429, 500, 503])
async def test_resolve_does_not_bisect_or_retry_in_http_adapter(mock_transport, status, caplog):
    captured = mock_transport(lambda request: httpx.Response(status, text="PHRASE_MARKER"))
    with pytest.raises(PeerServiceException) as error:
        await SeoServiceClient("http://seo.test").resolve_search_queries(equestrian_id=TENANT, items=["PHRASE_MARKER"])
    assert len(captured) == 1
    assert (error.value.service, error.value.status, error.value.category) == ("seo", status, "http")
    assert "PHRASE_MARKER" not in str(error.value) + caplog.text


@pytest.mark.parametrize("cause,category", [(httpx.ReadTimeout, "timeout"), (httpx.ConnectError, "transport")])
async def test_seo_network_failure(mock_transport, cause, category):
    def handler(request):
        raise cause("PHRASE_MARKER")

    mock_transport(handler)
    with pytest.raises(PeerServiceException) as error:
        await SeoServiceClient("http://seo.test").resolve_pages(equestrian_id=TENANT, items=["PHRASE_MARKER"])
    assert (error.value.status, error.value.category) == (None, category)
