from datetime import date

import httpx
import pytest

from clients.yandex_metrica_stat import YandexMetricaStatClient
from core.entities.metrica_parse import ParseKind
from core.exceptions import YandexClientException


async def fetch(client, kind=ParseKind.PAGES, **kwargs):
    return await client.get_stat_page(
        counter_id="105850153",
        access_token="TOKEN_MARKER",
        kind=kind,
        date_from=date(2026, 10, 21),
        date_to=date(2026, 10, 25),
        **kwargs,
    )


@pytest.mark.parametrize(
    ("kind", "dimension"), [(ParseKind.PAGES, "startURL"), (ParseKind.SEARCH_QUERIES, "lastSearchPhrase")]
)
async def test_exact_reporting_request_and_raw_rows(mock_transport, kind, dimension):
    rows = [
        {"dimensions": [{"name": "2026-10-23"}, {"name": None}], "metrics": [30.0, 30.0, 59.0]},
        None,
        {"invalid": "PHRASE_MARKER"},
    ]
    captured = mock_transport(
        lambda request: httpx.Response(
            200, json={"data": rows, "total_rows": 35, "sampled": True, "total_rows_rounded": True}
        )
    )
    page = await fetch(YandexMetricaStatClient(timeout=77), kind, offset=11, limit=10)
    assert page.data == rows
    assert (page.total_rows, page.sampled, page.total_rows_rounded) == (35, True, True)
    request = captured[0]
    assert str(request.url.copy_with(query=None)) == "https://api-metrika.yandex.net/stat/v1/data"
    assert dict(request.url.params) == {
        "ids": "105850153",
        "metrics": (
            "ym:s:visits,ym:s:users,ym:s:pageviews,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds"
        ),
        "dimensions": f"ym:s:date,ym:s:{dimension}",
        "date1": "2026-10-21",
        "date2": "2026-10-25",
        "limit": "10",
        "offset": "11",
        "sort": f"ym:s:date,ym:s:{dimension}",
        "accuracy": "full",
    }
    assert request.headers["Authorization"] == "OAuth TOKEN_MARKER"
    assert "TOKEN_MARKER" not in str(request.url)
    assert set(request.extensions["timeout"].values()) == {77}
    assert "PHRASE_MARKER" not in repr(page)


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"data": []},
        {"total_rows": 0},
        {"data": {}, "total_rows": 0},
        {"data": None, "total_rows": 0},
        {"data": [], "total_rows": "0"},
        {"data": [], "total_rows": 0.5},
        {"data": [], "total_rows": True},
        {"data": [], "total_rows": -1},
        {"data": [], "total_rows": 0, "sampled": "yes"},
        {"data": [], "total_rows": 0, "total_rows_rounded": 1},
    ],
)
async def test_invalid_envelope_is_response_failure(mock_transport, payload, caplog):
    mock_transport(lambda request: httpx.Response(200, json=payload))
    with pytest.raises(YandexClientException) as error:
        await fetch(YandexMetricaStatClient())
    assert (error.value.status, error.value.category) == (200, "response")
    assert "TOKEN_MARKER" not in caplog.text


async def test_empty_page_and_defaults(mock_transport):
    captured = mock_transport(lambda request: httpx.Response(200, json={"data": [], "total_rows": 0}))
    page = await fetch(YandexMetricaStatClient())
    assert (page.data, page.total_rows, page.sampled, page.total_rows_rounded) == ([], 0, False, False)
    assert captured[0].url.params["offset"] == "1"
    assert captured[0].url.params["limit"] == "100000"
    assert set(captured[0].extensions["timeout"].values()) == {120}


@pytest.mark.parametrize("status", [400, 403, 429, 500, 503])
async def test_reporting_http_errors(mock_transport, status, caplog):
    mock_transport(lambda request: httpx.Response(status, text="TOKEN_MARKER PHRASE_MARKER"))
    with pytest.raises(YandexClientException) as error:
        await fetch(YandexMetricaStatClient())
    assert (error.value.status, error.value.category) == (status, "http")
    for marker in ("TOKEN_MARKER", "PHRASE_MARKER"):
        assert marker not in str(error.value) + caplog.text


@pytest.mark.parametrize("cause,category", [(httpx.ReadTimeout, "timeout"), (httpx.ConnectError, "transport")])
async def test_reporting_transport_errors(mock_transport, cause, category):
    def handler(request):
        raise cause("TOKEN_MARKER")

    mock_transport(handler)
    with pytest.raises(YandexClientException) as error:
        await fetch(YandexMetricaStatClient())
    assert (error.value.status, error.value.category) == (None, category)


@pytest.mark.parametrize("kwargs", [{"offset": 0}, {"limit": 0}, {"limit": 100001}])
async def test_invalid_pagination_never_calls_provider(mock_transport, kwargs):
    captured = mock_transport(lambda request: pytest.fail("unexpected HTTP call"))
    with pytest.raises(ValueError):
        await fetch(YandexMetricaStatClient(), **kwargs)
    assert not captured
