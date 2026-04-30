from __future__ import annotations

import asyncio
import sys
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from app.market_data_cache import MarketDataCache
from app.providers.eodhd import EODHDProvider


def test_market_data_cache_excludes_tokens_and_tracks_expiry(tmp_path):
    cache = MarketDataCache(tmp_path / "market_data_cache.sqlite3")

    cache.set_json("quote", "https://example.test/quote", {"symbol": "AAPL.US", "api_token": "first"}, {"price": 1}, 60)

    assert cache.get_json("quote", "https://example.test/quote", {"symbol": "AAPL.US", "api_token": "second"}) == {"price": 1}
    stats = cache.stats()
    assert stats.entry_count == 1
    assert stats.expired_count == 0
    assert cache.namespace_stats()[0]["namespace"] == "quote"


def test_eodhd_provider_reuses_cached_intraday_across_tokens(tmp_path, monkeypatch):
    cache = MarketDataCache(tmp_path / "market_data_cache.sqlite3")
    calls: list[dict[str, object]] = []

    class FakeResponse:
        def __init__(self, params: dict[str, object]) -> None:
            self.params = params

        def raise_for_status(self) -> None:
            return None

        def json(self) -> list[dict[str, object]]:
            calls.append(dict(self.params))
            return [
                {
                    "timestamp": 1735722000,
                    "open": 100,
                    "high": 101,
                    "low": 99,
                    "close": 100,
                    "volume": 10,
                }
            ]

    class FakeClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            return None

        async def __aenter__(self) -> "FakeClient":
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

        async def get(self, _url: str, params: dict[str, object]) -> FakeResponse:
            return FakeResponse(params)

    class FakeTimeout:
        def __init__(self, *args: object, **kwargs: object) -> None:
            return None

    fake_httpx = SimpleNamespace(
        AsyncClient=FakeClient,
        Timeout=FakeTimeout,
        TimeoutException=TimeoutError,
        HTTPStatusError=RuntimeError,
    )
    monkeypatch.setitem(sys.modules, "httpx", fake_httpx)
    provider = EODHDProvider("first-token", base_url="https://example.test", cache=cache)

    first = asyncio.run(provider.historical_bars("NDX.INDX", "5min", "2025-01-01", "2025-01-10"))
    second = asyncio.run(provider.historical_bars("NDX.INDX", "5min", "2025-01-01", "2025-01-10"))
    provider_with_new_key = EODHDProvider("second-token", base_url="https://example.test", cache=cache)
    third = asyncio.run(provider_with_new_key.historical_bars("NDX.INDX", "5min", "2025-01-01", "2025-01-10"))

    assert len(first) == 1
    assert len(second) == 1
    assert len(third) == 1
    assert len(calls) == 1
    assert {call["api_token"] for call in calls} == {"first-token"}


def test_eodhd_provider_skips_incomplete_intraday_rows(tmp_path, monkeypatch):
    cache = MarketDataCache(tmp_path / "market_data_cache.sqlite3")

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> list[dict[str, object]]:
            return [
                {
                    "timestamp": 1735722000,
                    "open": None,
                    "high": 101,
                    "low": 99,
                    "close": 100,
                    "volume": 10,
                },
                {
                    "timestamp": 1735722300,
                    "open": 100,
                    "high": 102,
                    "low": 99,
                    "close": 101,
                    "volume": None,
                },
            ]

    class FakeClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            return None

        async def __aenter__(self) -> "FakeClient":
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

        async def get(self, _url: str, params: dict[str, object]) -> FakeResponse:
            return FakeResponse()

    class FakeTimeout:
        def __init__(self, *args: object, **kwargs: object) -> None:
            return None

    fake_httpx = SimpleNamespace(
        AsyncClient=FakeClient,
        Timeout=FakeTimeout,
        TimeoutException=TimeoutError,
        HTTPStatusError=RuntimeError,
    )
    monkeypatch.setitem(sys.modules, "httpx", fake_httpx)
    provider = EODHDProvider("token", base_url="https://example.test", cache=cache)

    bars = asyncio.run(provider.historical_bars("NDX.INDX", "5min", "2025-01-01", "2025-01-10"))

    assert len(bars) == 1
    assert bars[0].open == 100
    assert bars[0].volume == 0.0


def test_eodhd_commodity_endpoint_filters_date_range(tmp_path, monkeypatch):
    cache = MarketDataCache(tmp_path / "market_data_cache.sqlite3")
    requested: list[tuple[str, dict[str, object]]] = []

    class FakeResponse:
        def __init__(self, url: str, params: dict[str, object]) -> None:
            self.url = url
            self.params = params

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            requested.append((self.url, dict(self.params)))
            return {
                "meta": {"name": "Brent", "interval": "daily", "total": 3},
                "data": [
                    {"date": "2025-01-03", "value": 75.0},
                    {"date": "2025-01-02", "value": 74.0},
                    {"date": "2024-12-31", "value": 73.0},
                ],
            }

    class FakeClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            return None

        async def __aenter__(self) -> "FakeClient":
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

        async def get(self, url: str, params: dict[str, object]) -> FakeResponse:
            return FakeResponse(url, params)

    class FakeTimeout:
        def __init__(self, *args: object, **kwargs: object) -> None:
            return None

    monkeypatch.setitem(
        sys.modules,
        "httpx",
        SimpleNamespace(AsyncClient=FakeClient, Timeout=FakeTimeout, TimeoutException=TimeoutError, HTTPStatusError=RuntimeError),
    )

    provider = EODHDProvider("token", base_url="https://example.test", cache=cache)
    bars = asyncio.run(provider.historical_bars("COMMODITY:BRENT", "1day", "2025-01-01", "2025-01-02"))

    assert [bar.close for bar in bars] == [74.0]
    assert requested[0][0].endswith("/commodities/historical/BRENT")
    assert requested[0][1]["interval"] == "daily"


def test_market_data_cache_prunes_expired_entries(tmp_path):
    cache = MarketDataCache(tmp_path / "market_data_cache.sqlite3")
    cache.set_json("quote", "https://example.test/quote", {"symbol": "MSFT.US"}, {"price": 1}, 1)

    with cache._connect() as conn:
        conn.execute(
            "UPDATE market_data_cache SET expires_at = ?",
            ((datetime.now(UTC) - timedelta(seconds=1)).isoformat(),),
        )

    assert cache.stats().expired_count == 1
    assert cache.prune_expired() == 1
    assert cache.stats().entry_count == 0
