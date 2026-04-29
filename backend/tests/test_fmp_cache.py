from __future__ import annotations

import asyncio
import sys
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from app.fmp_cache import FMPCache
from app.providers.fmp import FMPProvider


def test_fmp_cache_excludes_api_key_and_tracks_expiry(tmp_path):
    cache = FMPCache(tmp_path / "fmp_cache.sqlite3")

    cache.set_json("quote", "https://example.test/quote", {"symbol": "AAPL", "apikey": "first"}, [{"price": 1}], 60)

    assert cache.get_json("quote", "https://example.test/quote", {"symbol": "AAPL", "apikey": "second"}) == [{"price": 1}]
    stats = cache.stats()
    assert stats.entry_count == 1
    assert stats.expired_count == 0


def test_fmp_provider_reuses_cached_historical_chunks_across_api_keys(tmp_path, monkeypatch):
    cache = FMPCache(tmp_path / "fmp_cache.sqlite3")
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
                    "date": f"{self.params['from']}T09:00:00+00:00",
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
    provider = FMPProvider("first-key", base_url="https://example.test", cache=cache)

    first = asyncio.run(provider.historical_bars("QQQ", "5min", "2025-01-01", "2025-01-10"))
    second = asyncio.run(provider.historical_bars("QQQ", "5min", "2025-01-01", "2025-01-10"))
    provider_with_new_key = FMPProvider("second-key", base_url="https://example.test", cache=cache)
    third = asyncio.run(provider_with_new_key.historical_bars("QQQ", "5min", "2025-01-01", "2025-01-10"))

    assert len(first) == 2
    assert len(second) == 2
    assert len(third) == 2
    assert len(calls) == 2
    assert {call["apikey"] for call in calls} == {"first-key"}


def test_fmp_cache_prunes_expired_entries(tmp_path):
    cache = FMPCache(tmp_path / "fmp_cache.sqlite3")
    cache.set_json("quote", "https://example.test/quote", {"symbol": "MSFT"}, [{"price": 1}], 1)

    with cache._connect() as conn:
        conn.execute(
            "UPDATE fmp_cache SET expires_at = ?",
            ((datetime.now(UTC) - timedelta(seconds=1)).isoformat(),),
        )

    assert cache.stats().expired_count == 1
    assert cache.prune_expired() == 1
    assert cache.stats().entry_count == 0
