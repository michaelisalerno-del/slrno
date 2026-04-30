from __future__ import annotations

import asyncio

import pytest

from app.fmp_cache import FMPCache
from app.fmp_proxy_fallbacks import install_fmp_proxy_fallbacks
from app.providers.base import OHLCBar
from app.providers.fmp import FMPProvider, FMPProviderError


def test_fmp_provider_retries_index_with_proxy_on_plan_gap(monkeypatch, tmp_path):
    calls: list[str] = []

    async def fixture_historical_bars(self: FMPProvider, symbol: str, interval: str, start: str, end: str) -> list[OHLCBar]:
        calls.append(symbol)
        if symbol == "^GDAXI":
            raise FMPProviderError("FMP historical bars returned HTTP 402 for ^GDAXI")
        return []

    monkeypatch.setattr(FMPProvider, "_slrno_proxy_fallbacks_installed", False, raising=False)
    monkeypatch.setattr(FMPProvider, "historical_bars", fixture_historical_bars)

    install_fmp_proxy_fallbacks()

    provider = FMPProvider("fixture-key", cache=FMPCache(tmp_path / "fmp_cache.sqlite3"), cache_enabled=False)
    assert asyncio.run(provider.historical_bars("^GDAXI", "5min", "2026-01-01", "2026-01-02")) == []
    assert calls == ["^GDAXI", "EWG"]


def test_fmp_provider_keeps_non_plan_errors(monkeypatch, tmp_path):
    async def fixture_historical_bars(self: FMPProvider, symbol: str, interval: str, start: str, end: str) -> list[OHLCBar]:
        raise FMPProviderError("FMP historical data request timed out after 30 seconds")

    monkeypatch.setattr(FMPProvider, "_slrno_proxy_fallbacks_installed", False, raising=False)
    monkeypatch.setattr(FMPProvider, "historical_bars", fixture_historical_bars)

    install_fmp_proxy_fallbacks()

    provider = FMPProvider("fixture-key", cache=FMPCache(tmp_path / "fmp_cache.sqlite3"), cache_enabled=False)
    with pytest.raises(FMPProviderError, match="timed out"):
        asyncio.run(provider.historical_bars("^GDAXI", "5min", "2026-01-01", "2026-01-02"))
