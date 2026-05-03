from __future__ import annotations

from app.market_registry import MarketMapping, MarketRegistry


def test_market_registry_upserts_and_lists_enabled_markets(tmp_path):
    registry = MarketRegistry(tmp_path / "markets.sqlite3")
    registry.upsert(
        MarketMapping(
            "GBPUSD",
            "GBP/USD",
            "forex",
            "GBPUSD.FOREX",
            "CS.D.GBPUSD.TODAY.IP",
            True,
            ig_name="GBP/USD",
            ig_search_terms="GBP/USD,GBPUSD",
        )
    )
    registry.upsert(MarketMapping("DISABLED", "Disabled", "index", "TEST.INDX", "", False))

    enabled = registry.list(enabled_only=True)

    assert [market.market_id for market in enabled] == ["GBPUSD"]
    assert registry.get("GBPUSD").ig_epic == "CS.D.GBPUSD.TODAY.IP"
    assert registry.get("GBPUSD").ig_search_terms == "GBP/USD,GBPUSD"


def test_market_registry_seeds_priority_ig_markets(tmp_path):
    registry = MarketRegistry(tmp_path / "markets.sqlite3")

    registry.seed_defaults()

    markets = {market.market_id: market for market in registry.list()}
    assert markets["NAS100"].ig_name == "US Tech 100"
    assert markets["NAS100"].eodhd_symbol == "NDX.INDX"
    assert markets["US500"].ig_name == "US 500"
    assert markets["XAUUSD"].asset_class == "commodity"
    assert markets["XAUUSD"].ig_epic == "CS.D.USCGC.TODAY.IP"
    assert markets["UK10Y"].eodhd_symbol == "UK10Y.GBOND"
    assert markets["KOSPI200"].default_timeframe == "1day"
    assert markets["KOSPI200"].enabled is True
    assert markets["SA40"].enabled is False
    assert markets["SA40"].ig_epic == "IX.D.SAF.DAILY.IP"
