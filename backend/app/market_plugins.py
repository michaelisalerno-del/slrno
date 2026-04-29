from __future__ import annotations

from dataclasses import dataclass

from .market_registry import MarketMapping


@dataclass(frozen=True)
class BacktestProfile:
    default_timeframe: str
    spread_bps: float
    slippage_bps: float
    min_backtest_bars: int


@dataclass(frozen=True)
class MarketPlugin:
    plugin_id: str
    market_id: str
    name: str
    asset_class: str
    fmp_symbol: str
    ig_name: str
    ig_search_terms: tuple[str, ...]
    backtest_profile: BacktestProfile
    source_url: str
    notes: str

    def to_mapping(self) -> MarketMapping:
        return MarketMapping(
            market_id=self.market_id,
            name=self.name,
            asset_class=self.asset_class,
            fmp_symbol=self.fmp_symbol,
            ig_epic="",
            enabled=True,
            plugin_id=self.plugin_id,
            ig_name=self.ig_name,
            ig_search_terms=",".join(self.ig_search_terms),
            default_timeframe=self.backtest_profile.default_timeframe,
            spread_bps=self.backtest_profile.spread_bps,
            slippage_bps=self.backtest_profile.slippage_bps,
            min_backtest_bars=self.backtest_profile.min_backtest_bars,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "plugin_id": self.plugin_id,
            "market_id": self.market_id,
            "name": self.name,
            "asset_class": self.asset_class,
            "fmp_symbol": self.fmp_symbol,
            "ig_name": self.ig_name,
            "ig_search_terms": list(self.ig_search_terms),
            "backtest_profile": self.backtest_profile.__dict__,
            "source_url": self.source_url,
            "notes": self.notes,
        }


BUILT_IN_MARKET_PLUGINS = [
    MarketPlugin(
        plugin_id="ig-us-tech-100",
        market_id="NAS100",
        name="Nasdaq 100",
        asset_class="index",
        fmp_symbol="^NDX",
        ig_name="US Tech 100",
        ig_search_terms=("US Tech 100", "Nasdaq", "NASDAQ 100"),
        backtest_profile=BacktestProfile("5min", 2.0, 1.0, 750),
        source_url="https://www.ig.com/en/indices/markets-indices/us-tech-100",
        notes="IG publicly lists Nasdaq exposure as US Tech 100. If FMP intraday index data is not included in your plan, use the QQQ proxy plugin for research and re-test on IG prices later.",
    ),
    MarketPlugin(
        plugin_id="fmp-qqq-nasdaq-proxy",
        market_id="QQQ",
        name="QQQ Nasdaq 100 ETF proxy",
        asset_class="etf",
        fmp_symbol="QQQ",
        ig_name="US Tech 100",
        ig_search_terms=("US Tech 100", "Nasdaq", "QQQ"),
        backtest_profile=BacktestProfile("5min", 2.0, 1.0, 750),
        source_url="https://www.invesco.com/qqq-etf/en/home.html",
        notes="FMP stock/ETF plans commonly support QQQ intraday data. Use it as a Nasdaq research proxy only; it is not the same instrument as an IG spread-bet EPIC.",
    ),
    MarketPlugin(
        plugin_id="ig-us-500",
        market_id="US500",
        name="S&P 500",
        asset_class="index",
        fmp_symbol="^GSPC",
        ig_name="US 500",
        ig_search_terms=("US 500", "S&P 500", "SPX"),
        backtest_profile=BacktestProfile("5min", 2.0, 1.0, 750),
        source_url="https://www.ig.com/en/indices/markets-indices/us-spx-500",
        notes="IG publicly lists S&P 500 exposure as US 500. If FMP intraday index data is not included in your plan, use the SPY proxy plugin for research and re-test on IG prices later.",
    ),
    MarketPlugin(
        plugin_id="fmp-spy-sp500-proxy",
        market_id="SPY",
        name="SPY S&P 500 ETF proxy",
        asset_class="etf",
        fmp_symbol="SPY",
        ig_name="US 500",
        ig_search_terms=("US 500", "S&P 500", "SPY"),
        backtest_profile=BacktestProfile("5min", 2.0, 1.0, 750),
        source_url="https://www.ssga.com/us/en/intermediary/etfs/spdr-sp-500-etf-trust-spy",
        notes="FMP stock/ETF plans commonly support SPY intraday data. Use it as an S&P 500 research proxy only; it is not the same instrument as an IG spread-bet EPIC.",
    ),
    MarketPlugin(
        plugin_id="ig-spot-gold",
        market_id="XAUUSD",
        name="Spot Gold",
        asset_class="commodity",
        fmp_symbol="XAUUSD",
        ig_name="Spot Gold",
        ig_search_terms=("Spot Gold", "Gold", "XAU/USD"),
        backtest_profile=BacktestProfile("5min", 3.0, 1.5, 750),
        source_url="https://www.ig.com/en/commodities/gold-trading",
        notes="IG publicly offers spot gold and gold futures. Use IG market search to bind the exact spot-gold EPIC available to the account.",
    ),
]


def list_market_plugins() -> list[MarketPlugin]:
    return list(BUILT_IN_MARKET_PLUGINS)


def get_market_plugin(plugin_id: str) -> MarketPlugin | None:
    return next((plugin for plugin in BUILT_IN_MARKET_PLUGINS if plugin.plugin_id == plugin_id), None)
