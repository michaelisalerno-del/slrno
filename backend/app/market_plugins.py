from __future__ import annotations

from dataclasses import dataclass

from .market_registry import DEFAULT_MARKETS, MarketMapping


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
    eodhd_symbol: str
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
            eodhd_symbol=self.eodhd_symbol,
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
            "eodhd_symbol": self.eodhd_symbol,
            "ig_name": self.ig_name,
            "ig_search_terms": list(self.ig_search_terms),
            "estimated_spread_bps": self.backtest_profile.spread_bps,
            "estimated_slippage_bps": self.backtest_profile.slippage_bps,
            "backtest_profile": self.backtest_profile.__dict__,
            "source_url": self.source_url,
            "notes": self.notes,
        }


def _plugin_from_market(market: MarketMapping, source_url: str, notes: str) -> MarketPlugin:
    return MarketPlugin(
        plugin_id=_plugin_id(market),
        market_id=market.market_id,
        name=market.name,
        asset_class=market.asset_class,
        eodhd_symbol=market.eodhd_symbol,
        ig_name=market.ig_name,
        ig_search_terms=tuple(term.strip() for term in market.ig_search_terms.split(",") if term.strip()),
        backtest_profile=BacktestProfile(market.default_timeframe, market.spread_bps, market.slippage_bps, market.min_backtest_bars),
        source_url=source_url,
        notes=notes,
    )


def _plugin_id(market: MarketMapping) -> str:
    if market.plugin_id:
        return market.plugin_id
    if market.market_id == "XAUUSD":
        return "ig-spot-gold"
    return f"ig-{market.market_id.lower()}"


_SOURCE_URLS = {
    "NAS100": "https://www.ig.com/en/indices/markets-indices/us-tech-100",
    "US500": "https://www.ig.com/en/indices/markets-indices/us-spx-500",
    "FTSE100": "https://www.ig.com/uk/indices/markets-indices/ftse-100",
    "DE40": "https://www.ig.com/uk/indices/markets-indices/germany-40",
    "XAUUSD": "https://www.ig.com/en/commodities/gold-trading",
}


BUILT_IN_MARKET_PLUGINS = [
    _plugin_from_market(
        market,
        _SOURCE_URLS.get(market.market_id, "https://www.ig.com/uk/markets"),
        "Core liquid research market. Bind the account-specific IG EPIC, then sync costs before trusting promotion evidence.",
    )
    for market in DEFAULT_MARKETS
]


def list_market_plugins() -> list[MarketPlugin]:
    return list(BUILT_IN_MARKET_PLUGINS)


def get_market_plugin(plugin_id: str) -> MarketPlugin | None:
    return next((plugin for plugin in BUILT_IN_MARKET_PLUGINS if plugin.plugin_id == plugin_id), None)
