from __future__ import annotations

from dataclasses import asdict, dataclass

from .market_registry import MarketMapping


IG_SHARE_PRODUCT_DETAILS_URL = (
    "https://www.ig.com/uk/help-and-support/spread-betting-and-cfds-5064504a/"
    "products-markets-and-trading-hours-008e7240/shares-and-etf-spread-bet-product-details-a5a09b95"
)

UK_MAJOR_SHARE_IDS = {
    "AZN",
    "BAE",
    "BARC",
    "BP",
    "DGE",
    "GLEN",
    "GSK",
    "HSBA",
    "LLOY",
    "RR",
    "SHEL",
    "TSCO",
    "VOD",
}
US_MAJOR_SHARE_IDS = {"AAPL", "MSFT", "NVDA", "TSLA"}


@dataclass(frozen=True)
class ShareSpreadBetModel:
    asset_region: str
    spread_category: str
    dealing_spread_bps: float
    slippage_bps: float
    margin_percent: float
    contract_point_size: float
    min_stop_distance_percent: float
    guaranteed_stop_premium_percent: float
    dealing_hours: str
    price_unit: str
    source_url: str = IG_SHARE_PRODUCT_DETAILS_URL
    notes: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def share_spread_bet_model(market: MarketMapping) -> ShareSpreadBetModel | None:
    if market.asset_class.lower() != "share":
        return None
    region = _share_region(market)
    category = _share_category(market, region)
    spread_bps = _dealing_spread_bps(region, category)
    slippage_bps = max(float(market.slippage_bps or 0.0), _default_slippage_bps(spread_bps))
    min_stop_percent, guaranteed_premium_percent = _stop_defaults(region, category)
    contract_point_size = _contract_point_size(region)
    return ShareSpreadBetModel(
        asset_region=region,
        spread_category=category,
        dealing_spread_bps=spread_bps,
        slippage_bps=slippage_bps,
        margin_percent=20.0,
        contract_point_size=contract_point_size,
        min_stop_distance_percent=min_stop_percent,
        guaranteed_stop_premium_percent=guaranteed_premium_percent,
        dealing_hours=_dealing_hours(region),
        price_unit=_price_unit(region),
        notes=_notes(region, contract_point_size),
    )


def share_public_spread_bps(market: MarketMapping) -> float:
    model = share_spread_bet_model(market)
    if model is None:
        return float(market.spread_bps or 0.0)
    return max(float(market.spread_bps or 0.0), model.dealing_spread_bps)


def _share_region(market: MarketMapping) -> str:
    symbol = market.eodhd_symbol.upper()
    if symbol.endswith(".US"):
        return "us"
    if symbol.endswith(".LSE"):
        return "uk"
    if symbol.endswith((".PA", ".F", ".XETRA", ".MI", ".AS", ".BR", ".SW")):
        return "europe"
    return "international"


def _share_category(market: MarketMapping, region: str) -> str:
    market_id = market.market_id.upper()
    if region == "uk":
        return "major" if market_id in UK_MAJOR_SHARE_IDS else "other"
    if region == "us":
        return "major" if market_id in US_MAJOR_SHARE_IDS else "other"
    if region in {"europe", "japan", "australia", "canada"}:
        return "major"
    return "other"


def _dealing_spread_bps(region: str, category: str) -> float:
    if region == "uk" and category == "major":
        return 10.0
    if region == "uk":
        return 50.0
    if region == "us" and category == "major":
        return 10.0
    if region == "us":
        return 15.0
    if region in {"europe", "japan", "australia", "canada"} and category == "major":
        return 10.0
    if region == "south_africa":
        return 50.0
    return 25.0


def _default_slippage_bps(spread_bps: float) -> float:
    return max(5.0, spread_bps / 2)


def _contract_point_size(region: str) -> float:
    if region == "uk":
        return 1.0
    return 0.01


def _price_unit(region: str) -> str:
    if region == "uk":
        return "pence"
    if region == "us":
        return "dollars"
    if region == "europe":
        return "euros"
    return "local_currency"


def _stop_defaults(region: str, category: str) -> tuple[float, float]:
    if region == "us":
        return 10.0, 0.3
    if region == "uk" and category == "major":
        return 5.0, 0.3
    if region == "uk":
        return 12.5, 1.0
    return 7.5, 0.7


def _dealing_hours(region: str) -> str:
    if region == "uk":
        return "08:00-16:30 London"
    if region == "us":
        return "09:30-16:00 New York; some names may support extended hours"
    if region == "europe":
        return "Relevant local exchange hours"
    return "Relevant local exchange hours"


def _notes(region: str, contract_point_size: float) -> tuple[str, ...]:
    base = [
        "Share spread bets use share-specific spread percentages, margin, and point denominations.",
        "Retail share margin is modelled with a 20% minimum before any IG tiering or account-specific rule changes.",
        "Short positions can be limited by borrow availability, recalls, uptick rules, and short-sale restrictions.",
    ]
    if contract_point_size != 1.0:
        base.append("The backtester converts price moves into spread-bet points for non-UK share quotes.")
    if region == "us":
        base.append("US shares are modelled as one point per cent; FX conversion remains part of the cost model.")
    return tuple(base)
