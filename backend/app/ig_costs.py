from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .backtesting import BacktestConfig
from .market_registry import MarketMapping


@dataclass(frozen=True)
class IGCostProfile:
    market_id: str
    product_mode: str = "spread_bet"
    epic: str = ""
    name: str = ""
    instrument_type: str = ""
    instrument_currency: str = "GBP"
    account_currency: str = "GBP"
    bid: float | None = None
    offer: float | None = None
    spread_points: float | None = None
    spread_bps: float = 2.0
    slippage_bps: float = 1.0
    slippage_factor: float | None = None
    min_deal_size: float | None = None
    min_stop_distance: float | None = None
    min_limit_distance: float | None = None
    margin_percent: float | None = None
    guaranteed_stop_premium_points: float = 0.0
    overnight_admin_fee_annual: float = 0.03
    overnight_interest_annual: float = 0.0
    fx_conversion_bps: float = 80.0
    market_status: str = "unknown"
    source: str = "public_baseline"
    confidence: str = "ig_public_spread_baseline"
    validation_status: str = "needs_ig_price_validation"
    notes: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def public_ig_cost_profile(market: MarketMapping, account_currency: str = "GBP") -> IGCostProfile:
    is_proxy = market.market_id in {"QQQ", "SPY"} or market.plugin_id.startswith("fmp-")
    admin_fee = 0.01 if market.asset_class == "forex" else 0.03
    confidence = "fmp_proxy_ig_cost_envelope" if is_proxy else "ig_public_spread_baseline"
    notes = [
        "Uses the market registry spread/slippage as an IG UK spread-betting cost envelope.",
        "Sync IG costs after binding an exact EPIC to upgrade this profile.",
    ]
    if is_proxy:
        notes.append("This is an FMP proxy, not the exact tradable IG instrument.")
    return IGCostProfile(
        market_id=market.market_id,
        epic=market.ig_epic,
        name=market.ig_name or market.name,
        instrument_type=market.asset_class,
        instrument_currency=_default_currency(market),
        account_currency=account_currency or "GBP",
        spread_bps=max(0.0, market.spread_bps),
        slippage_bps=max(0.0, market.slippage_bps),
        overnight_admin_fee_annual=admin_fee,
        source="market_registry",
        confidence=confidence,
        notes=tuple(notes),
    )


def profile_from_ig_market(market: MarketMapping, payload: dict[str, Any], account_currency: str = "GBP") -> IGCostProfile:
    instrument = payload.get("instrument") or {}
    snapshot = payload.get("snapshot") or {}
    dealing_rules = payload.get("dealingRules") or {}
    bid = _optional_float(snapshot.get("bid"))
    offer = _optional_float(snapshot.get("offer") or snapshot.get("ask"))
    spread_points = _spread_points(bid, offer)
    midpoint = _midpoint(bid, offer)
    spread_bps = spread_points / midpoint * 10_000 if spread_points is not None and midpoint else market.spread_bps
    slippage_factor = _rule_value(instrument.get("slippageFactor"))
    min_deal_size = _rule_value(dealing_rules.get("minDealSize"))
    min_stop_distance = _rule_value(dealing_rules.get("minNormalStopOrLimitDistance"))
    min_limit_distance = _rule_value(dealing_rules.get("minStepDistance"))
    margin_percent = _margin_percent(instrument)
    guaranteed_stop_premium = _rule_value(instrument.get("limitedRiskPremium")) or float(snapshot.get("controlledRiskExtraSpread") or 0.0)
    currency = _instrument_currency(instrument) or _default_currency(market)
    slippage_bps = max(market.slippage_bps, float(slippage_factor or 0.0))
    notes = ["Fetched from IG /markets/{epic}; spread is based on the latest bid/offer snapshot when present."]
    if bid is None or offer is None:
        notes.append("IG returned market rules but no usable bid/offer snapshot, so registry spread bps remains in force.")
    return IGCostProfile(
        market_id=market.market_id,
        epic=str(instrument.get("epic") or market.ig_epic),
        name=str(instrument.get("name") or market.ig_name or market.name),
        instrument_type=str(instrument.get("type") or market.asset_class),
        instrument_currency=currency,
        account_currency=account_currency or "GBP",
        bid=bid,
        offer=offer,
        spread_points=spread_points,
        spread_bps=round(max(0.0, spread_bps), 6),
        slippage_bps=round(max(0.0, slippage_bps), 6),
        slippage_factor=slippage_factor,
        min_deal_size=min_deal_size,
        min_stop_distance=min_stop_distance,
        min_limit_distance=min_limit_distance,
        margin_percent=margin_percent,
        guaranteed_stop_premium_points=max(0.0, guaranteed_stop_premium),
        overnight_admin_fee_annual=0.01 if market.asset_class == "forex" else 0.03,
        fx_conversion_bps=80.0,
        market_status=str(snapshot.get("marketStatus") or "unknown"),
        source="ig_markets_epic",
        confidence="ig_live_epic_cost_profile" if bid is not None and offer is not None else "ig_live_epic_rules_no_spread",
        validation_status="ig_rules_synced_needs_price_validation",
        notes=tuple(notes),
    )


def backtest_config_from_profile(profile: IGCostProfile, starting_cash: float = 10_000.0, position_size: float = 1.0) -> BacktestConfig:
    return BacktestConfig(
        starting_cash=starting_cash,
        position_size=position_size,
        spread_bps=profile.spread_bps,
        slippage_bps=profile.slippage_bps,
        commission_bps=0.0,
        overnight_admin_fee_annual=profile.overnight_admin_fee_annual,
        overnight_interest_annual=profile.overnight_interest_annual,
        fx_conversion_bps=profile.fx_conversion_bps,
        guaranteed_stop_premium_points=profile.guaranteed_stop_premium_points,
        instrument_currency=profile.instrument_currency,
        account_currency=profile.account_currency,
        cost_confidence=profile.confidence,
    )


def profile_badge(profile: IGCostProfile | dict[str, object] | None) -> str:
    if profile is None:
        return "Needs IG price validation"
    confidence = profile.confidence if isinstance(profile, IGCostProfile) else str(profile.get("confidence", ""))
    return {
        "ig_live_epic_cost_profile": "IG live EPIC cost profile",
        "ig_live_epic_rules_no_spread": "IG EPIC rules, public spread fallback",
        "ig_public_spread_baseline": "IG public spread baseline",
        "fmp_proxy_ig_cost_envelope": "FMP proxy with IG cost envelope",
    }.get(confidence, "Needs IG price validation")


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _rule_value(value: object) -> float | None:
    if isinstance(value, dict):
        return _optional_float(value.get("value"))
    return _optional_float(value)


def _spread_points(bid: float | None, offer: float | None) -> float | None:
    if bid is None or offer is None:
        return None
    return round(max(0.0, offer - bid), 8)


def _midpoint(bid: float | None, offer: float | None) -> float | None:
    if bid is None or offer is None:
        return None
    midpoint = (bid + offer) / 2
    return midpoint if midpoint > 0 else None


def _margin_percent(instrument: dict[str, Any]) -> float | None:
    bands = instrument.get("marginDepositBands") or []
    if bands:
        return _optional_float((bands[0] or {}).get("margin") or (bands[0] or {}).get("marginFactor"))
    return _optional_float(instrument.get("margin"))


def _instrument_currency(instrument: dict[str, Any]) -> str:
    currencies = instrument.get("currencies") or []
    for currency in currencies:
        if currency.get("isDefault"):
            return str(currency.get("code") or "")
    if currencies:
        return str(currencies[0].get("code") or "")
    return ""


def _default_currency(market: MarketMapping) -> str:
    if market.asset_class == "forex" and len(market.fmp_symbol) >= 6:
        return market.fmp_symbol[3:6].upper()
    if market.market_id in {"US500", "NAS100", "QQQ", "SPY", "XAUUSD", "XAGUSD", "BRENT", "NATGAS"}:
        return "USD"
    if market.market_id in {"FTSE100"}:
        return "GBP"
    if market.market_id in {"DE40"}:
        return "EUR"
    return "GBP"
