from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .backtesting import BacktestConfig
from .market_registry import MarketMapping
from .share_spread_betting import share_spread_bet_model, share_public_spread_bps


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
    reference_price: float | None = None
    spread_points: float | None = None
    spread_bps: float = 2.0
    slippage_bps: float = 1.0
    slippage_factor: float | None = None
    min_deal_size: float | None = None
    min_stop_distance: float | None = None
    min_limit_distance: float | None = None
    margin_percent: float | None = None
    contract_point_size: float = 1.0
    min_stop_distance_percent: float | None = None
    share_spread_category: str = ""
    share_region: str = ""
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
    admin_fee = 0.01 if market.asset_class == "forex" else 0.03
    share_model = share_spread_bet_model(market)
    spread_bps = max(0.0, market.spread_bps)
    slippage_bps = max(0.0, market.slippage_bps)
    margin_percent = None
    contract_point_size = 1.0
    min_stop_distance_percent = None
    share_spread_category = ""
    share_region = ""
    notes = [
        "Uses the market registry spread/slippage as an IG UK spread-betting cost envelope.",
        "Sync IG costs after binding an exact EPIC to upgrade this profile.",
    ]
    if share_model is not None:
        spread_bps = share_public_spread_bps(market)
        slippage_bps = share_model.slippage_bps
        margin_percent = share_model.margin_percent
        contract_point_size = share_model.contract_point_size
        min_stop_distance_percent = share_model.min_stop_distance_percent
        share_spread_category = share_model.spread_category
        share_region = share_model.asset_region
        notes = [
            "Uses IG share spread-bet product rules as the public cost envelope.",
            *share_model.notes,
            "Sync IG costs after binding an exact share EPIC to upgrade this profile.",
        ]
    return IGCostProfile(
        market_id=market.market_id,
        epic=market.ig_epic,
        name=market.ig_name or market.name,
        instrument_type=market.asset_class,
        instrument_currency=_default_currency(market),
        account_currency=account_currency or "GBP",
        spread_bps=spread_bps,
        slippage_bps=slippage_bps,
        margin_percent=margin_percent,
        contract_point_size=contract_point_size,
        min_stop_distance_percent=min_stop_distance_percent,
        share_spread_category=share_spread_category,
        share_region=share_region,
        overnight_admin_fee_annual=admin_fee,
        source="market_registry",
        confidence="ig_public_spread_baseline",
        notes=tuple(notes),
    )


def profile_from_ig_market(
    market: MarketMapping,
    payload: dict[str, Any],
    account_currency: str = "GBP",
    recent_price: dict[str, object] | None = None,
) -> IGCostProfile:
    instrument = payload.get("instrument") or {}
    snapshot = payload.get("snapshot") or {}
    dealing_rules = payload.get("dealingRules") or {}
    bid = _optional_float(snapshot.get("bid"))
    offer = _optional_float(snapshot.get("offer") or snapshot.get("ask"))
    recent_bid = _optional_float((recent_price or {}).get("bid"))
    recent_offer = _optional_float((recent_price or {}).get("offer"))
    if (bid is None or offer is None) and recent_bid is not None and recent_offer is not None:
        bid = recent_bid
        offer = recent_offer
    spread_points = _spread_points(bid, offer)
    midpoint = _midpoint(bid, offer)
    spread_bps = spread_points / midpoint * 10_000 if spread_points is not None and midpoint else market.spread_bps
    slippage_factor = _rule_value(instrument.get("slippageFactor"))
    min_deal_size = _rule_value(dealing_rules.get("minDealSize"))
    min_stop_distance = _rule_value(dealing_rules.get("minNormalStopOrLimitDistance"))
    min_limit_distance = _rule_value(dealing_rules.get("minStepDistance"))
    share_model = share_spread_bet_model(market)
    margin_percent = _margin_percent(instrument) or (share_model.margin_percent if share_model is not None else None)
    contract_point_size = share_model.contract_point_size if share_model is not None else 1.0
    guaranteed_stop_premium = _rule_value(instrument.get("limitedRiskPremium")) or float(snapshot.get("controlledRiskExtraSpread") or 0.0)
    currency = _instrument_currency(instrument) or _default_currency(market)
    slippage_bps = max(market.slippage_bps, float(slippage_factor or 0.0))
    notes = ["Fetched from IG /markets/{epic}; spread is based on the latest bid/offer snapshot when present."]
    if recent_bid is not None and recent_offer is not None and (snapshot.get("bid") is None or (snapshot.get("offer") or snapshot.get("ask")) is None):
        notes.append(
            "IG market details returned no live bid/offer, so recent IG /prices bid/ask history was used for price validation."
        )
        if recent_price and recent_price.get("snapshot_time"):
            notes.append(f"Recent IG price snapshot time: {recent_price.get('snapshot_time')}.")
    elif bid is None or offer is None:
        notes.append("IG returned market rules but no usable bid/offer snapshot, so registry spread bps remains in force.")
    confidence = "ig_live_epic_cost_profile" if snapshot.get("bid") is not None and (snapshot.get("offer") or snapshot.get("ask")) is not None else (
        "ig_recent_epic_price_profile" if bid is not None and offer is not None else "ig_live_epic_rules_no_spread"
    )
    return IGCostProfile(
        market_id=market.market_id,
        epic=str(instrument.get("epic") or market.ig_epic),
        name=str(instrument.get("name") or market.ig_name or market.name),
        instrument_type=str(instrument.get("type") or market.asset_class),
        instrument_currency=currency,
        account_currency=account_currency or "GBP",
        bid=bid,
        offer=offer,
        reference_price=midpoint,
        spread_points=spread_points,
        spread_bps=round(max(0.0, spread_bps), 6),
        slippage_bps=round(max(0.0, slippage_bps), 6),
        slippage_factor=slippage_factor,
        min_deal_size=min_deal_size,
        min_stop_distance=min_stop_distance,
        min_limit_distance=min_limit_distance,
        margin_percent=margin_percent,
        contract_point_size=contract_point_size,
        min_stop_distance_percent=share_model.min_stop_distance_percent if share_model is not None else None,
        share_spread_category=share_model.spread_category if share_model is not None else "",
        share_region=share_model.asset_region if share_model is not None else "",
        guaranteed_stop_premium_points=max(0.0, guaranteed_stop_premium),
        overnight_admin_fee_annual=0.01 if market.asset_class == "forex" else 0.03,
        fx_conversion_bps=80.0,
        market_status=str(snapshot.get("marketStatus") or "unknown"),
        source="ig_markets_epic",
        confidence=confidence,
        validation_status="ig_price_validated" if confidence != "ig_live_epic_rules_no_spread" else "ig_rules_synced_needs_price_validation",
        notes=tuple(notes),
    )


def backtest_config_from_profile(
    profile: IGCostProfile,
    starting_cash: float = 10_000.0,
    position_size: float = 1.0,
    compound_position_size: bool = False,
) -> BacktestConfig:
    return BacktestConfig(
        starting_cash=starting_cash,
        position_size=position_size,
        compound_position_size=compound_position_size,
        spread_bps=profile.spread_bps,
        slippage_bps=profile.slippage_bps,
        commission_bps=0.0,
        overnight_admin_fee_annual=profile.overnight_admin_fee_annual,
        overnight_interest_annual=profile.overnight_interest_annual,
        fx_conversion_bps=profile.fx_conversion_bps,
        guaranteed_stop_premium_points=profile.guaranteed_stop_premium_points,
        contract_point_size=profile.contract_point_size,
        instrument_currency=profile.instrument_currency,
        account_currency=profile.account_currency,
        cost_confidence=profile.confidence,
    )


def normalized_cost_profile_payload(market: MarketMapping, payload: dict[str, object], account_currency: str = "GBP") -> dict[str, object]:
    normalized = dict(payload)
    share_model = share_spread_bet_model(market)
    if share_model is None:
        return normalized
    public = public_ig_cost_profile(market, account_currency=account_currency).as_dict()
    confidence = str(normalized.get("confidence") or "")
    source = str(normalized.get("source") or "")
    public_like = confidence in {"", "ig_public_spread_baseline", "eodhd_ig_cost_envelope"} or source in {"", "market_registry"}
    if public_like:
        normalized["spread_bps"] = max(_optional_float(normalized.get("spread_bps")) or 0.0, float(public["spread_bps"]))
        normalized["slippage_bps"] = max(_optional_float(normalized.get("slippage_bps")) or 0.0, float(public["slippage_bps"]))
    for key in (
        "margin_percent",
        "contract_point_size",
        "min_stop_distance_percent",
        "share_spread_category",
        "share_region",
        "instrument_currency",
        "account_currency",
    ):
        if normalized.get(key) in (None, ""):
            normalized[key] = public.get(key)
    notes = list(normalized.get("notes") or [])
    repair_note = "Stored share cost profile was upgraded with share spread-bet point, margin, and spread defaults."
    if repair_note not in notes:
        notes.append(repair_note)
    normalized["notes"] = notes
    return normalized


def profile_badge(profile: IGCostProfile | dict[str, object] | None) -> str:
    if profile is None:
        return "Needs IG price validation"
    confidence = profile.confidence if isinstance(profile, IGCostProfile) else str(profile.get("confidence", ""))
    return {
        "ig_live_epic_cost_profile": "IG live EPIC cost profile",
        "ig_recent_epic_price_profile": "IG recent EPIC price profile",
        "ig_live_epic_rules_no_spread": "IG EPIC rules, public spread fallback",
        "ig_public_spread_baseline": "IG public spread baseline",
        "eodhd_ig_cost_envelope": "EODHD bars with IG cost envelope",
    }.get(confidence, "Needs IG price validation")


def select_ig_market_candidate(market: MarketMapping, candidates: list[dict[str, object]]) -> dict[str, object] | None:
    viable = [candidate for candidate in candidates if str(candidate.get("epic") or "").strip()]
    if not viable:
        return None
    if market.asset_class.lower() == "share":
        share_matches = [candidate for candidate in viable if _asset_class_matches("share", _normalize_search_text(candidate.get("type")))]
        if not share_matches:
            return None
        viable = share_matches
    return max(viable, key=lambda candidate: _ig_candidate_score(market, candidate))


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ig_candidate_score(market: MarketMapping, candidate: dict[str, object]) -> tuple[int, int]:
    candidate_name = _normalize_search_text(candidate.get("name"))
    candidate_type = _normalize_search_text(candidate.get("type"))
    phrases = [
        market.market_id,
        market.name,
        market.ig_name,
        *(part.strip() for part in market.ig_search_terms.split(",")),
    ]
    normalized_phrases = [_normalize_search_text(phrase) for phrase in phrases if _normalize_search_text(phrase)]
    score = 0
    if candidate_name in normalized_phrases:
        score += 100
    for phrase in normalized_phrases:
        if phrase and (phrase in candidate_name or candidate_name in phrase):
            score += 40
    market_tokens = set(" ".join(normalized_phrases).split())
    candidate_tokens = set(candidate_name.split())
    score += 5 * len(market_tokens & candidate_tokens)
    if _asset_class_matches(market.asset_class, candidate_type):
        score += 15
    # Prefer less generic names when scores tie.
    return score, len(candidate_name)


def _normalize_search_text(value: object) -> str:
    return " ".join("".join(character.lower() if character.isalnum() else " " for character in str(value or "")).split())


def _asset_class_matches(asset_class: str, candidate_type: str) -> bool:
    asset = asset_class.lower()
    candidate = candidate_type.lower()
    if asset == "forex":
        return "currenc" in candidate or "forex" in candidate
    if asset == "index":
        return "indice" in candidate or "index" in candidate
    if asset == "commodity":
        return "commod" in candidate
    if asset == "share":
        return "share" in candidate or "equit" in candidate
    return bool(asset and asset in candidate)


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
    symbol = market.eodhd_symbol.split(".")[0]
    if market.asset_class == "forex" and len(symbol) >= 6:
        return symbol[3:6].upper()
    if market.market_id in {"US500", "NAS100", "US30", "RUSSELL2000", "JP225", "HK50", "AUS200", "VIX", "XAUUSD", "XAGUSD", "BRENT", "WTI", "NATGAS", "COPPER"}:
        return "USD"
    if market.market_id in {"FTSE100"}:
        return "GBP"
    if market.market_id in {"DE40", "FR40", "EU50"}:
        return "EUR"
    if market.asset_class == "share":
        symbol = market.eodhd_symbol.upper()
        if symbol.endswith(".US"):
            return "USD"
        if symbol.endswith(".LSE"):
            return "GBP"
        if symbol.endswith((".PA", ".F", ".XETRA", ".MI", ".AS", ".BR", ".SW")):
            return "EUR"
    return "GBP"
