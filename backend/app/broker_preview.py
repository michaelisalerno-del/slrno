from __future__ import annotations

from .capital import DAILY_LOSS_FRACTION, MAX_MARGIN_FRACTION, RISK_PER_TRADE_FRACTION


def broker_order_preview(
    market: dict[str, object],
    cost_profile: dict[str, object],
    side: str,
    stake: float,
    account_size: float,
    entry_price: float | None = None,
    stop: float | None = None,
    limit: float | None = None,
) -> dict[str, object]:
    normalized_side = side.upper()
    requested_stake = max(0.0, float(stake or 0.0))
    account_size = max(0.0, float(account_size or 0.0))
    min_deal_size = _positive_float(cost_profile.get("min_deal_size"))
    effective_stake = max(requested_stake, min_deal_size or requested_stake)
    entry = _entry_price(cost_profile, normalized_side, entry_price)
    margin_percent = _positive_float(cost_profile.get("margin_percent")) or _fallback_margin_percent(cost_profile, market)
    estimated_margin = abs(entry * effective_stake * margin_percent / 100)
    planned_risk = _planned_risk(entry, normalized_side, effective_stake, stop)
    risk_budget = account_size * RISK_PER_TRADE_FRACTION
    daily_loss_limit = account_size * DAILY_LOSS_FRACTION
    violations = _rule_violations(
        normalized_side,
        requested_stake,
        effective_stake,
        min_deal_size,
        entry,
        stop,
        limit,
        cost_profile,
        planned_risk,
        risk_budget,
        estimated_margin,
        account_size,
    )
    return {
        "live_ordering_enabled": False,
        "order_placement": "disabled",
        "preview_only": True,
        "market_id": market.get("market_id"),
        "market_name": market.get("name"),
        "epic": market.get("ig_epic") or cost_profile.get("epic") or "",
        "side": normalized_side,
        "requested_stake": round(requested_stake, 6),
        "effective_stake": round(effective_stake, 6),
        "entry_price": round(entry, 8),
        "stop": stop,
        "limit": limit,
        "account_size": round(account_size, 2),
        "risk_budget": round(risk_budget, 4),
        "daily_loss_limit": round(daily_loss_limit, 4),
        "planned_risk": round(planned_risk, 4),
        "estimated_margin": round(estimated_margin, 4),
        "margin_percent": round(margin_percent, 6),
        "min_deal_size": min_deal_size,
        "min_stop_distance": cost_profile.get("min_stop_distance"),
        "min_limit_distance": cost_profile.get("min_limit_distance"),
        "instrument_currency": cost_profile.get("instrument_currency", "GBP"),
        "account_currency": cost_profile.get("account_currency", "GBP"),
        "rule_violations": violations,
        "feasible": not violations,
    }


def _rule_violations(
    side: str,
    requested_stake: float,
    effective_stake: float,
    min_deal_size: float,
    entry: float,
    stop: float | None,
    limit: float | None,
    cost_profile: dict[str, object],
    planned_risk: float,
    risk_budget: float,
    estimated_margin: float,
    account_size: float,
) -> list[str]:
    violations: list[str] = []
    if side not in {"BUY", "SELL"}:
        violations.append("invalid_side")
    if requested_stake <= 0:
        violations.append("stake_must_be_positive")
    if min_deal_size and requested_stake < min_deal_size:
        violations.append("below_ig_min_deal_size")
    if stop is None:
        violations.append("stop_required_for_risk_preview")
    elif side == "BUY" and stop >= entry:
        violations.append("stop_not_below_entry")
    elif side == "SELL" and stop <= entry:
        violations.append("stop_not_above_entry")
    if stop is not None and _distance(entry, stop) < _positive_float(cost_profile.get("min_stop_distance")):
        violations.append("stop_distance_below_ig_minimum")
    if limit is not None:
        if side == "BUY" and limit <= entry:
            violations.append("limit_not_above_entry")
        elif side == "SELL" and limit >= entry:
            violations.append("limit_not_below_entry")
        if _distance(entry, limit) < _positive_float(cost_profile.get("min_limit_distance")):
            violations.append("limit_distance_below_ig_minimum")
    if planned_risk > risk_budget:
        violations.append("risk_budget_exceeded")
    if estimated_margin > account_size * MAX_MARGIN_FRACTION:
        violations.append("margin_too_large")
    if estimated_margin > account_size:
        violations.append("insufficient_account_for_margin")
    if effective_stake <= 0 or entry <= 0:
        violations.append("missing_price_or_stake")
    return list(dict.fromkeys(violations))


def _entry_price(cost_profile: dict[str, object], side: str, entry_price: float | None) -> float:
    if entry_price and entry_price > 0:
        return float(entry_price)
    bid = _positive_float(cost_profile.get("bid"))
    offer = _positive_float(cost_profile.get("offer"))
    if side == "BUY" and offer:
        return offer
    if side == "SELL" and bid:
        return bid
    if bid and offer:
        return (bid + offer) / 2
    return 1.0


def _planned_risk(entry: float, side: str, stake: float, stop: float | None) -> float:
    if stop is None:
        return 0.0
    if side == "SELL":
        return max(0.0, float(stop) - entry) * stake
    return max(0.0, entry - float(stop)) * stake


def _distance(left: float, right: float) -> float:
    return abs(float(left) - float(right))


def _positive_float(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return number if number > 0 else 0.0


def _fallback_margin_percent(cost_profile: dict[str, object], market: dict[str, object]) -> float:
    instrument_type = str(cost_profile.get("instrument_type") or market.get("asset_class") or "").lower()
    if "currenc" in instrument_type or "forex" in instrument_type:
        return 3.33
    if "commod" in instrument_type:
        return 10.0
    if "share" in instrument_type:
        return 20.0
    return 5.0
