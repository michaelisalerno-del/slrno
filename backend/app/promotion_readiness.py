from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from math import isfinite
from typing import Any


MIN_PROMOTION_SHARPE_DAYS = 120
LIVE_VALIDATED_COST_CONFIDENCE = "ig_live_epic_cost_profile"
RECENT_VALIDATED_COST_CONFIDENCE = "ig_recent_epic_price_profile"
PRICE_VALIDATED_COST_CONFIDENCES = {
    LIVE_VALIDATED_COST_CONFIDENCE,
    RECENT_VALIDATED_COST_CONFIDENCE,
}
ACCEPTED_COST_CONFIDENCES = {
    *PRICE_VALIDATED_COST_CONFIDENCES,
    "ig_live_epic_rules_no_spread",
    "ig_public_spread_baseline",
    "eodhd_ig_cost_envelope",
}
MOVE_FORWARD_TIERS = {"paper_candidate", "validated_candidate"}
STALE_DATA_WARNINGS = {"legacy_sharpe_diagnostics", "missing_cost_profile", "missing_spread_slippage"}
FRESH_SAMPLE_WARNINGS = {"short_sharpe_sample", "limited_sharpe_sample"}
VALIDATION_WARNING_CODES = {"needs_ig_price_validation"}
COST_BLOCKING_WARNINGS = {
    "negative_after_costs",
    "costs_overwhelm_edge",
    "weak_net_cost_efficiency",
    "high_turnover_cost_drag",
    "negative_expectancy_after_costs",
    "fails_higher_slippage",
}
ROBUSTNESS_BLOCKING_WARNINGS = {
    "too_few_trades",
    "low_oos_trades",
    "drawdown_too_high",
    "profits_not_consistent_across_folds",
    "weak_oos_evidence",
    "below_ig_min_deal_size",
    "one_fold_dependency",
    "calendar_effect_needs_longer_history",
    "known_edge_needs_cross_market_validation",
    "calendar_dependent_edge",
    "calendar_filtered_oos_negative",
    "calendar_sample_too_thin",
    "high_sharpe_low_trade_count",
    "high_sharpe_short_sample",
    "high_sharpe_weak_folds",
    "isolated_parameter_peak",
    "multiple_testing_haircut",
    "best_trades_dominate",
    "fails_normal_volatility_regime",
    "high_volatility_only_edge",
    "headline_sharpe_not_regime_robust",
    "insufficient_regime_sample",
    "profit_concentrated_single_month",
    "profit_concentrated_single_regime",
    "regime_gated_backtest_negative",
    "regime_gated_oos_negative",
    "shock_regime_dependency",
    "target_regime_low_oos_trades",
    "event_strategy_requires_label",
    "major_event_window_dependency",
    "historical_daily_loss_stop_breached",
    "historical_drawdown_too_large",
    "insufficient_account_for_margin",
    "margin_too_large",
    "missing_reference_price",
    "risk_budget_exceeded",
}


def promotion_readiness(
    backtest: object,
    warnings: object = (),
    parameters: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Return the hard gate state for moving a research candidate forward."""
    parameters = parameters or {}
    warning_list = _normalize_warnings(warnings)
    warning_set = set(warning_list)
    blockers: list[str] = []
    validation_warnings: list[str] = []

    sharpe_days = int(_number(backtest, "sharpe_observations"))
    if "legacy_sharpe_diagnostics" in warning_set or sharpe_days <= 0:
        blockers.append("legacy_sharpe_diagnostics")
    elif sharpe_days < 60:
        blockers.append("short_sharpe_sample")
    elif sharpe_days < MIN_PROMOTION_SHARPE_DAYS:
        blockers.append("limited_sharpe_sample")

    spread_bps = _first_positive(_value(backtest, "estimated_spread_bps"), parameters.get("estimated_spread_bps"))
    slippage_bps = _first_positive(_value(backtest, "estimated_slippage_bps"), parameters.get("estimated_slippage_bps"))
    if spread_bps <= 0.0 or slippage_bps <= 0.0:
        blockers.append("missing_spread_slippage")

    total_cost = _number(backtest, "total_cost")
    trade_count = int(_number(backtest, "trade_count"))
    if "missing_cost_profile" in warning_set or (trade_count > 0 and total_cost <= 0.0):
        blockers.append("missing_cost_profile")

    cost_confidence = _cost_confidence(backtest, parameters)
    if cost_confidence not in ACCEPTED_COST_CONFIDENCES:
        blockers.append("missing_cost_profile")
    elif cost_confidence not in PRICE_VALIDATED_COST_CONFIDENCES:
        validation_warnings.append("needs_ig_price_validation")

    for warning in sorted((COST_BLOCKING_WARNINGS | ROBUSTNESS_BLOCKING_WARNINGS | FRESH_SAMPLE_WARNINGS) & warning_set):
        if _regime_gate_allows_warning(warning, parameters):
            continue
        blockers.append(warning)
    for warning in sorted(VALIDATION_WARNING_CODES & warning_set):
        validation_warnings.append(warning)

    blockers = _unique(blockers)
    validation_warnings = [warning for warning in _unique(validation_warnings) if warning not in blockers]
    if blockers:
        status = "blocked"
    elif validation_warnings:
        status = "needs_ig_validation"
    else:
        status = "ready_for_paper"

    return {
        "status": status,
        "move_forward_ready": status == "ready_for_paper",
        "blockers": blockers,
        "validation_warnings": validation_warnings,
        "next_action": _next_action(blockers, validation_warnings),
        "checks": {
            "fresh_sharpe_days": sharpe_days >= MIN_PROMOTION_SHARPE_DAYS,
            "sharpe_observations": sharpe_days,
            "has_daily_sharpe": _number(backtest, "daily_pnl_sharpe") != 0.0 or sharpe_days > 0,
            "has_spread_slippage": spread_bps > 0.0 and slippage_bps > 0.0,
            "has_realistic_costs": total_cost > 0.0 if trade_count > 0 else True,
            "cost_confidence": cost_confidence,
            "ig_price_validated": cost_confidence in PRICE_VALIDATED_COST_CONFIDENCES,
        },
    }


def readiness_warnings(readiness: Mapping[str, object]) -> list[str]:
    return _unique(
        list(readiness.get("blockers") or [])
        + list(readiness.get("validation_warnings") or [])
    )


def gate_promotion_tier(tier: str, readiness: Mapping[str, object]) -> str:
    if tier in MOVE_FORWARD_TIERS and readiness.get("status") != "ready_for_paper":
        return "research_candidate"
    return tier


def _next_action(blockers: list[str], validation_warnings: list[str]) -> str:
    blocker_set = set(blockers)
    if blocker_set & (STALE_DATA_WARNINGS | FRESH_SAMPLE_WARNINGS):
        return "rerun_with_fresh_diagnostics"
    if blocker_set & COST_BLOCKING_WARNINGS:
        return "reject_or_rework_cost_edge"
    if blocker_set & ROBUSTNESS_BLOCKING_WARNINGS:
        return "retest_or_reject_fragile_edge"
    if "needs_ig_price_validation" in validation_warnings:
        return "sync_ig_costs_and_validate_prices"
    return "paper_track"


def _cost_confidence(backtest: object, parameters: Mapping[str, object]) -> str:
    raw = str(_value(backtest, "cost_confidence") or parameters.get("cost_confidence") or "")
    return raw.removesuffix("_stress")


def _regime_gate_allows_warning(warning: str, parameters: Mapping[str, object]) -> bool:
    if warning not in {"fails_normal_volatility_regime", "high_volatility_only_edge", "profit_concentrated_single_regime", "shock_regime_dependency"}:
        return False
    analysis = parameters.get("bar_pattern_analysis")
    if not isinstance(analysis, Mapping):
        return False
    return bool(analysis.get("target_regime")) and analysis.get("regime_verdict") == "regime_tradeable"


def _normalize_warnings(warnings: object) -> list[str]:
    if warnings is None:
        return []
    if isinstance(warnings, str):
        return [warnings]
    if isinstance(warnings, Mapping):
        return [str(key) for key, value in warnings.items() if value]
    try:
        return _unique(str(item) for item in warnings if item)
    except TypeError:
        return [str(warnings)]


def _value(payload: object, key: str) -> object:
    if isinstance(payload, Mapping):
        return payload.get(key)
    if is_dataclass(payload) and not isinstance(payload, type):
        return asdict(payload).get(key)
    return getattr(payload, key, None)


def _number(payload: object, key: str) -> float:
    try:
        value = float(_value(payload, key) or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return value if isfinite(value) else 0.0


def _first_positive(*values: object) -> float:
    for value in values:
        try:
            number = float(value or 0.0)
        except (TypeError, ValueError):
            continue
        if isfinite(number) and number > 0.0:
            return number
    return 0.0


def _unique(values: Any) -> list[str]:
    return list(dict.fromkeys(str(value) for value in values if value))
