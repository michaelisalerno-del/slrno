from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import Any

from .research_store import ResearchStore

FOLD_LIMIT = 12
CURVE_LIMIT = 120


def install_payload_compaction() -> None:
    if getattr(ResearchStore, "_slrno_payload_compaction_installed", False):
        return

    original_list_trials = ResearchStore.list_trials
    original_list_candidates = ResearchStore.list_candidates
    original_get_candidate = ResearchStore.get_candidate

    @wraps(original_list_trials)
    def list_trials(self: ResearchStore, run_id: int | None = None, limit: int | None = None) -> list[dict[str, object]]:
        return [_compact_trial(trial) for trial in original_list_trials(self, run_id, limit)]

    @wraps(original_list_candidates)
    def list_candidates(self: ResearchStore, run_id: int | None = None, limit: int | None = None) -> list[dict[str, object]]:
        return [_compact_candidate(candidate) for candidate in original_list_candidates(self, run_id, limit)]

    @wraps(original_get_candidate)
    def get_candidate(self: ResearchStore, candidate_id: int) -> dict[str, object] | None:
        candidate = original_get_candidate(self, candidate_id)
        return _compact_candidate(candidate) if candidate is not None else None

    _replace_method(ResearchStore, "list_trials", list_trials)
    _replace_method(ResearchStore, "list_candidates", list_candidates)
    _replace_method(ResearchStore, "get_candidate", get_candidate)
    ResearchStore._slrno_payload_compaction_installed = True


def _replace_method(cls: type[ResearchStore], name: str, method: Callable[..., Any]) -> None:
    setattr(cls, name, method)


def _compact_trial(trial: dict[str, object]) -> dict[str, object]:
    compacted = dict(trial)
    compacted["parameters"] = _compact_parameters(compacted.get("parameters"))
    compacted["backtest"] = _compact_backtest(compacted.get("backtest"))
    compacted["folds"] = _compact_folds(compacted.get("folds"))
    return compacted


def _compact_candidate(candidate: dict[str, object]) -> dict[str, object]:
    compacted = dict(candidate)
    audit = compacted.get("audit")
    if isinstance(audit, dict):
        compacted_audit = dict(audit)
        compacted_audit["fold_results"] = _compact_folds(compacted_audit.get("fold_results"))
        compacted_audit["backtest"] = _compact_backtest(compacted_audit.get("backtest"))
        nested_candidate = compacted_audit.get("candidate")
        if isinstance(nested_candidate, dict):
            compacted_nested = dict(nested_candidate)
            compacted_nested["parameters"] = _compact_parameters(compacted_nested.get("parameters"))
            compacted_audit["candidate"] = compacted_nested
        compacted["audit"] = compacted_audit
    return compacted


def _compact_backtest(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        return {}
    compacted = dict(value)
    for key in ("equity_curve", "drawdown_curve", "daily_pnl_curve", "compounded_projection_daily_pnl_curve"):
        values = compacted.get(key)
        if isinstance(values, list) and len(values) > CURVE_LIMIT:
            compacted[key] = _sample_values(values, CURVE_LIMIT)
    return compacted


def _compact_parameters(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        return {}
    compacted = dict(value)
    analysis = compacted.get("bar_pattern_analysis")
    if isinstance(analysis, dict):
        compacted["bar_pattern_analysis"] = _compact_bar_pattern_analysis(analysis)
    return compacted


def _compact_bar_pattern_analysis(value: dict[str, object]) -> dict[str, object]:
    compacted = dict(value)
    market_regime = compacted.get("market_regime")
    if isinstance(market_regime, dict):
        compacted["market_regime"] = _compact_market_regime(market_regime)
    gated = compacted.get("regime_gated_backtest")
    if isinstance(gated, dict):
        compacted["regime_gated_backtest"] = _compact_backtest(gated)
    return compacted


def _compact_market_regime(value: dict[str, object]) -> dict[str, object]:
    keys = ("schema", "bar_count", "current_regime", "regime_counts", "start", "end")
    return {key: value[key] for key in keys if key in value}


def _compact_folds(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    return [_fold_summary(fold) for fold in value[:FOLD_LIMIT] if isinstance(fold, dict)]


def _fold_summary(fold: dict[str, object]) -> dict[str, object]:
    return {
        "net_profit": fold.get("net_profit", 0),
        "gross_profit": fold.get("gross_profit", 0),
        "sharpe": fold.get("sharpe", 0),
        "max_drawdown": fold.get("max_drawdown", 0),
        "win_rate": fold.get("win_rate", 0),
        "trade_count": fold.get("trade_count", 0),
        "total_cost": fold.get("total_cost", 0),
        "cost_confidence": fold.get("cost_confidence", ""),
    }


def _sample_values(values: list[object], limit: int) -> list[object]:
    if len(values) <= limit:
        return values
    step = max(1, len(values) // limit)
    return values[::step][:limit]
