from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date
from math import erf, log, sqrt
from random import Random

from .bar_patterns import analyze_strategy_patterns, eligible_specialist_regimes, gate_signals_to_regimes, market_regime_context
from .backtesting import BacktestConfig, BacktestResult, run_vector_backtest
from .capital import WORKING_ACCOUNT_SIZE_GBP
from .ig_costs import IGCostProfile, backtest_config_from_profile, profile_badge
from .promotion_readiness import LIVE_VALIDATED_COST_CONFIDENCE, MIN_PROMOTION_SHARPE_DAYS
from .providers.base import OHLCBar
from .research_lab import CandidateEvaluation, WalkForwardConfig, WalkForwardFold, walk_forward_splits
from .research_labels import TripleBarrierConfig, triple_barrier_labels
from .research_metrics import ClassificationMetrics, classification_metrics
from .research_strategies import ProbabilityCandidate


SEARCH_PRESETS = {
    "quick": 18,
    "balanced": 54,
    "deep": 120,
}
REGIME_SCAN_PRESET_BUDGETS = {
    "quick": 6,
    "balanced": 12,
    "deep": 18,
}
REGIME_SCAN_HARD_CAP = 96
MAX_WALK_FORWARD_FOLDS = 36

STYLE_FAMILIES = {
    "find_anything_robust": (
        "intraday_trend",
        "swing_trend",
        "calendar_turnaround_tuesday",
        "breakout",
        "liquidity_sweep_reversal",
        "month_end_seasonality",
        "mean_reversion",
        "volatility_expansion",
        "intraday_trend",
        "swing_trend",
    ),
    "intraday_only": ("intraday_trend", "breakout", "liquidity_sweep_reversal", "mean_reversion", "volatility_expansion", "intraday_trend", "scalping"),
    "swing_trades": ("swing_trend", "breakout", "liquidity_sweep_reversal", "mean_reversion"),
    "lower_drawdown": ("mean_reversion", "liquidity_sweep_reversal", "intraday_trend", "volatility_expansion"),
    "higher_profit": ("breakout", "liquidity_sweep_reversal", "swing_trend", "intraday_trend"),
    "research_ideas": ("calendar_turnaround_tuesday", "month_end_seasonality", "calendar_turnaround_tuesday", "month_end_seasonality"),
}

CALENDAR_FAMILIES = {"calendar_turnaround_tuesday", "month_end_seasonality"}

ENGINE_DEFINITIONS = [
    {
        "id": "probability_stack_v1",
        "label": "Probability stack v1",
        "description": "Triple-barrier labels with momentum, pullback, and breakout probability modules.",
    },
    {
        "id": "adaptive_ig_v1",
        "label": "Adaptive IG-aware search",
        "description": "Searches trading styles and risk settings, then ranks after IG spread-betting costs, slippage, and funding.",
    },
]


@dataclass(frozen=True)
class AdaptiveSearchConfig:
    preset: str = "balanced"
    trading_style: str = "find_anything_robust"
    objective: str = "balanced"
    search_budget: int | None = None
    risk_profile: str = "balanced"
    strategy_families: tuple[str, ...] = ()
    cost_stress_multiplier: float = 2.0
    include_regime_scans: bool = False
    regime_scan_budget_per_regime: int | None = None
    target_regime: str | None = None
    repair_mode: str = "standard"
    seed: int = 7


@dataclass(frozen=True)
class AdaptiveSearchResult:
    evaluations: tuple[CandidateEvaluation, ...]
    pareto: tuple[dict[str, object], ...]
    cost_profile: IGCostProfile
    regime_scan: dict[str, object] = field(default_factory=dict)


def available_research_engines() -> list[dict[str, object]]:
    return list(ENGINE_DEFINITIONS)


def run_adaptive_search(
    bars: list[OHLCBar],
    market_id: str,
    timeframe: str,
    cost_profile: IGCostProfile,
    config: AdaptiveSearchConfig | None = None,
) -> AdaptiveSearchResult:
    config = config or AdaptiveSearchConfig()
    if len(bars) < 40:
        raise ValueError("adaptive search needs at least 40 bars")
    budget = config.search_budget or SEARCH_PRESETS.get(config.preset, SEARCH_PRESETS["balanced"])
    budget = max(6, min(500, budget))
    families = config.strategy_families or STYLE_FAMILIES.get(config.trading_style, STYLE_FAMILIES["find_anything_robust"])
    rng = Random(_stable_seed(market_id, timeframe, config.seed, config.trading_style))
    labels = triple_barrier_labels(bars, TripleBarrierConfig())
    backtest_base = backtest_config_from_profile(
        cost_profile,
        starting_cash=WORKING_ACCOUNT_SIZE_GBP,
        compound_position_size=True,
    )
    folds = _adaptive_folds(len(bars))
    evaluations: list[CandidateEvaluation] = []
    market_regime, regime_by_date = market_regime_context(bars)
    eligible_regimes = eligible_specialist_regimes(bars)
    target_regime = _target_regime(config.target_regime)

    for trial_index in range(budget):
        family = families[trial_index % len(families)]
        parameters = _sample_parameters(rng, family, config.risk_profile, trial_index)
        raw_signals = _generate_signals(bars, family, parameters)
        signals = gate_signals_to_regimes(bars, raw_signals, {target_regime}, regime_by_date=regime_by_date) if target_regime else raw_signals
        backtest_config = replace(backtest_base, position_size=float(parameters["position_size"]))
        backtest = run_vector_backtest(bars, signals, backtest_config)
        fold_results = tuple(_fold_backtests(bars, signals, backtest_config, folds))
        evidence_profile = _evidence_profile(backtest, fold_results)
        stress = run_vector_backtest(
            bars,
            signals,
            replace(
                backtest_config,
                cost_stress_multiplier=max(1.0, config.cost_stress_multiplier),
                cost_confidence=f"{cost_profile.confidence}_stress",
            ),
        )
        probabilities = _signals_to_probabilities(signals)
        metrics = classification_metrics(labels, probabilities, top_quantile=0.2)
        score = balanced_score(backtest, fold_results, stress, backtest_config)
        pattern_analysis = analyze_strategy_patterns(
            bars,
            signals,
            backtest_config,
            backtest,
            target_regime=target_regime,
            market_regime=market_regime,
            regime_by_date=regime_by_date,
        )
        warnings = tuple(
            sorted(
                set(_warnings(backtest, fold_results, stress, backtest_config, family, cost_profile)).union(
                    str(warning) for warning in pattern_analysis.get("warnings", [])
                )
            )
        )
        parameters = {
            **parameters,
            "market_id": market_id,
            "timeframe": timeframe,
            "family": family,
            "style": config.trading_style,
            "objective": config.objective,
            "cost_confidence": cost_profile.confidence,
            "cost_badge": profile_badge(cost_profile),
            "estimated_spread_bps": cost_profile.spread_bps,
            "estimated_slippage_bps": cost_profile.slippage_bps,
            "stress_net_profit": round(stress.net_profit, 4),
            "stress_sharpe": round(stress.sharpe, 4),
            "repair_mode": config.repair_mode,
            "evidence_profile": evidence_profile,
            "bar_pattern_analysis": pattern_analysis,
        }
        if target_regime:
            parameters["target_regime"] = target_regime
            parameters["regime_targeted_refine"] = True
        candidate = ProbabilityCandidate(
            name=f"{config.trading_style}_{target_regime}_{family}_{trial_index + 1}" if target_regime else f"{config.trading_style}_{family}_{trial_index + 1}",
            module_stack=("adaptive_ig_v1", config.trading_style, "regime_targeted", target_regime, family)
            if target_regime
            else ("adaptive_ig_v1", config.trading_style, family),
            parameters=parameters,
            probabilities=probabilities,
        )
        evaluations.append(
            CandidateEvaluation(
                candidate=candidate,
                metrics=metrics,
                backtest=backtest,
                fold_results=fold_results,
                robustness_score=score,
                passed=len(warnings) == 0,
                warnings=warnings,
                research_only=True,
            )
        )

    regime_scan_trial_count = 0
    if config.include_regime_scans:
        per_regime_budget = _regime_scan_budget(config)
        for regime_info in eligible_regimes:
            if regime_scan_trial_count >= REGIME_SCAN_HARD_CAP:
                break
            target_regime = str(regime_info["regime"])
            for scan_index in range(per_regime_budget):
                if regime_scan_trial_count >= REGIME_SCAN_HARD_CAP:
                    break
                global_index = budget + regime_scan_trial_count
                family = families[global_index % len(families)]
                parameters = _sample_parameters(rng, family, config.risk_profile, global_index)
                raw_signals = _generate_signals(bars, family, parameters)
                signals = gate_signals_to_regimes(bars, raw_signals, {target_regime}, regime_by_date=regime_by_date)
                backtest_config = replace(backtest_base, position_size=float(parameters["position_size"]))
                backtest = run_vector_backtest(bars, signals, backtest_config)
                fold_results = tuple(_fold_backtests(bars, signals, backtest_config, folds))
                evidence_profile = _evidence_profile(backtest, fold_results)
                stress = run_vector_backtest(
                    bars,
                    signals,
                    replace(
                        backtest_config,
                        cost_stress_multiplier=max(1.0, config.cost_stress_multiplier),
                        cost_confidence=f"{cost_profile.confidence}_stress",
                    ),
                )
                probabilities = _signals_to_probabilities(signals)
                metrics = classification_metrics(labels, probabilities, top_quantile=0.2)
                score = balanced_score(backtest, fold_results, stress, backtest_config)
                pattern_analysis = analyze_strategy_patterns(
                    bars,
                    signals,
                    backtest_config,
                    backtest,
                    target_regime=target_regime,
                    market_regime=market_regime,
                    regime_by_date=regime_by_date,
                )
                warnings = tuple(
                    sorted(
                        set(_warnings(backtest, fold_results, stress, backtest_config, family, cost_profile)).union(
                            str(warning) for warning in pattern_analysis.get("warnings", [])
                        )
                    )
                )
                parameters = {
                    **parameters,
                    "market_id": market_id,
                    "timeframe": timeframe,
                    "family": family,
                    "style": config.trading_style,
                    "objective": config.objective,
                    "regime_scan": True,
                    "target_regime": target_regime,
                    "cost_confidence": cost_profile.confidence,
                    "cost_badge": profile_badge(cost_profile),
                    "estimated_spread_bps": cost_profile.spread_bps,
                    "estimated_slippage_bps": cost_profile.slippage_bps,
                    "stress_net_profit": round(stress.net_profit, 4),
                    "stress_sharpe": round(stress.sharpe, 4),
                    "repair_mode": config.repair_mode,
                    "evidence_profile": evidence_profile,
                    "bar_pattern_analysis": pattern_analysis,
                }
                candidate = ProbabilityCandidate(
                    name=f"{config.trading_style}_{target_regime}_{family}_{scan_index + 1}",
                    module_stack=("adaptive_ig_v1", config.trading_style, "regime_specialist", target_regime, family),
                    parameters=parameters,
                    probabilities=probabilities,
                )
                evaluations.append(
                    CandidateEvaluation(
                        candidate=candidate,
                        metrics=metrics,
                        backtest=backtest,
                        fold_results=fold_results,
                        robustness_score=score,
                        passed=len(warnings) == 0,
                        warnings=warnings,
                        research_only=True,
                    )
                )
                regime_scan_trial_count += 1

    total_trials = len(evaluations)
    ranked = _annotate_evaluations(tuple(evaluations), total_trials, tuple(families), config, cost_profile)
    regime_scan = {
        "enabled": config.include_regime_scans,
        "eligible_regimes": eligible_regimes,
        "trial_count": regime_scan_trial_count,
        "budget_per_regime": _regime_scan_budget(config) if config.include_regime_scans else 0,
        "hard_cap": REGIME_SCAN_HARD_CAP,
        "target_regime": target_regime,
    }
    return AdaptiveSearchResult(ranked, _pareto(ranked), cost_profile, regime_scan)


def balanced_score(
    backtest: BacktestResult,
    folds: tuple[BacktestResult, ...],
    stress: BacktestResult,
    config: BacktestConfig,
) -> float:
    profit_target = max(250.0, config.starting_cash * 0.04)
    profit_component = _clamp(backtest.test_profit / profit_target, 0.0, 1.0)
    stress_component = _clamp(stress.net_profit / profit_target, 0.0, 1.0)
    net_cost_component = _clamp(backtest.net_cost_ratio, 0.0, 1.0)
    expectancy_component = _expectancy_efficiency(backtest)
    sharpe_component = _clamp(backtest.daily_pnl_sharpe / 2.0, 0.0, 1.0)
    fold_component = _positive_fold_rate(folds)
    drawdown_component = 1.0 - _clamp(backtest.max_drawdown / max(1.0, config.starting_cash * 0.35), 0.0, 1.0)
    churn_penalty = _churn_penalty(backtest)
    return round(
        100
        * (
            0.30 * profit_component
            + 0.18 * stress_component
            + 0.17 * net_cost_component
            + 0.12 * expectancy_component
            + 0.10 * sharpe_component
            + 0.08 * fold_component
            + 0.05 * drawdown_component
            - 0.25 * churn_penalty
        ),
        4,
    )


def _regime_scan_budget(config: AdaptiveSearchConfig) -> int:
    preset_budget = REGIME_SCAN_PRESET_BUDGETS.get(config.preset, REGIME_SCAN_PRESET_BUDGETS["balanced"])
    requested = config.regime_scan_budget_per_regime
    if requested is None:
        return preset_budget
    return max(1, min(preset_budget, int(requested)))


def _target_regime(value: str | None) -> str | None:
    text = str(value or "").strip()
    return text or None


def _sample_parameters(rng: Random, family: str, risk_profile: str, trial_index: int) -> dict[str, float | int | str]:
    lookbacks = {
        "scalping": (3, 5, 8, 12),
        "intraday_trend": (8, 12, 16, 24, 36),
        "breakout": (12, 20, 32, 48),
        "liquidity_sweep_reversal": (12, 20, 32, 48, 72),
        "mean_reversion": (10, 16, 24, 36),
        "volatility_expansion": (12, 18, 30, 42),
        "swing_trend": (48, 72, 96, 144),
        "calendar_turnaround_tuesday": (1,),
        "month_end_seasonality": (1,),
    }.get(family, (12, 24, 36))
    direction = "long_only" if family in CALENDAR_FAMILIES else ("long_only", "short_only", "long_short")[trial_index % 3]
    risk_scale = {"conservative": 0.7, "balanced": 1.0, "aggressive": 1.35}.get(risk_profile, 1.0)
    parameters: dict[str, float | int | str] = {
        "lookback": rng.choice(lookbacks),
        "threshold_bps": round(rng.choice(_threshold_choices(family)) * risk_scale, 2),
        "z_threshold": round(rng.uniform(0.65, 2.2), 3),
        "volatility_multiplier": round(rng.uniform(1.1, 2.8), 3),
        "stop_loss_bps": round(rng.choice((18, 25, 35, 50, 75, 110)) * risk_scale, 2),
        "take_profit_bps": round(rng.choice((20, 35, 55, 80, 130, 180)) * risk_scale, 2),
        "max_hold_bars": rng.choice(_max_hold_choices(family)),
        "min_hold_bars": rng.choice((3, 5, 8, 12) if family in CALENDAR_FAMILIES else (2, 3, 5, 8)),
        "min_trade_spacing": rng.choice(_spacing_choices(family)),
        "confidence_quantile": 1.0 if family in CALENDAR_FAMILIES else rng.choice((0.1, 0.15, 0.2, 0.25, 0.3, 0.4)),
        "regime_filter": rng.choice(("any", "trend", "volatile", "calm")),
        "false_breakout_filter": rng.choice((0, 1)),
        "position_size": round(rng.choice((0.5, 1.0, 1.5, 2.0)) * risk_scale, 2),
        "direction": direction,
    }
    if family == "calendar_turnaround_tuesday":
        parameters.update(
            {
                "lookback": 1,
                "weekday": 1,
                "previous_day_filter": rng.choice(("monday_down", "any_down")),
                "confidence_quantile": 1.0,
                "regime_filter": "any",
                "false_breakout_filter": 0,
                "research_recipe": "turnaround_tuesday_after_down_previous_session",
                "known_edge_reference": "public-paper-style calendar anomaly; validate across markets before paper use",
            }
        )
    elif family == "month_end_seasonality":
        parameters.update(
            {
                "lookback": 1,
                "month_end_window": rng.choice((1, 2, 3, 4, 5)),
                "month_start_window": rng.choice((0, 1, 2)),
                "confidence_quantile": 1.0,
                "regime_filter": "any",
                "false_breakout_filter": 0,
                "research_recipe": "turn_of_month_long_bias",
                "known_edge_reference": "end-of-month seasonality idea; validate across assets and history depth",
            }
        )
    elif family == "liquidity_sweep_reversal":
        parameters.update(
            {
                "price_action_model": "bar_only_support_resistance_sweep_reclaim",
                "orderflow_limitation": "No footprint delta or order-book liquidity is available from OHLC bars; this only tests the visible price-action sweep/reclaim pattern.",
            }
        )
    return parameters


def _generate_signals(bars: list[OHLCBar], family: str, parameters: dict[str, float | int | str]) -> list[int]:
    closes = [bar.close for bar in bars]
    lookback = int(parameters["lookback"])
    threshold = float(parameters["threshold_bps"]) / 10_000
    threshold_bps = float(parameters["threshold_bps"])
    z_threshold = float(parameters["z_threshold"])
    previous_day_returns, previous_trading_dates = _previous_day_context(bars) if family == "calendar_turnaround_tuesday" else ({}, {})
    month_positions = _month_position_by_date(bars) if family == "month_end_seasonality" else {}
    raw: list[int] = []
    confidences: list[float] = []
    for index, bar in enumerate(bars):
        signal = 0
        confidence = 0.0
        if index >= lookback:
            if family in {"intraday_trend", "swing_trend"}:
                move = (bar.close - closes[index - lookback]) / closes[index - lookback]
                signal = 1 if move > threshold else -1 if move < -threshold else 0
                confidence = abs(move) / max(threshold, 1e-12)
            elif family == "breakout":
                window = bars[index - lookback : index]
                high = max(item.high for item in window)
                low = min(item.low for item in window)
                long_break = bar.close > high * (1 + threshold)
                short_break = bar.close < low * (1 - threshold)
                if int(parameters.get("false_breakout_filter", 0)):
                    long_break = long_break and bar.close >= bar.open
                    short_break = short_break and bar.close <= bar.open
                signal = 1 if long_break else -1 if short_break else 0
                breakout_distance = max((bar.close - high) / max(high, 1e-12), (low - bar.close) / max(low, 1e-12), 0.0)
                confidence = breakout_distance / max(threshold, 1e-12)
            elif family == "liquidity_sweep_reversal":
                window = bars[index - lookback : index]
                support = min(item.low for item in window)
                resistance = max(item.high for item in window)
                swept_support = bar.low < support * (1 - threshold)
                reclaimed_support = bar.close > support and bar.close >= bar.open
                swept_resistance = bar.high > resistance * (1 + threshold)
                rejected_resistance = bar.close < resistance and bar.close <= bar.open
                signal = 1 if swept_support and reclaimed_support else -1 if swept_resistance and rejected_resistance else 0
                sweep_distance = max((support - bar.low) / max(support, 1e-12), (bar.high - resistance) / max(resistance, 1e-12), 0.0)
                body_reclaim = abs(bar.close - bar.open) / max(bar.close, 1e-12)
                confidence = (sweep_distance + body_reclaim) / max(threshold, 1e-12)
            elif family == "mean_reversion":
                zscore = _zscore(closes[index - lookback : index], bar.close)
                signal = 1 if zscore < -z_threshold else -1 if zscore > z_threshold else 0
                confidence = abs(zscore) / max(z_threshold, 1e-12)
            elif family == "volatility_expansion":
                average_range = sum((item.high - item.low) / max(item.close, 1e-12) for item in bars[index - lookback : index]) / lookback
                current_range = (bar.high - bar.low) / max(bar.close, 1e-12)
                if current_range > average_range * float(parameters["volatility_multiplier"]):
                    signal = 1 if bar.close >= bar.open else -1
                confidence = current_range / max(average_range * float(parameters["volatility_multiplier"]), 1e-12)
            elif family == "scalping":
                one_bar = (bar.close - bars[index - 1].close) / bars[index - 1].close
                signal = -1 if one_bar > threshold else 1 if one_bar < -threshold else 0
                confidence = abs(one_bar) / max(threshold, 1e-12)
            elif family == "calendar_turnaround_tuesday":
                current_date = bar.timestamp.date()
                previous_return_bps = previous_day_returns.get(current_date)
                previous_trading_date = previous_trading_dates.get(current_date)
                previous_filter = str(parameters.get("previous_day_filter", "monday_down"))
                weekday_match = bar.timestamp.weekday() == int(parameters.get("weekday", 1))
                previous_day_match = previous_filter != "monday_down" or (previous_trading_date is not None and previous_trading_date.weekday() == 0)
                if previous_return_bps is not None and weekday_match and previous_day_match and previous_return_bps <= -threshold_bps:
                    signal = 1
                    confidence = min(8.0, abs(previous_return_bps) / max(threshold_bps, 1e-12))
            elif family == "month_end_seasonality":
                month_position = month_positions.get(bar.timestamp.date())
                if month_position is not None:
                    days_from_month_start, days_to_month_end = month_position
                    end_window = int(parameters.get("month_end_window", 3))
                    start_window = int(parameters.get("month_start_window", 0))
                    if days_to_month_end < end_window or days_from_month_start < start_window:
                        signal = 1
                        confidence = 1.0 + max(0.0, (end_window - days_to_month_end) / max(1, end_window))
            signal = _apply_regime_filter(bars, index, signal, parameters)
        raw.append(_apply_direction(signal, str(parameters["direction"])))
        confidences.append(confidence if signal else 0.0)
    raw = _apply_confidence_gate(raw, confidences, float(parameters.get("confidence_quantile", 1.0)))
    return _apply_risk_controls(bars, raw, parameters)


def _previous_day_context(bars: list[OHLCBar]) -> tuple[dict[date, float], dict[date, date]]:
    closes_by_date: dict[date, float] = {}
    for bar in bars:
        closes_by_date[bar.timestamp.date()] = bar.close
    ordered_dates = sorted(closes_by_date)
    previous_dates: dict[date, date] = {}
    previous_returns: dict[date, float] = {}
    for index in range(1, len(ordered_dates)):
        current = ordered_dates[index]
        previous = ordered_dates[index - 1]
        previous_dates[current] = previous
        if index < 2:
            continue
        prior = ordered_dates[index - 2]
        prior_close = closes_by_date[prior]
        if prior_close > 0:
            previous_returns[current] = (closes_by_date[previous] - prior_close) / prior_close * 10_000
    return previous_returns, previous_dates


def _month_position_by_date(bars: list[OHLCBar]) -> dict[date, tuple[int, int]]:
    month_dates: dict[tuple[int, int], list[date]] = {}
    for current_date in sorted({bar.timestamp.date() for bar in bars}):
        month_dates.setdefault((current_date.year, current_date.month), []).append(current_date)
    positions: dict[date, tuple[int, int]] = {}
    for dates in month_dates.values():
        final_index = len(dates) - 1
        for index, current_date in enumerate(dates):
            positions[current_date] = (index, final_index - index)
    return positions


def _apply_risk_controls(bars: list[OHLCBar], raw: list[int], parameters: dict[str, float | int | str]) -> list[int]:
    position = 0
    entry = 0.0
    hold = 0
    bars_since_trade = 10_000
    signals: list[int] = []
    for index, bar in enumerate(bars):
        desired = raw[index]
        if position != 0 and entry > 0:
            move_bps = (bar.close - entry) / entry * 10_000 * position
            if move_bps <= -float(parameters["stop_loss_bps"]) or move_bps >= float(parameters["take_profit_bps"]) or hold >= int(parameters["max_hold_bars"]):
                position = 0
                entry = 0.0
                hold = 0
                bars_since_trade = 0
        if desired != 0 and bars_since_trade >= int(parameters.get("min_trade_spacing", 0)):
            if position == 0:
                position = desired
                entry = bar.close
                hold = 0
                bars_since_trade = 0
            elif desired != position and hold >= int(parameters["min_hold_bars"]):
                position = desired
                entry = bar.close
                hold = 0
                bars_since_trade = 0
        signals.append(position)
        if position != 0:
            hold += 1
        bars_since_trade += 1
    return signals


def _apply_confidence_gate(raw: list[int], confidences: list[float], top_quantile: float) -> list[int]:
    if top_quantile >= 1:
        return raw
    active = sorted((confidence for signal, confidence in zip(raw, confidences) if signal and confidence > 0), reverse=True)
    if not active:
        return raw
    take = max(1, int(round(len(active) * max(0.01, top_quantile))))
    threshold = active[take - 1]
    return [signal if confidence >= threshold else 0 for signal, confidence in zip(raw, confidences)]


def _apply_regime_filter(bars: list[OHLCBar], index: int, signal: int, parameters: dict[str, float | int | str]) -> int:
    regime = str(parameters.get("regime_filter", "any"))
    lookback = int(parameters["lookback"])
    if signal == 0 or regime == "any" or index < lookback:
        return signal
    window = bars[index - lookback : index]
    average_range = sum((item.high - item.low) / max(item.close, 1e-12) for item in window) / lookback
    current_range = (bars[index].high - bars[index].low) / max(bars[index].close, 1e-12)
    move = abs((bars[index].close - bars[index - lookback].close) / max(bars[index - lookback].close, 1e-12))
    threshold = float(parameters["threshold_bps"]) / 10_000
    if regime == "trend" and move < threshold:
        return 0
    if regime == "volatile" and current_range < average_range:
        return 0
    if regime == "calm" and current_range > average_range * 1.5:
        return 0
    return signal


def _fold_backtests(
    bars: list[OHLCBar],
    signals: list[int],
    config: BacktestConfig,
    folds: tuple[WalkForwardFold, ...],
) -> list[BacktestResult]:
    results: list[BacktestResult] = []
    for fold in folds:
        if fold.test_end - fold.test_start >= 2:
            results.append(run_vector_backtest(bars[fold.test_start : fold.test_end], signals[fold.test_start : fold.test_end], config))
    return results


def _evidence_profile(backtest: BacktestResult, folds: tuple[BacktestResult, ...]) -> dict[str, object]:
    fold_net = [float(fold.net_profit) for fold in folds]
    positive_fold_net = [profit for profit in fold_net if profit > 0]
    positive_total = sum(positive_fold_net)
    return {
        "fold_count": len(folds),
        "positive_fold_rate": round(_positive_fold_rate(folds), 6),
        "single_fold_profit_share": round(max(positive_fold_net) / positive_total, 6) if positive_total > 0 else 0.0,
        "oos_net_profit": round(sum(fold_net), 4),
        "oos_trade_count": sum(int(fold.trade_count) for fold in folds),
        "worst_fold_net_profit": round(min(fold_net), 4) if fold_net else 0.0,
        "full_period_test_profit": round(backtest.test_profit, 4),
    }


def _adaptive_folds(total_bars: int) -> tuple[WalkForwardFold, ...]:
    train = max(20, min(500, total_bars // 4))
    test = max(10, min(150, total_bars // 10))
    config = WalkForwardConfig(train_bars=train, test_bars=test, step_bars=test, holdout_fraction=0.2)
    return _sample_folds(tuple(walk_forward_splits(total_bars, config)), MAX_WALK_FORWARD_FOLDS)


def _sample_folds(folds: tuple[WalkForwardFold, ...], limit: int) -> tuple[WalkForwardFold, ...]:
    if len(folds) <= limit:
        return folds
    if limit <= 1:
        return folds[:1]
    indexes = sorted({round(index * (len(folds) - 1) / (limit - 1)) for index in range(limit)})
    return tuple(folds[index] for index in indexes)


def _warnings(
    backtest: BacktestResult,
    folds: tuple[BacktestResult, ...],
    stress: BacktestResult,
    config: BacktestConfig,
    family: str,
    cost_profile: IGCostProfile,
) -> list[str]:
    warnings: list[str] = []
    if backtest.trade_count < 18:
        warnings.append("too_few_trades")
    if backtest.net_profit <= 0:
        warnings.append("negative_after_costs")
    if backtest.cost_to_gross_ratio > 0.65 or backtest.total_cost > max(1.0, abs(backtest.net_profit) * 1.5):
        warnings.append("costs_overwhelm_edge")
    if backtest.net_profit > 0 and backtest.net_cost_ratio < 0.5:
        warnings.append("weak_net_cost_efficiency")
    if backtest.trade_count > 180 and backtest.total_cost > max(1.0, abs(backtest.net_profit)):
        warnings.append("high_turnover_cost_drag")
    if _risk_adjusted_sharpe(backtest) < 0.55:
        warnings.append("weak_sharpe")
    if 0 < backtest.sharpe_observations < 60:
        warnings.append("short_sharpe_sample")
    elif 0 < backtest.sharpe_observations < 120:
        warnings.append("limited_sharpe_sample")
    if backtest.max_drawdown > config.starting_cash * 0.35:
        warnings.append("drawdown_too_high")
    if stress.net_profit <= 0:
        warnings.append("fails_higher_slippage")
    if folds and _positive_fold_rate(folds) < 0.55:
        warnings.append("profits_not_consistent_across_folds")
    evidence = _evidence_profile(backtest, folds)
    if folds and float(evidence["oos_net_profit"]) <= 0:
        warnings.append("weak_oos_evidence")
    if folds and int(evidence["oos_trade_count"]) < 18:
        warnings.append("low_oos_trades")
    if folds and float(evidence["single_fold_profit_share"]) >= 0.6:
        warnings.append("one_fold_dependency")
    if family == "swing_trend" and backtest.funding_cost > max(1.0, backtest.gross_profit * 0.35):
        warnings.append("funding_eats_swing_edge")
    if family in CALENDAR_FAMILIES and backtest.trade_count < 30:
        warnings.append("calendar_effect_needs_longer_history")
    if family in CALENDAR_FAMILIES and _positive_fold_rate(folds) < 0.65:
        warnings.append("known_edge_needs_cross_market_validation")
    if cost_profile.confidence != "ig_live_epic_cost_profile":
        warnings.append("needs_ig_price_validation")
    return warnings


def _annotate_evaluations(
    evaluations: tuple[CandidateEvaluation, ...],
    trial_count: int,
    families: tuple[str, ...],
    config: AdaptiveSearchConfig,
    cost_profile: IGCostProfile,
) -> tuple[CandidateEvaluation, ...]:
    prepared: list[tuple[CandidateEvaluation, float, dict[str, object]]] = []
    for evaluation in evaluations:
        stability = _parameter_stability_score(evaluation, evaluations)
        diagnostics = _sharpe_diagnostics(evaluation.backtest, evaluation.fold_results, trial_count, stability)
        tier = _promotion_tier(evaluation, stability, cost_profile)
        score = _cost_aware_score(evaluation, diagnostics, stability, config)
        warnings = tuple(sorted(set(evaluation.warnings).union(diagnostics["implausibility_flags"])))
        prepared.append(
            (
                replace(
                    evaluation,
                    robustness_score=score,
                    passed=tier in {"paper_candidate", "validated_candidate"},
                    warnings=warnings,
                    promotion_tier=tier,
                ),
                stability,
                diagnostics,
            )
        )
    ranked = tuple(sorted(prepared, key=lambda item: _trial_ranking_key(item[0], item[1], item[2]), reverse=True))
    output: list[CandidateEvaluation] = []
    for rank, (evaluation, stability, diagnostics) in enumerate(ranked, start=1):
        tier = evaluation.promotion_tier
        grade = _grade_profile(evaluation)
        parameters = {
            **evaluation.candidate.parameters,
            "promotion_tier": tier,
            "search_audit": {
                "trial_rank": rank,
                "trial_count": trial_count,
                "families_tested": list(families),
                "search_preset": config.preset,
                "trading_style": config.trading_style,
                "objective": config.objective,
                "risk_profile": config.risk_profile,
                "regime_scan_enabled": config.include_regime_scans,
                "regime_scan": bool(evaluation.candidate.parameters.get("regime_scan")),
                "target_regime": evaluation.candidate.parameters.get("target_regime"),
                "grade_mode": grade.get("mode"),
                "grade_regime": grade.get("target_regime"),
                "graded_net_profit": round(float(grade.get("net_profit") or 0.0), 4),
                "graded_test_profit": round(float(grade.get("test_profit") or 0.0), 4),
                "graded_daily_pnl_sharpe": round(float(grade.get("daily_pnl_sharpe") or 0.0), 4),
                "graded_sharpe_days": int(float(grade.get("sharpe_days") or 0.0)),
                "graded_trade_count": int(float(grade.get("trade_count") or 0.0)),
            },
            "parameter_stability_score": stability,
            "sharpe_diagnostics": diagnostics,
        }
        output.append(
            replace(
                evaluation,
                candidate=replace(evaluation.candidate, parameters=parameters),
            )
        )
    return tuple(output)


def _promotion_tier(evaluation: CandidateEvaluation, stability: float, cost_profile: IGCostProfile) -> str:
    backtest = evaluation.backtest
    stress_net_profit = float(evaluation.candidate.parameters.get("stress_net_profit") or 0.0)
    fold_rate = _positive_fold_rate(evaluation.fold_results)
    viable_research_lead = (
        backtest.net_profit > 0
        and backtest.test_profit > 0
        and stress_net_profit > 0
        and backtest.trade_count >= 10
        and backtest.cost_to_gross_ratio <= 0.85
        and backtest.net_cost_ratio >= 0.2
    )
    if backtest.trade_count < 5:
        return "reject"
    if backtest.max_drawdown > 7_500 and not viable_research_lead:
        return "reject"
    if backtest.net_profit <= 0 and backtest.test_profit <= 0:
        return "reject"
    cost_robust = (
        backtest.net_cost_ratio >= 0.5
        and _expectancy_efficiency(backtest) >= 0.35
        and backtest.cost_to_gross_ratio <= 0.65
    )
    fresh_costed_evidence = (
        backtest.sharpe_observations >= MIN_PROMOTION_SHARPE_DAYS
        and backtest.estimated_spread_bps > 0
        and backtest.estimated_slippage_bps > 0
        and backtest.total_cost > 0
    )
    paper_ready = (
        backtest.net_profit > 0
        and backtest.test_profit > 0
        and stress_net_profit > 0
        and backtest.trade_count >= 18
        and fold_rate >= 0.55
        and backtest.max_drawdown <= 3_500
        and cost_robust
        and stability >= 0.35
        and fresh_costed_evidence
    )
    if paper_ready and cost_profile.confidence == LIVE_VALIDATED_COST_CONFIDENCE and stability >= 0.55:
        return "validated_candidate"
    if paper_ready and cost_profile.confidence == LIVE_VALIDATED_COST_CONFIDENCE:
        return "paper_candidate"
    if (
        viable_research_lead
    ):
        return "research_candidate"
    if backtest.gross_profit > 0 or _risk_adjusted_sharpe(backtest) > 0.5:
        return "watchlist"
    return "reject"


def _cost_aware_score(
    evaluation: CandidateEvaluation,
    diagnostics: dict[str, object],
    stability: float,
    config: AdaptiveSearchConfig,
) -> float:
    backtest = evaluation.backtest
    grade = _grade_profile(evaluation)
    stress_net_profit = float(evaluation.candidate.parameters.get("stress_net_profit") or 0.0)
    grade_net_profit = float(grade["net_profit"])
    grade_test_profit = float(grade["test_profit"])
    grade_cost = float(grade["total_cost"])
    grade_drawdown = float(grade["max_drawdown"])
    grade_net_cost_ratio = float(grade["net_cost_ratio"])
    profit_target = max(250.0, abs(grade_cost) * 0.75, abs(grade_drawdown) * 0.25)
    profit_component = _clamp(grade_test_profit / profit_target, 0.0, 1.0)
    net_component = _clamp(grade_net_profit / profit_target, 0.0, 1.0)
    stress_component = _clamp(stress_net_profit / profit_target, 0.0, 1.0)
    deflated_sharpe_component = _grade_sharpe_component(grade, diagnostics)
    net_cost_component = _clamp(grade_net_cost_ratio, 0.0, 1.0)
    expectancy_component = _grade_expectancy_efficiency(grade)
    fold_component = _positive_fold_rate(evaluation.fold_results)
    concentration_penalty = _profit_concentration(evaluation.fold_results)
    drawdown_component = 1.0 - _clamp(grade_drawdown / 3_500.0, 0.0, 1.0)
    churn_penalty = _grade_churn_penalty(grade, backtest)
    if config.objective == "profit_first":
        profit_weight, sharpe_weight = 0.36, 0.10
    elif config.objective == "sharpe_first":
        profit_weight, sharpe_weight = 0.24, 0.24
    else:
        profit_weight, sharpe_weight = 0.30, 0.16
    evidence_mode = config.repair_mode in {"evidence_first", "auto_refine"}
    if evidence_mode:
        profit_weight *= 0.85
        sharpe_weight *= 0.75
    score = 100 * (
        profit_weight * profit_component
        + 0.10 * net_component
        + 0.15 * stress_component
        + sharpe_weight * deflated_sharpe_component
        + 0.12 * net_cost_component
        + 0.10 * expectancy_component
        + 0.08 * stability
        + (0.13 if evidence_mode else 0.05) * fold_component
        + 0.04 * drawdown_component
        - 0.28 * churn_penalty
        - (0.12 if evidence_mode else 0.04) * concentration_penalty
    )
    return round(_clamp(score, -100.0, 100.0), 4)


def _trial_ranking_key(
    evaluation: CandidateEvaluation,
    stability: float,
    diagnostics: dict[str, object],
) -> tuple[float, ...]:
    tier_rank = {
        "validated_candidate": 4,
        "paper_candidate": 3,
        "research_candidate": 2,
        "watchlist": 1,
        "reject": 0,
    }.get(evaluation.promotion_tier, 0)
    stress_net_profit = float(evaluation.candidate.parameters.get("stress_net_profit") or 0.0)
    grade = _grade_profile(evaluation)
    return (
        float(tier_rank),
        float(grade["test_profit"]),
        float(stress_net_profit),
        float(grade["net_profit"]),
        float(_grade_sharpe_component(grade, diagnostics)),
        float(grade["net_cost_ratio"]),
        float(stability),
        -float(grade["max_drawdown"]),
        -float(_grade_churn_penalty(grade, evaluation.backtest)),
    )


def _grade_profile(evaluation: CandidateEvaluation) -> dict[str, object]:
    targeted = _target_regime_grade_profile(evaluation)
    if targeted:
        return targeted
    backtest = evaluation.backtest
    return {
        "mode": "full_period",
        "target_regime": None,
        "net_profit": float(backtest.net_profit),
        "test_profit": float(backtest.test_profit),
        "daily_pnl_sharpe": float(backtest.daily_pnl_sharpe),
        "sharpe_days": int(backtest.sharpe_observations),
        "trade_count": int(backtest.trade_count),
        "max_drawdown": float(backtest.max_drawdown),
        "gross_profit": float(backtest.gross_profit),
        "total_cost": float(backtest.total_cost),
        "expectancy_per_trade": float(backtest.expectancy_per_trade),
        "average_cost_per_trade": float(backtest.average_cost_per_trade),
        "net_cost_ratio": float(backtest.net_cost_ratio),
        "cost_to_gross_ratio": float(backtest.cost_to_gross_ratio),
    }


def _target_regime_grade_profile(evaluation: CandidateEvaluation) -> dict[str, object] | None:
    parameters = evaluation.candidate.parameters
    target_regime = str(parameters.get("target_regime") or "").strip()
    if not target_regime:
        return None
    analysis = parameters.get("bar_pattern_analysis")
    if not isinstance(analysis, dict):
        return None
    evidence = analysis.get("regime_trade_evidence")
    if not isinstance(evidence, dict) or not evidence.get("available"):
        return None
    in_regime = evidence.get("in_regime")
    if not isinstance(in_regime, dict):
        return None
    trade_count = int(_safe_float(in_regime.get("trade_count")))
    net_profit = _safe_float(in_regime.get("net_profit"))
    gross_profit = _safe_float(in_regime.get("gross_profit"))
    total_cost = _safe_float(in_regime.get("cost"))
    return {
        "mode": "target_regime",
        "target_regime": target_regime,
        "net_profit": net_profit,
        "test_profit": _safe_float(in_regime.get("test_profit")),
        "daily_pnl_sharpe": _safe_float(in_regime.get("daily_pnl_sharpe")),
        "sharpe_days": int(_safe_float(in_regime.get("sharpe_days"))),
        "trade_count": trade_count,
        "max_drawdown": _safe_float(in_regime.get("max_drawdown")),
        "gross_profit": gross_profit,
        "total_cost": total_cost,
        "expectancy_per_trade": net_profit / max(1, trade_count),
        "average_cost_per_trade": total_cost / max(1, trade_count),
        "net_cost_ratio": net_profit / max(1.0, total_cost),
        "cost_to_gross_ratio": total_cost / max(1e-9, abs(gross_profit)),
        "regime_trading_days": int(_safe_float(evidence.get("regime_trading_days"))),
        "regime_history_share": _safe_float(evidence.get("regime_history_share")),
        "regime_episodes": int(_safe_float(evidence.get("regime_episodes"))),
    }


def _grade_sharpe_component(grade: dict[str, object], diagnostics: dict[str, object]) -> float:
    if grade.get("mode") == "target_regime":
        sharpe_component = _clamp(_safe_float(grade.get("daily_pnl_sharpe")) / 2.0, 0.0, 1.0)
        sample_component = _clamp(
            min(
                _safe_float(grade.get("sharpe_days")) / float(MIN_PROMOTION_SHARPE_DAYS),
                _safe_float(grade.get("trade_count")) / 25.0,
            ),
            0.0,
            1.0,
        )
        return sharpe_component * sample_component
    return _clamp(float(diagnostics.get("deflated_sharpe_probability") or 0.0), 0.0, 1.0)


def _grade_expectancy_efficiency(grade: dict[str, object]) -> float:
    trade_count = int(_safe_float(grade.get("trade_count")))
    if trade_count <= 0:
        return 0.0
    average_cost = _safe_float(grade.get("average_cost_per_trade"))
    expectancy = _safe_float(grade.get("expectancy_per_trade"))
    if average_cost <= 0:
        return 1.0 if expectancy > 0 else 0.0
    return _clamp(expectancy / max(1.0, average_cost), 0.0, 1.0)


def _grade_churn_penalty(grade: dict[str, object], backtest: BacktestResult) -> float:
    if grade.get("mode") != "target_regime":
        return _churn_penalty(backtest)
    cost_drag = _clamp((_safe_float(grade.get("cost_to_gross_ratio")) - 0.35) / 0.65, 0.0, 1.0)
    turnover_drag = _clamp((_safe_float(grade.get("trade_count")) - 160) / 640, 0.0, 1.0)
    poor_expectancy_drag = 1.0 if _safe_float(grade.get("trade_count")) > 0 and _safe_float(grade.get("expectancy_per_trade")) <= 0 else 0.0
    return _clamp(0.55 * cost_drag + 0.30 * turnover_drag + 0.15 * poor_expectancy_drag, 0.0, 1.0)


def _parameter_stability_score(evaluation: CandidateEvaluation, evaluations: tuple[CandidateEvaluation, ...]) -> float:
    params = evaluation.candidate.parameters
    family = params.get("family")
    neighbors = [
        item
        for item in evaluations
        if item is not evaluation
        and item.candidate.parameters.get("family") == family
        and _parameter_distance(params, item.candidate.parameters) <= 0.6
    ]
    if not neighbors:
        return round(max(0.0, _positive_fold_rate(evaluation.fold_results) * (1.0 - _profit_concentration(evaluation.fold_results))), 6)
    robust_neighbors = [
        item
        for item in neighbors
        if item.backtest.test_profit > 0 and float(item.candidate.parameters.get("stress_net_profit") or 0.0) > 0
    ]
    neighbor_score = len(robust_neighbors) / len(neighbors)
    fold_score = _positive_fold_rate(evaluation.fold_results)
    concentration_score = 1.0 - _profit_concentration(evaluation.fold_results)
    return round(_clamp(0.45 * neighbor_score + 0.35 * fold_score + 0.20 * concentration_score, 0.0, 1.0), 6)


def _parameter_distance(left: dict[str, object], right: dict[str, object]) -> float:
    numeric_keys = (
        "lookback",
        "threshold_bps",
        "z_threshold",
        "volatility_multiplier",
        "stop_loss_bps",
        "take_profit_bps",
        "max_hold_bars",
        "min_trade_spacing",
        "confidence_quantile",
        "position_size",
        "weekday",
        "month_end_window",
        "month_start_window",
    )
    distances: list[float] = []
    for key in numeric_keys:
        if key not in left or key not in right:
            continue
        left_value = float(left[key] or 0.0)
        right_value = float(right[key] or 0.0)
        scale = max(abs(left_value), abs(right_value), 1.0)
        distances.append(abs(left_value - right_value) / scale)
    if not distances:
        return 1.0
    direction_penalty = 0.25 if left.get("direction") != right.get("direction") else 0.0
    regime_penalty = 0.15 if left.get("regime_filter") != right.get("regime_filter") else 0.0
    return sum(distances) / len(distances) + direction_penalty + regime_penalty


def _sharpe_diagnostics(
    backtest: BacktestResult,
    folds: tuple[BacktestResult, ...],
    trial_count: int,
    parameter_stability_score: float,
) -> dict[str, object]:
    fold_sharpes = [_risk_adjusted_sharpe(fold) for fold in folds]
    daily_pnl = list(backtest.daily_pnl_curve)
    deflated = _deflated_sharpe_probability(daily_pnl, trial_count)
    haircut = _multiple_testing_sharpe_haircut(backtest.daily_pnl_sharpe, backtest.sharpe_observations, trial_count)
    return {
        "daily_pnl_sharpe": round(backtest.daily_pnl_sharpe, 4),
        "daily_pnl_sample_sharpe": round(backtest.daily_pnl_sample_sharpe, 4),
        "bar_period_annualized_sharpe": round(backtest.sharpe, 4),
        "bar_sample_sharpe": round(backtest.bar_sample_sharpe, 4),
        "holdout_sharpe": round(backtest.test_daily_pnl_sharpe, 4),
        "walk_forward_median_sharpe": round(_median(fold_sharpes), 4),
        "rolling_sharpe_min": round(backtest.rolling_sharpe_min, 4),
        "rolling_sharpe_median": round(backtest.rolling_sharpe_median, 4),
        "probabilistic_sharpe_ratio": round(backtest.probabilistic_sharpe_ratio, 6),
        "deflated_sharpe_probability": deflated,
        "haircut_adjusted_daily_sharpe": haircut,
        "trial_count": trial_count,
        "sharpe_observations": backtest.sharpe_observations,
        "bar_sharpe_observations": backtest.bar_sharpe_observations,
        "sample_calendar_days": backtest.sample_calendar_days,
        "sample_trading_days": backtest.sample_trading_days,
        "daily_periods_per_year": backtest.daily_periods_per_year,
        "bar_periods_per_year": round(backtest.bar_periods_per_year, 4),
        "annualization_note": backtest.sharpe_annualization_note,
        "parameter_stability_score": parameter_stability_score,
        "turnover_efficiency": round(backtest.turnover_efficiency, 6),
        "implausibility_flags": _implausibility_flags(backtest, folds, trial_count, parameter_stability_score),
    }


def _implausibility_flags(
    backtest: BacktestResult,
    folds: tuple[BacktestResult, ...],
    trial_count: int,
    parameter_stability_score: float,
) -> list[str]:
    flags: list[str] = []
    if backtest.daily_pnl_sharpe >= 2 and backtest.trade_count < 25:
        flags.append("high_sharpe_low_trade_count")
    if backtest.daily_pnl_sharpe >= 2 and backtest.sharpe_observations < 120:
        flags.append("high_sharpe_short_sample")
    if backtest.daily_pnl_sharpe >= 2 and _positive_fold_rate(folds) < 0.6:
        flags.append("high_sharpe_weak_folds")
    if backtest.daily_pnl_sharpe >= 2 and parameter_stability_score < 0.35:
        flags.append("isolated_parameter_peak")
    if backtest.total_cost <= max(1.0, abs(backtest.gross_profit) * 0.05) and backtest.trade_count > 25:
        flags.append("costs_small_vs_turnover")
    if backtest.cost_to_gross_ratio > 0.65:
        flags.append("costs_overwhelm_edge")
    if backtest.net_profit > 0 and backtest.net_cost_ratio < 0.5:
        flags.append("weak_net_cost_efficiency")
    if backtest.trade_count > 180 and backtest.total_cost > max(1.0, abs(backtest.net_profit)):
        flags.append("high_turnover_cost_drag")
    if trial_count >= 50 and _deflated_sharpe_probability(list(backtest.daily_pnl_curve), trial_count) < 0.5:
        flags.append("multiple_testing_haircut")
    return flags


def _deflated_sharpe_probability(daily_pnl: list[float], trial_count: int) -> float:
    if len(daily_pnl) < 3:
        return 0.0
    benchmark = _multiple_testing_sharpe_threshold(len(daily_pnl), trial_count)
    return round(_probabilistic_sharpe_ratio(daily_pnl, benchmark), 6)


def _multiple_testing_sharpe_haircut(observed_annual_sharpe: float, sample_size: int, trial_count: int) -> float:
    return round(observed_annual_sharpe - _multiple_testing_sharpe_threshold(sample_size, trial_count), 4)


def _multiple_testing_sharpe_threshold(sample_size: int, trial_count: int) -> float:
    if sample_size < 2 or trial_count <= 1:
        return 0.0
    return sqrt(252) * sqrt(2 * log(max(2, trial_count)) / sample_size)


def _probabilistic_sharpe_ratio(daily_pnl: list[float], target_annual_sharpe: float) -> float:
    sample_size = len(daily_pnl)
    if sample_size < 3:
        return 0.0
    mean = sum(daily_pnl) / sample_size
    variance = sum((value - mean) ** 2 for value in daily_pnl) / sample_size
    if variance <= 0:
        return 0.0
    std = sqrt(variance)
    observed = mean / std
    target = target_annual_sharpe / sqrt(252)
    skew = sum(((value - mean) / std) ** 3 for value in daily_pnl) / sample_size
    kurtosis = sum(((value - mean) / std) ** 4 for value in daily_pnl) / sample_size
    denominator = sqrt(max(1e-12, 1 - skew * observed + ((kurtosis - 1) / 4) * observed**2))
    z_score = (observed - target) * sqrt(sample_size - 1) / denominator
    return _normal_cdf(z_score)


def _normal_cdf(value: float) -> float:
    return 0.5 * (1 + erf(value / sqrt(2)))


def _profit_concentration(folds: tuple[BacktestResult, ...]) -> float:
    if not folds:
        return 1.0
    positive = [fold.net_profit for fold in folds if fold.net_profit > 0]
    if not positive:
        return 1.0
    total = sum(positive)
    return max(positive) / total if total > 0 else 1.0


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2


def _pareto(evaluations: tuple[CandidateEvaluation, ...]) -> tuple[dict[str, object], ...]:
    if not evaluations:
        return ()
    selections = (
        ("best_balanced", max(evaluations, key=lambda item: item.robustness_score)),
        ("highest_sharpe", max(evaluations, key=lambda item: _risk_adjusted_sharpe(item.backtest))),
        ("highest_profit", max(evaluations, key=lambda item: item.backtest.net_profit)),
    )
    seen: set[str] = set()
    output: list[dict[str, object]] = []
    for label, evaluation in selections:
        key = evaluation.candidate.name
        if key in seen:
            continue
        seen.add(key)
        output.append(
            {
                "kind": label,
                "strategy_name": evaluation.candidate.name,
                "family": evaluation.candidate.parameters.get("family"),
                "style": evaluation.candidate.parameters.get("style"),
                "robustness_score": evaluation.robustness_score,
                "sharpe": round(evaluation.backtest.sharpe, 4),
                "daily_pnl_sharpe": round(evaluation.backtest.daily_pnl_sharpe, 4),
                "deflated_sharpe_probability": (
                    evaluation.candidate.parameters.get("sharpe_diagnostics") or {}
                ).get("deflated_sharpe_probability")
                if isinstance(evaluation.candidate.parameters.get("sharpe_diagnostics"), dict)
                else 0.0,
                "net_profit": round(evaluation.backtest.net_profit, 4),
                "gross_profit": round(evaluation.backtest.gross_profit, 4),
                "total_cost": round(evaluation.backtest.total_cost, 4),
                "net_cost_ratio": round(evaluation.backtest.net_cost_ratio, 6),
                "expectancy_per_trade": round(evaluation.backtest.expectancy_per_trade, 4),
                "cost_to_gross_ratio": round(evaluation.backtest.cost_to_gross_ratio, 6),
                "max_drawdown": round(evaluation.backtest.max_drawdown, 4),
                "trade_count": evaluation.backtest.trade_count,
                "warnings": list(evaluation.warnings),
                "settings": evaluation.candidate.parameters,
                "promotion_tier": evaluation.promotion_tier,
            }
        )
    return tuple(output)


def _signals_to_probabilities(signals: list[int]) -> list[float]:
    return [0.75 if signal > 0 else 0.25 if signal < 0 else 0.5 for signal in signals]


def _positive_fold_rate(folds: tuple[BacktestResult, ...]) -> float:
    if not folds:
        return 0.0
    return sum(1 for fold in folds if fold.net_profit > 0) / len(folds)


def _risk_adjusted_sharpe(backtest: BacktestResult) -> float:
    if backtest.sharpe_observations >= 3 or backtest.daily_pnl_sharpe != 0:
        return backtest.daily_pnl_sharpe
    return backtest.sharpe


def _expectancy_efficiency(backtest: BacktestResult) -> float:
    if backtest.trade_count <= 0:
        return 0.0
    if backtest.average_cost_per_trade <= 0:
        return 1.0 if backtest.expectancy_per_trade > 0 else 0.0
    return _clamp(backtest.expectancy_per_trade / max(1.0, backtest.average_cost_per_trade), 0.0, 1.0)


def _churn_penalty(backtest: BacktestResult) -> float:
    cost_drag = _clamp((backtest.cost_to_gross_ratio - 0.35) / 0.65, 0.0, 1.0)
    turnover_drag = _clamp((backtest.trade_count - 160) / 640, 0.0, 1.0)
    poor_expectancy_drag = 1.0 if backtest.trade_count > 0 and backtest.expectancy_per_trade <= 0 else 0.0
    return _clamp(0.55 * cost_drag + 0.30 * turnover_drag + 0.15 * poor_expectancy_drag, 0.0, 1.0)


def _threshold_choices(family: str) -> tuple[int, ...]:
    if family == "scalping":
        return (12, 18, 25, 35, 50, 75)
    if family == "liquidity_sweep_reversal":
        return (6, 8, 12, 18, 25, 35, 50)
    if family in {"intraday_trend", "breakout", "volatility_expansion"}:
        return (12, 18, 25, 35, 50, 75, 110)
    if family == "swing_trend":
        return (18, 25, 35, 50, 75, 110, 150)
    return (8, 12, 18, 25, 35, 50, 75)


def _max_hold_choices(family: str) -> tuple[int, ...]:
    if family == "scalping":
        return (8, 12, 18, 24, 36)
    if family == "swing_trend":
        return (48, 96, 144, 192, 288)
    if family == "liquidity_sweep_reversal":
        return (12, 18, 24, 36, 48, 72)
    return (24, 48, 72, 96, 144)


def _spacing_choices(family: str) -> tuple[int, ...]:
    if family == "scalping":
        return (4, 8, 12, 18)
    if family == "swing_trend":
        return (12, 24, 36, 48)
    if family == "liquidity_sweep_reversal":
        return (6, 8, 12, 18, 24)
    return (4, 8, 12, 18, 24, 36)


def _zscore(values: list[float], current: float) -> float:
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    if variance == 0:
        return 0.0
    return (current - mean) / (variance**0.5)


def _apply_direction(signal: int, direction: str) -> int:
    if direction == "long_only":
        return max(0, signal)
    if direction == "short_only":
        return min(0, signal)
    return signal


def _stable_seed(market_id: str, timeframe: str, seed: int, style: str) -> int:
    text = f"{market_id}:{timeframe}:{style}:{seed}"
    return sum((index + 1) * ord(char) for index, char in enumerate(text))


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _safe_float(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
