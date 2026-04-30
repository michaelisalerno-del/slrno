from __future__ import annotations

from dataclasses import dataclass, replace
from random import Random

from .backtesting import BacktestConfig, BacktestResult, run_vector_backtest
from .ig_costs import IGCostProfile, backtest_config_from_profile, profile_badge
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

STYLE_FAMILIES = {
    "find_anything_robust": ("intraday_trend", "breakout", "mean_reversion", "volatility_expansion", "scalping", "swing_trend"),
    "intraday_only": ("intraday_trend", "breakout", "mean_reversion", "volatility_expansion", "scalping"),
    "swing_trades": ("swing_trend", "breakout", "mean_reversion"),
    "lower_drawdown": ("mean_reversion", "intraday_trend", "volatility_expansion"),
    "higher_profit": ("breakout", "swing_trend", "intraday_trend"),
}

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
    seed: int = 7


@dataclass(frozen=True)
class AdaptiveSearchResult:
    evaluations: tuple[CandidateEvaluation, ...]
    pareto: tuple[dict[str, object], ...]
    cost_profile: IGCostProfile


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
    backtest_base = backtest_config_from_profile(cost_profile)
    folds = _adaptive_folds(len(bars))
    evaluations: list[CandidateEvaluation] = []

    for trial_index in range(budget):
        family = families[trial_index % len(families)]
        parameters = _sample_parameters(rng, family, config.risk_profile, trial_index)
        signals = _generate_signals(bars, family, parameters)
        backtest_config = replace(backtest_base, position_size=float(parameters["position_size"]))
        backtest = run_vector_backtest(bars, signals, backtest_config)
        fold_results = tuple(_fold_backtests(bars, signals, backtest_config, folds))
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
        warnings = tuple(_warnings(backtest, fold_results, stress, backtest_config, family, cost_profile))
        parameters = {
            **parameters,
            "market_id": market_id,
            "timeframe": timeframe,
            "family": family,
            "style": config.trading_style,
            "objective": config.objective,
            "cost_confidence": cost_profile.confidence,
            "cost_badge": profile_badge(cost_profile),
            "stress_net_profit": round(stress.net_profit, 4),
            "stress_sharpe": round(stress.sharpe, 4),
        }
        candidate = ProbabilityCandidate(
            name=f"{config.trading_style}_{family}_{trial_index + 1}",
            module_stack=("adaptive_ig_v1", config.trading_style, family),
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

    ranked = tuple(sorted(evaluations, key=lambda item: item.robustness_score, reverse=True))
    return AdaptiveSearchResult(ranked, _pareto(ranked), cost_profile)


def balanced_score(
    backtest: BacktestResult,
    folds: tuple[BacktestResult, ...],
    stress: BacktestResult,
    config: BacktestConfig,
) -> float:
    sharpe_component = _clamp(backtest.sharpe / 2.0, 0.0, 1.0)
    profit_component = _clamp(backtest.test_profit / max(250.0, config.starting_cash * 0.04), 0.0, 1.0)
    fold_component = _positive_fold_rate(folds)
    stress_component = 1.0 if stress.net_profit > 0 else 0.0
    drawdown_component = 1.0 - _clamp(backtest.max_drawdown / max(1.0, config.starting_cash * 0.35), 0.0, 1.0)
    trade_component = _clamp(backtest.trade_count / 60, 0.0, 1.0)
    return round(
        100
        * (
            0.35 * sharpe_component
            + 0.25 * profit_component
            + 0.15 * fold_component
            + 0.10 * stress_component
            + 0.10 * drawdown_component
            + 0.05 * trade_component
        ),
        4,
    )


def _sample_parameters(rng: Random, family: str, risk_profile: str, trial_index: int) -> dict[str, float | int | str]:
    lookbacks = {
        "scalping": (3, 5, 8, 12),
        "intraday_trend": (8, 12, 16, 24, 36),
        "breakout": (12, 20, 32, 48),
        "mean_reversion": (10, 16, 24, 36),
        "volatility_expansion": (12, 18, 30, 42),
        "swing_trend": (48, 72, 96, 144),
    }.get(family, (12, 24, 36))
    direction = ("long_only", "short_only", "long_short")[trial_index % 3]
    risk_scale = {"conservative": 0.7, "balanced": 1.0, "aggressive": 1.35}.get(risk_profile, 1.0)
    return {
        "lookback": rng.choice(lookbacks),
        "threshold_bps": round(rng.choice((4, 8, 12, 18, 25, 35, 50)) * risk_scale, 2),
        "z_threshold": round(rng.uniform(0.65, 2.2), 3),
        "volatility_multiplier": round(rng.uniform(1.1, 2.8), 3),
        "stop_loss_bps": round(rng.choice((18, 25, 35, 50, 75, 110)) * risk_scale, 2),
        "take_profit_bps": round(rng.choice((20, 35, 55, 80, 130, 180)) * risk_scale, 2),
        "max_hold_bars": rng.choice((8, 12, 24, 48, 96, 144)),
        "min_hold_bars": rng.choice((1, 2, 3, 5)),
        "min_trade_spacing": rng.choice((0, 2, 4, 8, 12)),
        "confidence_quantile": rng.choice((0.2, 0.25, 0.3, 0.4, 1.0)),
        "regime_filter": rng.choice(("any", "trend", "volatile", "calm")),
        "false_breakout_filter": rng.choice((0, 1)),
        "position_size": round(rng.choice((0.5, 1.0, 1.5, 2.0)) * risk_scale, 2),
        "direction": direction,
    }


def _generate_signals(bars: list[OHLCBar], family: str, parameters: dict[str, float | int | str]) -> list[int]:
    closes = [bar.close for bar in bars]
    lookback = int(parameters["lookback"])
    threshold = float(parameters["threshold_bps"]) / 10_000
    z_threshold = float(parameters["z_threshold"])
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
            signal = _apply_regime_filter(bars, index, signal, parameters)
        raw.append(_apply_direction(signal, str(parameters["direction"])))
        confidences.append(confidence if signal else 0.0)
    raw = _apply_confidence_gate(raw, confidences, float(parameters.get("confidence_quantile", 1.0)))
    return _apply_risk_controls(bars, raw, parameters)


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


def _adaptive_folds(total_bars: int) -> tuple[WalkForwardFold, ...]:
    train = max(20, min(500, total_bars // 4))
    test = max(10, min(150, total_bars // 10))
    config = WalkForwardConfig(train_bars=train, test_bars=test, step_bars=test, holdout_fraction=0.2)
    return tuple(walk_forward_splits(total_bars, config))


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
    if backtest.sharpe < 0.55:
        warnings.append("weak_sharpe")
    if backtest.max_drawdown > config.starting_cash * 0.35:
        warnings.append("drawdown_too_high")
    if stress.net_profit <= 0:
        warnings.append("fails_higher_slippage")
    if folds and _positive_fold_rate(folds) < 0.55:
        warnings.append("profits_not_consistent_across_folds")
    if family == "swing_trend" and backtest.funding_cost > max(1.0, backtest.gross_profit * 0.35):
        warnings.append("funding_eats_swing_edge")
    if cost_profile.confidence != "ig_live_epic_cost_profile":
        warnings.append("needs_ig_price_validation")
    return warnings


def _pareto(evaluations: tuple[CandidateEvaluation, ...]) -> tuple[dict[str, object], ...]:
    if not evaluations:
        return ()
    selections = (
        ("best_balanced", max(evaluations, key=lambda item: item.robustness_score)),
        ("highest_sharpe", max(evaluations, key=lambda item: item.backtest.sharpe)),
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
                "net_profit": round(evaluation.backtest.net_profit, 4),
                "gross_profit": round(evaluation.backtest.gross_profit, 4),
                "total_cost": round(evaluation.backtest.total_cost, 4),
                "max_drawdown": round(evaluation.backtest.max_drawdown, 4),
                "trade_count": evaluation.backtest.trade_count,
                "warnings": list(evaluation.warnings),
                "settings": evaluation.candidate.parameters,
            }
        )
    return tuple(output)


def _signals_to_probabilities(signals: list[int]) -> list[float]:
    return [0.75 if signal > 0 else 0.25 if signal < 0 else 0.5 for signal in signals]


def _positive_fold_rate(folds: tuple[BacktestResult, ...]) -> float:
    if not folds:
        return 0.0
    return sum(1 for fold in folds if fold.net_profit > 0) / len(folds)


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
