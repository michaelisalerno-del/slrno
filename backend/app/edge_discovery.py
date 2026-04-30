from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from .adaptive_research import AdaptiveSearchConfig, run_adaptive_search
from .config import app_home
from .ig_costs import IGCostProfile, public_ig_cost_profile
from .market_registry import MarketMapping, MarketRegistry
from .providers.eodhd import EODHDProvider
from .research_lab import CandidateEvaluation
from .research_store import ResearchStore
from .settings_store import SettingsStore


DEFAULT_COMMAND = "python -m app.edge_discovery --config configs/edge_discovery.yaml --mode deep"


@dataclass(frozen=True)
class EdgeGateConfig:
    min_trade_count: int = 30
    max_drawdown_fraction: float = 0.35
    min_fold_consistency: float = 0.55
    max_cost_gross_ratio: float = 0.45
    max_single_fold_profit_share: float = 0.55
    stress_multiplier: float = 2.0


@dataclass(frozen=True)
class EdgeRuntimeConfig:
    mode: str = "quick"
    markets: tuple[str, ...] = ("NAS100", "US500", "DE40", "EURUSD", "GBPUSD", "XAUUSD")
    start: str = ""
    end: str = ""
    interval: str = "5min"
    trading_style: str = "find_anything_robust"
    risk_profile: str = "balanced"
    quick_budget: int = 18
    deep_budget: int = 120
    strategy_families: tuple[str, ...] = ("intraday_trend", "breakout", "mean_reversion", "volatility_expansion")
    artifact_root: str = ""
    starting_cash: float = 10_000.0
    gates: EdgeGateConfig = field(default_factory=EdgeGateConfig)


@dataclass(frozen=True)
class EdgeCandidate:
    candidate_id: str
    market_id: str
    strategy_name: str
    strategy_family: str
    detection_logic: str
    test_net_profit: float
    net_profit: float
    gross_profit: float
    holdout_sharpe: float
    walk_forward_sharpe: float
    total_cost: float
    cost_gross_ratio: float
    trade_count: int
    max_drawdown: float
    stressed_net_profit: float
    fold_consistency_score: float
    max_single_fold_profit_share: float
    cost_efficiency_score: float
    promotion_tier: str
    keep: bool
    failed_gates: tuple[str, ...]
    warnings: tuple[str, ...]
    settings: dict[str, object]
    cost_confidence: str
    validation_status: str
    equity_curve: tuple[float, ...]

    def as_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["decision"] = "KEEP" if self.keep else "REJECT"
        return payload


@dataclass(frozen=True)
class EdgeAggregateComponent:
    candidate_id: str
    market_id: str
    strategy_family: str
    weight: float
    test_net_profit: float
    stressed_net_profit: float
    holdout_sharpe: float
    max_drawdown: float
    contribution: float
    correlation_to_primary: float


@dataclass(frozen=True)
class EdgeAggregationReport:
    components: tuple[EdgeAggregateComponent, ...]
    correlations: tuple[dict[str, object], ...]
    aggregate_test_net_profit: float
    aggregate_stressed_net_profit: float
    aggregate_max_drawdown: float
    aggregate_holdout_sharpe: float
    best_standalone_candidate_id: str
    best_standalone_test_net_profit: float
    profit_delta_vs_best_standalone: float

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class EdgeDiscoveryOutput:
    artifact_dir: Path
    leaderboard: tuple[EdgeCandidate, ...]
    shortlist: tuple[EdgeCandidate, ...]
    aggregation_report: EdgeAggregationReport
    market_failures: tuple[dict[str, str], ...]
    robust_edge_found: bool


def load_config(path: Path, mode_override: str | None = None) -> EdgeRuntimeConfig:
    if not path.exists():
        raise FileNotFoundError(f"Edge discovery config not found: {path}")
    try:
        import yaml
    except ModuleNotFoundError as exc:
        raise RuntimeError("PyYAML is required to load edge discovery YAML configs. Install backend dependencies first.") from exc
    raw = yaml.safe_load(path.read_text()) or {}
    if not isinstance(raw, dict):
        raise ValueError("Edge discovery config must be a mapping")
    gates_raw = raw.get("gates", {}) or {}
    if not isinstance(gates_raw, dict):
        raise ValueError("Edge discovery gates config must be a mapping")
    mode = mode_override or str(raw.get("mode", "quick"))
    config = EdgeRuntimeConfig(
        mode=mode,
        markets=tuple(str(item) for item in raw.get("markets", EdgeRuntimeConfig.markets)),
        start=str(raw.get("start", "")),
        end=str(raw.get("end", "")),
        interval=str(raw.get("interval", "5min")),
        trading_style=str(raw.get("trading_style", "find_anything_robust")),
        risk_profile=str(raw.get("risk_profile", "balanced")),
        quick_budget=int(raw.get("quick_budget", 18)),
        deep_budget=int(raw.get("deep_budget", 120)),
        strategy_families=tuple(str(item) for item in raw.get("strategy_families", EdgeRuntimeConfig.strategy_families)),
        artifact_root=str(raw.get("artifact_root", "")),
        starting_cash=float(raw.get("starting_cash", 10_000.0)),
        gates=EdgeGateConfig(
            min_trade_count=int(gates_raw.get("min_trade_count", 30)),
            max_drawdown_fraction=float(gates_raw.get("max_drawdown_fraction", 0.35)),
            min_fold_consistency=float(gates_raw.get("min_fold_consistency", 0.55)),
            max_cost_gross_ratio=float(gates_raw.get("max_cost_gross_ratio", 0.45)),
            max_single_fold_profit_share=float(gates_raw.get("max_single_fold_profit_share", 0.55)),
            stress_multiplier=float(gates_raw.get("stress_multiplier", 2.0)),
        ),
    )
    _validate_config(config)
    return config


async def run_edge_discovery(
    config: EdgeRuntimeConfig,
    provider: EODHDProvider | None = None,
    market_registry: MarketRegistry | None = None,
    research_store: ResearchStore | None = None,
    command: str = DEFAULT_COMMAND,
) -> EdgeDiscoveryOutput:
    market_registry = market_registry or MarketRegistry()
    market_registry.seed_defaults()
    research_store = research_store or ResearchStore()
    provider = provider or EODHDProvider(_eodhd_api_token())
    artifact_dir = _artifact_dir(config)
    artifact_dir.mkdir(parents=True, exist_ok=True)

    leaderboard: list[EdgeCandidate] = []
    failures: list[dict[str, str]] = []
    for market_id in config.markets:
        market = market_registry.get(market_id)
        if market is None:
            failures.append({"market_id": market_id, "error": "market_not_configured"})
            continue
        if not market.enabled:
            failures.append({"market_id": market_id, "error": "market_disabled"})
            continue
        try:
            bars = await provider.historical_bars(market.eodhd_symbol, config.interval or market.default_timeframe, config.start, config.end)
        except Exception as exc:
            failures.append({"market_id": market.market_id, "error": str(exc)})
            continue
        if len(bars) < market.min_backtest_bars:
            failures.append(
                {
                    "market_id": market.market_id,
                    "error": f"insufficient_bars: need {market.min_backtest_bars}, got {len(bars)}",
                }
            )
            continue
        cost_profile = _cost_profile(market, research_store)
        result = run_adaptive_search(
            bars,
            market.market_id,
            config.interval or market.default_timeframe,
            cost_profile,
            AdaptiveSearchConfig(
                preset=config.mode,
                trading_style=config.trading_style,
                objective="profit_first",
                search_budget=_budget(config),
                risk_profile=config.risk_profile,
                strategy_families=config.strategy_families,
                cost_stress_multiplier=config.gates.stress_multiplier,
            ),
        )
        leaderboard.extend(_candidate_from_evaluation(market, cost_profile, item, config) for item in result.evaluations)

    ranked = tuple(sorted(leaderboard, key=_ranking_key, reverse=True))
    aggregation_report = _build_aggregation_report(ranked)
    selected_ids = {item.candidate_id for item in aggregation_report.components}
    shortlist = tuple(item for item in ranked if item.candidate_id in selected_ids)
    output = EdgeDiscoveryOutput(
        artifact_dir=artifact_dir,
        leaderboard=ranked,
        shortlist=shortlist,
        aggregation_report=aggregation_report,
        market_failures=tuple(failures),
        robust_edge_found=bool(shortlist),
    )
    _write_artifacts(output, config, command)
    if not ranked and failures:
        raise RuntimeError(f"No candidates evaluated. Market failures: {failures}")
    return output


def _candidate_from_evaluation(
    market: MarketMapping,
    cost_profile: IGCostProfile,
    evaluation: CandidateEvaluation,
    config: EdgeRuntimeConfig,
) -> EdgeCandidate:
    backtest = evaluation.backtest
    settings = dict(evaluation.candidate.parameters)
    stressed_net_profit = float(settings.get("stress_net_profit") or 0.0)
    cost_gross_ratio = _cost_gross_ratio(backtest.total_cost, backtest.gross_profit)
    fold_consistency = _fold_consistency(evaluation)
    profit_concentration = _profit_concentration(evaluation)
    failed = _failed_gates(evaluation, stressed_net_profit, cost_gross_ratio, fold_consistency, profit_concentration, config)
    family = str(settings.get("family") or evaluation.candidate.module_stack[-1])
    return EdgeCandidate(
        candidate_id=f"{market.market_id}:{evaluation.candidate.name}",
        market_id=market.market_id,
        strategy_name=evaluation.candidate.name,
        strategy_family=family,
        detection_logic=_detection_summary(family, settings),
        test_net_profit=round(backtest.test_profit, 4),
        net_profit=round(backtest.net_profit, 4),
        gross_profit=round(backtest.gross_profit, 4),
        holdout_sharpe=round(backtest.test_sharpe, 4),
        walk_forward_sharpe=round(_walk_forward_sharpe(evaluation), 4),
        total_cost=round(backtest.total_cost, 4),
        cost_gross_ratio=round(cost_gross_ratio, 6),
        trade_count=backtest.trade_count,
        max_drawdown=round(backtest.max_drawdown, 4),
        stressed_net_profit=round(stressed_net_profit, 4),
        fold_consistency_score=round(fold_consistency, 6),
        max_single_fold_profit_share=round(profit_concentration, 6),
        cost_efficiency_score=round(max(0.0, 1.0 - cost_gross_ratio), 6),
        promotion_tier=str(settings.get("promotion_tier") or evaluation.promotion_tier),
        keep=len(failed) == 0,
        failed_gates=tuple(failed),
        warnings=tuple(sorted(set(evaluation.warnings))),
        settings=settings,
        cost_confidence=cost_profile.confidence,
        validation_status=cost_profile.validation_status,
        equity_curve=tuple(backtest.equity_curve),
    )


def _failed_gates(
    evaluation: CandidateEvaluation,
    stressed_net_profit: float,
    cost_gross_ratio: float,
    fold_consistency: float,
    profit_concentration: float,
    config: EdgeRuntimeConfig,
) -> list[str]:
    backtest = evaluation.backtest
    failed: list[str] = []
    if backtest.test_profit <= 0:
        failed.append("holdout_net_profit")
    if stressed_net_profit <= 0:
        failed.append("stressed_cost_net_profit")
    if fold_consistency < config.gates.min_fold_consistency:
        failed.append("fold_consistency")
    if backtest.trade_count < config.gates.min_trade_count:
        failed.append("trade_count")
    if backtest.max_drawdown > config.starting_cash * config.gates.max_drawdown_fraction:
        failed.append("max_drawdown")
    if backtest.gross_profit <= 0 or cost_gross_ratio > config.gates.max_cost_gross_ratio:
        failed.append("cost_gross_efficiency")
    if profit_concentration > config.gates.max_single_fold_profit_share:
        failed.append("profit_concentration")
    return failed


def _fold_consistency(evaluation: CandidateEvaluation) -> float:
    folds = evaluation.fold_results
    if not folds:
        return 0.0
    positive = [fold.net_profit for fold in folds if fold.net_profit > 0]
    positive_rate = len(positive) / len(folds)
    if not positive:
        return 0.0
    total_positive = sum(positive)
    concentration = max(positive) / total_positive if total_positive > 0 else 1.0
    concentration_penalty = max(0.0, concentration - 0.55)
    return max(0.0, positive_rate - concentration_penalty)


def _profit_concentration(evaluation: CandidateEvaluation) -> float:
    folds = evaluation.fold_results
    if not folds:
        return 1.0
    positive = [fold.net_profit for fold in folds if fold.net_profit > 0]
    if not positive:
        return 1.0
    total_positive = sum(positive)
    return max(positive) / total_positive if total_positive > 0 else 1.0


def _walk_forward_sharpe(evaluation: CandidateEvaluation) -> float:
    values = [fold.sharpe for fold in evaluation.fold_results]
    if not values:
        return 0.0
    midpoint = len(values) // 2
    ordered = sorted(values)
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2


def _build_aggregation_report(leaderboard: tuple[EdgeCandidate, ...]) -> EdgeAggregationReport:
    keepers = [item for item in leaderboard if item.keep]
    if not keepers:
        return EdgeAggregationReport((), (), 0.0, 0.0, 0.0, 0.0, "", 0.0, 0.0)

    selected: list[EdgeCandidate] = [keepers[0]]
    for candidate in keepers[1:]:
        if len(selected) >= 3:
            break
        correlations = [abs(_candidate_correlation(candidate, existing)) for existing in selected]
        if all(value <= 0.75 for value in correlations):
            selected.append(candidate)

    weights = _controlled_weights(tuple(selected))
    primary = selected[0]
    components = tuple(
        EdgeAggregateComponent(
            candidate_id=item.candidate_id,
            market_id=item.market_id,
            strategy_family=item.strategy_family,
            weight=round(weights[item.candidate_id], 6),
            test_net_profit=item.test_net_profit,
            stressed_net_profit=item.stressed_net_profit,
            holdout_sharpe=item.holdout_sharpe,
            max_drawdown=item.max_drawdown,
            contribution=round(weights[item.candidate_id] * item.test_net_profit, 4),
            correlation_to_primary=round(_candidate_correlation(item, primary), 6) if item is not primary else 1.0,
        )
        for item in selected
    )
    correlations: list[dict[str, object]] = []
    for left_index, left in enumerate(selected):
        for right in selected[left_index + 1 :]:
            correlations.append(
                {
                    "left": left.candidate_id,
                    "right": right.candidate_id,
                    "correlation": round(_candidate_correlation(left, right), 6),
                }
            )
    aggregate_test = sum(weights[item.candidate_id] * item.test_net_profit for item in selected)
    aggregate_stress = sum(weights[item.candidate_id] * item.stressed_net_profit for item in selected)
    aggregate_drawdown = sum(weights[item.candidate_id] * item.max_drawdown for item in selected)
    aggregate_sharpe = sum(weights[item.candidate_id] * item.holdout_sharpe for item in selected)
    return EdgeAggregationReport(
        components=components,
        correlations=tuple(correlations),
        aggregate_test_net_profit=round(aggregate_test, 4),
        aggregate_stressed_net_profit=round(aggregate_stress, 4),
        aggregate_max_drawdown=round(aggregate_drawdown, 4),
        aggregate_holdout_sharpe=round(aggregate_sharpe, 4),
        best_standalone_candidate_id=primary.candidate_id,
        best_standalone_test_net_profit=primary.test_net_profit,
        profit_delta_vs_best_standalone=round(aggregate_test - primary.test_net_profit, 4),
    )


def _controlled_weights(candidates: tuple[EdgeCandidate, ...]) -> dict[str, float]:
    if len(candidates) == 1:
        return {candidates[0].candidate_id: 1.0}
    raw = {
        item.candidate_id: max(0.01, item.test_net_profit / max(item.max_drawdown, 1.0))
        for item in candidates
    }
    total = sum(raw.values())
    weights = {candidate_id: value / total for candidate_id, value in raw.items()}
    cap = 0.6
    for _ in range(4):
        capped = {candidate_id: min(weight, cap) for candidate_id, weight in weights.items()}
        remainder = 1.0 - sum(capped.values())
        uncapped = [candidate_id for candidate_id, weight in weights.items() if weight < cap]
        if abs(remainder) < 1e-9 or not uncapped:
            weights = capped
            break
        uncapped_total = sum(weights[candidate_id] for candidate_id in uncapped)
        weights = {
            candidate_id: capped[candidate_id] + (remainder * weights[candidate_id] / uncapped_total if candidate_id in uncapped and uncapped_total > 0 else 0.0)
            for candidate_id in weights
        }
    normalizer = sum(weights.values()) or 1.0
    return {candidate_id: weight / normalizer for candidate_id, weight in weights.items()}


def _candidate_correlation(left: EdgeCandidate, right: EdgeCandidate) -> float:
    left_returns = _equity_returns(left.equity_curve)
    right_returns = _equity_returns(right.equity_curve)
    size = min(len(left_returns), len(right_returns))
    if size < 3:
        return 1.0 if left.market_id == right.market_id else 0.5
    return _correlation(left_returns[-size:], right_returns[-size:])


def _equity_returns(curve: tuple[float, ...]) -> list[float]:
    values = list(curve)
    return [
        (values[index] - values[index - 1]) / max(abs(values[index - 1]), 1e-9)
        for index in range(1, len(values))
    ]


def _correlation(left: list[float], right: list[float]) -> float:
    if len(left) != len(right) or len(left) < 2:
        return 0.0
    left_mean = sum(left) / len(left)
    right_mean = sum(right) / len(right)
    numerator = sum((left_value - left_mean) * (right_value - right_mean) for left_value, right_value in zip(left, right))
    left_variance = sum((left_value - left_mean) ** 2 for left_value in left)
    right_variance = sum((right_value - right_mean) ** 2 for right_value in right)
    denominator = (left_variance * right_variance) ** 0.5
    return 0.0 if denominator == 0 else numerator / denominator


def _cost_profile(market: MarketMapping, research_store: ResearchStore) -> IGCostProfile:
    stored = research_store.get_cost_profile(market.market_id)
    if not stored:
        return public_ig_cost_profile(market)
    allowed = {field.name for field in IGCostProfile.__dataclass_fields__.values()}
    payload = {key: value for key, value in stored.items() if key in allowed}
    return IGCostProfile(**payload)


def _write_artifacts(output: EdgeDiscoveryOutput, config: EdgeRuntimeConfig, command: str) -> None:
    leaderboard = [item.as_dict() for item in output.leaderboard]
    shortlist = [item.as_dict() for item in output.shortlist]
    _write_json(output.artifact_dir / "config_used.json", asdict(config))
    _write_json(output.artifact_dir / "leaderboard.json", leaderboard)
    _write_json(output.artifact_dir / "shortlist.json", shortlist)
    _write_json(output.artifact_dir / "aggregation.json", output.aggregation_report.as_dict())
    _write_json(output.artifact_dir / "market_failures.json", list(output.market_failures))
    (output.artifact_dir / "commands.txt").write_text(command + "\n")
    (output.artifact_dir / "report.md").write_text(_markdown_report(output, config, command))


def _markdown_report(output: EdgeDiscoveryOutput, config: EdgeRuntimeConfig, command: str) -> str:
    verdict = (
        "Robust edge found under the configured hard gates."
        if output.robust_edge_found
        else "No robust edge found under the configured hard gates."
    )
    sharpe_verdict = _sharpe_verdict(output.shortlist)
    lines = [
        "# Automated Edge Discovery Report",
        "",
        "## Executive summary",
        "",
        verdict,
        sharpe_verdict,
        "",
        f"- Mode: `{config.mode}`",
        f"- Markets: `{', '.join(config.markets)}`",
        f"- Window: `{config.start}` to `{config.end}` at `{config.interval}`",
        f"- Candidates evaluated: `{len(output.leaderboard)}`",
        f"- KEEP candidates: `{len(output.shortlist)}`",
        "- Ranking objective: holdout net profit after costs first; Sharpe is tracked as a secondary quality target.",
        "- Detection controls: confidence-scored signals, top-quantile execution gating, regime gating, false-breakout suppression, and anti-churn trade spacing.",
        "",
        "## Profit-first leaderboard",
        "",
        "| Decision | Tier | Candidate | Family | Test net | Holdout Sharpe | WF Sharpe | Gross | Costs | Est spread/slip | Cost/gross | Trades | Max DD | Stress net | Fold score | Profit concentration | Failed gates |",
        "|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for item in output.leaderboard[:30]:
        lines.append(
            "| {decision} | {tier} | {candidate} | {family} | {test:.2f} | {holdout_sharpe:.2f} | {wf_sharpe:.2f} | {gross:.2f} | {costs:.2f} | {spread:.2f}/{slippage:.2f} bps | {ratio:.3f} | {trades} | {dd:.2f} | {stress:.2f} | {fold:.3f} | {concentration:.3f} | {failed} |".format(
                decision="KEEP" if item.keep else "REJECT",
                tier=item.promotion_tier,
                candidate=item.candidate_id,
                family=item.strategy_family,
                test=item.test_net_profit,
                holdout_sharpe=item.holdout_sharpe,
                wf_sharpe=item.walk_forward_sharpe,
                gross=item.gross_profit,
                costs=item.total_cost,
                spread=float(item.settings.get("estimated_spread_bps") or 0.0),
                slippage=float(item.settings.get("estimated_slippage_bps") or 0.0),
                ratio=item.cost_gross_ratio,
                trades=item.trade_count,
                dd=item.max_drawdown,
                stress=item.stressed_net_profit,
                fold=item.fold_consistency_score,
                concentration=item.max_single_fold_profit_share,
                failed=", ".join(item.failed_gates) or "-",
            )
        )
    lines.extend(["", "## Aggregation report", ""])
    aggregate = output.aggregation_report
    if aggregate.components:
        lines.extend(
            [
                f"- Aggregate test net: `{aggregate.aggregate_test_net_profit:.2f}` vs best standalone `{aggregate.best_standalone_test_net_profit:.2f}`.",
                f"- Aggregate stressed net: `{aggregate.aggregate_stressed_net_profit:.2f}`.",
                f"- Aggregate holdout Sharpe: `{aggregate.aggregate_holdout_sharpe:.2f}`.",
                f"- Profit delta vs best standalone: `{aggregate.profit_delta_vs_best_standalone:.2f}`.",
                "",
                "| Component | Weight | Correlation to primary | Test net | Stress net | Holdout Sharpe | Max DD | Contribution |",
                "|---|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for component in aggregate.components:
            lines.append(
                "| {candidate} | {weight:.0%} | {correlation:.3f} | {test:.2f} | {stress:.2f} | {sharpe:.2f} | {drawdown:.2f} | {contribution:.2f} |".format(
                    candidate=component.candidate_id,
                    weight=component.weight,
                    correlation=component.correlation_to_primary,
                    test=component.test_net_profit,
                    stress=component.stressed_net_profit,
                    sharpe=component.holdout_sharpe,
                    drawdown=component.max_drawdown,
                    contribution=component.contribution,
                )
            )
        if aggregate.correlations:
            lines.extend(["", "Pair correlations:"])
            lines.extend(
                f"- `{item['left']}` / `{item['right']}`: `{item['correlation']}`"
                for item in aggregate.correlations
            )
    else:
        lines.append("- None. No KEEP candidates survived hard gates, so no blend was proposed.")
    lines.extend(["", "## Deployment shortlist", ""])
    if output.shortlist:
        allocations = {item.candidate_id: item.weight for item in output.aggregation_report.components}
        for index, item in enumerate(output.shortlist, start=1):
            role = "Primary" if index == 1 else f"Backup {index - 1}"
            lines.append(
                f"- {role}: `{item.candidate_id}` allocation `{allocations.get(item.candidate_id, 0):.0%}`. "
                f"Regime notes: {item.detection_logic}; stress net `{item.stressed_net_profit:.2f}`, drawdown `{item.max_drawdown:.2f}`, holdout Sharpe `{item.holdout_sharpe:.2f}`."
            )
    else:
        lines.append("- None. Every candidate failed at least one hard gate.")
    lines.extend(
        [
            "",
            "## 30-day live-paper protocol",
            "",
            "- Run paper-only for 30 calendar days with daily review and weekly gate review.",
            "- Kill if cumulative paper net P/L drops below -1.0R or exceeds the configured drawdown cap.",
            "- Kill if live slippage plus spread exceeds the modeled stressed-cost envelope for three sessions.",
            "- Kill if trade count is below 40% of expected pace by day 15 without a documented market-regime reason.",
            "- Promote only if net P/L remains positive after actual costs, drawdown remains inside cap, and no single day contributes more than half of total profit.",
            "",
            "## Market/data failures",
            "",
        ]
    )
    if output.market_failures:
        lines.extend(f"- `{item['market_id']}`: {item['error']}" for item in output.market_failures)
    else:
        lines.append("- None.")
    lines.extend(
        [
            "",
            "## Reproducibility",
            "",
            f"- Command: `{command}`",
            f"- Artifact directory: `{output.artifact_dir}`",
            "- Assumption: EODHD is the discovery data source; IG profiles/cost envelopes are applied during evaluation.",
            "- Limitation: Candidates remain research-only until live-paper review and IG price validation pass.",
            "- Integrity rule: Sharpe >= 2 is aspirational only; high Sharpe without trade count, fold stability, and stress survival remains REJECT.",
        ]
    )
    if not output.robust_edge_found:
        lines.extend(
            [
                "",
                "## Next-best iteration",
                "",
                "- Narrow to the two markets with the highest rejected test net profit.",
                "- Reduce detector complexity and extend the lookback window before increasing risk.",
                "- Re-run deep mode on alternate holding horizons and a higher minimum trade spacing.",
            ]
        )
    return "\n".join(lines) + "\n"


def _sharpe_verdict(shortlist: tuple[EdgeCandidate, ...]) -> str:
    if not shortlist:
        return "Sharpe >= 2 was not assessed as robust because no candidate passed all hard gates."
    best = max(item.holdout_sharpe for item in shortlist)
    if best >= 2:
        return f"At least one KEEP candidate reached holdout Sharpe >= 2 (`{best:.2f}`), subject to the same robustness gates."
    return f"No KEEP candidate reached holdout Sharpe >= 2; best robust holdout Sharpe was `{best:.2f}`."


def _allocations(shortlist: tuple[EdgeCandidate, ...]) -> dict[str, float]:
    if len(shortlist) == 1:
        return {shortlist[0].candidate_id: 1.0}
    if len(shortlist) == 2:
        return {shortlist[0].candidate_id: 0.7, shortlist[1].candidate_id: 0.3}
    return {shortlist[0].candidate_id: 0.6, shortlist[1].candidate_id: 0.25, shortlist[2].candidate_id: 0.15}


def _ranking_key(item: EdgeCandidate) -> tuple[object, ...]:
    return (
        item.keep,
        item.test_net_profit,
        -item.max_drawdown,
        item.stressed_net_profit,
        item.fold_consistency_score,
        item.cost_efficiency_score,
        item.trade_count,
        item.holdout_sharpe,
        item.walk_forward_sharpe,
    )


def _detection_summary(family: str, settings: dict[str, object]) -> str:
    parts = [
        f"{family}",
        f"lookback={settings.get('lookback')}",
        f"threshold_bps={settings.get('threshold_bps')}",
        f"direction={settings.get('direction')}",
        f"confidence_quantile={settings.get('confidence_quantile')}",
        f"regime={settings.get('regime_filter')}",
        f"trade_spacing={settings.get('min_trade_spacing')}",
        f"false_breakout_filter={settings.get('false_breakout_filter')}",
        f"stop={settings.get('stop_loss_bps')}bps",
        f"take_profit={settings.get('take_profit_bps')}bps",
    ]
    return ", ".join(parts)


def _budget(config: EdgeRuntimeConfig) -> int:
    if config.mode == "quick":
        return config.quick_budget
    if config.mode == "deep":
        return config.deep_budget
    return max(config.quick_budget, min(config.deep_budget, config.deep_budget // 2))


def _artifact_dir(config: EdgeRuntimeConfig) -> Path:
    root = Path(config.artifact_root) if config.artifact_root else app_home() / "artifacts" / "edge_discovery"
    timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    return root / timestamp


def _cost_gross_ratio(total_cost: float, gross_profit: float) -> float:
    return total_cost / max(abs(gross_profit), 1e-9)


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _eodhd_api_token() -> str:
    env_key = os.environ.get("EODHD_API_TOKEN")
    if env_key:
        return env_key
    key = SettingsStore().get_secret("eodhd", "api_token")
    if not key:
        raise RuntimeError("EODHD API token is required. Save it in settings or set EODHD_API_TOKEN.")
    return key


def _validate_config(config: EdgeRuntimeConfig) -> None:
    if config.mode not in {"quick", "balanced", "deep"}:
        raise ValueError("mode must be quick, balanced, or deep")
    if not config.markets:
        raise ValueError("at least one market is required")
    if not config.start or not config.end:
        raise ValueError("start and end dates are required")
    if config.start > config.end:
        raise ValueError("start date must be before end date")
    if config.starting_cash <= 0:
        raise ValueError("starting_cash must be positive")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run automated profit-first edge discovery.")
    parser.add_argument("--config", default="configs/edge_discovery.yaml", help="Path to edge discovery YAML config.")
    parser.add_argument("--mode", choices=("quick", "balanced", "deep"), default=None, help="Override config mode.")
    parser.add_argument("--command", default=DEFAULT_COMMAND, help="Command string to record in artifacts.")
    return parser.parse_args()


async def _amain() -> int:
    args = _parse_args()
    command = args.command
    if args.mode:
        command = f"python -m app.edge_discovery --config {args.config} --mode {args.mode}"
    config = load_config(Path(args.config), args.mode)
    output = await run_edge_discovery(config, command=command)
    print(f"artifact_dir={output.artifact_dir}")
    print(f"robust_edge_found={str(output.robust_edge_found).lower()}")
    if output.shortlist:
        print(f"primary={output.shortlist[0].candidate_id}")
    else:
        print("primary=NONE")
        print("No robust edge found under the configured hard gates.")
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(_amain()))


if __name__ == "__main__":
    main()
