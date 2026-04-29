from __future__ import annotations

from dataclasses import dataclass

from .backtesting import BacktestConfig, BacktestResult, run_vector_backtest
from .providers.base import OHLCBar
from .research_labels import TripleBarrierConfig, triple_barrier_labels
from .research_metrics import ClassificationMetrics, classification_metrics
from .research_strategies import ProbabilityCandidate, ProbabilityModule, default_probability_modules


@dataclass(frozen=True)
class CandidateGate:
    min_total_trades: int = 50
    min_oos_trades: int = 10
    min_roc_auc: float = 0.55
    min_pr_auc_lift: float = 0.05
    min_top_precision_lift: float = 0.08
    min_oos_sharpe: float = 0.7
    max_drawdown_fraction: float = 0.35
    min_positive_fold_rate: float = 0.6


@dataclass(frozen=True)
class WalkForwardConfig:
    train_bars: int = 500
    test_bars: int = 125
    step_bars: int = 125
    holdout_fraction: float = 0.2


@dataclass(frozen=True)
class WalkForwardFold:
    train_start: int
    train_end: int
    test_start: int
    test_end: int


@dataclass(frozen=True)
class CandidateEvaluation:
    candidate: ProbabilityCandidate
    metrics: ClassificationMetrics
    backtest: BacktestResult
    fold_results: tuple[BacktestResult, ...]
    robustness_score: float
    passed: bool
    warnings: tuple[str, ...]
    research_only: bool = True


@dataclass(frozen=True)
class ResearchStack:
    label_config: TripleBarrierConfig
    walk_forward: WalkForwardConfig
    gate: CandidateGate
    modules: tuple[ProbabilityModule, ...]

    @classmethod
    def default(cls) -> "ResearchStack":
        return cls(
            label_config=TripleBarrierConfig(),
            walk_forward=WalkForwardConfig(),
            gate=CandidateGate(),
            modules=tuple(default_probability_modules()),
        )

    def evaluate(self, bars: list[OHLCBar], backtest_config: BacktestConfig) -> list[CandidateEvaluation]:
        labels = triple_barrier_labels(bars, self.label_config)
        return [
            evaluate_candidate(bars, labels, module.generate(bars), backtest_config, self.walk_forward, self.gate)
            for module in self.modules
        ]


def evaluate_candidate(
    bars: list[OHLCBar],
    labels: list[int],
    candidate: ProbabilityCandidate,
    backtest_config: BacktestConfig,
    walk_forward: WalkForwardConfig | None = None,
    gate: CandidateGate | None = None,
) -> CandidateEvaluation:
    walk_forward = walk_forward or WalkForwardConfig()
    gate = gate or CandidateGate()
    if len(bars) != len(labels) or len(labels) != len(candidate.probabilities):
        raise ValueError("bars, labels, and probabilities must have the same length")

    signals = top_probability_signals(candidate.probabilities, top_quantile=0.2)
    metrics = classification_metrics(labels, candidate.probabilities, top_quantile=0.2)
    backtest = run_vector_backtest(bars, signals, backtest_config)
    folds = tuple(
        run_vector_backtest(
            bars[fold.test_start : fold.test_end],
            signals[fold.test_start : fold.test_end],
            backtest_config,
        )
        for fold in walk_forward_splits(len(bars), walk_forward)
        if fold.test_end - fold.test_start >= 2
    )
    warnings = candidate_warnings(metrics, backtest, folds, gate, backtest_config)
    return CandidateEvaluation(
        candidate=candidate,
        metrics=metrics,
        backtest=backtest,
        fold_results=folds,
        robustness_score=robustness_score(metrics, backtest, folds),
        passed=len(warnings) == 0,
        warnings=tuple(warnings),
    )


def top_probability_signals(probabilities: list[float], top_quantile: float = 0.2) -> list[int]:
    if not 0 < top_quantile <= 1:
        raise ValueError("top_quantile must be between 0 and 1")
    take = max(1, int(round(len(probabilities) * top_quantile)))
    threshold = sorted(probabilities, reverse=True)[take - 1]
    return [1 if probability >= threshold else 0 for probability in probabilities]


def walk_forward_splits(total_bars: int, config: WalkForwardConfig) -> list[WalkForwardFold]:
    if not 0 < config.holdout_fraction < 1:
        raise ValueError("holdout_fraction must be between 0 and 1")
    if min(config.train_bars, config.test_bars, config.step_bars) <= 0:
        raise ValueError("walk-forward window sizes must be positive")

    usable_end = int(total_bars * (1 - config.holdout_fraction))
    folds: list[WalkForwardFold] = []
    train_start = 0
    while train_start + config.train_bars + config.test_bars <= usable_end:
        train_end = train_start + config.train_bars
        test_end = train_end + config.test_bars
        folds.append(WalkForwardFold(train_start, train_end, train_end, test_end))
        train_start += config.step_bars
    return folds


def candidate_warnings(
    metrics: ClassificationMetrics,
    backtest: BacktestResult,
    folds: tuple[BacktestResult, ...],
    gate: CandidateGate,
    backtest_config: BacktestConfig,
) -> list[str]:
    warnings: list[str] = []
    if backtest.trade_count < gate.min_total_trades:
        warnings.append("low_trades")
    if metrics.roc_auc is None or metrics.roc_auc < gate.min_roc_auc:
        warnings.append("weak_roc_auc")
    baseline = metrics.positive_rate
    if metrics.pr_auc is None or metrics.pr_auc < baseline + gate.min_pr_auc_lift:
        warnings.append("weak_pr_auc")
    if metrics.precision_at_top_quantile < baseline + gate.min_top_precision_lift:
        warnings.append("weak_top_precision")
    if backtest.sharpe < gate.min_oos_sharpe:
        warnings.append("weak_sharpe")
    if backtest.net_profit <= 0:
        warnings.append("negative_profit")
    if backtest.max_drawdown > backtest_config.starting_cash * gate.max_drawdown_fraction:
        warnings.append("excess_drawdown")
    if folds:
        positive_rate = sum(1 for fold in folds if fold.net_profit > 0) / len(folds)
        if positive_rate < gate.min_positive_fold_rate:
            warnings.append("unstable_folds")
        if sum(fold.trade_count for fold in folds) < gate.min_oos_trades:
            warnings.append("low_oos_trades")
    else:
        warnings.append("no_walk_forward_folds")
    return warnings


def robustness_score(metrics: ClassificationMetrics, backtest: BacktestResult, folds: tuple[BacktestResult, ...]) -> float:
    auc_component = 0.0 if metrics.roc_auc is None else max(0.0, (metrics.roc_auc - 0.5) * 2)
    precision_lift = max(0.0, metrics.precision_at_top_quantile - metrics.positive_rate)
    profit_component = max(0.0, min(2.0, backtest.sharpe)) / 2
    drawdown_penalty = min(1.0, backtest.max_drawdown / max(1.0, abs(backtest.net_profit) + backtest.max_drawdown))
    fold_component = 0.0
    if folds:
        fold_component = sum(1 for fold in folds if fold.net_profit > 0) / len(folds)
    return round(
        100 * (0.3 * auc_component + 0.25 * precision_lift + 0.3 * profit_component + 0.25 * fold_component - 0.1 * drawdown_penalty),
        4,
    )
