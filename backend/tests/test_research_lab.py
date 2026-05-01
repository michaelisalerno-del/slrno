from __future__ import annotations

from datetime import datetime, timedelta

from app.backtesting import BacktestConfig, BacktestResult
from app.providers.base import OHLCBar
from app.research_lab import CandidateGate, WalkForwardConfig, candidate_warnings, evaluate_candidate, top_probability_signals, walk_forward_splits
from app.research_metrics import ClassificationMetrics
from app.research_strategies import ProbabilityCandidate


def test_top_probability_signals_turns_auc_model_into_trade_filter():
    signals = top_probability_signals([0.1, 0.9, 0.8, 0.2], top_quantile=0.5)

    assert signals == [0, 1, 1, 0]


def test_walk_forward_splits_leave_final_holdout_untouched():
    splits = walk_forward_splits(100, WalkForwardConfig(train_bars=20, test_bars=10, step_bars=10, holdout_fraction=0.2))

    assert splits[-1].test_end <= 80


def test_evaluate_candidate_keeps_auc_and_backtest_gates_modular():
    bars = _bars(80)
    labels = [1 if index % 4 in (1, 2) else 0 for index in range(80)]
    probabilities = [0.9 if label else 0.1 for label in labels]
    candidate = ProbabilityCandidate("test_meta_label", ("test", "auc_gate"), {"kind": "fixture"}, probabilities)

    evaluation = evaluate_candidate(
        bars,
        labels,
        candidate,
        BacktestConfig(starting_cash=10_000, spread_bps=0, slippage_bps=0),
        WalkForwardConfig(train_bars=20, test_bars=10, step_bars=10, holdout_fraction=0.2),
        CandidateGate(
            min_total_trades=1,
            min_oos_trades=1,
            min_roc_auc=0.7,
            min_pr_auc_lift=0.0,
            min_top_precision_lift=0.0,
            min_oos_sharpe=-10,
            min_positive_fold_rate=0.0,
        ),
    )

    assert evaluation.metrics.roc_auc == 1.0
    assert evaluation.research_only is True
    assert "weak_roc_auc" not in evaluation.warnings


def test_candidate_gate_uses_daily_pnl_sharpe_for_sharpe_warning():
    warnings = candidate_warnings(
        ClassificationMetrics(0.8, 0.8, 0.01, 0.2, 0.8, 0.5, 20),
        BacktestResult(
            net_profit=500,
            sharpe=0.1,
            max_drawdown=100,
            win_rate=0.55,
            trade_count=40,
            exposure=0.2,
            turnover=40,
            train_profit=250,
            test_profit=250,
            daily_pnl_sharpe=1.1,
            sharpe_observations=20,
        ),
        (),
        CandidateGate(min_total_trades=10, min_oos_sharpe=0.7, max_drawdown_fraction=0.5),
        BacktestConfig(),
    )

    assert "weak_sharpe" not in warnings


def _bars(count: int) -> list[OHLCBar]:
    start = datetime(2026, 1, 1, 9)
    bars: list[OHLCBar] = []
    price = 100.0
    for index in range(count):
        price += 1 if index % 4 in (0, 1) else -0.25
        bars.append(OHLCBar("TEST", start + timedelta(hours=index), price, price + 0.5, price - 0.5, price))
    return bars
