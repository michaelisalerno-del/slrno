from __future__ import annotations

from dataclasses import replace

from app.backtesting import BacktestResult
from app.edge_discovery import (
    EdgeRuntimeConfig,
    _build_aggregation_report,
    _failed_gates,
    _fold_consistency,
    _profit_concentration,
    _ranking_key,
)
from app.research_lab import CandidateEvaluation
from app.research_metrics import ClassificationMetrics
from app.research_strategies import ProbabilityCandidate


def test_profit_first_gates_keep_only_cost_robust_holdout_profit():
    evaluation = _evaluation(
        backtest=_backtest(test_profit=125, net_profit=250, gross_profit=350, total_cost=80, trade_count=42, max_drawdown=400),
        folds=(
            _backtest(net_profit=10),
            _backtest(net_profit=20),
            _backtest(net_profit=30),
        ),
    )

    failed = _failed_gates(
        evaluation,
        stressed_net_profit=90,
        cost_gross_ratio=80 / 350,
        fold_consistency=1.0,
        profit_concentration=0.5,
        config=EdgeRuntimeConfig(),
    )

    assert failed == []


def test_profit_first_gates_reject_any_failed_hard_gate():
    evaluation = _evaluation(
        backtest=_backtest(test_profit=-1, gross_profit=20, total_cost=30, trade_count=2, max_drawdown=5000),
        folds=(
            _backtest(net_profit=-5),
            _backtest(net_profit=30),
        ),
    )

    failed = _failed_gates(
        evaluation,
        stressed_net_profit=-10,
        cost_gross_ratio=1.5,
        fold_consistency=0.2,
        profit_concentration=0.9,
        config=EdgeRuntimeConfig(),
    )

    assert failed == [
        "holdout_net_profit",
        "stressed_cost_net_profit",
        "fold_consistency",
        "trade_count",
        "max_drawdown",
        "cost_gross_efficiency",
        "profit_concentration",
    ]


def test_ranking_prefers_keep_then_holdout_profit():
    weak_keep = _candidate_like(keep=True, test_net_profit=10, max_drawdown=100)
    strong_keep = _candidate_like(keep=True, test_net_profit=20, max_drawdown=150)
    reject = _candidate_like(keep=False, test_net_profit=1_000, max_drawdown=10)

    ranked = sorted([weak_keep, reject, strong_keep], key=_ranking_key, reverse=True)

    assert ranked == [strong_keep, weak_keep, reject]


def test_fold_consistency_penalizes_one_fold_luck():
    evaluation = _evaluation(
        backtest=_backtest(),
        folds=(
            _backtest(net_profit=1),
            _backtest(net_profit=1),
            _backtest(net_profit=98),
            _backtest(net_profit=-1),
        ),
    )

    assert _fold_consistency(evaluation) < 0.75
    assert _profit_concentration(evaluation) > 0.55


def test_aggregation_uses_controlled_weights_and_correlations():
    primary = _candidate_like(keep=True, test_net_profit=100, max_drawdown=100, equity_curve=(100, 110, 120, 130))
    backup = _candidate_like(keep=True, test_net_profit=60, max_drawdown=80, equity_curve=(100, 98, 103, 101))
    reject = _candidate_like(keep=False, test_net_profit=500, max_drawdown=20, equity_curve=(100, 120, 140, 160))

    report = _build_aggregation_report((primary, backup, reject))

    assert [item.candidate_id for item in report.components] == [primary.candidate_id, backup.candidate_id]
    assert round(sum(item.weight for item in report.components), 6) == 1
    assert max(item.weight for item in report.components) <= 0.6
    assert report.aggregate_test_net_profit > 0


def _candidate_like(keep: bool, test_net_profit: float, max_drawdown: float, equity_curve: tuple[float, ...] = (100, 101, 102)):
    from app.edge_discovery import EdgeCandidate

    return EdgeCandidate(
        candidate_id=str(test_net_profit),
        market_id="TEST",
        strategy_name="fixture",
        strategy_family="trend",
        detection_logic="fixture",
        test_net_profit=test_net_profit,
        net_profit=test_net_profit,
        gross_profit=test_net_profit,
        holdout_sharpe=1.2,
        walk_forward_sharpe=1.0,
        total_cost=1,
        cost_gross_ratio=0.1,
        trade_count=40,
        max_drawdown=max_drawdown,
        stressed_net_profit=test_net_profit,
        fold_consistency_score=0.8,
        max_single_fold_profit_share=0.5,
        cost_efficiency_score=0.9,
        promotion_tier="paper_candidate" if keep else "research_candidate",
        keep=keep,
        failed_gates=(),
        warnings=(),
        settings={},
        cost_confidence="fixture",
        validation_status="fixture",
        equity_curve=equity_curve,
    )


def _evaluation(backtest: BacktestResult, folds: tuple[BacktestResult, ...]) -> CandidateEvaluation:
    return CandidateEvaluation(
        candidate=ProbabilityCandidate("fixture", ("fixture",), {"family": "fixture"}, [0.5, 0.6]),
        metrics=ClassificationMetrics(
            roc_auc=0.6,
            pr_auc=0.6,
            brier_score=0.2,
            log_loss=0.6,
            precision_at_top_quantile=0.6,
            positive_rate=0.5,
            sample_count=2,
        ),
        backtest=backtest,
        fold_results=folds,
        robustness_score=1,
        passed=True,
        warnings=(),
    )


def _backtest(
    test_profit: float = 50,
    net_profit: float = 100,
    gross_profit: float = 150,
    total_cost: float = 10,
    trade_count: int = 50,
    max_drawdown: float = 250,
) -> BacktestResult:
    result = BacktestResult(
        net_profit=net_profit,
        sharpe=1,
        train_sharpe=1,
        test_sharpe=1,
        max_drawdown=max_drawdown,
        win_rate=0.55,
        trade_count=trade_count,
        exposure=0.5,
        turnover=trade_count,
        train_profit=net_profit - test_profit,
        test_profit=test_profit,
        gross_profit=gross_profit,
        total_cost=total_cost,
    )
    return replace(result)
