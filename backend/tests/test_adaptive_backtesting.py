from __future__ import annotations

from datetime import datetime, timedelta

from app.adaptive_research import AdaptiveSearchConfig, _promotion_tier, balanced_score, run_adaptive_search
from app.backtesting import BacktestConfig, BacktestResult, run_vector_backtest
from app.ig_costs import public_ig_cost_profile
from app.market_registry import MarketMapping
from app.providers.base import OHLCBar
from app.research_lab import CandidateEvaluation
from app.research_metrics import ClassificationMetrics
from app.research_strategies import ProbabilityCandidate


def test_backtest_reports_spread_slippage_funding_and_fx_costs():
    bars = [
        OHLCBar("TEST", datetime(2026, 1, 1, 21), 100, 101, 99, 100),
        OHLCBar("TEST", datetime(2026, 1, 1, 23), 101, 102, 100, 101),
        OHLCBar("TEST", datetime(2026, 1, 2, 9), 102, 103, 101, 102),
    ]
    signals = [1, 1, 0]

    result = run_vector_backtest(
        bars,
        signals,
        BacktestConfig(
            spread_bps=10,
            slippage_bps=5,
            position_size=1,
            overnight_admin_fee_annual=0.365,
            fx_conversion_bps=50,
            instrument_currency="USD",
            account_currency="GBP",
        ),
    )

    assert result.gross_profit == 2
    assert result.spread_cost > 0
    assert result.slippage_cost > 0
    assert result.funding_cost > 0
    assert result.fx_cost > 0
    assert result.net_profit < result.gross_profit
    assert result.expectancy_per_trade != 0
    assert result.average_cost_per_trade > 0
    assert result.cost_to_gross_ratio > 0


def test_adaptive_search_returns_ranked_trials_with_cost_warnings():
    market = MarketMapping("TEST", "Synthetic", "index", "TEST", "", spread_bps=1, slippage_bps=0.5)
    profile = public_ig_cost_profile(market)
    bars = _trend_bars(140)

    result = run_adaptive_search(
        bars,
        "TEST",
        "5min",
        profile,
        AdaptiveSearchConfig(preset="quick", trading_style="intraday_only", search_budget=9, seed=3),
    )

    assert len(result.evaluations) == 9
    assert result.evaluations[0].robustness_score >= result.evaluations[-1].robustness_score
    assert result.pareto
    assert any("needs_ig_price_validation" in evaluation.warnings for evaluation in result.evaluations)
    best = result.evaluations[0]
    assert best.promotion_tier in {"watchlist", "research_candidate", "paper_candidate", "validated_candidate", "reject"}
    assert best.candidate.parameters["promotion_tier"] == best.promotion_tier
    assert best.candidate.parameters["search_audit"]["trial_count"] == 9
    assert "deflated_sharpe_probability" in best.candidate.parameters["sharpe_diagnostics"]
    assert "parameter_stability_score" in best.candidate.parameters
    assert "net_cost_ratio" in best.backtest.__dict__
    assert best.backtest.cost_to_gross_ratio >= 0


def test_balanced_score_prefers_same_profit_with_less_cost_churn():
    config = BacktestConfig()
    efficient = BacktestResult(
        net_profit=600,
        sharpe=1.2,
        max_drawdown=200,
        win_rate=0.55,
        trade_count=24,
        exposure=0.4,
        turnover=24,
        train_profit=300,
        test_profit=300,
        gross_profit=750,
        total_cost=150,
        daily_pnl_sharpe=1.2,
        expectancy_per_trade=25,
        average_cost_per_trade=6.25,
        net_cost_ratio=4.0,
        cost_to_gross_ratio=0.2,
    )
    churn = BacktestResult(
        net_profit=600,
        sharpe=1.2,
        max_drawdown=200,
        win_rate=0.55,
        trade_count=520,
        exposure=0.9,
        turnover=520,
        train_profit=300,
        test_profit=300,
        gross_profit=3_300,
        total_cost=2_700,
        daily_pnl_sharpe=1.2,
        expectancy_per_trade=1.15,
        average_cost_per_trade=5.19,
        net_cost_ratio=0.22,
        cost_to_gross_ratio=0.82,
    )

    assert balanced_score(efficient, (efficient,), efficient, config) > balanced_score(churn, (churn,), churn, config)


def test_negative_total_net_does_not_promote_to_research_candidate():
    market = MarketMapping("TEST", "Synthetic", "index", "TEST", "", spread_bps=2, slippage_bps=1)
    profile = public_ig_cost_profile(market)
    backtest = BacktestResult(
        net_profit=-100,
        sharpe=1.1,
        max_drawdown=250,
        win_rate=0.55,
        trade_count=60,
        exposure=0.5,
        turnover=60,
        train_profit=-200,
        test_profit=100,
        gross_profit=1_200,
        total_cost=1_300,
        daily_pnl_sharpe=1.1,
        expectancy_per_trade=-1.66,
        average_cost_per_trade=21.66,
        net_cost_ratio=-0.08,
        cost_to_gross_ratio=1.08,
    )
    evaluation = CandidateEvaluation(
        candidate=ProbabilityCandidate("fixture", ("adaptive_ig_v1",), {"stress_net_profit": 50}, [0.5, 0.6]),
        metrics=ClassificationMetrics(0.6, 0.6, 0.2, 0.6, 0.6, 0.5, 2),
        backtest=backtest,
        fold_results=(backtest,),
        robustness_score=50,
        passed=True,
        warnings=(),
    )

    assert _promotion_tier(evaluation, stability=1.0, cost_profile=profile) == "watchlist"


def _trend_bars(count: int) -> list[OHLCBar]:
    start = datetime(2026, 1, 1, 9)
    price = 100.0
    bars: list[OHLCBar] = []
    for index in range(count):
        drift = 0.35 if index % 30 < 22 else -0.15
        price += drift
        bars.append(
            OHLCBar(
                "TEST",
                start + timedelta(minutes=5 * index),
                price - 0.1,
                price + 0.4,
                price - 0.4,
                price,
            )
        )
    return bars
