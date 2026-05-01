from __future__ import annotations

from datetime import datetime, timedelta

from app.adaptive_research import AdaptiveSearchConfig, _generate_signals, _promotion_tier, _warnings, balanced_score, run_adaptive_search
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


def test_high_drawdown_profitable_lead_remains_research_candidate():
    market = MarketMapping("TEST", "Synthetic", "index", "TEST", "", spread_bps=2, slippage_bps=1)
    profile = public_ig_cost_profile(market)
    backtest = BacktestResult(
        net_profit=20_000,
        sharpe=0.8,
        max_drawdown=12_000,
        win_rate=0.52,
        trade_count=120,
        exposure=0.5,
        turnover=120,
        train_profit=10_000,
        test_profit=10_000,
        gross_profit=25_000,
        total_cost=5_000,
        daily_pnl_sharpe=1.2,
        expectancy_per_trade=166.66,
        average_cost_per_trade=41.66,
        net_cost_ratio=4.0,
        cost_to_gross_ratio=0.2,
    )
    evaluation = CandidateEvaluation(
        candidate=ProbabilityCandidate("high_drawdown_fixture", ("adaptive_ig_v1",), {"stress_net_profit": 8_000}, [0.5, 0.6]),
        metrics=ClassificationMetrics(0.6, 0.6, 0.2, 0.6, 0.6, 0.5, 2),
        backtest=backtest,
        fold_results=(backtest,),
        robustness_score=49,
        passed=False,
        warnings=("drawdown_too_high",),
    )

    assert _promotion_tier(evaluation, stability=0.2, cost_profile=profile) == "research_candidate"


def test_weak_sharpe_warning_uses_daily_pnl_sharpe_for_intraday_results():
    market = MarketMapping("TEST", "Synthetic", "index", "TEST", "", spread_bps=2, slippage_bps=1)
    profile = public_ig_cost_profile(market)
    backtest = BacktestResult(
        net_profit=1_000,
        sharpe=0.1,
        max_drawdown=200,
        win_rate=0.55,
        trade_count=40,
        exposure=0.3,
        turnover=40,
        train_profit=500,
        test_profit=500,
        gross_profit=1_200,
        total_cost=200,
        daily_pnl_sharpe=1.4,
        sharpe_observations=30,
        expectancy_per_trade=25,
        average_cost_per_trade=5,
        net_cost_ratio=5,
        cost_to_gross_ratio=0.1667,
    )

    warnings = _warnings(backtest, (backtest,), backtest, BacktestConfig(), "mean_reversion", profile)

    assert "weak_sharpe" not in warnings


def test_weak_sharpe_warning_still_flags_low_daily_pnl_sharpe():
    market = MarketMapping("TEST", "Synthetic", "index", "TEST", "", spread_bps=2, slippage_bps=1)
    profile = public_ig_cost_profile(market)
    backtest = BacktestResult(
        net_profit=1_000,
        sharpe=1.5,
        max_drawdown=200,
        win_rate=0.55,
        trade_count=40,
        exposure=0.3,
        turnover=40,
        train_profit=500,
        test_profit=500,
        gross_profit=1_200,
        total_cost=200,
        daily_pnl_sharpe=0.2,
        sharpe_observations=30,
        expectancy_per_trade=25,
        average_cost_per_trade=5,
        net_cost_ratio=5,
        cost_to_gross_ratio=0.1667,
    )

    warnings = _warnings(backtest, (backtest,), backtest, BacktestConfig(), "mean_reversion", profile)

    assert "weak_sharpe" in warnings


def test_turnaround_tuesday_signals_after_down_monday():
    bars = [
        OHLCBar("TEST", datetime(2026, 1, 2, 16), 100, 101, 99, 100),
        OHLCBar("TEST", datetime(2026, 1, 5, 16), 96, 97, 94, 95),
        OHLCBar("TEST", datetime(2026, 1, 6, 9), 95, 97, 94, 96),
        OHLCBar("TEST", datetime(2026, 1, 6, 10), 96, 98, 95, 97),
        OHLCBar("TEST", datetime(2026, 1, 7, 9), 97, 98, 96, 97),
    ]
    signals = _generate_signals(
        bars,
        "calendar_turnaround_tuesday",
        {
            "lookback": 1,
            "threshold_bps": 100,
            "z_threshold": 1,
            "volatility_multiplier": 1,
            "stop_loss_bps": 500,
            "take_profit_bps": 500,
            "max_hold_bars": 4,
            "min_hold_bars": 1,
            "min_trade_spacing": 0,
            "confidence_quantile": 1.0,
            "regime_filter": "any",
            "direction": "long_only",
            "weekday": 1,
            "previous_day_filter": "monday_down",
        },
    )

    assert all(signal >= 0 for signal in signals)
    assert any(signal > 0 for bar, signal in zip(bars, signals) if bar.timestamp.weekday() == 1)


def test_month_end_seasonality_signals_last_trading_days():
    bars = [
        OHLCBar("TEST", datetime(2026, 1, 26 + offset, 16), 100 + offset, 101 + offset, 99 + offset, 100 + offset)
        for offset in range(5)
    ]
    signals = _generate_signals(
        bars,
        "month_end_seasonality",
        {
            "lookback": 1,
            "threshold_bps": 10,
            "z_threshold": 1,
            "volatility_multiplier": 1,
            "stop_loss_bps": 500,
            "take_profit_bps": 500,
            "max_hold_bars": 4,
            "min_hold_bars": 1,
            "min_trade_spacing": 0,
            "confidence_quantile": 1.0,
            "regime_filter": "any",
            "direction": "long_only",
            "month_end_window": 2,
            "month_start_window": 0,
        },
    )

    active_dates = {bar.timestamp.date() for bar, signal in zip(bars, signals) if signal > 0}
    assert datetime(2026, 1, 29).date() in active_dates
    assert datetime(2026, 1, 30).date() in active_dates


def test_research_ideas_style_runs_calendar_family_trials():
    market = MarketMapping("TEST", "Synthetic", "index", "TEST", "", spread_bps=1, slippage_bps=0.5)
    profile = public_ig_cost_profile(market)
    bars = _calendar_research_bars()

    result = run_adaptive_search(
        bars,
        "TEST",
        "1day",
        profile,
        AdaptiveSearchConfig(preset="quick", trading_style="research_ideas", search_budget=8, seed=4),
    )

    families = {evaluation.candidate.parameters["family"] for evaluation in result.evaluations}
    assert {"calendar_turnaround_tuesday", "month_end_seasonality"}.issubset(families)
    assert all(evaluation.candidate.parameters["direction"] == "long_only" for evaluation in result.evaluations)


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


def _calendar_research_bars() -> list[OHLCBar]:
    start = datetime(2026, 1, 1, 16)
    price = 100.0
    bars: list[OHLCBar] = []
    for index in range(180):
        timestamp = start + timedelta(days=index)
        if timestamp.weekday() >= 5:
            continue
        if timestamp.weekday() == 0:
            price *= 0.992
        elif timestamp.weekday() == 1:
            price *= 1.008
        elif index % 17 == 0:
            price *= 1.004
        else:
            price *= 1.001
        bars.append(
            OHLCBar(
                "TEST",
                timestamp,
                price * 0.998,
                price * 1.004,
                price * 0.996,
                price,
            )
        )
    return bars
