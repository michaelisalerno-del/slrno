from __future__ import annotations

from datetime import datetime, timedelta

from app.adaptive_research import AdaptiveSearchConfig, _cost_aware_score, _generate_signals, _promotion_tier, _warnings, balanced_score, run_adaptive_search
from app.backtesting import BacktestConfig, BacktestResult, run_vector_backtest
from app.ig_costs import IGCostProfile, public_ig_cost_profile
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
    assert "bar_pattern_analysis" in best.candidate.parameters
    assert "evidence_profile" in best.candidate.parameters
    assert "positive_fold_rate" in best.candidate.parameters["evidence_profile"]
    assert "parameter_stability_score" in best.candidate.parameters
    assert "net_cost_ratio" in best.backtest.__dict__
    assert best.backtest.starting_cash == 2000
    assert best.backtest.compounded_position_sizing is True
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


def test_target_regime_scoring_uses_in_regime_evidence():
    flat_full_period = BacktestResult(
        net_profit=0,
        sharpe=0,
        max_drawdown=900,
        win_rate=0.5,
        trade_count=30,
        exposure=0.1,
        turnover=30,
        train_profit=0,
        test_profit=0,
        gross_profit=1_000,
        total_cost=1_000,
        daily_pnl_sharpe=0,
        expectancy_per_trade=0,
        average_cost_per_trade=33,
        net_cost_ratio=0,
        cost_to_gross_ratio=1,
    )
    metrics = ClassificationMetrics(0.6, 0.6, 0.2, 0.6, 0.6, 0.5, 2)
    untargeted = CandidateEvaluation(
        candidate=ProbabilityCandidate("full", ("adaptive_ig_v1",), {"stress_net_profit": 0}, [0.5, 0.6]),
        metrics=metrics,
        backtest=flat_full_period,
        fold_results=(flat_full_period,),
        robustness_score=0,
        passed=False,
        warnings=(),
    )
    targeted = CandidateEvaluation(
        candidate=ProbabilityCandidate(
            "target",
            ("adaptive_ig_v1",),
            {
                "target_regime": "trend_up",
                "stress_net_profit": 500,
                "bar_pattern_analysis": {
                    "regime_trade_evidence": {
                        "available": True,
                        "target_regime": "trend_up",
                        "regime_trading_days": 140,
                        "regime_history_share": 0.4,
                        "regime_episodes": 8,
                        "in_regime": {
                            "net_profit": 900,
                            "test_profit": 450,
                            "daily_pnl_sharpe": 2.1,
                            "sharpe_days": 140,
                            "trade_count": 30,
                            "max_drawdown": 180,
                            "gross_profit": 1_100,
                            "cost": 200,
                        },
                    },
                },
            },
            [0.5, 0.6],
        ),
        metrics=metrics,
        backtest=flat_full_period,
        fold_results=(flat_full_period,),
        robustness_score=0,
        passed=False,
        warnings=(),
    )

    diagnostics = {"deflated_sharpe_probability": 0.0}
    config = AdaptiveSearchConfig(repair_mode="auto_refine")

    assert _cost_aware_score(targeted, diagnostics, 0.5, config) > _cost_aware_score(untargeted, diagnostics, 0.5, config)


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


def test_promotion_tier_requires_fresh_sharpe_days_and_live_ig_costs():
    public_profile = public_ig_cost_profile(MarketMapping("TEST", "Synthetic", "index", "TEST", "", spread_bps=2, slippage_bps=1))
    live_profile = IGCostProfile(
        market_id="TEST",
        spread_bps=2,
        slippage_bps=1,
        confidence="ig_live_epic_cost_profile",
    )
    backtest = BacktestResult(
        net_profit=1_000,
        sharpe=1.5,
        max_drawdown=200,
        win_rate=0.55,
        trade_count=40,
        exposure=0.4,
        turnover=40,
        train_profit=500,
        test_profit=500,
        gross_profit=1_400,
        total_cost=200,
        daily_pnl_sharpe=1.6,
        sharpe_observations=140,
        expectancy_per_trade=25,
        average_cost_per_trade=5,
        net_cost_ratio=5,
        cost_to_gross_ratio=0.1428,
        estimated_spread_bps=2,
        estimated_slippage_bps=1,
    )
    evaluation = CandidateEvaluation(
        candidate=ProbabilityCandidate("ready_fixture", ("adaptive_ig_v1",), {"stress_net_profit": 500}, [0.5, 0.6]),
        metrics=ClassificationMetrics(0.6, 0.6, 0.2, 0.6, 0.6, 0.5, 2),
        backtest=backtest,
        fold_results=(backtest, backtest),
        robustness_score=80,
        passed=True,
        warnings=(),
    )
    short_sample = CandidateEvaluation(
        candidate=evaluation.candidate,
        metrics=evaluation.metrics,
        backtest=BacktestResult(**{**backtest.__dict__, "sharpe_observations": 40}),
        fold_results=evaluation.fold_results,
        robustness_score=evaluation.robustness_score,
        passed=True,
        warnings=(),
    )

    assert _promotion_tier(evaluation, stability=0.7, cost_profile=public_profile) == "research_candidate"
    assert _promotion_tier(short_sample, stability=0.7, cost_profile=live_profile) == "research_candidate"
    assert _promotion_tier(evaluation, stability=0.7, cost_profile=live_profile) == "validated_candidate"


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


def test_short_sharpe_sample_is_flagged_separately_from_weak_sharpe():
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
        daily_pnl_sharpe=2.2,
        sharpe_observations=35,
        expectancy_per_trade=25,
        average_cost_per_trade=5,
        net_cost_ratio=5,
        cost_to_gross_ratio=0.1667,
    )

    warnings = _warnings(backtest, (backtest,), backtest, BacktestConfig(), "mean_reversion", profile)

    assert "weak_sharpe" not in warnings
    assert "short_sharpe_sample" in warnings


def test_warnings_flag_weak_oos_and_fold_concentration():
    market = MarketMapping("TEST", "Synthetic", "index", "TEST", "", spread_bps=2, slippage_bps=1)
    profile = public_ig_cost_profile(market)
    backtest = BacktestResult(
        net_profit=600,
        sharpe=1.0,
        max_drawdown=100,
        win_rate=0.55,
        trade_count=40,
        exposure=0.3,
        turnover=40,
        train_profit=800,
        test_profit=100,
        gross_profit=900,
        total_cost=300,
        daily_pnl_sharpe=1.0,
        sharpe_observations=140,
        expectancy_per_trade=15,
        average_cost_per_trade=7.5,
        net_cost_ratio=2,
        cost_to_gross_ratio=0.333,
    )
    positive_fold = BacktestResult(**{**backtest.__dict__, "net_profit": 100, "trade_count": 4})
    negative_fold = BacktestResult(**{**backtest.__dict__, "net_profit": -150, "trade_count": 4})

    warnings = _warnings(backtest, (positive_fold, negative_fold), backtest, BacktestConfig(), "mean_reversion", profile)

    assert "weak_oos_evidence" in warnings
    assert "low_oos_trades" in warnings
    assert "one_fold_dependency" in warnings


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


def test_liquidity_sweep_reversal_signals_support_reclaim():
    start = datetime(2026, 1, 1, 9)
    bars = [
        OHLCBar("TEST", start + timedelta(minutes=5 * index), 100.2, 100.6, 100.0, 100.3)
        for index in range(12)
    ]
    bars.append(OHLCBar("TEST", start + timedelta(minutes=60), 100.1, 100.7, 99.7, 100.45))
    bars.extend(
        OHLCBar("TEST", start + timedelta(minutes=65 + 5 * index), 100.45, 100.9, 100.3, 100.7)
        for index in range(4)
    )

    signals = _generate_signals(
        bars,
        "liquidity_sweep_reversal",
        {
            "lookback": 12,
            "threshold_bps": 8,
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
        },
    )

    assert signals[12] > 0


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


def test_regime_specialist_scans_are_opt_in_and_gated_to_target_regime():
    market = MarketMapping("TEST", "Synthetic", "index", "TEST", "", spread_bps=1, slippage_bps=0.5)
    profile = public_ig_cost_profile(market)
    bars = _daily_trend_bars(90)

    normal = run_adaptive_search(
        bars,
        "TEST",
        "1day",
        profile,
        AdaptiveSearchConfig(preset="quick", trading_style="intraday_only", search_budget=6, seed=5),
    )
    thorough = run_adaptive_search(
        bars,
        "TEST",
        "1day",
        profile,
        AdaptiveSearchConfig(
            preset="quick",
            trading_style="intraday_only",
            search_budget=6,
            include_regime_scans=True,
            regime_scan_budget_per_regime=2,
            seed=5,
        ),
    )

    assert normal.regime_scan["enabled"] is False
    assert len(normal.evaluations) == 6
    assert thorough.regime_scan["enabled"] is True
    assert thorough.regime_scan["trial_count"] > 0
    specialist = [evaluation for evaluation in thorough.evaluations if evaluation.candidate.parameters.get("regime_scan")]
    assert specialist
    for evaluation in specialist:
        target = evaluation.candidate.parameters["target_regime"]
        analysis = evaluation.candidate.parameters["bar_pattern_analysis"]
        assert analysis["target_regime"] == target
        for row in analysis["regime_summary"]:
            if row["key"] != target:
                assert row["active_bars"] == 0


def test_target_regime_refine_gates_base_trials_to_that_regime():
    market = MarketMapping("TEST", "Synthetic", "index", "TEST", "", spread_bps=1, slippage_bps=0.5)
    profile = public_ig_cost_profile(market)
    bars = _daily_trend_bars(90)

    result = run_adaptive_search(
        bars,
        "TEST",
        "1day",
        profile,
        AdaptiveSearchConfig(
            preset="quick",
            trading_style="intraday_only",
            search_budget=6,
            target_regime="trend_up",
            seed=5,
        ),
    )

    assert result.regime_scan["target_regime"] == "trend_up"
    assert len(result.evaluations) == 6
    for evaluation in result.evaluations:
        parameters = evaluation.candidate.parameters
        analysis = parameters["bar_pattern_analysis"]
        assert parameters["target_regime"] == "trend_up"
        assert parameters["regime_targeted_refine"] is True
        assert parameters["search_audit"]["grade_mode"] == "target_regime"
        assert parameters["search_audit"]["grade_regime"] == "trend_up"
        assert analysis["target_regime"] == "trend_up"
        assert analysis["regime_trade_evidence"]["target_regime"] == "trend_up"
        for row in analysis["regime_summary"]:
            if row["key"] != "trend_up":
                assert row["active_bars"] == 0


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


def _daily_trend_bars(count: int) -> list[OHLCBar]:
    start = datetime(2025, 1, 1, 16)
    price = 100.0
    bars: list[OHLCBar] = []
    for index in range(count):
        price *= 1.004 if index % 12 < 10 else 0.998
        bars.append(
            OHLCBar(
                "TEST",
                start + timedelta(days=index),
                price * 0.999,
                price * 1.004,
                price * 0.996,
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
