from __future__ import annotations

from datetime import datetime, timedelta

from app.adaptive_research import AdaptiveSearchConfig, run_adaptive_search
from app.backtesting import BacktestConfig, run_vector_backtest
from app.ig_costs import public_ig_cost_profile
from app.market_registry import MarketMapping
from app.providers.base import OHLCBar


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
