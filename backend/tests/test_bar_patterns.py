from __future__ import annotations

from datetime import datetime, timedelta

from app.bar_patterns import analyze_market_regimes, analyze_strategy_patterns
from app.backtesting import BacktestConfig, run_vector_backtest
from app.providers.base import OHLCBar


def test_market_regime_analysis_labels_rebound_after_selloff():
    bars = _selloff_rebound_bars()

    analysis = analyze_market_regimes(bars)

    assert analysis["bar_count"] == len(bars)
    assert analysis["regime_counts"]["rebound_after_selloff"] >= 1
    assert analysis["segments"]


def test_strategy_pattern_analysis_warns_when_edge_only_works_in_shock_regime():
    bars = _selloff_rebound_bars()
    signals = [0] * len(bars)
    signals[8] = 1
    config = BacktestConfig(spread_bps=0, slippage_bps=0, fx_conversion_bps=0)
    backtest = run_vector_backtest(bars, signals, config)

    analysis = analyze_strategy_patterns(bars, signals, config, backtest)

    assert "rebound_after_selloff" in analysis["allowed_regimes"]
    assert analysis["regime_gated_backtest"]["net_profit"] > 0
    assert analysis["regime_summary"][0]["sharpe_days"] >= 1
    assert analysis["regime_summary"][0]["max_drawdown"] >= 0
    assert analysis["worst_regime"]["key"] is not None
    assert "high_volatility_only_edge" in analysis["warnings"]
    assert "shock_regime_dependency" in analysis["warnings"]
    assert analysis["trade_summary"]["trade_segments"] >= 1


def test_strategy_pattern_analysis_marks_negative_gated_oos_as_headline_only():
    bars = _selloff_rebound_bars()
    signals = [0] * len(bars)
    signals[8] = 1
    config = BacktestConfig(spread_bps=0, slippage_bps=0, fx_conversion_bps=0, train_fraction=0.8)
    backtest = run_vector_backtest(bars, signals, config)

    analysis = analyze_strategy_patterns(bars, signals, config, backtest)

    assert "regime_gated_oos_negative" in analysis["warnings"]
    assert analysis["regime_verdict"] in {"headline_only", "thin_regime_sample"}


def _selloff_rebound_bars() -> list[OHLCBar]:
    start = datetime(2026, 1, 1, 16)
    closes = [100, 99, 98, 97, 96, 94, 91, 88, 86, 104, 104.2, 104.1, 104.3, 104.2, 104.4]
    bars: list[OHLCBar] = []
    for index, close in enumerate(closes):
        open_price = closes[index - 1] if index else close
        high = max(open_price, close) + 0.2
        low = min(open_price, close) - 0.2
        bars.append(OHLCBar("TEST", start + timedelta(days=index), open_price, high, low, close))
    return bars
