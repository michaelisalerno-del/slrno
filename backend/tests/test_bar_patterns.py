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
    config = BacktestConfig(spread_bps=0, slippage_bps=0, fx_conversion_bps=0, overnight_admin_fee_annual=0)
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


def test_strategy_pattern_analysis_reports_target_regime_trade_evidence():
    bars = _selloff_rebound_bars()
    signals = [0] * len(bars)
    signals[8] = 1
    config = BacktestConfig(spread_bps=0, slippage_bps=0, fx_conversion_bps=0, overnight_admin_fee_annual=0)
    backtest = run_vector_backtest(bars, signals, config)

    analysis = analyze_strategy_patterns(bars, signals, config, backtest, target_regime="rebound_after_selloff")
    evidence = analysis["regime_trade_evidence"]
    in_regime = evidence["in_regime"]

    assert evidence["available"] is True
    assert evidence["target_regime"] == "rebound_after_selloff"
    assert evidence["is_targeted"] is True
    assert evidence["regime_trading_days"] >= 1
    assert 0 < evidence["regime_history_share"] <= 1
    assert evidence["regime_episodes"] >= 1
    assert in_regime["net_profit"] > 0
    assert in_regime["trade_count"] >= 1
    assert evidence["full_history_gated"]["net_profit"] == analysis["regime_gated_backtest"]["net_profit"]


def test_strategy_pattern_analysis_does_not_mark_negative_target_regime_tradeable():
    bars = _selloff_rebound_bars()
    signals = [0] * len(bars)
    signals[8] = -1
    config = BacktestConfig(spread_bps=0, slippage_bps=0, fx_conversion_bps=0, overnight_admin_fee_annual=0)
    backtest = run_vector_backtest(bars, signals, config)

    analysis = analyze_strategy_patterns(bars, signals, config, backtest, target_regime="rebound_after_selloff")

    assert backtest.net_profit < 0
    assert "regime_gated_backtest_negative" in analysis["warnings"]
    assert analysis["regime_verdict"] == "headline_only"


def test_strategy_pattern_analysis_marks_negative_gated_oos_as_headline_only():
    bars = _selloff_rebound_bars()
    signals = [0] * len(bars)
    signals[8] = 1
    config = BacktestConfig(spread_bps=0, slippage_bps=0, fx_conversion_bps=0, train_fraction=0.8)
    backtest = run_vector_backtest(bars, signals, config)

    analysis = analyze_strategy_patterns(bars, signals, config, backtest)

    assert "regime_gated_oos_negative" in analysis["warnings"]
    assert analysis["regime_verdict"] in {"headline_only", "thin_regime_sample"}


def test_trade_summary_assigns_flip_pnl_to_new_position():
    start = datetime(2026, 1, 1, 16)
    closes = [100, 110, 105, 95]
    bars = [
        OHLCBar("TEST", start + timedelta(days=index), close, close, close, close)
        for index, close in enumerate(closes)
    ]
    signals = [1, -1, 0, 0]
    config = BacktestConfig(spread_bps=0, slippage_bps=0, fx_conversion_bps=0, overnight_admin_fee_annual=0)
    backtest = run_vector_backtest(bars, signals, config)

    analysis = analyze_strategy_patterns(bars, signals, config, backtest)
    top_trades = analysis["trade_summary"]["top_trades"]

    assert analysis["trade_summary"]["trade_segments"] == 2
    assert [trade["side"] for trade in top_trades] == ["long", "short"]
    assert [trade["net_profit"] for trade in top_trades] == [10, 5]


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
