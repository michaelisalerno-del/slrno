from __future__ import annotations

from datetime import datetime, timedelta

from app.backtesting import BacktestConfig, run_vector_backtest
from app.providers.base import OHLCBar


def test_vector_backtest_reports_profit_and_risk_metrics():
    start = datetime(2026, 1, 1, 9)
    closes = [100, 101, 102, 101, 103]
    bars = [
        OHLCBar("TEST", start + timedelta(hours=index), close, close, close, close)
        for index, close in enumerate(closes)
    ]
    signals = [1, 1, -1, 1, 0]

    result = run_vector_backtest(
        bars,
        signals,
        BacktestConfig(starting_cash=10_000, position_size=1, spread_bps=1.2, slippage_bps=0.7),
    )

    assert result.trade_count == 3
    assert result.gross_profit == 5
    assert result.net_profit < result.gross_profit
    assert result.estimated_spread_bps == 1.2
    assert result.estimated_slippage_bps == 0.7
    assert result.win_rate > 0
    assert result.exposure == 1
    assert result.daily_pnl_curve
    assert result.sharpe_observations >= 1
    assert result.turnover_efficiency != 0
    assert 0 <= result.probabilistic_sharpe_ratio <= 1


def test_vector_backtest_validates_input_lengths():
    bar = OHLCBar("TEST", datetime(2026, 1, 1), 1, 1, 1, 1)

    try:
        run_vector_backtest([bar, bar], [1])
    except ValueError as exc:
        assert "same length" in str(exc)
    else:
        raise AssertionError("expected ValueError")
