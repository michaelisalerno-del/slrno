from __future__ import annotations

from datetime import datetime, timedelta
from math import isclose, sqrt

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


def test_intraday_sharpe_reports_annualization_context():
    start = datetime(2026, 1, 1, 9)
    closes = [100, 101, 99, 100, 102, 101]
    bars = []
    for index, close in enumerate(closes):
        day_offset = 0 if index < 3 else 1
        bar_time = start + timedelta(days=day_offset, minutes=5 * (index % 3))
        bars.append(OHLCBar("TEST", bar_time, close, close, close, close))
    signals = [1] * len(bars)

    result = run_vector_backtest(
        bars,
        signals,
        BacktestConfig(starting_cash=10_000, position_size=1, spread_bps=0, slippage_bps=0),
    )

    assert result.sharpe_observations == 2
    assert result.bar_sharpe_observations == 5
    assert result.sample_trading_days == 2
    assert result.sample_calendar_days == 2
    assert result.daily_periods_per_year == 252
    assert result.bar_periods_per_year == 756
    assert isclose(result.sharpe, result.bar_sample_sharpe * sqrt(result.bar_periods_per_year))
    assert "2 daily PnL observations" in result.sharpe_annualization_note


def test_compound_position_sizing_scales_new_entries_with_equity():
    start = datetime(2026, 1, 1, 9)
    closes = [100, 110, 110, 120]
    bars = [
        OHLCBar("TEST", start + timedelta(hours=index), close, close, close, close)
        for index, close in enumerate(closes)
    ]
    signals = [1, 0, 1, 0]

    fixed = run_vector_backtest(
        bars,
        signals,
        BacktestConfig(starting_cash=100, position_size=1, spread_bps=0, slippage_bps=0),
    )
    compounded = run_vector_backtest(
        bars,
        signals,
        BacktestConfig(starting_cash=100, position_size=1, compound_position_size=True, spread_bps=0, slippage_bps=0),
    )

    assert fixed.net_profit == 20
    assert fixed.compounded_position_sizing is False
    assert fixed.compounded_projection_final_equity == 121
    assert fixed.compounded_projection_return_pct == 21
    assert compounded.net_profit == 21
    assert compounded.final_equity == 121
    assert compounded.compounded_projection_final_equity == 121
    assert compounded.compounded_position_sizing is True
    assert compounded.max_effective_position_size == 1.1


def test_vector_backtest_converts_share_price_units_to_spread_bet_points():
    start = datetime(2026, 1, 1, 9)
    bars = [
        OHLCBar("AAPL", start, 100, 100, 100, 100),
        OHLCBar("AAPL", start + timedelta(days=1), 101, 101, 101, 101),
    ]

    result = run_vector_backtest(
        bars,
        [1, 0],
        BacktestConfig(
            starting_cash=1_000,
            position_size=1,
            spread_bps=0,
            slippage_bps=0,
            overnight_admin_fee_annual=0,
            contract_point_size=0.01,
        ),
    )

    assert result.gross_profit == 100
    assert result.net_profit == 100
    assert result.contract_point_size == 0.01


def test_vector_backtest_validates_input_lengths():
    bar = OHLCBar("TEST", datetime(2026, 1, 1), 1, 1, 1, 1)

    try:
        run_vector_backtest([bar, bar], [1])
    except ValueError as exc:
        assert "same length" in str(exc)
    else:
        raise AssertionError("expected ValueError")
