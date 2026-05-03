from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.backtesting import BacktestConfig, run_vector_backtest
from app.calendar_diagnostics import analyze_calendar_strategy_patterns, gate_signals_away_from_dates
from app.providers.base import OHLCBar


def test_calendar_diagnostics_flags_event_dependent_edge():
    bars = _bars([100, 120, 119, 119, 119, 119])
    signals = [1, 1, 1, 0, 0, 0]
    config = BacktestConfig(spread_bps=0, slippage_bps=0, overnight_admin_fee_annual=0)
    backtest = run_vector_backtest(bars, signals, config)
    context = {
        "available": True,
        "source": "fmp_economic_calendar",
        "calendar_risk": "elevated",
        "blackout_dates": ["2026-01-02"],
        "events": [{"day": "2026-01-02", "event": "FOMC Interest Rate Decision", "importance": "major"}],
        "major_event_count": 1,
        "high_impact_count": 1,
    }

    analysis = analyze_calendar_strategy_patterns(bars, signals, config, backtest, context, strategy_family="breakout")

    assert analysis["available"] is True
    assert analysis["event_day_summary"]["net_profit"] == 20
    assert "calendar_dependent_edge" in analysis["warnings"]
    assert "event_strategy_requires_label" in analysis["warnings"]
    avoid_event = next(item for item in analysis["policy_backtests"] if item["policy"] == "avoid_major_event_days")
    assert avoid_event["net_profit"] < backtest.net_profit


def test_gate_signals_away_from_dates_forces_flat_during_calendar_risk():
    bars = _bars([100, 101, 102])
    signals = [1, 1, 1]

    gated = gate_signals_away_from_dates(bars, signals, {bars[1].timestamp.date()})

    assert gated == [0, 1, 0]


def test_calendar_diagnostics_marks_partial_calendar_history():
    bars = _bars([100, 101, 102])
    signals = [1, 1, 1]
    config = BacktestConfig(spread_bps=0, slippage_bps=0, overnight_admin_fee_annual=0)
    backtest = run_vector_backtest(bars, signals, config)
    context = {
        "available": True,
        "source": "fmp_economic_calendar",
        "calendar_risk": "clear",
        "coverage_status": "partial_recent",
        "requested_start": "2020-01-01",
        "requested_end": "2026-04-01",
        "coverage_start": "2026-01-01",
        "coverage_end": "2026-04-01",
        "blackout_dates": [],
        "events": [],
        "data_completeness": {"events_exact_for_full_range": False},
    }

    analysis = analyze_calendar_strategy_patterns(bars, signals, config, backtest, context, strategy_family="breakout")

    assert analysis["coverage_status"] == "partial_recent"
    assert analysis["coverage_start"] == "2026-01-01"
    assert "calendar_history_partial" in analysis["warnings"]


def _bars(closes: list[float]) -> list[OHLCBar]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    return [
        OHLCBar(
            symbol="TEST",
            timestamp=start + timedelta(days=index),
            open=close,
            high=close,
            low=close,
            close=close,
            volume=1_000,
        )
        for index, close in enumerate(closes)
    ]
