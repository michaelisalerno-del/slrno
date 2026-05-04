from __future__ import annotations

from dataclasses import asdict
from datetime import date, timedelta
from typing import Any

from .backtesting import BacktestConfig, BacktestResult, run_vector_backtest
from .providers.base import OHLCBar

EVENT_STRATEGY_FAMILIES = {"calendar_turnaround_tuesday", "month_end_seasonality"}


def analyze_calendar_strategy_patterns(
    bars: list[OHLCBar],
    signals: list[int],
    config: BacktestConfig,
    backtest: BacktestResult,
    market_context: dict[str, object] | None = None,
    strategy_family: str = "",
) -> dict[str, object]:
    context = market_context if isinstance(market_context, dict) else {}
    if len(bars) != len(signals):
        raise ValueError("bars and signals must have the same length")
    if not context.get("available"):
        return _unavailable_calendar_analysis(str(context.get("reason") or "Calendar context unavailable"))

    event_dates = _event_dates(context)
    coverage = _coverage_payload(context)
    coverage_warnings = _coverage_warnings(context)
    if not event_dates:
        return {
            "schema": "calendar_context_analysis_v1",
            "available": True,
            "source": context.get("source") or "market_context",
            **coverage,
            "calendar_risk": context.get("calendar_risk") or "clear",
            "major_event_count": int(float(context.get("major_event_count") or 0)),
            "high_impact_count": int(float(context.get("high_impact_count") or 0)),
            "event_dates": [],
            "event_window_dates": [],
            "event_day_summary": _empty_bucket(),
            "event_window_summary": _empty_bucket(),
            "normal_day_summary": _empty_bucket(),
            "policy_backtests": [_policy_payload("baseline", backtest)],
            "recommended_policy": "none",
            "warnings": coverage_warnings,
        }

    event_window_dates = _expand_dates(event_dates, days_before=1, days_after=1)
    rows = _pnl_rows(bars, signals, config)
    event_day_summary = _bucket_summary(rows, event_dates)
    event_window_summary = _bucket_summary(rows, event_window_dates)
    normal_day_summary = _bucket_summary(rows, event_window_dates, invert=True)
    avoid_event_backtest = run_vector_backtest(bars, gate_signals_away_from_dates(bars, signals, event_dates), config)
    avoid_window_backtest = run_vector_backtest(bars, gate_signals_away_from_dates(bars, signals, event_window_dates), config)
    warnings = _calendar_warnings(
        backtest,
        avoid_event_backtest,
        avoid_window_backtest,
        event_day_summary,
        event_window_summary,
        normal_day_summary,
        event_dates,
        strategy_family,
    )
    warnings = sorted(set(warnings + coverage_warnings))
    return {
        "schema": "calendar_context_analysis_v1",
        "available": True,
        "source": context.get("source") or "market_context",
        **coverage,
        "calendar_risk": context.get("calendar_risk") or "unknown",
        "major_event_count": int(float(context.get("major_event_count") or 0)),
        "high_impact_count": int(float(context.get("high_impact_count") or 0)),
        "event_dates": sorted(day.isoformat() for day in event_dates),
        "event_window_dates": sorted(day.isoformat() for day in event_window_dates),
        "event_day_summary": event_day_summary,
        "event_window_summary": event_window_summary,
        "normal_day_summary": normal_day_summary,
        "policy_backtests": [
            _policy_payload("baseline", backtest),
            _policy_payload("avoid_major_event_days", avoid_event_backtest),
            _policy_payload("avoid_event_window", avoid_window_backtest),
            _reduce_size_policy_payload(backtest, event_window_summary),
        ],
        "recommended_policy": _recommended_policy(backtest, avoid_event_backtest, avoid_window_backtest),
        "warnings": warnings,
    }


def gate_signals_away_from_dates(bars: list[OHLCBar], signals: list[int], blocked_dates: set[date]) -> list[int]:
    if len(bars) != len(signals):
        raise ValueError("bars and signals must have the same length")
    if not blocked_dates:
        return list(signals)
    return [
        signal if index + 1 < len(bars) and bars[index + 1].timestamp.date() not in blocked_dates else 0
        for index, signal in enumerate(signals)
    ]


def _calendar_warnings(
    backtest: BacktestResult,
    avoid_event_backtest: BacktestResult,
    avoid_window_backtest: BacktestResult,
    event_day: dict[str, object],
    event_window: dict[str, object],
    normal_day: dict[str, object],
    event_dates: set[date],
    strategy_family: str,
) -> list[str]:
    warnings: list[str] = []
    net = float(backtest.net_profit or 0.0)
    event_share = float(event_day.get("positive_profit_share") or 0.0)
    window_share = float(event_window.get("positive_profit_share") or 0.0)
    event_trades = int(float(event_day.get("trade_count") or 0))
    window_trades = int(float(event_window.get("trade_count") or 0))
    normal_net = float(normal_day.get("net_profit") or 0.0)
    if net > 0 and event_trades > 0 and event_share >= 0.55:
        warnings.append("calendar_dependent_edge")
    if net > 0 and window_trades > 0 and window_share >= 0.65:
        warnings.append("major_event_window_dependency")
    if net > 0 and normal_net <= 0 and window_trades > 0:
        warnings.append("calendar_filtered_oos_negative")
    if backtest.test_profit > 0 and avoid_window_backtest.test_profit <= 0:
        warnings.append("calendar_filtered_oos_negative")
    if avoid_event_backtest.net_profit > net * 1.1 or avoid_window_backtest.net_profit > net * 1.1:
        warnings.append("calendar_blackout_improves_result")
    if strategy_family not in EVENT_STRATEGY_FAMILIES and (event_share >= 0.65 or window_share >= 0.75):
        warnings.append("event_strategy_requires_label")
    if len(event_dates) < 4 and event_trades > 0 and event_share >= 0.4:
        warnings.append("calendar_sample_too_thin")
    return sorted(set(warnings))


def _recommended_policy(baseline: BacktestResult, avoid_event: BacktestResult, avoid_window: BacktestResult) -> str:
    net = float(baseline.net_profit or 0.0)
    if avoid_window.net_profit > net and avoid_window.test_profit >= baseline.test_profit * 0.9:
        return "avoid_event_window"
    if avoid_event.net_profit > net and avoid_event.test_profit >= baseline.test_profit * 0.9:
        return "avoid_major_event_days"
    if avoid_window.net_profit > 0 and avoid_window.max_drawdown < baseline.max_drawdown * 0.8:
        return "reduce_or_avoid_event_window"
    return "none"


def _coverage_payload(context: dict[str, object]) -> dict[str, object]:
    return {
        "coverage_status": context.get("coverage_status") or "full",
        "requested_start": context.get("requested_start") or context.get("start"),
        "requested_end": context.get("requested_end") or context.get("end"),
        "coverage_start": context.get("coverage_start") or context.get("start"),
        "coverage_end": context.get("coverage_end") or context.get("end"),
    }


def _coverage_warnings(context: dict[str, object]) -> list[str]:
    completeness = context.get("data_completeness") if isinstance(context.get("data_completeness"), dict) else {}
    if context.get("coverage_status") == "partial_recent" or completeness.get("events_exact_for_full_range") is False:
        return ["calendar_history_partial"]
    return []


def _event_dates(context: dict[str, object]) -> set[date]:
    output: set[date] = set()
    for value in context.get("blackout_dates", []) if isinstance(context.get("blackout_dates"), list) else []:
        parsed = _parse_date(value)
        if parsed is not None:
            output.add(parsed)
    for event in context.get("events", []) if isinstance(context.get("events"), list) else []:
        if not isinstance(event, dict):
            continue
        if str(event.get("importance") or "") not in {"major", "high"}:
            continue
        parsed = _parse_date(event.get("day"))
        if parsed is not None:
            output.add(parsed)
    return output


def _expand_dates(days: set[date], days_before: int, days_after: int) -> set[date]:
    output: set[date] = set()
    for day in days:
        for offset in range(-days_before, days_after + 1):
            output.add(day + timedelta(days=offset))
    return output


def _pnl_rows(bars: list[OHLCBar], signals: list[int], config: BacktestConfig) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    previous_exposure = 0.0
    equity = config.starting_cash
    for index in range(1, len(bars)):
        previous_bar = bars[index - 1]
        current_bar = bars[index]
        direction = _normalize_signal(signals[index - 1])
        position = _target_exposure(direction, previous_exposure, equity, config)
        exposure_delta = abs(position - previous_exposure)
        point_size = _contract_point_size(config)
        gross = position * ((current_bar.close - previous_bar.close) / point_size)
        notional = (previous_bar.close / point_size) * abs(position)
        stress = max(0.0, config.cost_stress_multiplier)
        spread = (previous_bar.close / point_size) * (config.spread_bps / 10_000) * stress * exposure_delta / 2
        slippage = (previous_bar.close / point_size) * (config.slippage_bps / 10_000) * stress * exposure_delta
        commission = (previous_bar.close / point_size) * (config.commission_bps / 10_000) * exposure_delta / 2
        guaranteed = (config.guaranteed_stop_premium_points / point_size) * exposure_delta / 2 if config.use_guaranteed_stop else 0.0
        funding = 0.0
        if position != 0 and current_bar.timestamp.date() > previous_bar.timestamp.date():
            annual_rate = max(0.0, config.overnight_admin_fee_annual + config.overnight_interest_annual)
            funding = notional * (annual_rate / 365) * stress
        fx = abs(gross) * (config.fx_conversion_bps / 10_000) if config.account_currency != config.instrument_currency else 0.0
        cost = spread + slippage + commission + guaranteed + funding + fx
        pnl = gross - cost
        equity += pnl
        rows.append(
            {
                "date": current_bar.timestamp.date(),
                "pnl": pnl,
                "gross": gross,
                "cost": cost,
                "trade": exposure_delta > 0,
                "active": position != 0,
            }
        )
        previous_exposure = position
    return rows


def _bucket_summary(rows: list[dict[str, object]], dates: set[date], invert: bool = False) -> dict[str, object]:
    selected = [row for row in rows if (row["date"] not in dates if invert else row["date"] in dates)]
    net = sum(float(row.get("pnl") or 0.0) for row in selected)
    gross = sum(float(row.get("gross") or 0.0) for row in selected)
    cost = sum(float(row.get("cost") or 0.0) for row in selected)
    positive_total = sum(max(0.0, float(row.get("pnl") or 0.0)) for row in rows)
    selected_positive = sum(max(0.0, float(row.get("pnl") or 0.0)) for row in selected)
    trade_count = sum(1 for row in selected if row.get("trade"))
    active_days = {row["date"] for row in selected if row.get("active") or row.get("trade")}
    return {
        "net_profit": round(net, 4),
        "gross_profit": round(gross, 4),
        "cost": round(cost, 4),
        "trade_count": trade_count,
        "active_days": len(active_days),
        "positive_profit_share": round(selected_positive / positive_total, 6) if positive_total > 0 else 0.0,
    }


def _policy_payload(name: str, backtest: BacktestResult) -> dict[str, object]:
    payload = asdict(backtest)
    return {
        "policy": name,
        "net_profit": round(float(payload.get("net_profit") or 0.0), 4),
        "test_profit": round(float(payload.get("test_profit") or 0.0), 4),
        "daily_pnl_sharpe": round(float(payload.get("daily_pnl_sharpe") or 0.0), 4),
        "sharpe_days": int(float(payload.get("sharpe_observations") or 0.0)),
        "trade_count": int(float(payload.get("trade_count") or 0.0)),
        "max_drawdown": round(float(payload.get("max_drawdown") or 0.0), 4),
        "total_cost": round(float(payload.get("total_cost") or 0.0), 4),
    }


def _reduce_size_policy_payload(backtest: BacktestResult, event_window: dict[str, object]) -> dict[str, object]:
    window_net = float(event_window.get("net_profit") or 0.0)
    return {
        "policy": "reduce_size_event_window_50pct_estimate",
        "net_profit": round(float(backtest.net_profit) - window_net * 0.5, 4),
        "test_profit": None,
        "daily_pnl_sharpe": None,
        "sharpe_days": int(backtest.sharpe_observations),
        "trade_count": int(backtest.trade_count),
        "max_drawdown": None,
        "total_cost": None,
        "note": "Estimate only: dynamic per-window position sizing is not used by the vector backtest yet.",
    }


def _empty_bucket() -> dict[str, object]:
    return {"net_profit": 0.0, "gross_profit": 0.0, "cost": 0.0, "trade_count": 0, "active_days": 0, "positive_profit_share": 0.0}


def _unavailable_calendar_analysis(reason: str) -> dict[str, object]:
    return {
        "schema": "calendar_context_analysis_v1",
        "available": False,
        "reason": reason,
        "event_dates": [],
        "event_window_dates": [],
        "policy_backtests": [],
        "recommended_policy": "unavailable",
        "warnings": [],
    }


def _normalize_signal(value: int) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _target_exposure(direction: int, previous_exposure: float, equity: float, config: BacktestConfig) -> float:
    if direction == 0:
        return 0.0
    previous_direction = 1 if previous_exposure > 0 else -1 if previous_exposure < 0 else 0
    if previous_direction == direction:
        return previous_exposure
    stake = config.position_size
    if config.compound_position_size:
        stake *= max(0.0, equity) / config.starting_cash
    return direction * max(0.0, stake)


def _contract_point_size(config: BacktestConfig) -> float:
    try:
        value = float(config.contract_point_size)
    except (TypeError, ValueError):
        return 1.0
    return value if value > 0 else 1.0


def _parse_date(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None
