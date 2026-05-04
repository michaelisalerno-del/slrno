from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from math import isfinite, sqrt

from .backtesting import BacktestConfig, BacktestResult, run_vector_backtest
from .providers.base import OHLCBar

VOLATILE_REGIMES = {"shock_event", "rebound_after_selloff", "high_volatility"}
NORMAL_REGIMES = {"normal", "range_chop", "low_volatility", "trend_up", "trend_down"}
REGIME_SCAN_MIN_DAYS = {"shock_event": 5, "rebound_after_selloff": 5}
DEFAULT_REGIME_SCAN_MIN_DAYS = 20


@dataclass(frozen=True)
class DailyRegime:
    date: date
    open: float
    high: float
    low: float
    close: float
    bar_count: int
    return_pct: float
    range_pct: float
    five_day_return_pct: float
    ma20_gap_pct: float
    regime: str


def analyze_market_regimes(bars: list[OHLCBar]) -> dict[str, object]:
    days = _daily_regimes(bars)
    return _market_regime_payload(bars, days)


def market_regime_context(bars: list[OHLCBar]) -> tuple[dict[str, object], dict[date, str]]:
    days = _daily_regimes(bars)
    return _market_regime_payload(bars, days), {day.date: day.regime for day in days}


def _market_regime_payload(bars: list[OHLCBar], days: list[DailyRegime]) -> dict[str, object]:
    regime_counts = Counter(day.regime for day in days)
    return {
        "schema": "market_regime_v1",
        "bar_count": len(bars),
        "trading_days": len(days),
        "start": bars[0].timestamp.isoformat() if bars else None,
        "end": bars[-1].timestamp.isoformat() if bars else None,
        "current_regime": days[-1].regime if days else "unknown",
        "regime_counts": dict(regime_counts),
        "segments": _regime_segments(days),
    }


def analyze_strategy_patterns(
    bars: list[OHLCBar],
    signals: list[int],
    config: BacktestConfig,
    backtest: BacktestResult,
    target_regime: str | None = None,
    market_regime: dict[str, object] | None = None,
    regime_by_date: dict[date, str] | None = None,
) -> dict[str, object]:
    if len(bars) != len(signals):
        raise ValueError("bars and signals must have the same length")
    if market_regime is None or regime_by_date is None:
        market_regime, regime_by_date = market_regime_context(bars)
    pnl_rows = _pnl_rows(bars, signals, config, regime_by_date)
    trades = _trade_ledger(bars, pnl_rows)
    monthly_summary = _group_summary(pnl_rows, "month", config.train_fraction)
    regime_summary = _group_summary(pnl_rows, "regime", config.train_fraction)
    session_summary = _group_summary(pnl_rows, "session", config.train_fraction)
    allowed_regimes = _allowed_regimes(regime_summary, target_regime)
    blocked_regimes = [
        str(row["key"])
        for row in regime_summary
        if float(row["net_profit"] or 0.0) <= 0.0 and int(row["active_bars"] or 0) > 0
    ]
    gated_backtest = _regime_gated_backtest(bars, signals, config, allowed_regimes, regime_by_date)
    warnings = _pattern_warnings(backtest, monthly_summary, regime_summary, trades, gated_backtest, target_regime)
    verdict = _regime_verdict(regime_summary, gated_backtest, warnings, target_regime)
    return {
        "schema": "bar_pattern_analysis_v1",
        "market_regime": {
            "current_regime": market_regime["current_regime"],
            "regime_counts": market_regime["regime_counts"],
        },
        "target_regime": target_regime,
        "allowed_regimes": allowed_regimes,
        "blocked_regimes": blocked_regimes,
        "dominant_profit_month": _dominant_positive_bucket(monthly_summary),
        "dominant_profit_regime": _dominant_positive_bucket(regime_summary),
        "worst_regime": _worst_bucket(regime_summary),
        "regime_verdict": verdict,
        "regime_gated_backtest": gated_backtest,
        "regime_trade_evidence": _regime_trade_evidence(market_regime, regime_summary, gated_backtest, target_regime, allowed_regimes),
        "regime_summary": regime_summary,
        "monthly_summary": monthly_summary,
        "session_summary": session_summary,
        "trade_summary": _trade_summary(trades),
        "warnings": warnings,
    }


def eligible_specialist_regimes(bars: list[OHLCBar]) -> list[dict[str, object]]:
    """Return regimes with enough historical days for opt-in specialist scans."""
    days = _daily_regimes(bars)
    counts = Counter(day.regime for day in days)
    output: list[dict[str, object]] = []
    for regime, trading_days in sorted(counts.items()):
        minimum_days = regime_minimum_days(regime)
        if trading_days >= minimum_days:
            output.append({"regime": regime, "trading_days": trading_days, "minimum_days": minimum_days})
    return output


def gate_signals_to_regimes(
    bars: list[OHLCBar],
    signals: list[int],
    allowed_regimes: set[str] | list[str] | tuple[str, ...],
    regime_by_date: dict[date, str] | None = None,
) -> list[int]:
    if len(bars) != len(signals):
        raise ValueError("bars and signals must have the same length")
    allowed = {str(regime) for regime in allowed_regimes if regime}
    if regime_by_date is None:
        regime_by_date = _regime_by_date(bars)
    return [
        signal if index + 1 < len(bars) and regime_by_date.get(bars[index + 1].timestamp.date(), "unknown") in allowed else 0
        for index, signal in enumerate(signals)
    ]


def regime_minimum_days(regime: str) -> int:
    return REGIME_SCAN_MIN_DAYS.get(regime, DEFAULT_REGIME_SCAN_MIN_DAYS)


def _daily_regimes(bars: list[OHLCBar]) -> list[DailyRegime]:
    grouped: dict[date, list[OHLCBar]] = defaultdict(list)
    for bar in bars:
        grouped[bar.timestamp.date()].append(bar)
    days: list[dict[str, object]] = []
    previous_close = 0.0
    for current_date in sorted(grouped):
        items = sorted(grouped[current_date], key=lambda bar: bar.timestamp)
        open_price = items[0].open
        high = max(bar.high for bar in items)
        low = min(bar.low for bar in items)
        close = items[-1].close
        return_pct = ((close / previous_close) - 1.0) * 100 if previous_close > 0 else 0.0
        range_pct = ((high - low) / max(close, 1e-12)) * 100
        days.append(
            {
                "date": current_date,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "bar_count": len(items),
                "return_pct": return_pct,
                "range_pct": range_pct,
            }
        )
        previous_close = close

    output: list[DailyRegime] = []
    for index, item in enumerate(days):
        close = float(item["close"])
        five_start = max(0, index - 5)
        five_base = float(days[five_start]["close"]) if index > five_start else close
        five_day_return_pct = ((close / five_base) - 1.0) * 100 if five_base > 0 else 0.0
        ma_start = max(0, index - 19)
        ma_values = [float(day["close"]) for day in days[ma_start : index + 1]]
        ma20 = sum(ma_values) / len(ma_values) if ma_values else close
        ma20_gap_pct = ((close / ma20) - 1.0) * 100 if ma20 > 0 else 0.0
        previous_3d_return = _window_return(days, index - 3, index - 1)
        regime = _classify_regime(
            return_pct=float(item["return_pct"]),
            range_pct=float(item["range_pct"]),
            five_day_return_pct=five_day_return_pct,
            ma20_gap_pct=ma20_gap_pct,
            previous_3d_return=previous_3d_return,
        )
        output.append(
            DailyRegime(
                date=item["date"],  # type: ignore[arg-type]
                open=float(item["open"]),
                high=float(item["high"]),
                low=float(item["low"]),
                close=close,
                bar_count=int(item["bar_count"]),
                return_pct=round(float(item["return_pct"]), 4),
                range_pct=round(float(item["range_pct"]), 4),
                five_day_return_pct=round(five_day_return_pct, 4),
                ma20_gap_pct=round(ma20_gap_pct, 4),
                regime=regime,
            )
        )
    return output


def _classify_regime(
    *,
    return_pct: float,
    range_pct: float,
    five_day_return_pct: float,
    ma20_gap_pct: float,
    previous_3d_return: float,
) -> str:
    if return_pct >= 2.5 and previous_3d_return <= -3.0:
        return "rebound_after_selloff"
    if abs(return_pct) >= 3.5 or range_pct >= 5.5:
        return "shock_event"
    if abs(return_pct) >= 2.0 or range_pct >= 2.5:
        return "high_volatility"
    if five_day_return_pct >= 1.5 and ma20_gap_pct >= 0.8:
        return "trend_up"
    if five_day_return_pct <= -1.5 and ma20_gap_pct <= -0.8:
        return "trend_down"
    if range_pct <= 0.7 and abs(five_day_return_pct) <= 1.0:
        return "low_volatility"
    if abs(five_day_return_pct) <= 1.25 and range_pct <= 1.6:
        return "range_chop"
    return "normal"


def _window_return(days: list[dict[str, object]], start: int, end: int) -> float:
    if start < 0 or end <= start or end >= len(days):
        return 0.0
    first = float(days[start]["close"])
    last = float(days[end]["close"])
    return ((last / first) - 1.0) * 100 if first > 0 else 0.0


def _regime_by_date(bars: list[OHLCBar]) -> dict[date, str]:
    return {day.date: day.regime for day in _daily_regimes(bars)}


def _regime_segments(days: list[DailyRegime]) -> list[dict[str, object]]:
    if not days:
        return []
    segments: list[dict[str, object]] = []
    start = days[0]
    previous = days[0]
    trading_days = 1
    for day in days[1:]:
        if day.regime != previous.regime:
            segments.append(_segment_row(start, previous, trading_days))
            start = day
            trading_days = 1
        else:
            trading_days += 1
        previous = day
    segments.append(_segment_row(start, previous, trading_days))
    return segments[:250]


def _segment_row(start: DailyRegime, end: DailyRegime, trading_days: int) -> dict[str, object]:
    return {
        "start": start.date.isoformat(),
        "end": end.date.isoformat(),
        "regime": start.regime,
        "trading_days": trading_days,
        "calendar_days": (end.date - start.date).days + 1,
    }


def _pnl_rows(
    bars: list[OHLCBar],
    signals: list[int],
    config: BacktestConfig,
    regime_by_date: dict[date, str],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    previous_exposure = 0.0
    equity = config.starting_cash
    for index in range(1, len(bars)):
        previous_bar = bars[index - 1]
        current_bar = bars[index]
        direction = _normalize_signal(signals[index - 1])
        position = _target_exposure(direction, previous_exposure, equity, config)
        position_delta = abs(position - previous_exposure)
        point_size = _contract_point_size(config)
        price_change = current_bar.close - previous_bar.close
        gross = position * (price_change / point_size)
        notional = (previous_bar.close / point_size) * abs(position)
        stress = max(0.0, config.cost_stress_multiplier)
        spread = (previous_bar.close / point_size) * (config.spread_bps / 10_000) * stress * position_delta / 2
        slippage = (previous_bar.close / point_size) * (config.slippage_bps / 10_000) * stress * position_delta
        commission = (previous_bar.close / point_size) * (config.commission_bps / 10_000) * position_delta / 2
        guaranteed = (config.guaranteed_stop_premium_points / point_size) * position_delta / 2 if config.use_guaranteed_stop else 0.0
        funding = 0.0
        if position != 0 and _crosses_funding_cutoff(previous_bar.timestamp, current_bar.timestamp, config.funding_cutoff_hour):
            funding = notional * (max(0.0, config.overnight_admin_fee_annual + config.overnight_interest_annual) / 365) * stress
        fx = 0.0
        if config.account_currency and config.instrument_currency and config.account_currency != config.instrument_currency:
            fx = abs(gross) * (config.fx_conversion_bps / 10_000)
        cost = spread + slippage + commission + guaranteed + funding + fx
        pnl = gross - cost
        equity += pnl
        timestamp = current_bar.timestamp
        rows.append(
            {
                "timestamp": timestamp.isoformat(),
                "date": timestamp.date().isoformat(),
                "month": timestamp.strftime("%Y-%m"),
                "session": _session_label(timestamp),
                "regime": regime_by_date.get(timestamp.date(), "unknown"),
                "position": position,
                "previous_position": previous_exposure,
                "position_delta": position_delta,
                "gross_profit": gross,
                "cost": cost,
                "net_profit": pnl,
            }
        )
        previous_exposure = position
    return rows


def _session_label(timestamp: datetime) -> str:
    hour = timestamp.hour
    if 13 <= hour < 16:
        return "us_open"
    if 16 <= hour < 21:
        return "us_afternoon"
    if 21 <= hour or hour < 7:
        return "overnight"
    return "other"


def _group_summary(rows: list[dict[str, object]], key: str, train_fraction: float = 0.7) -> list[dict[str, object]]:
    grouped: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "net_profit": 0.0,
            "gross_profit": 0.0,
            "cost": 0.0,
            "active_bars": 0.0,
            "transitions": 0.0,
            "test_transitions": 0.0,
            "train_profit": 0.0,
            "test_profit": 0.0,
            "daily_pnl": defaultdict(float),
            "trading_days": set(),
            "active_days": set(),
            "pnl": [],
        }
    )
    split = max(1, int(len(rows) * train_fraction)) if rows else 0
    for index, row in enumerate(rows):
        bucket = str(row.get(key) or "unknown")
        net_profit = _number(row.get("net_profit"))
        gross_profit = _number(row.get("gross_profit"))
        cost = _number(row.get("cost"))
        grouped[bucket]["net_profit"] = _number(grouped[bucket]["net_profit"]) + net_profit
        grouped[bucket]["gross_profit"] = _number(grouped[bucket]["gross_profit"]) + gross_profit
        grouped[bucket]["cost"] = _number(grouped[bucket]["cost"]) + cost
        split_key = "train_profit" if index < split else "test_profit"
        grouped[bucket][split_key] = _number(grouped[bucket][split_key]) + net_profit
        grouped[bucket]["pnl"].append(net_profit)  # type: ignore[union-attr]
        str_date = str(row.get("date") or "")
        if str_date:
            grouped[bucket]["trading_days"].add(str_date)  # type: ignore[union-attr]
            grouped[bucket]["daily_pnl"][str_date] += net_profit  # type: ignore[index]
        if abs(_number(row.get("position"))) > 0:
            grouped[bucket]["active_bars"] = _number(grouped[bucket]["active_bars"]) + 1
            if str_date:
                grouped[bucket]["active_days"].add(str_date)  # type: ignore[union-attr]
        if _number(row.get("position_delta")) > 0:
            grouped[bucket]["transitions"] = _number(grouped[bucket]["transitions"]) + 1
            if index >= split:
                grouped[bucket]["test_transitions"] = _number(grouped[bucket]["test_transitions"]) + 1
    summaries = [
        {
            "key": bucket,
            "net_profit": round(_number(values["net_profit"]), 4),
            "gross_profit": round(_number(values["gross_profit"]), 4),
            "cost": round(_number(values["cost"]), 4),
            "active_bars": int(_number(values["active_bars"])),
            "active_days": len(values["active_days"]),  # type: ignore[arg-type]
            "trading_days": len(values["trading_days"]),  # type: ignore[arg-type]
            "transitions": int(_number(values["transitions"])),
            "test_transitions": int(_number(values["test_transitions"])),
            "train_profit": round(_number(values["train_profit"]), 4),
            "test_profit": round(_number(values["test_profit"]), 4),
            "daily_pnl_sharpe": _sharpe(list(values["daily_pnl"].values())),  # type: ignore[union-attr]
            "sharpe_days": len(values["daily_pnl"]),  # type: ignore[arg-type]
            "max_drawdown": _drawdown(values["pnl"]),  # type: ignore[arg-type]
            "verdict": _bucket_verdict(bucket, values),
        }
        for bucket, values in sorted(grouped.items(), key=lambda item: item[1]["net_profit"], reverse=True)
    ]
    positive_total = sum(max(0.0, _number(row.get("net_profit"))) for row in summaries)
    for row in summaries:
        row["positive_profit_share"] = round(max(0.0, _number(row.get("net_profit"))) / positive_total, 4) if positive_total > 0 else 0.0
    return summaries


def _trade_ledger(bars: list[OHLCBar], rows: list[dict[str, object]]) -> list[dict[str, object]]:
    trades: list[dict[str, object]] = []
    current: dict[str, object] | None = None
    for row_index, row in enumerate(rows, start=1):
        position = _sign(row["position"])
        previous_position = _sign(row["previous_position"])
        if position != previous_position:
            if current is not None:
                current["exit_at"] = bars[row_index - 1].timestamp.isoformat()
                current["exit_price"] = bars[row_index - 1].close
                trades.append(_rounded_trade(current))
                current = None
            if position != 0:
                current = _new_trade(position, bars[row_index - 1])
        elif current is None and position != 0:
            current = _new_trade(position, bars[row_index - 1])

        if current is not None and position != 0:
            current["exit_at"] = row["timestamp"]
            current["exit_price"] = bars[row_index].close
            current["bars"] = int(current["bars"]) + 1
            current["gross_profit"] = _number(current["gross_profit"]) + _number(row["gross_profit"])
            current["cost"] = _number(current["cost"]) + _number(row["cost"])
            current["net_profit"] = _number(current["net_profit"]) + _number(row["net_profit"])
    if current is not None:
        trades.append(_rounded_trade(current))
    return trades


def _new_trade(side: int, bar: OHLCBar) -> dict[str, object]:
    return {
        "side": "long" if side > 0 else "short",
        "entry_at": bar.timestamp.isoformat(),
        "exit_at": bar.timestamp.isoformat(),
        "entry_price": bar.close,
        "exit_price": bar.close,
        "bars": 0,
        "gross_profit": 0.0,
        "cost": 0.0,
        "net_profit": 0.0,
    }


def _rounded_trade(trade: dict[str, object]) -> dict[str, object]:
    return {
        **trade,
        "entry_price": round(_number(trade.get("entry_price")), 6),
        "exit_price": round(_number(trade.get("exit_price")), 6),
        "gross_profit": round(_number(trade.get("gross_profit")), 4),
        "cost": round(_number(trade.get("cost")), 4),
        "net_profit": round(_number(trade.get("net_profit")), 4),
    }


def _trade_summary(trades: list[dict[str, object]]) -> dict[str, object]:
    sorted_trades = sorted(trades, key=lambda trade: _number(trade.get("net_profit")), reverse=True)
    positive = [trade for trade in trades if _number(trade.get("net_profit")) > 0.0]
    negative = [trade for trade in trades if _number(trade.get("net_profit")) <= 0.0]
    return {
        "trade_segments": len(trades),
        "positive_segments": len(positive),
        "negative_segments": len(negative),
        "top_trades": sorted_trades[:5],
        "worst_trades": list(reversed(sorted_trades[-5:])),
        "top_5_profit_share": _top_trade_share(positive),
    }


def _top_trade_share(positive_trades: list[dict[str, object]]) -> float:
    total = sum(max(0.0, _number(trade.get("net_profit"))) for trade in positive_trades)
    if total <= 0.0:
        return 0.0
    top = sum(max(0.0, _number(trade.get("net_profit"))) for trade in sorted(positive_trades, key=lambda trade: _number(trade.get("net_profit")), reverse=True)[:5])
    return round(top / total, 4)


def _pattern_warnings(
    backtest: BacktestResult,
    monthly_summary: list[dict[str, object]],
    regime_summary: list[dict[str, object]],
    trades: list[dict[str, object]],
    gated_backtest: dict[str, object],
    target_regime: str | None,
) -> list[str]:
    warnings: list[str] = []
    if _number(gated_backtest.get("net_profit")) <= 0.0:
        warnings.append("regime_gated_backtest_negative")
    if _number(gated_backtest.get("test_profit")) <= 0.0:
        warnings.append("regime_gated_oos_negative")
    if backtest.net_profit <= 0.0:
        return list(dict.fromkeys(warnings))
    dominant_month = _dominant_positive_bucket(monthly_summary)
    dominant_regime = _dominant_positive_bucket(regime_summary)
    if float(dominant_month.get("positive_profit_share") or 0.0) >= 0.55:
        warnings.append("profit_concentrated_single_month")
    if str(dominant_regime.get("key") or "") in VOLATILE_REGIMES and float(dominant_regime.get("positive_profit_share") or 0.0) >= 0.45:
        warnings.append("high_volatility_only_edge")
    if float(dominant_regime.get("positive_profit_share") or 0.0) >= 0.65:
        warnings.append("profit_concentrated_single_regime")
    normal_net = sum(_number(row.get("net_profit")) for row in regime_summary if str(row.get("key")) in NORMAL_REGIMES)
    normal_active = sum(int(row.get("active_bars") or 0) for row in regime_summary if str(row.get("key")) in NORMAL_REGIMES)
    if normal_active > 0 and normal_net <= 0.0:
        warnings.append("fails_normal_volatility_regime")
    if str(dominant_regime.get("key") or "") in {"shock_event", "rebound_after_selloff"} and float(dominant_regime.get("positive_profit_share") or 0.0) >= 0.35:
        warnings.append("shock_regime_dependency")
    if _top_trade_share([trade for trade in trades if _number(trade.get("net_profit")) > 0.0]) >= 0.60 and len(trades) >= 5:
        warnings.append("best_trades_dominate")
    target_row = next((row for row in regime_summary if str(row.get("key")) == str(target_regime)), None) if target_regime else None
    if target_row is not None and int(target_row.get("test_transitions") or 0) < 8:
        warnings.append("target_regime_low_oos_trades")
    sample_days = _allowed_regime_sample_days(regime_summary, target_regime)
    sample_regime = str(target_regime or dominant_regime.get("key") or "normal")
    if sample_days < regime_minimum_days(sample_regime):
        warnings.append("insufficient_regime_sample")
    if (
        backtest.daily_pnl_sharpe >= 0.55
        and float(dominant_regime.get("positive_profit_share") or 0.0) >= 0.65
        and not target_regime
    ):
        warnings.append("headline_sharpe_not_regime_robust")
    return list(dict.fromkeys(warnings))


def _dominant_positive_bucket(summary: list[dict[str, object]]) -> dict[str, object]:
    positive_total = sum(max(0.0, _number(row.get("net_profit"))) for row in summary)
    if positive_total <= 0.0:
        return {"key": None, "net_profit": 0.0, "positive_profit_share": 0.0}
    top = max(summary, key=lambda row: max(0.0, _number(row.get("net_profit"))))
    return {
        "key": top.get("key"),
        "net_profit": top.get("net_profit"),
        "positive_profit_share": round(max(0.0, _number(top.get("net_profit"))) / positive_total, 4),
    }


def _worst_bucket(summary: list[dict[str, object]]) -> dict[str, object]:
    if not summary:
        return {"key": None, "net_profit": 0.0}
    worst = min(summary, key=lambda row: _number(row.get("net_profit")))
    return {"key": worst.get("key"), "net_profit": worst.get("net_profit")}


def _allowed_regimes(regime_summary: list[dict[str, object]], target_regime: str | None) -> list[str]:
    if target_regime:
        return [target_regime]
    return [
        str(row["key"])
        for row in regime_summary
        if float(row["net_profit"] or 0.0) > 0.0 and int(row["active_bars"] or 0) > 0
    ]


def _regime_gated_backtest(
    bars: list[OHLCBar],
    signals: list[int],
    config: BacktestConfig,
    allowed_regimes: list[str],
    regime_by_date: dict[date, str],
) -> dict[str, object]:
    allowed = set(allowed_regimes)
    gated_signals = [
        signal if index + 1 < len(bars) and regime_by_date.get(bars[index + 1].timestamp.date(), "unknown") in allowed else 0
        for index, signal in enumerate(signals)
    ]
    result = run_vector_backtest(bars, gated_signals, config)
    return _backtest_summary(result)


def _regime_trade_evidence(
    market_regime: dict[str, object],
    regime_summary: list[dict[str, object]],
    gated_backtest: dict[str, object],
    target_regime: str | None,
    allowed_regimes: list[str],
) -> dict[str, object]:
    target = str(target_regime or "")
    if not target:
        dominant = _dominant_positive_bucket(regime_summary)
        target = str(dominant.get("key") or (allowed_regimes[0] if len(allowed_regimes) == 1 else ""))
    if not target:
        return {"schema": "regime_trade_evidence_v1", "available": False}

    target_row = next((row for row in regime_summary if str(row.get("key")) == target), None)
    trading_days = int(market_regime.get("trading_days") or 0)
    segments = market_regime.get("segments") if isinstance(market_regime.get("segments"), list) else []
    target_segments = [segment for segment in segments if isinstance(segment, dict) and str(segment.get("regime")) == target]
    outside_rows = [row for row in regime_summary if str(row.get("key")) != target]
    outside_active_bars = sum(int(row.get("active_bars") or 0) for row in outside_rows)
    outside_transitions = sum(int(row.get("transitions") or 0) for row in outside_rows)
    outside_net_profit = sum(_number(row.get("net_profit")) for row in outside_rows)
    regime_days = int(target_row.get("trading_days") or 0) if target_row else 0
    in_regime = _regime_evidence_summary(target_row) if target_row else {}
    return {
        "schema": "regime_trade_evidence_v1",
        "available": target_row is not None,
        "target_regime": target,
        "is_targeted": bool(target_regime),
        "history_trading_days": trading_days,
        "history_bar_count": market_regime.get("bar_count"),
        "history_start": market_regime.get("start"),
        "history_end": market_regime.get("end"),
        "regime_trading_days": regime_days,
        "regime_history_share": round(regime_days / trading_days, 4) if trading_days > 0 else 0.0,
        "regime_episodes": len(target_segments),
        "longest_regime_episode_days": max((int(segment.get("trading_days") or 0) for segment in target_segments), default=0),
        "first_regime_start": target_segments[0].get("start") if target_segments else None,
        "last_regime_end": target_segments[-1].get("end") if target_segments else None,
        "flat_days_outside_regime": max(0, trading_days - regime_days),
        "outside_active_bars": outside_active_bars,
        "outside_trade_count": outside_transitions,
        "outside_net_profit": round(outside_net_profit, 4),
        "in_regime": in_regime,
        "full_history_gated": gated_backtest,
    }


def _regime_evidence_summary(row: dict[str, object] | None) -> dict[str, object]:
    if row is None:
        return {}
    return {
        "net_profit": row.get("net_profit"),
        "gross_profit": row.get("gross_profit"),
        "cost": row.get("cost"),
        "trade_count": row.get("transitions"),
        "test_trade_count": row.get("test_transitions"),
        "active_bars": row.get("active_bars"),
        "active_days": row.get("active_days"),
        "trading_days": row.get("trading_days"),
        "train_profit": row.get("train_profit"),
        "test_profit": row.get("test_profit"),
        "daily_pnl_sharpe": row.get("daily_pnl_sharpe"),
        "sharpe_days": row.get("sharpe_days"),
        "max_drawdown": row.get("max_drawdown"),
        "positive_profit_share": row.get("positive_profit_share"),
        "verdict": row.get("verdict"),
    }


def _backtest_summary(result: BacktestResult) -> dict[str, object]:
    return {
        "net_profit": round(result.net_profit, 4),
        "test_profit": round(result.test_profit, 4),
        "daily_pnl_sharpe": round(result.daily_pnl_sharpe, 4),
        "sharpe_observations": result.sharpe_observations,
        "max_drawdown": round(result.max_drawdown, 4),
        "trade_count": result.trade_count,
        "total_cost": round(result.total_cost, 4),
        "net_cost_ratio": round(result.net_cost_ratio, 6),
        "cost_to_gross_ratio": round(result.cost_to_gross_ratio, 6),
    }


def _regime_verdict(
    regime_summary: list[dict[str, object]],
    gated_backtest: dict[str, object],
    warnings: list[str],
    target_regime: str | None,
) -> str:
    warning_set = set(warnings)
    if not regime_summary:
        return "unavailable"
    if "regime_gated_backtest_negative" in warning_set or "regime_gated_oos_negative" in warning_set:
        return "headline_only"
    if "insufficient_regime_sample" in warning_set:
        return "thin_regime_sample"
    if target_regime:
        return "regime_tradeable"
    if warning_set & {"profit_concentrated_single_regime", "shock_regime_dependency", "high_volatility_only_edge", "fails_normal_volatility_regime"}:
        return "regime_specific"
    if _number(gated_backtest.get("net_profit")) > 0.0 and _number(gated_backtest.get("test_profit")) > 0.0:
        return "tradeable_across_regimes"
    return "research_only"


def _allowed_regime_sample_days(regime_summary: list[dict[str, object]], target_regime: str | None) -> int:
    if target_regime:
        return max((int(row.get("sharpe_days") or 0) for row in regime_summary if row.get("key") == target_regime), default=0)
    positive_days = [
        int(row.get("sharpe_days") or 0)
        for row in regime_summary
        if _number(row.get("net_profit")) > 0.0 and int(row.get("active_bars") or 0) > 0
    ]
    return max(positive_days, default=0)


def _bucket_verdict(bucket: str, values: dict[str, object]) -> str:
    net_profit = _number(values.get("net_profit"))
    test_profit = _number(values.get("test_profit"))
    active_bars = int(_number(values.get("active_bars")))
    sharpe_days = len(values["daily_pnl"])  # type: ignore[arg-type]
    if active_bars <= 0:
        return "no_trades"
    if sharpe_days < regime_minimum_days(bucket):
        return "thin_sample"
    if net_profit > 0.0 and test_profit > 0.0:
        return "works"
    if net_profit > 0.0:
        return "in_sample_only"
    return "loses"


def _sharpe(values: list[float], periods_per_year: float = 252.0) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    if variance <= 0:
        return 0.0
    return round((mean / (variance**0.5)) * sqrt(periods_per_year), 4)


def _drawdown(values: object) -> float:
    equity = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in values if isinstance(values, list) else []:
        equity += _number(value)
        peak = max(peak, equity)
        drawdown = min(drawdown, equity - peak)
    return round(abs(drawdown), 4)


def _normalize_signal(value: int) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _sign(value: object) -> int:
    number = _number(value)
    if number > 0:
        return 1
    if number < 0:
        return -1
    return 0


def _target_exposure(direction: int, previous_exposure: float, equity: float, config: BacktestConfig) -> float:
    if direction == 0:
        return 0.0
    previous_direction = 1 if previous_exposure > 0 else -1 if previous_exposure < 0 else 0
    if previous_direction == direction:
        return previous_exposure
    stake = config.position_size
    if config.compound_position_size and config.starting_cash > 0:
        stake *= max(0.0, equity) / config.starting_cash
    return direction * max(0.0, stake)


def _contract_point_size(config: BacktestConfig) -> float:
    try:
        value = float(config.contract_point_size)
    except (TypeError, ValueError):
        return 1.0
    return value if value > 0 else 1.0


def _crosses_funding_cutoff(previous: datetime, current: datetime, cutoff_hour: int) -> bool:
    if current.date() > previous.date():
        return True
    return previous.hour < cutoff_hour <= current.hour


def _number(value: object) -> float:
    try:
        number = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return number if isfinite(number) else 0.0
