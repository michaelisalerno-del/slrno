from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from math import isfinite

from .backtesting import BacktestConfig, BacktestResult
from .providers.base import OHLCBar

VOLATILE_REGIMES = {"shock_event", "rebound_after_selloff", "high_volatility"}
NORMAL_REGIMES = {"normal", "range_chop", "low_volatility", "trend_up", "trend_down"}


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
) -> dict[str, object]:
    if len(bars) != len(signals):
        raise ValueError("bars and signals must have the same length")
    market_regime = analyze_market_regimes(bars)
    regime_by_date = _regime_by_date(bars)
    pnl_rows = _pnl_rows(bars, signals, config, regime_by_date)
    trades = _trade_ledger(bars, pnl_rows)
    monthly_summary = _group_summary(pnl_rows, "month")
    regime_summary = _group_summary(pnl_rows, "regime")
    session_summary = _group_summary(pnl_rows, "session")
    warnings = _pattern_warnings(backtest, monthly_summary, regime_summary, trades)
    allowed_regimes = [
        str(row["key"])
        for row in regime_summary
        if float(row["net_profit"] or 0.0) > 0.0 and int(row["active_bars"] or 0) > 0
    ]
    blocked_regimes = [
        str(row["key"])
        for row in regime_summary
        if float(row["net_profit"] or 0.0) <= 0.0 and int(row["active_bars"] or 0) > 0
    ]
    return {
        "schema": "bar_pattern_analysis_v1",
        "market_regime": {
            "current_regime": market_regime["current_regime"],
            "regime_counts": market_regime["regime_counts"],
        },
        "allowed_regimes": allowed_regimes,
        "blocked_regimes": blocked_regimes,
        "dominant_profit_month": _dominant_positive_bucket(monthly_summary),
        "dominant_profit_regime": _dominant_positive_bucket(regime_summary),
        "regime_summary": regime_summary,
        "monthly_summary": monthly_summary,
        "session_summary": session_summary,
        "trade_summary": _trade_summary(trades),
        "warnings": warnings,
    }


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
    for day in days[1:]:
        if day.regime != previous.regime:
            segments.append(_segment_row(start, previous))
            start = day
        previous = day
    segments.append(_segment_row(start, previous))
    return segments[:250]


def _segment_row(start: DailyRegime, end: DailyRegime) -> dict[str, object]:
    return {
        "start": start.date.isoformat(),
        "end": end.date.isoformat(),
        "regime": start.regime,
        "trading_days": (end.date - start.date).days + 1,
    }


def _pnl_rows(
    bars: list[OHLCBar],
    signals: list[int],
    config: BacktestConfig,
    regime_by_date: dict[date, str],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    previous_position = 0
    for index in range(1, len(bars)):
        previous_bar = bars[index - 1]
        current_bar = bars[index]
        position = _normalize_signal(signals[index - 1])
        position_delta = abs(position - previous_position)
        price_change = current_bar.close - previous_bar.close
        gross = position * price_change * config.position_size
        notional = previous_bar.close * config.position_size
        stress = max(0.0, config.cost_stress_multiplier)
        spread = notional * (config.spread_bps / 10_000) * stress * position_delta / 2
        slippage = notional * (config.slippage_bps / 10_000) * stress * position_delta
        commission = notional * (config.commission_bps / 10_000) * position_delta / 2
        guaranteed = config.guaranteed_stop_premium_points * config.position_size * position_delta / 2 if config.use_guaranteed_stop else 0.0
        funding = 0.0
        if position != 0 and _crosses_funding_cutoff(previous_bar.timestamp, current_bar.timestamp, config.funding_cutoff_hour):
            funding = notional * (max(0.0, config.overnight_admin_fee_annual + config.overnight_interest_annual) / 365) * stress
        fx = 0.0
        if config.account_currency and config.instrument_currency and config.account_currency != config.instrument_currency:
            fx = abs(gross) * (config.fx_conversion_bps / 10_000)
        cost = spread + slippage + commission + guaranteed + funding + fx
        pnl = gross - cost
        timestamp = current_bar.timestamp
        rows.append(
            {
                "timestamp": timestamp.isoformat(),
                "date": timestamp.date().isoformat(),
                "month": timestamp.strftime("%Y-%m"),
                "session": _session_label(timestamp),
                "regime": regime_by_date.get(timestamp.date(), "unknown"),
                "position": position,
                "previous_position": previous_position,
                "position_delta": position_delta,
                "gross_profit": gross,
                "cost": cost,
                "net_profit": pnl,
            }
        )
        previous_position = position
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


def _group_summary(rows: list[dict[str, object]], key: str) -> list[dict[str, object]]:
    grouped: dict[str, dict[str, float]] = defaultdict(lambda: {"net_profit": 0.0, "gross_profit": 0.0, "cost": 0.0, "active_bars": 0.0, "transitions": 0.0})
    for row in rows:
        bucket = str(row.get(key) or "unknown")
        grouped[bucket]["net_profit"] += _number(row.get("net_profit"))
        grouped[bucket]["gross_profit"] += _number(row.get("gross_profit"))
        grouped[bucket]["cost"] += _number(row.get("cost"))
        if int(row.get("position") or 0) != 0:
            grouped[bucket]["active_bars"] += 1
        if int(row.get("position_delta") or 0) > 0:
            grouped[bucket]["transitions"] += 1
    return [
        {
            "key": bucket,
            "net_profit": round(values["net_profit"], 4),
            "gross_profit": round(values["gross_profit"], 4),
            "cost": round(values["cost"], 4),
            "active_bars": int(values["active_bars"]),
            "transitions": int(values["transitions"]),
        }
        for bucket, values in sorted(grouped.items(), key=lambda item: item[1]["net_profit"], reverse=True)
    ]


def _trade_ledger(bars: list[OHLCBar], rows: list[dict[str, object]]) -> list[dict[str, object]]:
    trades: list[dict[str, object]] = []
    current: dict[str, object] | None = None
    for row_index, row in enumerate(rows, start=1):
        position = int(row["position"])
        previous_position = int(row["previous_position"])
        if current is None and position != 0:
            current = _new_trade(position, bars[row_index - 1])
        if current is not None:
            current["exit_at"] = row["timestamp"]
            current["exit_price"] = bars[row_index].close
            current["bars"] = int(current["bars"]) + 1
            current["gross_profit"] = _number(current["gross_profit"]) + _number(row["gross_profit"])
            current["cost"] = _number(current["cost"]) + _number(row["cost"])
            current["net_profit"] = _number(current["net_profit"]) + _number(row["net_profit"])
        if current is not None and previous_position != 0 and (position == 0 or position != previous_position):
            trades.append(_rounded_trade(current))
            current = _new_trade(position, bars[row_index - 1]) if position != 0 else None
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
) -> list[str]:
    if backtest.net_profit <= 0.0:
        return []
    warnings: list[str] = []
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


def _normalize_signal(value: int) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


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
