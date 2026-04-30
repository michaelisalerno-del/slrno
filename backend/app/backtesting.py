from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import erf, sqrt

from .providers.base import OHLCBar


@dataclass(frozen=True)
class BacktestConfig:
    starting_cash: float = 10_000.0
    position_size: float = 1.0
    spread_bps: float = 2.0
    slippage_bps: float = 1.0
    train_fraction: float = 0.7
    commission_bps: float = 0.0
    overnight_admin_fee_annual: float = 0.03
    overnight_interest_annual: float = 0.0
    fx_conversion_bps: float = 0.0
    guaranteed_stop_premium_points: float = 0.0
    use_guaranteed_stop: bool = False
    cost_stress_multiplier: float = 1.0
    instrument_currency: str = "GBP"
    account_currency: str = "GBP"
    funding_cutoff_hour: int = 22
    cost_confidence: str = "ig_public_spread_baseline"


@dataclass(frozen=True)
class BacktestResult:
    net_profit: float
    sharpe: float
    max_drawdown: float
    win_rate: float
    trade_count: int
    exposure: float
    turnover: float
    train_profit: float
    test_profit: float
    train_sharpe: float = 0.0
    test_sharpe: float = 0.0
    gross_profit: float = 0.0
    spread_cost: float = 0.0
    slippage_cost: float = 0.0
    commission_cost: float = 0.0
    funding_cost: float = 0.0
    fx_cost: float = 0.0
    guaranteed_stop_cost: float = 0.0
    total_cost: float = 0.0
    cost_confidence: str = "ig_public_spread_baseline"
    estimated_spread_bps: float = 0.0
    estimated_slippage_bps: float = 0.0
    daily_pnl_sharpe: float = 0.0
    rolling_sharpe_min: float = 0.0
    rolling_sharpe_median: float = 0.0
    probabilistic_sharpe_ratio: float = 0.0
    sharpe_observations: int = 0
    turnover_efficiency: float = 0.0
    expectancy_per_trade: float = 0.0
    average_cost_per_trade: float = 0.0
    net_cost_ratio: float = 0.0
    cost_to_gross_ratio: float = 0.0
    equity_curve: tuple[float, ...] = ()
    drawdown_curve: tuple[float, ...] = ()
    daily_pnl_curve: tuple[float, ...] = ()


def run_vector_backtest(bars: list[OHLCBar], signals: list[int], config: BacktestConfig | None = None) -> BacktestResult:
    config = config or BacktestConfig()
    if len(bars) != len(signals):
        raise ValueError("bars and signals must have the same length")
    if len(bars) < 2:
        raise ValueError("at least two bars are required")
    if not 0 < config.train_fraction < 1:
        raise ValueError("train_fraction must be between 0 and 1")

    pnl: list[float] = []
    equity_values: list[float] = [config.starting_cash]
    drawdown_values: list[float] = [0.0]
    equity = config.starting_cash
    peak = equity
    max_drawdown = 0.0
    wins = 0
    trade_count = 0
    active_periods = 0
    previous_position = 0
    gross_profit = 0.0
    spread_cost = 0.0
    slippage_cost = 0.0
    commission_cost = 0.0
    funding_cost = 0.0
    fx_cost = 0.0
    guaranteed_stop_cost = 0.0
    daily_pnl: dict[str, float] = {}

    for index in range(1, len(bars)):
        previous_bar = bars[index - 1]
        current_bar = bars[index]
        position = _normalize_signal(signals[index - 1])
        if position != 0:
            active_periods += 1
        position_delta = abs(position - previous_position)
        if position_delta > 0:
            trade_count += 1

        price_change = current_bar.close - previous_bar.close
        trade_gross = position * price_change * config.position_size
        gross_profit += trade_gross

        notional = previous_bar.close * config.position_size
        stress = max(0.0, config.cost_stress_multiplier)
        trade_spread = notional * (config.spread_bps / 10_000) * stress * position_delta / 2
        trade_slippage = notional * (config.slippage_bps / 10_000) * stress * position_delta
        trade_commission = notional * (config.commission_bps / 10_000) * position_delta / 2
        trade_guaranteed = (
            config.guaranteed_stop_premium_points * config.position_size * position_delta / 2
            if config.use_guaranteed_stop
            else 0.0
        )
        trade_funding = 0.0
        if position != 0 and _crosses_funding_cutoff(previous_bar.timestamp, current_bar.timestamp, config.funding_cutoff_hour):
            annual_rate = max(0.0, config.overnight_admin_fee_annual + config.overnight_interest_annual)
            trade_funding = notional * (annual_rate / 365) * stress
        trade_fx = 0.0
        if config.account_currency and config.instrument_currency and config.account_currency != config.instrument_currency:
            trade_fx = abs(trade_gross) * (config.fx_conversion_bps / 10_000)

        trade_cost = trade_spread + trade_slippage + trade_commission + trade_funding + trade_fx + trade_guaranteed
        trade_pnl = trade_gross - trade_cost
        spread_cost += trade_spread
        slippage_cost += trade_slippage
        commission_cost += trade_commission
        funding_cost += trade_funding
        fx_cost += trade_fx
        guaranteed_stop_cost += trade_guaranteed
        pnl.append(trade_pnl)
        pnl_date = current_bar.timestamp.date().isoformat()
        daily_pnl[pnl_date] = daily_pnl.get(pnl_date, 0.0) + trade_pnl
        equity += trade_pnl
        wins += 1 if trade_pnl > 0 else 0
        peak = max(peak, equity)
        current_drawdown = equity - peak
        max_drawdown = min(max_drawdown, current_drawdown)
        equity_values.append(equity)
        drawdown_values.append(abs(current_drawdown))
        previous_position = position

    split = max(1, int(len(pnl) * config.train_fraction))
    train_profit = sum(pnl[:split])
    test_profit = sum(pnl[split:])
    sharpe = _sharpe(pnl)
    train_sharpe = _sharpe(pnl[:split])
    test_sharpe = _sharpe(pnl[split:])
    total_cost = spread_cost + slippage_cost + commission_cost + funding_cost + fx_cost + guaranteed_stop_cost
    net_profit = sum(pnl)
    daily_pnl_values = list(daily_pnl.values())
    rolling_sharpes = _rolling_sharpes(daily_pnl_values, window=20)
    trade_denominator = max(1, trade_count)

    return BacktestResult(
        net_profit=net_profit,
        sharpe=sharpe,
        train_sharpe=train_sharpe,
        test_sharpe=test_sharpe,
        max_drawdown=abs(max_drawdown),
        win_rate=wins / len(pnl),
        trade_count=trade_count,
        exposure=active_periods / (len(bars) - 1),
        turnover=trade_count * config.position_size,
        train_profit=train_profit,
        test_profit=test_profit,
        gross_profit=gross_profit,
        spread_cost=spread_cost,
        slippage_cost=slippage_cost,
        commission_cost=commission_cost,
        funding_cost=funding_cost,
        fx_cost=fx_cost,
        guaranteed_stop_cost=guaranteed_stop_cost,
        total_cost=total_cost,
        cost_confidence=config.cost_confidence,
        estimated_spread_bps=config.spread_bps,
        estimated_slippage_bps=config.slippage_bps,
        daily_pnl_sharpe=_sharpe(daily_pnl_values),
        rolling_sharpe_min=min(rolling_sharpes) if rolling_sharpes else 0.0,
        rolling_sharpe_median=_median(rolling_sharpes),
        probabilistic_sharpe_ratio=_probabilistic_sharpe_ratio(daily_pnl_values),
        sharpe_observations=len(daily_pnl_values),
        turnover_efficiency=net_profit / max(1e-9, trade_count * config.position_size),
        expectancy_per_trade=net_profit / trade_denominator,
        average_cost_per_trade=total_cost / trade_denominator,
        net_cost_ratio=net_profit / max(1.0, total_cost),
        cost_to_gross_ratio=total_cost / max(1e-9, abs(gross_profit)),
        equity_curve=_sample_curve(equity_values),
        drawdown_curve=_sample_curve(drawdown_values),
        daily_pnl_curve=_sample_curve(daily_pnl_values),
    )


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


def _sample_curve(values: list[float], max_points: int = 250) -> tuple[float, ...]:
    if len(values) <= max_points:
        return tuple(round(value, 4) for value in values)
    step = max(1, len(values) // max_points)
    sampled = values[::step]
    if sampled[-1] != values[-1]:
        sampled.append(values[-1])
    return tuple(round(value, 4) for value in sampled[:max_points])


def _sharpe(pnl: list[float]) -> float:
    if not pnl:
        return 0.0
    mean = sum(pnl) / len(pnl)
    variance = sum((value - mean) ** 2 for value in pnl) / len(pnl)
    return 0.0 if variance == 0 else (mean / sqrt(variance)) * sqrt(252)


def _rolling_sharpes(pnl: list[float], window: int) -> list[float]:
    if len(pnl) < max(5, window):
        return []
    return [_sharpe(pnl[index - window : index]) for index in range(window, len(pnl) + 1)]


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2


def _probabilistic_sharpe_ratio(pnl: list[float], target_annual_sharpe: float = 0.0) -> float:
    sample_size = len(pnl)
    if sample_size < 3:
        return 0.0
    mean = sum(pnl) / sample_size
    variance = sum((value - mean) ** 2 for value in pnl) / sample_size
    if variance <= 0:
        return 0.0
    std = sqrt(variance)
    observed = mean / std
    target = target_annual_sharpe / sqrt(252)
    skew = sum(((value - mean) / std) ** 3 for value in pnl) / sample_size
    kurtosis = sum(((value - mean) / std) ** 4 for value in pnl) / sample_size
    denominator = sqrt(max(1e-12, 1 - skew * observed + ((kurtosis - 1) / 4) * observed**2))
    z_score = (observed - target) * sqrt(sample_size - 1) / denominator
    return round(_normal_cdf(z_score), 6)


def _normal_cdf(value: float) -> float:
    return 0.5 * (1 + erf(value / sqrt(2)))
