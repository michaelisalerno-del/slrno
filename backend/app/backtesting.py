from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import erf, sqrt

from .providers.base import OHLCBar


@dataclass(frozen=True)
class BacktestConfig:
    starting_cash: float = 10_000.0
    position_size: float = 1.0
    compound_position_size: bool = False
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
    bar_sharpe_observations: int = 0
    sample_calendar_days: int = 0
    sample_trading_days: int = 0
    daily_periods_per_year: float = 252.0
    bar_periods_per_year: float = 252.0
    daily_pnl_sample_sharpe: float = 0.0
    bar_sample_sharpe: float = 0.0
    train_daily_pnl_sharpe: float = 0.0
    test_daily_pnl_sharpe: float = 0.0
    sharpe_annualization_note: str = ""
    turnover_efficiency: float = 0.0
    expectancy_per_trade: float = 0.0
    average_cost_per_trade: float = 0.0
    net_cost_ratio: float = 0.0
    cost_to_gross_ratio: float = 0.0
    starting_cash: float = 10_000.0
    final_equity: float = 10_000.0
    return_pct: float = 0.0
    compounded_position_sizing: bool = False
    min_effective_position_size: float = 0.0
    max_effective_position_size: float = 0.0
    average_effective_position_size: float = 0.0
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
    if config.starting_cash <= 0:
        raise ValueError("starting_cash must be positive")

    pnl: list[float] = []
    equity_values: list[float] = [config.starting_cash]
    drawdown_values: list[float] = [0.0]
    equity = config.starting_cash
    peak = equity
    max_drawdown = 0.0
    wins = 0
    trade_count = 0
    active_periods = 0
    previous_exposure = 0.0
    gross_profit = 0.0
    spread_cost = 0.0
    slippage_cost = 0.0
    commission_cost = 0.0
    funding_cost = 0.0
    fx_cost = 0.0
    guaranteed_stop_cost = 0.0
    pnl_dates: list[str] = []
    daily_pnl: dict[str, float] = {}
    exposure_turnover = 0.0
    active_position_sizes: list[float] = []

    for index in range(1, len(bars)):
        previous_bar = bars[index - 1]
        current_bar = bars[index]
        direction = _normalize_signal(signals[index - 1])
        position = _target_exposure(direction, previous_exposure, equity, config)
        if position != 0:
            active_periods += 1
            active_position_sizes.append(abs(position))
        exposure_delta = abs(position - previous_exposure)
        if exposure_delta > 0:
            trade_count += 1
            exposure_turnover += exposure_delta

        price_change = current_bar.close - previous_bar.close
        trade_gross = position * price_change
        gross_profit += trade_gross

        notional = previous_bar.close * abs(position)
        stress = max(0.0, config.cost_stress_multiplier)
        trade_spread = previous_bar.close * (config.spread_bps / 10_000) * stress * exposure_delta / 2
        trade_slippage = previous_bar.close * (config.slippage_bps / 10_000) * stress * exposure_delta
        trade_commission = previous_bar.close * (config.commission_bps / 10_000) * exposure_delta / 2
        trade_guaranteed = (
            config.guaranteed_stop_premium_points * exposure_delta / 2
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
        pnl_dates.append(pnl_date)
        daily_pnl[pnl_date] = daily_pnl.get(pnl_date, 0.0) + trade_pnl
        equity += trade_pnl
        wins += 1 if trade_pnl > 0 else 0
        peak = max(peak, equity)
        current_drawdown = equity - peak
        max_drawdown = min(max_drawdown, current_drawdown)
        equity_values.append(equity)
        drawdown_values.append(abs(current_drawdown))
        previous_exposure = position

    split = max(1, int(len(pnl) * config.train_fraction))
    train_profit = sum(pnl[:split])
    test_profit = sum(pnl[split:])
    bar_periods_per_year = _bar_periods_per_year(bars)
    sharpe = _sharpe(pnl, periods_per_year=bar_periods_per_year)
    train_sharpe = _sharpe(pnl[:split], periods_per_year=bar_periods_per_year)
    test_sharpe = _sharpe(pnl[split:], periods_per_year=bar_periods_per_year)
    total_cost = spread_cost + slippage_cost + commission_cost + funding_cost + fx_cost + guaranteed_stop_cost
    net_profit = sum(pnl)
    daily_pnl_values = list(daily_pnl.values())
    train_daily_pnl_values = _daily_pnl_values(pnl[:split], pnl_dates[:split])
    test_daily_pnl_values = _daily_pnl_values(pnl[split:], pnl_dates[split:])
    rolling_sharpes = _rolling_sharpes(daily_pnl_values, window=20)
    trade_denominator = max(1, trade_count)
    sample_trading_days = len(daily_pnl_values)
    sample_calendar_days = (bars[-1].timestamp.date() - bars[0].timestamp.date()).days + 1

    return BacktestResult(
        starting_cash=config.starting_cash,
        final_equity=equity,
        return_pct=(net_profit / config.starting_cash) * 100,
        net_profit=net_profit,
        sharpe=sharpe,
        train_sharpe=train_sharpe,
        test_sharpe=test_sharpe,
        max_drawdown=abs(max_drawdown),
        win_rate=wins / len(pnl),
        trade_count=trade_count,
        exposure=active_periods / (len(bars) - 1),
        turnover=exposure_turnover,
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
        bar_sharpe_observations=len(pnl),
        sample_calendar_days=sample_calendar_days,
        sample_trading_days=sample_trading_days,
        daily_periods_per_year=252.0,
        bar_periods_per_year=bar_periods_per_year,
        daily_pnl_sample_sharpe=_sample_sharpe(daily_pnl_values),
        bar_sample_sharpe=_sample_sharpe(pnl),
        train_daily_pnl_sharpe=_sharpe(train_daily_pnl_values),
        test_daily_pnl_sharpe=_sharpe(test_daily_pnl_values),
        sharpe_annualization_note=_sharpe_note(sample_trading_days),
        turnover_efficiency=net_profit / max(1e-9, exposure_turnover),
        expectancy_per_trade=net_profit / trade_denominator,
        average_cost_per_trade=total_cost / trade_denominator,
        net_cost_ratio=net_profit / max(1.0, total_cost),
        cost_to_gross_ratio=total_cost / max(1e-9, abs(gross_profit)),
        compounded_position_sizing=config.compound_position_size,
        min_effective_position_size=min(active_position_sizes) if active_position_sizes else 0.0,
        max_effective_position_size=max(active_position_sizes) if active_position_sizes else 0.0,
        average_effective_position_size=sum(active_position_sizes) / len(active_position_sizes) if active_position_sizes else 0.0,
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


def _sharpe(pnl: list[float], periods_per_year: float = 252.0) -> float:
    sample_sharpe = _sample_sharpe(pnl)
    return sample_sharpe * sqrt(max(0.0, periods_per_year)) if sample_sharpe else 0.0


def _sample_sharpe(pnl: list[float]) -> float:
    if not pnl:
        return 0.0
    mean = sum(pnl) / len(pnl)
    variance = sum((value - mean) ** 2 for value in pnl) / len(pnl)
    return 0.0 if variance == 0 else mean / sqrt(variance)


def _rolling_sharpes(pnl: list[float], window: int, periods_per_year: float = 252.0) -> list[float]:
    if len(pnl) < max(5, window):
        return []
    return [_sharpe(pnl[index - window : index], periods_per_year) for index in range(window, len(pnl) + 1)]


def _daily_pnl_values(pnl: list[float], pnl_dates: list[str]) -> list[float]:
    daily: dict[str, float] = {}
    for value, pnl_date in zip(pnl, pnl_dates):
        daily[pnl_date] = daily.get(pnl_date, 0.0) + value
    return list(daily.values())


def _bar_periods_per_year(bars: list[OHLCBar]) -> float:
    counts: dict[str, int] = {}
    for bar in bars:
        date_key = bar.timestamp.date().isoformat()
        counts[date_key] = counts.get(date_key, 0) + 1
    if counts:
        return max(1.0, _median([float(count) for count in counts.values()]) * 252.0)
    return 252.0


def _sharpe_note(sample_trading_days: int) -> str:
    if sample_trading_days < 1:
        return "Sharpe is unavailable without daily PnL observations."
    return f"Annualized estimate from {sample_trading_days} daily PnL observations."


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
