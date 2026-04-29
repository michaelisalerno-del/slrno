from __future__ import annotations

from dataclasses import dataclass
from math import sqrt

from .providers.base import OHLCBar


@dataclass(frozen=True)
class BacktestConfig:
    starting_cash: float = 10_000.0
    position_size: float = 1.0
    spread_bps: float = 2.0
    slippage_bps: float = 1.0
    train_fraction: float = 0.7


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


def run_vector_backtest(bars: list[OHLCBar], signals: list[int], config: BacktestConfig | None = None) -> BacktestResult:
    config = config or BacktestConfig()
    if len(bars) != len(signals):
        raise ValueError("bars and signals must have the same length")
    if len(bars) < 2:
        raise ValueError("at least two bars are required")
    if not 0 < config.train_fraction < 1:
        raise ValueError("train_fraction must be between 0 and 1")

    costs = (config.spread_bps + config.slippage_bps) / 10_000
    pnl: list[float] = []
    equity = config.starting_cash
    peak = equity
    max_drawdown = 0.0
    wins = 0
    trade_count = 0
    active_periods = 0
    previous_position = 0

    for index in range(1, len(bars)):
        position = _normalize_signal(signals[index - 1])
        if position != 0:
            active_periods += 1
        if position != previous_position:
            trade_count += 1
        previous_position = position

        price_change = bars[index].close - bars[index - 1].close
        trade_pnl = position * price_change * config.position_size
        if position != 0:
            trade_pnl -= bars[index - 1].close * config.position_size * costs
        pnl.append(trade_pnl)
        equity += trade_pnl
        wins += 1 if trade_pnl > 0 else 0
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)

    split = max(1, int(len(pnl) * config.train_fraction))
    train_profit = sum(pnl[:split])
    test_profit = sum(pnl[split:])
    mean = sum(pnl) / len(pnl)
    variance = sum((value - mean) ** 2 for value in pnl) / len(pnl)
    sharpe = 0.0 if variance == 0 else (mean / sqrt(variance)) * sqrt(252)

    return BacktestResult(
        net_profit=sum(pnl),
        sharpe=sharpe,
        max_drawdown=abs(max_drawdown),
        win_rate=wins / len(pnl),
        trade_count=trade_count,
        exposure=active_periods / (len(bars) - 1),
        turnover=trade_count * config.position_size,
        train_profit=train_profit,
        test_profit=test_profit,
    )


def _normalize_signal(value: int) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0
