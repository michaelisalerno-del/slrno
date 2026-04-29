from __future__ import annotations

from dataclasses import dataclass

from .providers.base import PaperOrder, Position


@dataclass(frozen=True)
class RiskLimits:
    allowed_markets: set[str]
    max_position_size: float
    max_open_trades: int
    max_daily_loss: float
    kill_switch_enabled: bool = False


@dataclass(frozen=True)
class RiskDecision:
    allowed: bool
    reason: str


class RiskEngine:
    def __init__(self, limits: RiskLimits) -> None:
        self.limits = limits

    def evaluate(self, order: PaperOrder, open_positions: list[Position], realized_daily_pnl: float) -> RiskDecision:
        if self.limits.kill_switch_enabled:
            return RiskDecision(False, "kill_switch_enabled")
        if order.market_id not in self.limits.allowed_markets:
            return RiskDecision(False, "market_not_allowed")
        if order.size <= 0:
            return RiskDecision(False, "size_must_be_positive")
        if order.size > self.limits.max_position_size:
            return RiskDecision(False, "max_position_size_exceeded")
        if len(open_positions) >= self.limits.max_open_trades:
            return RiskDecision(False, "max_open_trades_exceeded")
        if realized_daily_pnl <= -abs(self.limits.max_daily_loss):
            return RiskDecision(False, "max_daily_loss_exceeded")
        return RiskDecision(True, "allowed")
