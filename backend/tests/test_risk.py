from __future__ import annotations

from app.providers.base import PaperOrder
from app.risk import RiskEngine, RiskLimits


def test_risk_engine_allows_valid_paper_order():
    engine = RiskEngine(RiskLimits({"GBPUSD"}, 2.0, 3, 100.0))
    order = PaperOrder("GBPUSD", "BUY", 1.0, 1.25)

    decision = engine.evaluate(order, [], realized_daily_pnl=0)

    assert decision.allowed is True


def test_risk_engine_blocks_kill_switch():
    engine = RiskEngine(RiskLimits({"GBPUSD"}, 2.0, 3, 100.0, kill_switch_enabled=True))
    order = PaperOrder("GBPUSD", "BUY", 1.0, 1.25)

    decision = engine.evaluate(order, [], realized_daily_pnl=0)

    assert decision.allowed is False
    assert decision.reason == "kill_switch_enabled"


def test_risk_engine_blocks_daily_loss():
    engine = RiskEngine(RiskLimits({"GBPUSD"}, 2.0, 3, 100.0))
    order = PaperOrder("GBPUSD", "BUY", 1.0, 1.25)

    decision = engine.evaluate(order, [], realized_daily_pnl=-101.0)

    assert decision.allowed is False
    assert decision.reason == "max_daily_loss_exceeded"
