from __future__ import annotations

from app.promotion_readiness import promotion_readiness


def test_promotion_readiness_blocks_negative_regime_gated_retest():
    backtest = {
        "daily_pnl_sharpe": 1.8,
        "estimated_slippage_bps": 1,
        "estimated_spread_bps": 2,
        "sharpe_observations": 160,
        "total_cost": 100,
        "trade_count": 30,
        "cost_confidence": "ig_live_epic_cost_profile",
    }

    readiness = promotion_readiness(backtest, ["regime_gated_backtest_negative"], {})

    assert readiness["status"] == "blocked"
    assert "regime_gated_backtest_negative" in readiness["blockers"]


def test_promotion_readiness_allows_expected_concentration_for_tradeable_specialist():
    backtest = {
        "daily_pnl_sharpe": 1.8,
        "estimated_slippage_bps": 1,
        "estimated_spread_bps": 2,
        "sharpe_observations": 160,
        "total_cost": 100,
        "trade_count": 30,
        "cost_confidence": "ig_live_epic_cost_profile",
    }
    parameters = {
        "bar_pattern_analysis": {
            "target_regime": "trend_up",
            "regime_verdict": "regime_tradeable",
        }
    }

    readiness = promotion_readiness(backtest, ["profit_concentrated_single_regime"], parameters)

    assert readiness["status"] == "ready_for_paper"
    assert "profit_concentrated_single_regime" not in readiness["blockers"]


def test_promotion_readiness_accepts_recent_ig_price_profile():
    backtest = {
        "daily_pnl_sharpe": 1.8,
        "estimated_slippage_bps": 1,
        "estimated_spread_bps": 2,
        "sharpe_observations": 160,
        "total_cost": 100,
        "trade_count": 30,
        "cost_confidence": "ig_recent_epic_price_profile",
    }

    readiness = promotion_readiness(backtest, [], {})

    assert readiness["status"] == "ready_for_paper"
    assert readiness["checks"]["ig_price_validated"] is True
    assert "needs_ig_price_validation" not in readiness["validation_warnings"]
