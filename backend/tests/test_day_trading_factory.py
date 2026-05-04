from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

import app.main as main


def test_day_trading_factory_summary_ranks_queue_and_marks_unsuitable(monkeypatch):
    ready = {
        "id": 1,
        "run_id": 10,
        "strategy_name": "intraday_ready",
        "market_id": "ABC",
        "promotion_tier": "paper_candidate",
        "robustness_score": 78,
        "audit": {
            "promotion_tier": "paper_candidate",
            "warnings": [],
            "promotion_readiness": {"status": "ready_for_paper", "blockers": [], "validation_warnings": []},
            "candidate": {
                "parameters": {
                    "market_id": "ABC",
                    "timeframe": "5min",
                    "family": "intraday_trend",
                    "day_trading_mode": True,
                    "holding_period": "intraday",
                    "force_flat_before_close": True,
                    "no_overnight": True,
                    "evidence_profile": {"oos_net_profit": 120, "oos_trade_count": 22},
                    "search_audit": {"paper_readiness_score": 82, "day_trading_mode": True},
                },
            },
            "backtest": {"net_profit": 300, "test_profit": 120, "trade_count": 44, "cost_to_gross_ratio": 0.2, "funding_cost": 0},
        },
        "capital_scenarios": [{"account_size": 3000.0, "feasible": True, "violations": []}],
    }
    oversized = {
        **ready,
        "id": 2,
        "strategy_name": "oversized",
        "promotion_tier": "watchlist",
        "audit": {
            **ready["audit"],
            "warnings": ["ig_minimum_margin_too_large_for_account"],
            "promotion_readiness": {
                "status": "blocked",
                "blockers": ["ig_minimum_margin_too_large_for_account"],
                "validation_warnings": [],
            },
        },
        "capital_scenarios": [
            {
                "account_size": 3000.0,
                "feasible": False,
                "violations": ["ig_minimum_margin_too_large_for_account"],
            }
        ],
    }

    class Store:
        def list_candidates(self, limit=None):
            return [ready, oversized]

        def list_templates(self, limit=None, include_inactive=False):
            return []

        def get_cost_profile(self, market_id):
            return None

    monkeypatch.setattr(main, "research_store", Store())

    summary = main.day_trading_factory_summary(account_size=3000.0, paper_limit=3, review_limit=10)

    assert summary["live_ordering_enabled"] is False
    assert summary["counts"]["daily_paper_queue"] == 1
    assert summary["daily_paper_queue"][0]["strategy_name"] == "intraday_ready"
    assert summary["daily_paper_queue"][0]["order_placement"] == "disabled"
    assert summary["counts"]["unsuitable"] == 1
    assert summary["unsuitable"][0]["strategy_name"] == "oversized"
    assert "minimum margin" in summary["unsuitable"][0]["unsuitable_reason"]
