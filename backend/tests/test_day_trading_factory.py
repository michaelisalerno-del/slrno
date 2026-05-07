from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

import app.main as main


def test_day_trading_factory_summary_uses_frozen_templates_not_candidates(monkeypatch):
    discovery_candidate = {
        "id": 1,
        "run_id": 10,
        "strategy_name": "research_lead_not_live",
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
    ready_template = {
        "id": 101,
        "name": "liquid_midcap_pullback",
        "status": "active",
        "market_id": "ABC",
        "interval": "5min",
        "strategy_family": "intraday_trend",
        "target_regime": "trend_up",
        "promotion_tier": "paper_candidate",
        "readiness_status": "ready_for_paper",
        "robustness_score": 84,
        "source_template": {
            "market_id": "ABC",
            "family": "intraday_trend",
            "interval": "5min",
            "target_regime": "trend_up",
            "holding_period": "intraday",
            "force_flat_before_close": True,
            "no_overnight": True,
            "parameters": {"lookback": 20, "threshold_bps": 12, "max_hold_bars": 10},
        },
        "parameters": {
            "market_id": "ABC",
            "timeframe": "5min",
            "family": "intraday_trend",
            "day_trading_mode": True,
            "holding_period": "intraday",
            "force_flat_before_close": True,
            "no_overnight": True,
            "evidence_profile": {"oos_net_profit": 120, "oos_trade_count": 22},
            "search_audit": {"paper_readiness_score": 86, "day_trading_mode": True},
        },
        "backtest": {"net_profit": 300, "test_profit": 120, "trade_count": 44, "cost_to_gross_ratio": 0.2, "funding_cost": 0},
        "readiness": {"status": "ready_for_paper", "blockers": [], "validation_warnings": []},
        "warnings": [],
        "capital_scenarios": [{"account_size": 3000.0, "feasible": True, "violations": []}],
    }
    oversized = {
        **ready_template,
        "id": 102,
        "name": "oversized",
        "promotion_tier": "watchlist",
        "readiness_status": "blocked",
        "readiness": {"status": "blocked", "blockers": ["ig_minimum_margin_too_large_for_account"], "validation_warnings": []},
        "warnings": ["ig_minimum_margin_too_large_for_account"],
        "capital_scenarios": [
            {
                "account_size": 3000.0,
                "feasible": False,
                "violations": ["ig_minimum_margin_too_large_for_account"],
            }
        ],
    }
    unfrozen_template = {
        **ready_template,
        "id": 103,
        "name": "not_frozen_yet",
        "source_template": {},
    }

    class Store:
        def list_candidates(self, limit=None):
            return [discovery_candidate]

        def list_templates(self, limit=None, include_inactive=False):
            return [ready_template, oversized, unfrozen_template]

        def get_cost_profile(self, market_id):
            return None

    monkeypatch.setattr(main, "research_store", Store())

    summary = main.day_trading_factory_summary(account_size=3000.0, paper_limit=3, review_limit=10)

    assert summary["live_ordering_enabled"] is False
    assert summary["strategy_generation_allowed"] is False
    assert summary["counts"]["daily_paper_queue"] == 0
    assert summary["counts"]["template_ready_for_scan"] == 1
    assert summary["template_ready_without_scan"][0]["strategy_name"] == "liquid_midcap_pullback"
    assert summary["template_ready_without_scan"][0]["order_placement"] == "disabled"
    assert summary["counts"]["discovery_leads_needing_freeze"] == 1
    assert summary["discovery_leads_not_live"][0]["strategy_name"] == "research_lead_not_live"
    assert summary["counts"]["non_frozen_day_templates"] == 1
    assert summary["counts"]["unsuitable"] == 1
    assert summary["unsuitable"][0]["strategy_name"] == "oversized"
    assert "minimum margin" in summary["unsuitable"][0]["unsuitable_reason"]
