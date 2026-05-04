from __future__ import annotations

from app.broker_preview import broker_order_preview


def test_broker_order_preview_surfaces_rules_without_enabling_orders():
    preview = broker_order_preview(
        {"market_id": "GBPUSD", "name": "GBP/USD", "asset_class": "forex", "ig_epic": "CS.D.GBPUSD.TODAY.IP"},
        {
            "market_id": "GBPUSD",
            "bid": 1.25,
            "offer": 1.2502,
            "min_deal_size": 0.5,
            "min_stop_distance": 0.001,
            "min_limit_distance": 0.001,
            "margin_percent": 3.33,
            "instrument_currency": "USD",
            "account_currency": "GBP",
        },
        "BUY",
        stake=0.25,
        account_size=500,
        stop=1.2498,
        limit=1.251,
    )

    assert preview["preview_only"] is True
    assert preview["live_ordering_enabled"] is False
    assert preview["order_placement"] == "disabled"
    assert "below_ig_min_deal_size" in preview["rule_violations"]
    assert "stop_distance_below_ig_minimum" in preview["rule_violations"]


def test_broker_order_preview_allows_clear_preview():
    preview = broker_order_preview(
        {"market_id": "GBPUSD", "name": "GBP/USD", "asset_class": "forex"},
        {"market_id": "GBPUSD", "bid": 1.25, "offer": 1.2502, "min_deal_size": 0.5, "margin_percent": 3.33},
        "BUY",
        stake=1.0,
        account_size=500,
        stop=1.22,
        limit=1.28,
    )

    assert preview["feasible"] is True
    assert preview["rule_violations"] == []


def test_broker_order_preview_converts_share_points():
    preview = broker_order_preview(
        {"market_id": "AAPL", "name": "Apple", "asset_class": "share"},
        {
            "market_id": "AAPL",
            "bid": 200,
            "offer": 200.1,
            "min_deal_size": 1.0,
            "margin_percent": 20.0,
            "contract_point_size": 0.01,
        },
        "BUY",
        stake=1.0,
        account_size=3000,
        stop=198,
        limit=205,
    )

    assert preview["contract_point_size"] == 0.01
    assert preview["planned_risk"] == 210
    assert preview["estimated_margin"] == 4002
    assert "risk_budget_exceeded" in preview["rule_violations"]
    assert "margin_too_large" in preview["rule_violations"]
