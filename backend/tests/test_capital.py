from __future__ import annotations

from app.capital import capital_scenarios, capital_summary


def test_capital_scenarios_include_default_account_sizes_and_feasibility():
    scenarios = capital_scenarios(
        {"net_profit": 100},
        {"position_size": 0.25, "stop_loss_bps": 100},
        {"bid": 100, "offer": 101, "min_deal_size": 1.0, "margin_percent": 5.0},
    )

    assert [item["account_size"] for item in scenarios] == [250.0, 500.0, 1000.0, 10000.0]
    assert all("below_ig_min_deal_size" in item["violations"] for item in scenarios)
    assert capital_summary(scenarios)["smallest_feasible_account"] is None
