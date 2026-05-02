from __future__ import annotations

from app.capital import capital_scenarios, capital_summary


def test_capital_scenarios_include_default_account_sizes_and_feasibility():
    scenarios = capital_scenarios(
        {"net_profit": 100},
        {"position_size": 0.25, "stop_loss_bps": 100},
        {"bid": 100, "offer": 101, "min_deal_size": 1.0, "margin_percent": 5.0},
    )

    assert [item["account_size"] for item in scenarios] == [250.0, 500.0, 1000.0, 3000.0, 10000.0]
    assert all("below_ig_min_deal_size" in item["violations"] for item in scenarios)
    assert capital_summary(scenarios)["smallest_feasible_account"] is None


def test_capital_scenarios_include_selected_testing_account_size():
    scenarios = capital_scenarios(
        {"net_profit": 100, "max_drawdown": 20},
        {"position_size": 0.25, "stop_loss_bps": 50},
        {"bid": 100, "offer": 101, "min_deal_size": 0.1, "margin_percent": 5.0},
        account_sizes=(2000,),
    )

    assert [item["account_size"] for item in scenarios] == [250.0, 500.0, 1000.0, 2000.0, 3000.0, 10000.0]


def test_capital_scenarios_block_accounts_with_historical_drawdown_or_daily_loss():
    scenarios = capital_scenarios(
        {"max_drawdown": 600, "daily_pnl_curve": [100, -30, 20]},
        {"position_size": 1.0, "stop_loss_bps": 100},
        {"bid": 100, "offer": 100, "min_deal_size": 1.0, "margin_percent": 5.0},
    )

    by_account = {item["account_size"]: item for item in scenarios}
    assert "historical_drawdown_too_large" in by_account[250.0]["violations"]
    assert "historical_daily_loss_stop_breached" in by_account[250.0]["violations"]
    assert "historical_drawdown_too_large" in by_account[1000.0]["violations"]
    assert "historical_daily_loss_stop_breached" not in by_account[1000.0]["violations"]
    assert "historical_drawdown_too_large" not in by_account[3000.0]["violations"]
    assert by_account[3000.0]["feasible"] is True
    assert by_account[10000.0]["feasible"] is True
    assert capital_summary(scenarios)["smallest_feasible_account"] == 3000.0


def test_capital_scenarios_project_compounded_balance_by_account_size():
    scenarios = capital_scenarios(
        {
            "starting_cash": 2000,
            "net_profit": 200,
            "return_pct": 10,
            "max_drawdown": 100,
            "compounded_position_sizing": True,
        },
        {"position_size": 1.0, "stop_loss_bps": 100},
        {"bid": 100, "offer": 100, "min_deal_size": 1.0, "margin_percent": 5.0},
    )

    by_account = {item["account_size"]: item for item in scenarios}
    assert by_account[3000.0]["compounding_enabled"] is True
    assert by_account[3000.0]["projected_final_balance"] == 3300
    assert by_account[10000.0]["projected_net_profit"] == 1000
    assert by_account[10000.0]["historical_max_drawdown"] == 500


def test_capital_scenarios_use_compounded_projection_without_changing_headline_backtest():
    scenarios = capital_scenarios(
        {
            "starting_cash": 2000,
            "net_profit": 200,
            "return_pct": 10,
            "max_drawdown": 100,
            "compounded_position_sizing": False,
            "compounded_projection_return_pct": 12,
            "compounded_projection_max_drawdown": 120,
            "compounded_projection_daily_pnl_curve": [50, -40, 30],
        },
        {"position_size": 1.0, "stop_loss_bps": 100},
        {"bid": 100, "offer": 100, "min_deal_size": 1.0, "margin_percent": 5.0},
    )

    by_account = {item["account_size"]: item for item in scenarios}
    assert by_account[3000.0]["compounding_enabled"] is True
    assert by_account[3000.0]["projected_final_balance"] == 3360
    assert by_account[10000.0]["projected_net_profit"] == 1200
    assert by_account[10000.0]["historical_max_drawdown"] == 600


def test_capital_scenarios_require_reference_price_for_margin_and_stop_estimates():
    scenarios = capital_scenarios({"max_drawdown": 10}, {"position_size": 1.0, "stop_loss_bps": 100}, {})

    assert all("missing_reference_price" in item["violations"] for item in scenarios)
    assert capital_summary(scenarios)["smallest_feasible_account"] is None
