from __future__ import annotations

from app.ig_costs import backtest_config_from_profile, profile_badge, profile_from_ig_market, public_ig_cost_profile
from app.market_registry import MarketMapping


def test_public_cost_profile_marks_fmp_proxy_envelope():
    market = MarketMapping("QQQ", "QQQ proxy", "etf", "QQQ", "", plugin_id="fmp-qqq-nasdaq-proxy")

    profile = public_ig_cost_profile(market)

    assert profile.confidence == "fmp_proxy_ig_cost_envelope"
    assert profile_badge(profile) == "FMP proxy with IG cost envelope"
    assert profile.overnight_admin_fee_annual == 0.03


def test_ig_market_payload_sets_live_spread_and_rules():
    market = MarketMapping("GBPUSD", "GBP/USD", "forex", "GBPUSD", "CS.D.GBPUSD.TODAY.IP", spread_bps=2.0, slippage_bps=0.8)
    payload = {
        "instrument": {
            "epic": "CS.D.GBPUSD.TODAY.IP",
            "name": "GBP/USD",
            "type": "CURRENCIES",
            "slippageFactor": {"value": 1.2},
            "limitedRiskPremium": {"value": 0.3},
            "currencies": [{"code": "USD", "isDefault": True}],
            "marginDepositBands": [{"margin": 3.33}],
        },
        "snapshot": {"bid": 1.2500, "offer": 1.2502, "marketStatus": "TRADEABLE"},
        "dealingRules": {
            "minDealSize": {"value": 0.5},
            "minNormalStopOrLimitDistance": {"value": 2.0},
        },
    }

    profile = profile_from_ig_market(market, payload, account_currency="GBP")
    config = backtest_config_from_profile(profile)

    assert profile.confidence == "ig_live_epic_cost_profile"
    assert profile.spread_points == 0.0002
    assert profile.spread_bps > 0
    assert profile.slippage_bps == 1.2
    assert profile.margin_percent == 3.33
    assert config.instrument_currency == "USD"
    assert config.account_currency == "GBP"
