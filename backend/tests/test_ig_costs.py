from __future__ import annotations

from app.ig_costs import backtest_config_from_profile, profile_badge, profile_from_ig_market, public_ig_cost_profile, select_ig_market_candidate
from app.market_registry import MarketMapping


def test_public_cost_profile_uses_ig_public_baseline():
    market = MarketMapping("NAS100", "Nasdaq 100", "index", "NDX.INDX", "", plugin_id="ig-us-tech-100")

    profile = public_ig_cost_profile(market)

    assert profile.confidence == "ig_public_spread_baseline"
    assert profile_badge(profile) == "IG public spread baseline"
    assert profile.overnight_admin_fee_annual == 0.03


def test_share_public_cost_profile_uses_share_spread_bet_rules():
    market = MarketMapping("AAPL", "Apple", "share", "AAPL.US", "", plugin_id="ig-aapl", spread_bps=4.0, slippage_bps=2.0)

    profile = public_ig_cost_profile(market)
    config = backtest_config_from_profile(profile)

    assert profile.spread_bps == 10.0
    assert profile.slippage_bps == 5.0
    assert profile.margin_percent == 20.0
    assert profile.contract_point_size == 0.01
    assert profile.share_region == "us"
    assert config.contract_point_size == 0.01


def test_uk_share_public_cost_profile_uses_pence_points():
    market = MarketMapping("SHEL", "Shell", "share", "SHEL.LSE", "", plugin_id="ig-shell", spread_bps=5.0, slippage_bps=2.5)

    profile = public_ig_cost_profile(market)

    assert profile.spread_bps == 10.0
    assert profile.margin_percent == 20.0
    assert profile.contract_point_size == 1.0
    assert profile.share_region == "uk"


def test_ig_market_payload_sets_live_spread_and_rules():
    market = MarketMapping("GBPUSD", "GBP/USD", "forex", "GBPUSD.FOREX", "CS.D.GBPUSD.TODAY.IP", spread_bps=2.0, slippage_bps=0.8)
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


def test_select_ig_market_candidate_prefers_named_market_match():
    market = MarketMapping(
        "NAS100",
        "Nasdaq 100",
        "index",
        "NDX.INDX",
        "",
        ig_name="US Tech 100",
        ig_search_terms="US Tech 100,Nasdaq,NASDAQ 100",
    )

    selected = select_ig_market_candidate(
        market,
        [
            {"epic": "CS.D.UNRELATED.IP", "name": "US Treasury Bond", "type": "BOND"},
            {"epic": "IX.D.NASDAQ.IFMM.IP", "name": "US Tech 100", "type": "INDICES"},
        ],
    )

    assert selected is not None
    assert selected["epic"] == "IX.D.NASDAQ.IFMM.IP"


def test_profile_from_ig_market_stores_reference_midpoint():
    market = MarketMapping("NAS100", "Nasdaq 100", "index", "NDX.INDX", "IX.D.NASDAQ.IFMM.IP")

    profile = profile_from_ig_market(
        market,
        {
            "instrument": {"epic": "IX.D.NASDAQ.IFMM.IP", "type": "INDICES"},
            "snapshot": {"bid": 10_000, "offer": 10_002},
            "dealingRules": {},
        },
    )

    assert profile.reference_price == 10_001
    assert profile.confidence == "ig_live_epic_cost_profile"


def test_profile_from_ig_market_uses_recent_ig_price_when_live_snapshot_missing():
    market = MarketMapping("NAS100", "Nasdaq 100", "index", "NDX.INDX", "IX.D.NASDAQ.CASH.IP", spread_bps=2.0)

    profile = profile_from_ig_market(
        market,
        {
            "instrument": {"epic": "IX.D.NASDAQ.CASH.IP", "type": "INDICES"},
            "snapshot": {"marketStatus": "EDITS_ONLY"},
            "dealingRules": {"minDealSize": {"value": 0.01}},
        },
        recent_price={"bid": 27_677.1, "offer": 27_679.1, "snapshot_time": "2026-05-01T20:15:00"},
    )

    assert profile.bid == 27_677.1
    assert profile.offer == 27_679.1
    assert profile.reference_price == 27_678.1
    assert profile.spread_bps > 0
    assert profile.confidence == "ig_recent_epic_price_profile"
    assert profile.validation_status == "ig_price_validated"
    assert profile_badge(profile) == "IG recent EPIC price profile"
