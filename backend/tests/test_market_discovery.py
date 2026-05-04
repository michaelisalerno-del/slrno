from __future__ import annotations

from app.market_discovery import MidcapDiscoveryCriteria, build_midcap_candidates


def test_midcap_discovery_builds_installable_uk_share_mapping():
    candidates = build_midcap_candidates(
        [
            {
                "symbol": "JD.L",
                "companyName": "JD Sports Fashion",
                "exchangeShortName": "LSE",
                "country": "GB",
                "marketCap": 4_500_000_000,
                "price": 120,
                "volume": 5_000_000,
            }
        ],
        MidcapDiscoveryCriteria(country="UK", account_size=3000),
        "fmp_company_screener",
    )

    candidate = candidates[0]
    mapping = candidate.market_mapping()

    assert candidate.eligible is True
    assert candidate.market_id == "JD"
    assert candidate.eodhd_symbol == "JD.LSE"
    assert candidate.estimated_spread_bps == 50.0
    assert candidate.contract_point_size == 1.0
    assert mapping.asset_class == "share"
    assert mapping.default_timeframe == "5min"
    assert mapping.min_backtest_bars == 750


def test_midcap_discovery_blocks_expensive_us_share_for_small_account_probe_stake():
    candidates = build_midcap_candidates(
        [
            {
                "symbol": "TEST",
                "companyName": "Test Software",
                "exchangeShortName": "NASDAQ",
                "country": "US",
                "marketCap": 5_000_000_000,
                "price": 500,
                "volume": 2_000_000,
            }
        ],
        MidcapDiscoveryCriteria(country="US", account_size=3000),
        "fmp_company_screener",
    )

    candidate = candidates[0]

    assert candidate.contract_point_size == 0.01
    assert candidate.estimated_margin_for_probe_stake == 10_000
    assert candidate.eligible is False
    assert "probe_stake_margin_too_large" in candidate.blockers


def test_midcap_discovery_ig_match_controls_eligibility_after_verification():
    candidate = build_midcap_candidates(
        [
            {
                "symbol": "MKS.L",
                "companyName": "Marks and Spencer Group",
                "exchangeShortName": "LSE",
                "country": "GB",
                "marketCap": 5_000_000_000,
                "price": 300,
                "volume": 2_000_000,
            }
        ],
        MidcapDiscoveryCriteria(country="UK", account_size=3000),
        "fmp_company_screener",
    )[0]

    missing = candidate.with_ig_match("", "")
    matched = candidate.with_ig_match("KA.D.MKS.DAILY.IP", "Marks and Spencer Group")

    assert missing.eligible is False
    assert "ig_market_not_found" in missing.blockers
    assert matched.eligible is True
    assert matched.market_mapping().ig_epic == "KA.D.MKS.DAILY.IP"


def test_midcap_discovery_blocks_when_ig_catalogue_is_required_but_unchecked():
    candidate = build_midcap_candidates(
        [
            {
                "symbol": "MKS.L",
                "companyName": "Marks and Spencer Group",
                "exchangeShortName": "LSE",
                "country": "GB",
                "marketCap": 5_000_000_000,
                "price": 300,
                "volume": 2_000_000,
            }
        ],
        MidcapDiscoveryCriteria(country="UK", account_size=3000),
        "fmp_company_screener",
    )[0]

    blocked = candidate.with_ig_blocker("ig_not_configured", "ig_credentials_required", "ig_catalogue_required")

    assert blocked.eligible is False
    assert "ig_credentials_required" in blocked.blockers
    assert "ig_catalogue_required" in blocked.warnings
