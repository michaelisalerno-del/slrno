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


def test_midcap_discovery_accepts_eodhd_screener_rows():
    candidates = build_midcap_candidates(
        [
            {
                "code": "MKS",
                "name": "Marks and Spencer Group",
                "exchange": "LSE",
                "currency_symbol": "GBp",
                "market_capitalization": 6_500_000_000,
                "adjusted_close": 323.95,
                "avgvol_200d": 5_000_000,
            }
        ],
        MidcapDiscoveryCriteria(country="UK", account_size=3000),
        "eodhd_stock_screener",
    )

    candidate = candidates[0]

    assert candidate.eligible is True
    assert candidate.market_id == "MKS"
    assert candidate.eodhd_symbol == "MKS.LSE"
    assert candidate.exchange == "LSE"
    assert candidate.currency == "GBp"
    assert "starter_universe_not_live_constituents" not in candidate.warnings
    assert candidate.turnover > 10_000_000


def test_midcap_discovery_penalizes_speculative_us_profiles():
    candidates = build_midcap_candidates(
        [
            {
                "code": "RIOT",
                "name": "Riot Blockchain Inc",
                "exchange": "US",
                "currency_symbol": "$",
                "sector": "Financial Services",
                "industry": "Capital Markets",
                "market_capitalization": 7_000_000_000,
                "adjusted_close": 18.68,
                "avgvol_200d": 25_000_000,
                "earnings_share": -2.51,
                "dividend_yield": None,
            },
            {
                "code": "PEGA",
                "name": "Pegasystems Inc",
                "exchange": "US",
                "currency_symbol": "$",
                "sector": "Technology",
                "industry": "Software - Application",
                "market_capitalization": 6_100_000_000,
                "adjusted_close": 36.79,
                "avgvol_200d": 1_700_000,
                "earnings_share": 1.88,
                "dividend_yield": 0.0029,
            },
        ],
        MidcapDiscoveryCriteria(country="US", account_size=3000),
        "eodhd_stock_screener",
    )

    assert candidates[0].market_id == "PEGA"
    riot = next(candidate for candidate in candidates if candidate.market_id == "RIOT")
    assert "speculative_share_profile" in riot.warnings
    assert "speculative_share_profile" in riot.blockers
    assert "negative_earnings_share" in riot.warnings


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


def test_midcap_discovery_blocks_low_turnover_day_trading_watchlist_names():
    candidates = build_midcap_candidates(
        [
            {
                "code": "QUIET",
                "name": "Quiet Industrials",
                "exchange": "US",
                "currency_symbol": "$",
                "market_capitalization": 3_000_000_000,
                "adjusted_close": 20,
                "avgvol_200d": 500_000,
                "earnings_share": 1.2,
            }
        ],
        MidcapDiscoveryCriteria(country="US", account_size=3000),
        "eodhd_stock_screener",
    )

    candidate = candidates[0]

    assert candidate.turnover == 10_000_000
    assert candidate.eligible is False
    assert "turnover_too_low_for_day_trading_watchlist" in candidate.blockers


def test_midcap_discovery_blocks_low_priced_us_shares_for_guided_watchlist():
    candidates = build_midcap_candidates(
        [
            {
                "code": "VLY",
                "name": "Valley National Bancorp",
                "exchange": "US",
                "currency_symbol": "$",
                "market_capitalization": 5_000_000_000,
                "adjusted_close": 8.25,
                "avgvol_200d": 20_000_000,
                "earnings_share": 1.0,
            }
        ],
        MidcapDiscoveryCriteria(country="US", account_size=3000),
        "eodhd_stock_screener",
    )

    candidate = candidates[0]

    assert candidate.eligible is False
    assert "low_priced_us_share" in candidate.blockers


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
    assert "ig_availability_not_checked" not in matched.warnings
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
