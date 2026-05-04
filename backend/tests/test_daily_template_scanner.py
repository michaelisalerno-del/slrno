from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest

pytest.importorskip("fastapi")

import app.main as main
from app.market_registry import MarketMapping
from app.providers.base import OHLCBar
from app.research_store import ResearchStore


def test_daily_template_scanner_builds_and_stores_queue_from_frozen_templates(tmp_path, monkeypatch):
    store = ResearchStore(tmp_path / "research.sqlite3")
    market = MarketMapping(
        "ABC",
        "ABC plc",
        "share",
        "ABC.LSE",
        "IX.D.ABC.DAILY.IP",
        True,
        "discovered-uk-midcap",
        "ABC",
        "ABC",
        "5min",
        20.0,
        5.0,
        250,
    )

    class FakeMarkets:
        def get(self, market_id):
            return market if market_id == "ABC" else None

        def list(self, enabled_only=False):
            return [market]

    class FakeSettings:
        def get_secret(self, provider, key):
            return "token" if provider == "eodhd" and key == "api_token" else None

    class FakeEODHDProvider:
        def __init__(self, api_token):
            self.api_token = api_token

        async def historical_bars(self, symbol, interval, start, end):
            base = datetime(2026, 5, 4, 8, 0)
            return [
                OHLCBar(
                    symbol,
                    base + timedelta(minutes=5 * index),
                    100 + index,
                    101 + index,
                    99 + index,
                    100 + index,
                    100_000,
                )
                for index in range(30)
            ]

    monkeypatch.setattr(main, "research_store", store)
    monkeypatch.setattr(main, "markets", FakeMarkets())
    monkeypatch.setattr(main, "settings", FakeSettings())
    monkeypatch.setattr(main, "EODHDProvider", FakeEODHDProvider)

    store.save_template(
        {
            "name": "liquid_midcap_pullback",
            "market_id": "ABC",
            "interval": "5min",
            "strategy_family": "intraday_trend",
            "target_regime": "",
            "promotion_tier": "paper_candidate",
            "readiness_status": "ready_for_paper",
            "robustness_score": 86.0,
            "testing_account_size": 3000.0,
            "payload": {
                "source_template": {
                    "name": "liquid_midcap_pullback",
                    "market_id": "ABC",
                    "family": "intraday_trend",
                    "interval": "5min",
                    "holding_period": "intraday",
                    "force_flat_before_close": True,
                    "no_overnight": True,
                    "parameters": {
                        "lookback": 1,
                        "threshold_bps": 0,
                        "position_size": 1,
                        "stop_loss_bps": 20,
                        "take_profit_bps": 40,
                        "max_hold_bars": 5,
                        "min_hold_bars": 0,
                        "min_trade_spacing": 0,
                        "regime_filter": "any",
                        "confidence_quantile": 1.0,
                        "direction": "long_only",
                    },
                },
                "parameters": {
                    "market_id": "ABC",
                    "timeframe": "5min",
                    "family": "intraday_trend",
                    "day_trading_mode": True,
                    "holding_period": "intraday",
                    "force_flat_before_close": True,
                    "no_overnight": True,
                    "search_audit": {"paper_readiness_score": 88},
                },
                "backtest": {"net_profit": 250, "test_profit": 90, "trade_count": 28, "cost_to_gross_ratio": 0.25, "funding_cost": 0},
                "evidence": {"oos_net_profit": 90, "oos_trade_count": 12},
                "readiness": {"status": "ready_for_paper", "blockers": [], "validation_warnings": []},
                "warnings": [],
                "capital_scenarios": [{"account_size": 3000.0, "feasible": True, "violations": []}],
            },
        }
    )

    result = asyncio.run(
        main.start_daily_template_scanner(
            main.DailyTemplateScannerPayload(
                trading_date="2026-05-04",
                market_ids=["ABC"],
                account_size=3000.0,
                paper_limit=3,
                review_limit=10,
            )
        )
    )

    assert result["schema"] == "daily_template_scanner_v1"
    assert result["strategy_generation_allowed"] is False
    assert result["counts"]["daily_paper_queue"] == 1
    assert result["daily_paper_queue"][0]["source_type"] == "daily_frozen_template_scan"
    assert result["daily_paper_queue"][0]["side"] == "BUY"
    assert result["daily_paper_queue"][0]["broker_preview"]["order_placement"] == "disabled"

    latest = store.latest_day_trading_scan()
    assert latest is not None
    assert latest["id"] == result["scan_id"]
    assert latest["counts"]["daily_paper_queue"] == 1

    reviewed = main.record_daily_template_after_close(
        result["scan_id"],
        main.DailyTemplateAfterClosePayload(results={"notes": "matched expected direction"}),
    )
    assert reviewed["status"] == "reviewed"
    assert reviewed["after_close_results"]["notes"] == "matched expected direction"
