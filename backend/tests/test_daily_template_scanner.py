from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

import pytest

pytest.importorskip("fastapi")

import app.main as main
from app.market_registry import MarketMapping
from app.providers.base import OHLCBar
from app.research_store import ResearchStore


def test_midcap_template_designs_are_country_specific():
    designs = main.day_trading_template_designs()["designs"]
    us_ids = {design["id"] for design in designs if design["country"] == "US"}
    uk_ids = {design["id"] for design in designs if design["country"] == "UK"}

    assert "liquid_uk_midcap_breakout" in uk_ids
    assert "liquid_us_midcap_breakout" in us_ids
    assert "liquid_us_midcap_trend_pullback" in us_ids


def test_midcap_template_pipeline_rejects_design_country_mismatch():
    design = main._midcap_template_design("liquid_uk_midcap_breakout")

    with pytest.raises(main.HTTPException) as exc_info:
        main._validate_midcap_design_country(design, "US")

    assert exc_info.value.status_code == 400
    assert "UK template design" in str(exc_info.value.detail)


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
    assert result["daily_paper_queue"][0]["manual_playbook"]["id"] == "vwap_trend_pullback"
    assert result["daily_paper_queue"][0]["today_tape"]["relative_volume"] == 1.0
    assert result["daily_paper_queue"][0]["manual_setup_score"] > 0
    assert result["daily_paper_queue"][0]["signal_explainer"]["rule_change_allowed"] is False
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


def test_manual_playbook_blocks_stale_intraday_bars():
    template = {
        "id": 1,
        "name": "UK opening range breakout",
        "market_id": "ABC",
        "strategy_family": "breakout",
        "interval": "5min",
        "source_template": {
            "holding_period": "intraday",
            "force_flat_before_close": True,
            "no_overnight": True,
            "parameters": {"lookback": 6, "threshold_bps": 4, "position_size": 1},
        },
    }
    market = MarketMapping("ABC", "ABC plc", "share", "ABC.LSE", "IX.D.ABC.DAILY.IP", True, "", "ABC", "ABC", "5min", 20, 5, 250)
    base = datetime(2026, 5, 1, 8, 0)
    bars = [
        OHLCBar("ABC.LSE", base + timedelta(minutes=5 * index), 100 + index, 101 + index, 99 + index, 100 + index, 100_000)
        for index in range(12)
    ]

    gate = main._manual_setup_gate(template, market, bars, datetime(2026, 5, 4).date(), 1, {"spread_bps": 20})

    assert gate["passed"] is False
    assert "stale_intraday_bars" in gate["blockers"]
    assert gate["manual_playbook"]["id"] == "opening_range_breakout"


def test_midcap_template_pipeline_installs_markets_and_starts_design_run(tmp_path, monkeypatch):
    store = ResearchStore(tmp_path / "research.sqlite3")
    installed = {}

    class FakeMarkets:
        def get(self, market_id):
            return installed.get(market_id)

        def list(self, enabled_only=False):
            return list(installed.values())

        def upsert(self, mapping):
            installed[mapping.market_id] = mapping

    class FakeSettings:
        def get_secret(self, provider, key):
            return "token" if provider == "eodhd" and key == "api_token" else None

    class FakeBackgroundTasks:
        def __init__(self):
            self.tasks = []

        def add_task(self, func, *args):
            self.tasks.append((func, args))

    async def fake_discover_midcaps(**kwargs):
        return {
            "schema": "midcap_discovery_v1",
            "country": kwargs["country"],
            "data_source": "test",
            "ig_status": "checked",
            "eligible_count": 1,
            "criteria": {"country": kwargs["country"], "product_mode": kwargs["product_mode"]},
            "candidates": [
                {
                    "market_id": "ABC",
                    "name": "ABC plc",
                    "volume": 900000,
                    "market_cap": 2_500_000_000,
                    "score": 91,
                    "eligible": True,
                    "ig_status": "ig_matched",
                    "blockers": [],
                    "warnings": [],
                    "market_mapping": {
                        "market_id": "ABC",
                        "name": "ABC plc",
                        "asset_class": "share",
                        "eodhd_symbol": "ABC.LSE",
                        "ig_epic": "IX.D.ABC.DAILY.IP",
                        "enabled": True,
                        "plugin_id": "discovered-abc",
                        "ig_name": "ABC",
                        "ig_search_terms": "ABC,ABC plc",
                        "default_timeframe": "5min",
                        "spread_bps": 18.0,
                        "slippage_bps": 5.0,
                        "min_backtest_bars": 750,
                    },
                }
            ],
        }

    async def fake_sync_costs(payload):
        return {
            "status": "synced",
            "profile_count": len(payload.market_ids),
            "price_validated_count": len(payload.market_ids),
            "profiles": [{"market_id": "ABC", "confidence": "ig_recent_epic_reference_profile", "validation_status": "ig_price_validated"}],
        }

    monkeypatch.setattr(main, "research_store", store)
    monkeypatch.setattr(main, "markets", FakeMarkets())
    monkeypatch.setattr(main, "settings", FakeSettings())
    monkeypatch.setattr(main, "discover_midcap_markets", fake_discover_midcaps)
    monkeypatch.setattr(main, "sync_ig_market_costs", fake_sync_costs)

    background_tasks = FakeBackgroundTasks()
    result = asyncio.run(
        main.start_midcap_template_pipeline(
            main.MidcapTemplatePipelinePayload(
                design_id="liquid_uk_midcap_trend_pullback",
                country="UK",
                broker_validation_mode="validate_before_research",
                account_size=3000.0,
                max_markets=4,
            ),
            background_tasks,
        )
    )

    assert result["schema"] == "midcap_template_pipeline_v1"
    assert result["status"] == "running"
    assert result["strategy_generation_allowed_in_daily_mode"] is False
    assert result["selected_markets"][0]["market_id"] == "ABC"
    assert installed["ABC"].default_timeframe == "5min"
    assert result["research_run_id"] is not None
    assert result["auto_freeze_policy"]["enabled"] is True
    assert len(background_tasks.tasks) == 1

    run = store.get_run(result["research_run_id"])
    assert run is not None
    assert run["status"] == "running"
    assert run["config"]["day_trading_mode"] is True
    assert run["config"]["force_flat_before_close"] is True
    assert run["config"]["search_budget"] == main.TWO_VCPU_MIDCAP_SEARCH_BUDGET
    assert run["config"]["diagnostic_limit"] == main.TWO_VCPU_MIDCAP_DIAGNOSTIC_LIMIT
    assert run["config"]["include_market_context"] is False
    assert run["config"]["pipeline"]["design_id"] == "liquid_uk_midcap_trend_pullback"
    assert run["config"]["pipeline"]["server_profile"] == "guided_midcap_2vcpu_profile_v1"
    assert run["config"]["pipeline"]["broker_validation_mode"] == "validate_before_research"
    assert run["config"]["pipeline"]["broker_validated_market_ids"] == ["ABC"]
    assert run["config"]["pipeline"]["daily_mode_source"] == "active_frozen_template_library_only"
    assert run["config"]["pipeline"]["auto_freeze"]["enabled"] is True
    assert run["config"]["pipeline"]["auto_freeze"]["status"] == "waiting_for_design_run"
    assert run["config"]["strategy_families"] == ["intraday_trend", "mean_reversion", "liquidity_sweep_reversal"]


def test_midcap_template_pipeline_runs_research_first_without_ig_price_validation(tmp_path, monkeypatch):
    store = ResearchStore(tmp_path / "research.sqlite3")
    installed = {}

    class FakeMarkets:
        def get(self, market_id):
            return installed.get(market_id)

        def list(self, enabled_only=False):
            return list(installed.values())

        def upsert(self, mapping):
            installed[mapping.market_id] = mapping

    class FakeSettings:
        def get_secret(self, provider, key):
            return "token" if provider == "eodhd" and key == "api_token" else None

    class FakeBackgroundTasks:
        def __init__(self):
            self.tasks = []

        def add_task(self, func, *args):
            self.tasks.append((func, args))

    async def fake_discover_midcaps(**kwargs):
        assert kwargs["verify_ig"] is False
        assert kwargs["require_ig_catalogue"] is False
        return {
            "schema": "midcap_discovery_v1",
            "country": kwargs["country"],
            "data_source": "test",
            "ig_status": "checked",
            "eligible_count": 1,
            "criteria": {"country": kwargs["country"], "product_mode": kwargs["product_mode"]},
            "candidates": [
                {
                    "market_id": "ABC",
                    "name": "ABC plc",
                    "volume": 900000,
                    "market_cap": 2_500_000_000,
                    "score": 91,
                    "eligible": True,
                    "ig_status": "ig_matched",
                    "blockers": [],
                    "warnings": [],
                    "market_mapping": {
                        "market_id": "ABC",
                        "name": "ABC plc",
                        "asset_class": "share",
                        "eodhd_symbol": "ABC.LSE",
                        "ig_epic": "IX.D.ABC.DAILY.IP",
                        "enabled": True,
                        "plugin_id": "discovered-abc",
                        "ig_name": "ABC",
                        "ig_search_terms": "ABC,ABC plc",
                        "default_timeframe": "5min",
                        "spread_bps": 18.0,
                        "slippage_bps": 5.0,
                        "min_backtest_bars": 750,
                    },
                }
            ],
        }

    async def fake_sync_costs(payload):
        raise AssertionError("research-first guided mode should not call IG price validation before research")

    monkeypatch.setattr(main, "research_store", store)
    monkeypatch.setattr(main, "markets", FakeMarkets())
    monkeypatch.setattr(main, "settings", FakeSettings())
    monkeypatch.setattr(main, "discover_midcap_markets", fake_discover_midcaps)
    monkeypatch.setattr(main, "sync_ig_market_costs", fake_sync_costs)

    background_tasks = FakeBackgroundTasks()
    result = asyncio.run(
        main.start_midcap_template_pipeline(
            main.MidcapTemplatePipelinePayload(
                design_id="liquid_uk_midcap_trend_pullback",
                country="UK",
                account_size=3000.0,
            ),
            background_tasks,
        )
    )

    assert result["status"] == "running_research_only"
    assert result["research_run_id"] is not None
    assert result["run_ready_market_ids"] == []
    assert result["research_market_ids"] == ["ABC"]
    assert result["selected_markets"][0]["market_id"] == "ABC"
    assert result["cost_sync"]["status"] == "deferred_research_first"
    assert result["auto_freeze_policy"]["enabled"] is False
    assert result["auto_freeze_policy"]["blocked_reason"] == "research_only_until_top_candidates_are_ig_price_validated"
    assert len(background_tasks.tasks) == 1

    run = store.get_run(result["research_run_id"])
    assert run is not None
    assert run["config"]["pipeline"]["broker_validation_mode"] == "research_first"
    assert run["config"]["pipeline"]["research_cost_mode"] == "public_proxy_until_ig_finalist_validation"
    assert run["config"]["pipeline"]["broker_validated_market_ids"] == []
    assert run["config"]["pipeline"]["auto_freeze"]["enabled"] is False
    assert run["config"]["pipeline"]["auto_freeze"]["status"] == "research_only_awaiting_ig_finalist_validation"


def test_guided_auto_freeze_selects_best_freezeable_intraday_trial():
    deferred = {
        "id": 1,
        "strategy_name": "deferred",
        "market_id": "ABC",
        "promotion_tier": "research_candidate",
        "robustness_score": 90,
        "warnings": ["diagnostics_deferred_fast_scan"],
        "promotion_readiness": {"status": "blocked", "blockers": ["diagnostics_deferred_fast_scan"], "validation_warnings": []},
        "parameters": {
            "market_id": "ABC",
            "timeframe": "5min",
            "family": "intraday_trend",
            "style": "intraday_only",
            "day_trading_mode": True,
            "force_flat_before_close": True,
            "no_overnight": True,
            "lookback": 12,
            "threshold_bps": 10,
            "position_size": 1,
            "stress_net_profit": 40,
            "evidence_profile": {"oos_trade_count": 12},
            "bar_pattern_analysis": {"regime_gated_backtest": {"test_profit": 30}},
        },
        "backtest": {"trade_count": 30, "net_profit": 200, "test_profit": 80, "net_cost_ratio": 0.6, "max_drawdown": 120},
    }
    freezeable = {
        **deferred,
        "id": 2,
        "strategy_name": "freezeable",
        "robustness_score": 52,
        "warnings": [],
        "promotion_readiness": {"status": "ready_for_paper", "blockers": [], "validation_warnings": []},
    }

    selected, skipped = main._select_guided_auto_freeze_trial([deferred, freezeable])

    assert selected["id"] == 2
    assert skipped["diagnostics_deferred_fast_scan"] == 1
    source_template = main._guided_auto_freeze_source_template(selected)
    assert source_template["holding_period"] == "intraday"
    assert source_template["force_flat_before_close"] is True
    assert source_template["no_overnight"] is True
    assert source_template["parameters"] == {"lookback": 12, "position_size": 1, "threshold_bps": 10}


def test_guided_auto_freeze_rejects_fragile_watchlist_trial():
    fragile = {
        "id": 3,
        "strategy_name": "fragile",
        "market_id": "ABC",
        "promotion_tier": "watchlist",
        "robustness_score": 80,
        "warnings": ["multiple_testing_haircut"],
        "promotion_readiness": {"status": "blocked", "blockers": ["one_fold_dependency"], "validation_warnings": ["needs_ig_price_validation"]},
        "parameters": {
            "market_id": "ABC",
            "timeframe": "5min",
            "family": "intraday_trend",
            "style": "intraday_only",
            "day_trading_mode": True,
            "force_flat_before_close": True,
            "no_overnight": True,
            "lookback": 12,
            "threshold_bps": 10,
            "position_size": 1,
            "stress_net_profit": 30,
            "evidence_profile": {"oos_trade_count": 12},
            "bar_pattern_analysis": {"regime_gated_backtest": {"test_profit": 20}},
        },
        "backtest": {"trade_count": 30, "net_profit": 200, "test_profit": 80, "net_cost_ratio": 0.6, "max_drawdown": 120},
    }

    selected, skipped = main._select_guided_auto_freeze_trial([fragile])

    assert selected is None
    assert skipped["multiple_testing_haircut"] == 1
