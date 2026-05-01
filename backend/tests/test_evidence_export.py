from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from zipfile import ZipFile

from app.backtesting import BacktestResult
from app.evidence_export import build_research_export_zip
from app.ig_costs import IGCostProfile
from app.market_data_cache import MarketDataCache
from app.providers.base import OHLCBar
from app.research_lab import CandidateEvaluation
from app.research_metrics import ClassificationMetrics
from app.research_store import ResearchStore
from app.research_strategies import ProbabilityCandidate


def test_research_export_bundle_contains_capital_scenarios_bars_and_no_secrets(tmp_path):
    store = ResearchStore(tmp_path / "research.sqlite3")
    run_id = store.create_run(
        "NAS100",
        {
            "start": "2025-01-01",
            "end": "2025-01-03",
            "interval": "5min",
            "market_ids": ["NAS100"],
            "api_token": "do-not-export",
        },
        status="finished",
    )
    store.save_cost_profile(
        IGCostProfile(
            "NAS100",
            bid=20_000,
            offer=20_002,
            min_deal_size=0.5,
            margin_percent=5.0,
            confidence="ig_live_epic_cost_profile",
        )
    )
    evaluation = _evaluation("accepted")
    store.save_trial(run_id, evaluation)
    store.save_candidate(run_id, "NAS100", evaluation)
    snapshot = store.save_bar_snapshot(run_id, "NAS100", "5min", "eodhd_primary_symbol", "2025-01-01", "2025-01-03", _bars())

    payload = build_research_export_zip(store, run_id, include_bars=True)

    archive_path = tmp_path / "bundle.zip"
    archive_path.write_bytes(payload)
    with ZipFile(archive_path) as archive:
        names = set(archive.namelist())
        assert "manifest.json" in names
        assert "trials.json" in names
        assert "candidates.csv" in names
        assert "capital_scenarios.csv" in names
        assert "bar_analysis.json" in names
        assert "regime_pnl.csv" in names
        assert "regime_gated_backtests.csv" in names
        assert "monthly_pnl.csv" in names
        assert "session_pnl.csv" in names
        assert "pattern_warnings.csv" in names
        assert "bars/NAS100_5min.csv" in names
        manifest = json.loads(archive.read("manifest.json"))
        run = json.loads(archive.read("run.json"))
        bar_analysis = json.loads(archive.read("bar_analysis.json"))
        capital_csv = archive.read("capital_scenarios.csv").decode()
        bars_csv = archive.read("bars/NAS100_5min.csv").decode()

    assert manifest["data_completeness"]["exact_run_bars_available"] is True
    assert manifest["bar_snapshots"][0]["sha256"] == snapshot["sha256"]
    assert snapshot["sha256"]
    assert run["config"]["api_token"] == "***"
    assert bar_analysis["items"][0]["analysis"]["warnings"] == ["profit_concentrated_single_month"]
    assert "2000.0" in capital_csv
    assert "10000.0" in capital_csv
    assert "timestamp" in bars_csv


def test_export_marks_old_runs_without_exact_bars(tmp_path):
    store = ResearchStore(tmp_path / "research.sqlite3")
    run_id = store.create_run("NAS100", {"interval": "5min"}, status="finished")

    payload = build_research_export_zip(store, run_id, include_bars=True)

    archive_path = tmp_path / "old.zip"
    archive_path.write_bytes(payload)
    with ZipFile(archive_path) as archive:
        manifest = json.loads(archive.read("manifest.json"))
        assert "bars/README.md" in archive.namelist()
    assert manifest["data_completeness"]["exact_run_bars_available"] is False
    assert manifest["data_completeness"]["cached_bars_exported"] is False


def test_export_includes_best_available_cached_bars_for_old_runs(tmp_path):
    store = ResearchStore(tmp_path / "research.sqlite3")
    cache = MarketDataCache(tmp_path / "cache.sqlite3")
    run_id = store.create_run(
        "NAS100",
        {
            "start": "2025-01-01",
            "end": "2025-01-03",
            "interval": "5min",
            "market_statuses": [
                {
                    "market_id": "NAS100",
                    "eodhd_symbol": "NDX.INDX",
                    "interval": "5min",
                    "bar_count": 3,
                }
            ],
        },
        status="finished",
    )
    cache.set_json(
        "historical_bars",
        "https://eodhd.com/api/intraday/NDX.INDX",
        {"interval": "5m", "from": 1735689600, "to": 1735948799},
        [
            {"datetime": "2025-01-02 14:30:00", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 10},
            {"datetime": "2025-01-02 14:35:00", "open": 100, "high": 102, "low": 99, "close": 101, "volume": 11},
            {"datetime": "2025-01-02 14:40:00", "open": 101, "high": 103, "low": 100, "close": 102, "volume": 12},
        ],
        60,
    )

    payload = build_research_export_zip(store, run_id, include_bars=True, cache=cache)

    archive_path = tmp_path / "cached.zip"
    archive_path.write_bytes(payload)
    with ZipFile(archive_path) as archive:
        manifest = json.loads(archive.read("manifest.json"))
        bars_csv = archive.read("bars/NAS100_5min_cached_not_exact.csv").decode()
        bars_readme = archive.read("bars/README.md").decode()

    assert manifest["data_completeness"]["exact_run_bars_available"] is False
    assert manifest["data_completeness"]["cached_bars_exported"] is True
    assert manifest["best_available_bars"][0]["not_guaranteed_exact"] is True
    assert "2025-01-02T14:30:00" in bars_csv
    assert "not guaranteed" in bars_readme


def test_export_can_use_single_legacy_cache_match_when_request_metadata_is_missing(tmp_path):
    store = ResearchStore(tmp_path / "research.sqlite3")
    cache = MarketDataCache(tmp_path / "cache.sqlite3")
    run_id = store.create_run(
        "NAS100",
        {
            "start": "2025-01-01",
            "end": "2025-01-03",
            "interval": "5min",
            "market_statuses": [
                {
                    "market_id": "NAS100",
                    "eodhd_symbol": "NDX.INDX",
                    "interval": "5min",
                    "bar_count": 2,
                }
            ],
        },
        status="finished",
    )
    legacy_payload = json.dumps(
        [
            {"datetime": "2025-01-02 14:30:00", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 10},
            {"datetime": "2025-01-02 14:35:00", "open": 100, "high": 102, "low": 99, "close": 101, "volume": 11},
        ]
    )
    with sqlite3.connect(cache.db_path) as conn:
        conn.execute(
            """
            INSERT INTO market_data_cache(
              cache_key, namespace, created_at, expires_at, payload_json,
              base_url, params_json, metadata_json, last_accessed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("legacy", "historical_bars", "2026-01-01T00:00:00+00:00", "2026-01-02T00:00:00+00:00", legacy_payload, "", "{}", "{}", ""),
        )

    payload = build_research_export_zip(store, run_id, include_bars=True, cache=cache)

    archive_path = tmp_path / "legacy.zip"
    archive_path.write_bytes(payload)
    with ZipFile(archive_path) as archive:
        manifest = json.loads(archive.read("manifest.json"))

    assert manifest["data_completeness"]["cached_bars_exported"] is True
    assert manifest["best_available_bars"][0]["cache_match"] == "legacy_bar_count_and_date_range"


def _evaluation(name: str) -> CandidateEvaluation:
    return CandidateEvaluation(
        candidate=ProbabilityCandidate(
            name,
            ("fixture",),
            {
                "market_id": "NAS100",
                "family": "fixture",
                "position_size": 1.0,
                "stop_loss_bps": 20.0,
                "bar_pattern_analysis": {
                    "schema": "bar_pattern_analysis_v1",
                    "warnings": ["profit_concentrated_single_month"],
                    "regime_verdict": "regime_specific",
                    "allowed_regimes": ["trend_up"],
                    "blocked_regimes": [],
                    "regime_gated_backtest": {"net_profit": 100, "test_profit": 40, "daily_pnl_sharpe": 1.0},
                    "monthly_summary": [{"key": "2025-01", "net_profit": 100, "active_bars": 3}],
                    "session_summary": [{"key": "us_open", "net_profit": 100, "active_bars": 3}],
                    "regime_summary": [{"key": "trend_up", "net_profit": 100, "active_bars": 3}],
                },
            },
            [0.1, 0.9],
        ),
        metrics=ClassificationMetrics(1.0, 1.0, 0.01, 0.1, 1.0, 0.5, 2),
        backtest=BacktestResult(
            net_profit=100,
            sharpe=1.0,
            max_drawdown=10,
            win_rate=0.6,
            trade_count=20,
            exposure=0.3,
            turnover=2,
            train_profit=60,
            test_profit=40,
            gross_profit=150,
            total_cost=50,
            daily_pnl_sharpe=1.0,
            sharpe_observations=140,
            estimated_spread_bps=2,
            estimated_slippage_bps=1,
            cost_confidence="ig_live_epic_cost_profile",
        ),
        fold_results=(BacktestResult(10, 0.8, 1, 0.6, 5, 0.2, 1, 6, 4),),
        robustness_score=75.0,
        passed=True,
        warnings=(),
        promotion_tier="paper_candidate",
    )


def _bars() -> list[OHLCBar]:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    return [
        OHLCBar("NAS100", start + timedelta(minutes=5 * index), 100 + index, 101 + index, 99 + index, 100 + index, 10)
        for index in range(3)
    ]
