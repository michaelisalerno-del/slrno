from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

pytest.importorskip("fastapi")

import app.main as main
from app.backtesting import BacktestResult
from app.market_registry import MarketMapping, MarketRegistry
from app.providers.base import OHLCBar
from app.research_lab import CandidateEvaluation
from app.research_metrics import ClassificationMetrics
from app.research_store import ResearchStore
from app.research_strategies import ProbabilityCandidate


def test_multi_market_run_finishes_with_warnings_when_one_market_fails(tmp_path, monkeypatch):
    store = ResearchStore(tmp_path / "research.sqlite3")
    registry = MarketRegistry(tmp_path / "markets.sqlite3")
    registry.upsert(_market("OK", "OK.INDX"))
    registry.upsert(_market("DE40", "GDAXI.INDX"))
    monkeypatch.setattr(main, "research_store", store)
    monkeypatch.setattr(main, "markets", registry)
    monkeypatch.setattr(main, "EODHDProvider", lambda _token: FakeProvider(fail_symbols={"GDAXI.INDX"}))
    monkeypatch.setattr(main, "run_adaptive_search", lambda *args, **kwargs: SimpleNamespace(evaluations=[_evaluation("accepted")]))

    payload = main.ResearchRunPayload(start="2025-01-01", end="2025-01-02", market_ids=["OK", "DE40"], search_budget=2)
    run_id = store.create_run("MULTI", main._research_run_config(payload, [registry.get("OK"), registry.get("DE40")]), status="running")

    asyncio.run(main._execute_research_run(run_id, payload, "token"))

    run = store.get_run(run_id)
    assert run is not None
    assert run["status"] == "finished_with_warnings"
    assert run["trial_count"] == 1
    assert "DE40 skipped" in str(run["error"])
    statuses = run["config"]["market_statuses"]
    assert [item["status"] for item in statuses] == ["completed", "failed"]
    assert run["config"]["market_failures"][0]["eodhd_symbol"] == "GDAXI.INDX"


def test_research_run_passes_cost_stress_multiplier_to_adaptive_search(tmp_path, monkeypatch):
    store = ResearchStore(tmp_path / "research.sqlite3")
    registry = MarketRegistry(tmp_path / "markets.sqlite3")
    registry.upsert(_market("OK", "OK.INDX"))
    captured: dict[str, object] = {}
    monkeypatch.setattr(main, "research_store", store)
    monkeypatch.setattr(main, "markets", registry)
    monkeypatch.setattr(main, "EODHDProvider", lambda _token: FakeProvider(fail_symbols=set()))

    def fake_search(*args, **kwargs):
        captured["config"] = args[4]
        return SimpleNamespace(evaluations=[_evaluation("accepted")])

    monkeypatch.setattr(main, "run_adaptive_search", fake_search)
    payload = main.ResearchRunPayload(start="2025-01-01", end="2025-01-02", market_ids=["OK"], search_budget=2, cost_stress_multiplier=3.0)
    run_id = store.create_run("OK", main._research_run_config(payload, [registry.get("OK")]), status="running")

    asyncio.run(main._execute_research_run(run_id, payload, "token"))

    assert captured["config"].cost_stress_multiplier == 3.0
    assert store.get_run(run_id)["config"]["cost_stress_multiplier"] == 3.0


def test_market_default_interval_uses_each_market_timeframe(tmp_path, monkeypatch):
    store = ResearchStore(tmp_path / "research.sqlite3")
    registry = MarketRegistry(tmp_path / "markets.sqlite3")
    registry.upsert(_market("OK", "OK.INDX"))
    registry.upsert(MarketMapping("EURUSD", "EUR/USD", "forex", "EURUSD.FOREX", "", True, "", "EUR/USD", "EURUSD", "5min", 1.2, 0.8, 2))
    registry.upsert(MarketMapping("XAUUSD", "Spot Gold", "commodity", "XAUUSD.FOREX", "", True, "", "Spot Gold", "Gold", "5min", 3.0, 1.5, 2))
    registry.upsert(MarketMapping("COPPER", "Copper", "commodity", "COMMODITY:COPPER", "", True, "", "Copper", "Copper", "1day", 4.0, 2.0, 2))
    calls: list[tuple[str, str, str]] = []
    monkeypatch.setattr(main, "research_store", store)
    monkeypatch.setattr(main, "markets", registry)

    class CaptureProvider:
        cache = FakeCache()

        async def historical_bars(self, symbol: str, interval: str, start_value: str, _end: str) -> list[OHLCBar]:
            calls.append((symbol, interval, start_value))
            start = datetime(2025, 1, 1, tzinfo=UTC)
            return [
                OHLCBar(symbol=symbol, timestamp=start + timedelta(days=index), open=100 + index, high=101 + index, low=99 + index, close=100 + index, volume=10)
                for index in range(3)
            ]

    monkeypatch.setattr(main, "EODHDProvider", lambda _token: CaptureProvider())
    monkeypatch.setattr(main, "run_adaptive_search", lambda *args, **kwargs: SimpleNamespace(evaluations=[_evaluation("accepted")], regime_scan={}))
    payload = main.ResearchRunPayload(start="2025-01-01", end="2026-04-01", market_ids=["OK", "EURUSD", "XAUUSD", "COPPER"], interval="market_default", search_budget=2)
    run_id = store.create_run("MULTI", main._research_run_config(payload, [registry.get("OK"), registry.get("EURUSD"), registry.get("XAUUSD"), registry.get("COPPER")]), status="running")

    asyncio.run(main._execute_research_run(run_id, payload, "token"))

    assert calls == [("OK.INDX", "5min", "2025-01-01"), ("EURUSD.FOREX", "1hour", "2025-01-01"), ("XAUUSD.FOREX", "1day", "2025-01-01"), ("COMMODITY:COPPER", "1month", "2020-04-01")]
    statuses = store.get_run(run_id)["config"]["market_statuses"]
    assert [item["interval"] for item in statuses] == ["5min", "1hour", "1day", "1month"]
    assert statuses[3]["history_expanded"] is True


def test_multi_market_default_budget_is_auto_capped_but_manual_budget_is_respected():
    default_payload = main.ResearchRunPayload(start="2025-01-01", end="2026-04-01", search_preset="balanced")
    manual_payload = main.ResearchRunPayload(start="2025-01-01", end="2026-04-01", search_preset="balanced", search_budget=54)
    deep_payload = main.ResearchRunPayload(start="2025-01-01", end="2026-04-01", search_preset="deep")

    assert main._effective_search_budget(default_payload, 1) is None
    assert main._effective_search_budget(default_payload, 30) == 9
    assert main._effective_search_budget(manual_payload, 30) == 54
    assert main._effective_search_budget(deep_payload, 38) == 12


def test_frozen_validation_budget_is_one_when_template_parameters_are_locked():
    payload = main.ResearchRunPayload(
        start="2025-01-01",
        end="2026-04-01",
        search_budget=1,
        repair_mode="frozen_validation",
        source_template={"parameters": {"lookback": 20, "threshold_bps": 12}},
    )

    assert main._effective_search_budget(payload, 1) == 1
    assert main._research_run_config(payload, [main.markets.get("NAS100")])["effective_search_budget"] == 1


def test_empty_intraday_index_falls_back_to_daily_bars(tmp_path, monkeypatch):
    store = ResearchStore(tmp_path / "research.sqlite3")
    registry = MarketRegistry(tmp_path / "markets.sqlite3")
    registry.upsert(MarketMapping("FTSE100", "FTSE 100", "index", "FTSE.INDX", "", True, "", "FTSE 100", "FTSE", "5min", 2.0, 1.0, 750))
    calls: list[tuple[str, str, str]] = []
    captured: dict[str, object] = {}
    monkeypatch.setattr(main, "research_store", store)
    monkeypatch.setattr(main, "markets", registry)

    class EmptyIntradayProvider:
        cache = FakeCache()

        async def historical_bars(self, symbol: str, interval: str, start_value: str, _end: str) -> list[OHLCBar]:
            calls.append((symbol, interval, start_value))
            if interval == "5min":
                return []
            start = datetime(2025, 1, 1, tzinfo=UTC)
            return [
                OHLCBar(symbol=symbol, timestamp=start + timedelta(days=index), open=100 + index, high=101 + index, low=99 + index, close=100 + index, volume=10)
                for index in range(260)
            ]

    def fake_search(*args, **kwargs):
        captured["interval"] = args[2]
        captured["bar_count"] = len(args[0])
        return SimpleNamespace(evaluations=[_evaluation("accepted")], regime_scan={})

    monkeypatch.setattr(main, "EODHDProvider", lambda _token: EmptyIntradayProvider())
    monkeypatch.setattr(main, "run_adaptive_search", fake_search)
    payload = main.ResearchRunPayload(start="2025-01-01", end="2026-04-01", market_ids=["FTSE100"], interval="market_default", search_budget=2)
    run_id = store.create_run("FTSE100", main._research_run_config(payload, [registry.get("FTSE100")]), status="running")

    asyncio.run(main._execute_research_run(run_id, payload, "token"))

    assert calls == [("FTSE.INDX", "5min", "2025-01-01"), ("FTSE.INDX", "1day", "2025-01-01")]
    assert captured == {"interval": "1day", "bar_count": 260}
    run = store.get_run(run_id)
    assert run["status"] == "finished"
    status = run["config"]["market_statuses"][0]
    assert status["requested_interval"] == "5min"
    assert status["interval"] == "1day"
    assert status["data_source_status"] == "eodhd_daily_fallback"
    assert status["intraday_bar_count"] == 0
    assert status["fallback_bar_count"] == 260


def test_empty_intraday_fallback_failure_reports_daily_requirement(tmp_path, monkeypatch):
    store = ResearchStore(tmp_path / "research.sqlite3")
    registry = MarketRegistry(tmp_path / "markets.sqlite3")
    registry.upsert(MarketMapping("RUSSELL2000", "US Russell 2000", "index", "RUT.INDX", "", True, "", "Russell 2000", "RUT", "5min", 2.5, 1.2, 750))
    monkeypatch.setattr(main, "research_store", store)
    monkeypatch.setattr(main, "markets", registry)

    class ShortDailyProvider:
        cache = FakeCache()

        async def historical_bars(self, symbol: str, interval: str, _start: str, _end: str) -> list[OHLCBar]:
            if interval == "5min":
                return []
            start = datetime(2025, 1, 1, tzinfo=UTC)
            return [
                OHLCBar(symbol=symbol, timestamp=start + timedelta(days=index), open=100 + index, high=101 + index, low=99 + index, close=100 + index, volume=10)
                for index in range(27)
            ]

    monkeypatch.setattr(main, "EODHDProvider", lambda _token: ShortDailyProvider())
    payload = main.ResearchRunPayload(start="2026-01-01", end="2026-02-01", market_ids=["RUSSELL2000"], interval="market_default", search_budget=2)
    run_id = store.create_run("RUSSELL2000", main._research_run_config(payload, [registry.get("RUSSELL2000")]), status="running")

    asyncio.run(main._execute_research_run(run_id, payload, "token"))

    run = store.get_run(run_id)
    assert run["status"] == "error"
    assert "need at least 250 1day bars; received 27" in str(run["error"])
    status = run["config"]["market_statuses"][0]
    assert status["requested_interval"] == "5min"
    assert status["interval"] == "1day"
    assert status["data_source_status"] == "eodhd_daily_fallback"


def test_daily_market_default_uses_daily_minimum_bar_floor():
    market = MarketMapping("WTI", "US Crude", "commodity", "COMMODITY:WTI", "", True, "", "US Crude", "WTI", "1day", 3.5, 2.0, 750)

    assert main._minimum_bars_for_interval(market, "1day") == 250
    assert main._minimum_bars_for_interval(market, "5min") == 750


def test_monthly_commodity_forces_monthly_interval_and_expands_history():
    market = MarketMapping("COPPER", "Copper", "commodity", "COMMODITY:COPPER", "", True, "", "Copper", "Copper", "1day", 4.0, 2.0, 750)
    payload = main.ResearchRunPayload(start="2025-01-01", end="2026-04-01", market_ids=["COPPER"], interval="1day", search_budget=2)

    interval = main._run_interval_for_market(payload, market)

    assert interval == "1month"
    assert main._run_start_for_market(payload, market, interval) == "2020-04-01"


def test_research_run_excludes_months_before_snapshot_and_search(tmp_path, monkeypatch):
    store = ResearchStore(tmp_path / "research.sqlite3")
    registry = MarketRegistry(tmp_path / "markets.sqlite3")
    registry.upsert(_market("OK", "OK.INDX"))
    captured: dict[str, object] = {}
    monkeypatch.setattr(main, "research_store", store)
    monkeypatch.setattr(main, "markets", registry)
    monkeypatch.setattr(main, "EODHDProvider", lambda _token: MonthProvider())

    def fake_search(*args, **kwargs):
        captured["bars"] = args[0]
        return SimpleNamespace(evaluations=[_evaluation("accepted")], regime_scan={})

    monkeypatch.setattr(main, "run_adaptive_search", fake_search)
    payload = main.ResearchRunPayload(start="2025-01-01", end="2025-02-28", market_ids=["OK"], search_budget=2, excluded_months=["2025-01", "bad"])
    run_id = store.create_run("OK", main._research_run_config(payload, [registry.get("OK")]), status="running")

    asyncio.run(main._execute_research_run(run_id, payload, "token"))

    bars = captured["bars"]
    assert [bar.timestamp.strftime("%Y-%m") for bar in bars] == ["2025-02", "2025-02"]
    snapshots = store.list_bar_snapshots(run_id, include_payload=True)
    assert snapshots[0]["bar_count"] == 2
    assert all(not item["timestamp"].startswith("2025-01") for item in snapshots[0]["bars"])
    run = store.get_run(run_id)
    assert run["config"]["excluded_months"] == ["2025-01"]
    assert run["config"]["market_statuses"][0]["excluded_bar_count"] == 2


def test_multi_market_run_errors_when_all_markets_fail(tmp_path, monkeypatch):
    store = ResearchStore(tmp_path / "research.sqlite3")
    registry = MarketRegistry(tmp_path / "markets.sqlite3")
    registry.upsert(_market("DE40", "GDAXI.INDX"))
    monkeypatch.setattr(main, "research_store", store)
    monkeypatch.setattr(main, "markets", registry)
    monkeypatch.setattr(main, "EODHDProvider", lambda _token: FakeProvider(fail_symbols={"GDAXI.INDX"}))

    payload = main.ResearchRunPayload(start="2025-01-01", end="2025-01-02", market_ids=["DE40"], search_budget=2)
    run_id = store.create_run("DE40", main._research_run_config(payload, [registry.get("DE40")]), status="running")

    asyncio.run(main._execute_research_run(run_id, payload, "token"))

    run = store.get_run(run_id)
    assert run is not None
    assert run["status"] == "error"
    assert run["trial_count"] == 0
    assert "GDAXI.INDX EODHD data load failed" in str(run["error"])
    assert run["config"]["market_statuses"][0]["status"] == "failed"


def test_delete_research_run_endpoint_removes_finished_run_and_blocks_running(tmp_path, monkeypatch):
    store = ResearchStore(tmp_path / "research.sqlite3")
    monkeypatch.setattr(main, "research_store", store)
    finished_id = store.create_run("NAS100", {"interval": "1h"}, status="finished")
    running_id = store.create_run("US500", {"interval": "1h"}, status="running")
    store.save_trial(finished_id, _evaluation("accepted"))
    store.save_candidate(finished_id, "NAS100", _evaluation("accepted"))

    result = main.delete_research_run(finished_id)

    assert result == {"status": "deleted", "run_id": finished_id, "deleted_trials": 1, "deleted_candidates": 1}
    assert store.get_run(finished_id) is None
    with pytest.raises(main.HTTPException) as exc_info:
        main.delete_research_run(running_id)
    assert exc_info.value.status_code == 409


def test_delete_research_run_endpoint_blocks_paper_candidate_evidence(tmp_path, monkeypatch):
    store = ResearchStore(tmp_path / "research.sqlite3")
    monkeypatch.setattr(main, "research_store", store)
    run_id = store.create_run("XAUUSD", {"interval": "1day"}, status="finished")
    store.save_candidate(run_id, "XAUUSD", _paper_evaluation("paper"))

    with pytest.raises(main.HTTPException) as exc_info:
        main.delete_research_run(run_id)

    assert exc_info.value.status_code == 409
    assert "Archive" in str(exc_info.value.detail)
    assert store.get_run(run_id) is not None


class FakeCache:
    def prune_expired(self) -> int:
        return 0


class FakeProvider:
    def __init__(self, fail_symbols: set[str]) -> None:
        self.fail_symbols = fail_symbols
        self.cache = FakeCache()

    async def historical_bars(self, symbol: str, _interval: str, _start: str, _end: str) -> list[OHLCBar]:
        if symbol in self.fail_symbols:
            raise RuntimeError("EODHD historical bars returned HTTP 502 for fixture")
        start = datetime(2025, 1, 1, tzinfo=UTC)
        return [
            OHLCBar(symbol=symbol, timestamp=start + timedelta(minutes=index * 5), open=100 + index, high=101 + index, low=99 + index, close=100 + index, volume=10)
            for index in range(3)
        ]


class MonthProvider:
    cache = FakeCache()

    async def historical_bars(self, symbol: str, _interval: str, _start: str, _end: str) -> list[OHLCBar]:
        return [
            OHLCBar(symbol=symbol, timestamp=datetime(2025, 1, 2, tzinfo=UTC), open=100, high=101, low=99, close=100, volume=10),
            OHLCBar(symbol=symbol, timestamp=datetime(2025, 1, 3, tzinfo=UTC), open=101, high=102, low=100, close=101, volume=10),
            OHLCBar(symbol=symbol, timestamp=datetime(2025, 2, 3, tzinfo=UTC), open=102, high=103, low=101, close=102, volume=10),
            OHLCBar(symbol=symbol, timestamp=datetime(2025, 2, 4, tzinfo=UTC), open=103, high=104, low=102, close=103, volume=10),
        ]


def _market(market_id: str, symbol: str) -> MarketMapping:
    return MarketMapping(market_id, market_id, "index", symbol, "", True, "", market_id, market_id, "5min", 2.0, 1.0, 2)


def _evaluation(name: str) -> CandidateEvaluation:
    return CandidateEvaluation(
        candidate=ProbabilityCandidate(name, ("fixture",), {"family": "fixture"}, [0.1, 0.9]),
        metrics=ClassificationMetrics(1.0, 1.0, 0.01, 0.1, 1.0, 0.5, 2),
        backtest=BacktestResult(100, 1.0, 10, 0.6, 20, 0.3, 2, 60, 40),
        fold_results=(BacktestResult(10, 0.8, 1, 0.6, 5, 0.2, 1, 6, 4),),
        robustness_score=75.0,
        passed=True,
        warnings=(),
    )


def _paper_evaluation(name: str) -> CandidateEvaluation:
    return CandidateEvaluation(
        candidate=ProbabilityCandidate(name, ("fixture",), {"family": "fixture"}, [0.1, 0.9]),
        metrics=ClassificationMetrics(1.0, 1.0, 0.01, 0.1, 1.0, 0.5, 2),
        backtest=BacktestResult(
            100,
            1.0,
            10,
            0.6,
            20,
            0.3,
            2,
            60,
            40,
            gross_profit=150,
            total_cost=50,
            daily_pnl_sharpe=1.0,
            sharpe_observations=140,
            estimated_spread_bps=2.0,
            estimated_slippage_bps=1.0,
            cost_confidence="ig_live_epic_cost_profile",
        ),
        fold_results=(BacktestResult(10, 0.8, 1, 0.6, 5, 0.2, 1, 6, 4),),
        robustness_score=75.0,
        passed=True,
        warnings=(),
    )
