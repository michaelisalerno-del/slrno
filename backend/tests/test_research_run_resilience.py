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
