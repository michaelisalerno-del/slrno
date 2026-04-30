from __future__ import annotations

from app.backtesting import BacktestResult
from app.research_lab import CandidateEvaluation
from app.research_metrics import ClassificationMetrics
from app.research_store import ResearchStore
from app.research_strategies import ProbabilityCandidate


def test_research_store_records_rejected_trials_and_promoted_candidates(tmp_path):
    store = ResearchStore(tmp_path / "research.sqlite3")
    run_id = store.create_run("NAS100", {"interval": "1h"}, status="running")
    rejected = _evaluation("rejected", passed=False)
    accepted = _evaluation("accepted", passed=True)

    store.save_trial(run_id, rejected)
    store.save_trial(run_id, accepted)
    store.save_candidate(run_id, "NAS100", rejected)
    store.save_candidate(run_id, "NAS100", accepted)
    store.update_run_status(run_id, "finished")

    [run] = store.list_runs()
    trials = store.list_trials(run_id)
    candidates = store.list_candidates()
    candidate = candidates[0]
    run_detail = store.get_run(run_id)
    assert run["trial_count"] == 2
    assert run["passed_count"] == 1
    assert run["status"] == "finished"
    assert run["error"] == ""
    assert run_detail is not None
    assert run_detail["error"] == ""
    assert [trial["strategy_name"] for trial in trials] == ["rejected", "accepted"]
    assert "estimated_spread_bps" in trials[0]["costs"]
    assert "net_cost_ratio" in trials[0]["costs"]
    assert "expectancy_per_trade" in trials[0]["costs"]
    assert "estimated_slippage_bps" in trials[0]["backtest"]
    assert trials[1]["promotion_tier"] == "paper_candidate"
    assert candidate["strategy_name"] == "accepted"
    assert candidate["promotion_tier"] == "paper_candidate"
    assert candidate["research_only"] is True
    assert [item["strategy_name"] for item in candidates] == ["accepted", "rejected"]
    assert candidates[1]["promotion_tier"] == "watchlist"
    assert "not_paper_ready_research_lead" in candidates[1]["audit"]["warnings"]
    assert "probabilities" not in candidate["audit"]["candidate"]
    assert candidate["audit"]["candidate"]["probability_count"] == 2

    store.update_run_status(run_id, "error", "fixture failure")
    error_run = store.get_run(run_id)
    assert error_run is not None
    assert error_run["error"] == "fixture failure"


def test_research_store_deletes_run_trials_and_candidates(tmp_path):
    store = ResearchStore(tmp_path / "research.sqlite3")
    run_id = store.create_run("NAS100", {"interval": "1h"}, status="finished")
    other_run_id = store.create_run("US500", {"interval": "1h"}, status="finished")
    accepted = _evaluation("accepted", passed=True)

    store.save_trial(run_id, accepted)
    store.save_candidate(run_id, "NAS100", accepted)
    store.save_trial(other_run_id, accepted)

    result = store.delete_run(run_id)

    assert result == {"run_id": run_id, "deleted_trials": 1, "deleted_candidates": 1}
    assert store.get_run(run_id) is None
    assert store.list_trials(run_id) == []
    assert store.list_candidates(run_id) == []
    assert store.get_run(other_run_id) is not None
    assert len(store.list_trials(other_run_id)) == 1
    assert store.delete_run(999_999) is None


def test_research_store_records_material_watchlist_leads(tmp_path):
    store = ResearchStore(tmp_path / "research.sqlite3")
    run_id = store.create_run("US500", {"interval": "5min"}, status="finished")
    watchlist = _evaluation("watchlist", passed=False, promotion_tier="watchlist", robustness_score=40)

    store.save_candidate(run_id, "US500", watchlist)

    [candidate] = store.list_candidates(run_id)
    assert candidate["strategy_name"] == "watchlist"
    assert candidate["promotion_tier"] == "watchlist"
    assert candidate["research_only"] is True


def _evaluation(name: str, passed: bool, promotion_tier: str = "reject", robustness_score: float = 75.0) -> CandidateEvaluation:
    return CandidateEvaluation(
        candidate=ProbabilityCandidate(name, ("fixture",), {}, [0.1, 0.9]),
        metrics=ClassificationMetrics(1.0, 1.0, 0.01, 0.1, 1.0, 0.5, 2),
        backtest=BacktestResult(100, 1.0, 10, 0.6, 20, 0.3, 2, 60, 40),
        fold_results=(BacktestResult(10, 0.8, 1, 0.6, 5, 0.2, 1, 6, 4),),
        robustness_score=robustness_score,
        passed=passed,
        warnings=() if passed else ("weak_roc_auc",),
        promotion_tier=promotion_tier,
    )
