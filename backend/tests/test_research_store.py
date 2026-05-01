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
    assert trials[1]["promotion_readiness"]["status"] == "ready_for_paper"
    assert candidate["strategy_name"] == "accepted"
    assert candidate["promotion_tier"] == "paper_candidate"
    assert candidate["audit"]["promotion_readiness"]["status"] == "ready_for_paper"
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


def test_research_store_normalizes_stale_weak_sharpe_warning(tmp_path):
    store = ResearchStore(tmp_path / "research.sqlite3")
    run_id = store.create_run("NAS100", {"interval": "5min"}, status="finished")
    evaluation = CandidateEvaluation(
        candidate=ProbabilityCandidate("daily_sharpe_lead", ("fixture",), {}, [0.1, 0.9]),
        metrics=ClassificationMetrics(1.0, 1.0, 0.01, 0.1, 1.0, 0.5, 2),
        backtest=BacktestResult(
            net_profit=100,
            sharpe=0.1,
            max_drawdown=10,
            win_rate=0.6,
            trade_count=20,
            exposure=0.3,
            turnover=20,
            train_profit=60,
            test_profit=40,
            daily_pnl_sharpe=1.2,
            sharpe_observations=20,
        ),
        fold_results=(BacktestResult(10, 0.8, 1, 0.6, 5, 0.2, 1, 6, 4),),
        robustness_score=40,
        passed=False,
        warnings=("weak_sharpe", "needs_ig_price_validation"),
        promotion_tier="research_candidate",
    )

    store.save_trial(run_id, evaluation)
    store.save_candidate(run_id, "NAS100", evaluation)

    [trial] = store.list_trials(run_id)
    [candidate] = store.list_candidates(run_id)
    assert "weak_sharpe" not in trial["warnings"]
    assert "weak_sharpe" not in candidate["audit"]["warnings"]
    assert "needs_ig_price_validation" in candidate["audit"]["warnings"]


def test_research_store_repairs_legacy_candidate_cost_ratios_and_flags_missing_sharpe_sample(tmp_path):
    store = ResearchStore(tmp_path / "research.sqlite3")
    run_id = store.create_run("BRENT", {"interval": "1day"}, status="finished")
    evaluation = CandidateEvaluation(
        candidate=ProbabilityCandidate(
            "legacy_brent",
            ("fixture",),
            {"market_id": "BRENT", "estimated_spread_bps": 3.5, "estimated_slippage_bps": 2.0},
            [0.1, 0.9],
        ),
        metrics=ClassificationMetrics(1.0, 1.0, 0.01, 0.1, 1.0, 0.5, 2),
        backtest=BacktestResult(
            net_profit=1_814,
            sharpe=0.87,
            max_drawdown=100,
            win_rate=0.51,
            trade_count=6_939,
            exposure=0.5,
            turnover=6_939,
            train_profit=900,
            test_profit=914,
            gross_profit=2_462,
            total_cost=648,
        ),
        fold_results=(),
        robustness_score=65,
        passed=False,
        warnings=("needs_ig_price_validation",),
        promotion_tier="research_candidate",
    )

    store.save_candidate(run_id, "BRENT", evaluation)

    [candidate] = store.list_candidates(run_id)
    backtest = candidate["audit"]["backtest"]
    assert backtest["estimated_spread_bps"] == 3.5
    assert backtest["estimated_slippage_bps"] == 2.0
    assert round(backtest["net_cost_ratio"], 2) == 2.8
    assert round(backtest["expectancy_per_trade"], 2) == 0.26
    assert "legacy_sharpe_diagnostics" in candidate["audit"]["warnings"]
    assert candidate["audit"]["promotion_readiness"]["status"] == "blocked"


def test_research_store_demotes_stale_paper_candidate_on_read(tmp_path):
    store = ResearchStore(tmp_path / "research.sqlite3")
    run_id = store.create_run("BRENT", {"interval": "1day"}, status="finished")
    stale = CandidateEvaluation(
        candidate=ProbabilityCandidate("stale_paper", ("fixture",), {}, [0.1, 0.9]),
        metrics=ClassificationMetrics(1.0, 1.0, 0.01, 0.1, 1.0, 0.5, 2),
        backtest=BacktestResult(
            net_profit=1_000,
            sharpe=1.2,
            max_drawdown=100,
            win_rate=0.55,
            trade_count=20,
            exposure=0.4,
            turnover=20,
            train_profit=500,
            test_profit=500,
            gross_profit=1_200,
            total_cost=200,
            estimated_spread_bps=2,
            estimated_slippage_bps=1,
            cost_confidence="ig_live_epic_cost_profile",
        ),
        fold_results=(),
        robustness_score=60,
        passed=True,
        warnings=(),
        promotion_tier="paper_candidate",
    )

    store.save_candidate(run_id, "BRENT", stale)

    [candidate] = store.list_candidates(run_id)
    assert candidate["promotion_tier"] == "research_candidate"
    assert "legacy_sharpe_diagnostics" in candidate["audit"]["promotion_readiness"]["blockers"]


def _evaluation(name: str, passed: bool, promotion_tier: str = "reject", robustness_score: float = 75.0) -> CandidateEvaluation:
    return CandidateEvaluation(
        candidate=ProbabilityCandidate(name, ("fixture",), {}, [0.1, 0.9]),
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
        robustness_score=robustness_score,
        passed=passed,
        warnings=() if passed else ("weak_roc_auc",),
        promotion_tier=promotion_tier,
    )
