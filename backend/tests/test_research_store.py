from __future__ import annotations

from dataclasses import replace

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
    assert [trial["market_id"] for trial in trials] == ["NAS100", "NAS100"]
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


def test_research_store_surfaces_weak_research_leads_as_incubator(tmp_path):
    store = ResearchStore(tmp_path / "research.sqlite3")
    run_id = store.create_run("NAS100", {"interval": "1h"}, status="running")
    evaluation = _evaluation("thin_oos", passed=False, promotion_tier="research_candidate")
    evaluation = replace(
        evaluation,
        candidate=replace(evaluation.candidate, parameters={**evaluation.candidate.parameters, "stress_net_profit": 25}),
        warnings=("low_oos_trades", "weak_oos_evidence"),
    )

    store.save_trial(run_id, evaluation)

    [trial] = store.list_trials(run_id)
    assert trial["promotion_tier"] == "incubator"
    assert "low_oos_trades" in trial["promotion_readiness"]["blockers"]


def test_research_store_lists_top_picks_by_regime(tmp_path):
    store = ResearchStore(tmp_path / "research.sqlite3")
    run_id = store.create_run("NAS100", {"interval": "1h"}, status="running")
    trend = _evaluation("trend_candidate", passed=False, promotion_tier="research_candidate", robustness_score=70)
    range_candidate = _evaluation("range_candidate", passed=False, promotion_tier="research_candidate", robustness_score=80)
    trend = replace(
        trend,
        candidate=replace(
            trend.candidate,
            parameters={
                **trend.candidate.parameters,
                "target_regime": "trend_up",
                "stress_net_profit": 20,
                "bar_pattern_analysis": {"target_regime": "trend_up", "regime_verdict": "regime_specific"},
                "evidence_profile": {"oos_net_profit": 5, "oos_trade_count": 6},
            },
        ),
    )
    range_candidate = replace(
        range_candidate,
        candidate=replace(
            range_candidate.candidate,
            parameters={
                **range_candidate.candidate.parameters,
                "target_regime": "range_chop",
                "stress_net_profit": 25,
                "bar_pattern_analysis": {"target_regime": "range_chop", "regime_verdict": "regime_specific"},
                "evidence_profile": {"oos_net_profit": 7, "oos_trade_count": 8},
            },
        ),
    )

    store.save_trial(run_id, trend)
    store.save_trial(run_id, range_candidate)

    picks = store.list_regime_picks(run_id)
    assert [item["regime"] for item in picks] == ["range_chop", "trend_up"]
    assert picks[0]["trials"][0]["strategy_name"] == "range_candidate"
    assert picks[1]["trials"][0]["target_regime"] == "trend_up"


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


def test_research_store_detects_move_forward_candidates_before_delete(tmp_path):
    store = ResearchStore(tmp_path / "research.sqlite3")
    run_id = store.create_run("XAUUSD", {"interval": "1day"}, status="finished")
    paper = _evaluation("paper", passed=True)

    store.save_candidate(run_id, "XAUUSD", paper)

    assert store.run_has_move_forward_candidate(run_id) is True


def test_research_store_archives_runs_without_deleting_evidence(tmp_path):
    store = ResearchStore(tmp_path / "research.sqlite3")
    run_id = store.create_run("NAS100", {"interval": "1h"}, status="finished")
    accepted = _evaluation("accepted", passed=True)
    store.save_trial(run_id, accepted)

    result = store.archive_run(run_id)

    assert result == {"run_id": run_id, "archived": 1}
    assert store.list_runs() == []
    [archived] = store.list_runs(include_archived=True)
    assert archived["archived"] is True
    assert len(store.list_trials(run_id)) == 1


def test_research_store_saves_and_archives_strategy_templates(tmp_path):
    store = ResearchStore(tmp_path / "research.sqlite3")
    payload = {
        "name": "find_anything_robust_trend_1",
        "market_id": "XAUUSD",
        "interval": "1day",
        "strategy_family": "intraday_trend",
        "style": "find_anything_robust",
        "target_regime": "trend_up",
        "source_run_id": 7,
        "source_trial_id": 42,
        "promotion_tier": "research_candidate",
        "readiness_status": "blocked",
        "robustness_score": 72.0,
        "testing_account_size": 3000,
        "payload": {
            "source_template": {
                "name": "find_anything_robust_trend_1",
                "market_id": "XAUUSD",
                "interval": "1day",
                "target_regime": "trend_up",
                "parameters": {"lookback": 20, "threshold_bps": 15},
            },
            "parameters": {"market_id": "XAUUSD", "family": "intraday_trend", "timeframe": "1day"},
            "backtest": {"net_profit": 100, "test_profit": 40, "trade_count": 12},
            "pattern": {"warnings": ["target_regime_low_oos_trades"], "regime_verdict": "regime_specific"},
            "readiness": {"status": "blocked", "blockers": ["target_regime_low_oos_trades"]},
            "warnings": ["needs_ig_price_validation"],
            "capital_scenarios": [{"account_size": 3000, "feasible": False}],
        },
    }

    saved = store.save_template(payload)
    updated = store.save_template({**payload, "robustness_score": 80.0})

    assert updated["id"] == saved["id"]
    assert updated["robustness_score"] == 80.0
    assert updated["source_template"]["parameters"]["lookback"] == 20
    assert updated["warnings"] == ["needs_ig_price_validation", "target_regime_low_oos_trades"]
    assert updated["capital_scenarios"][0]["account_size"] == 3000
    assert store.list_templates()[0]["name"] == "find_anything_robust_trend_1"

    archived = store.update_template_status(saved["id"], "archived")

    assert archived is not None
    assert archived["status"] == "archived"
    assert store.list_templates() == []
    assert store.list_templates(include_inactive=True)[0]["status"] == "archived"


def test_research_store_limits_trial_and_candidate_reads(tmp_path):
    store = ResearchStore(tmp_path / "research.sqlite3")
    run_id = store.create_run("NAS100", {"interval": "1h"}, status="finished")

    for name, score in (("low", 10), ("high", 90), ("mid", 40)):
        evaluation = _evaluation(name, passed=True, robustness_score=score)
        store.save_trial(run_id, evaluation)
        store.save_candidate(run_id, "NAS100", evaluation)

    trials = store.list_trials(run_id, limit=2)
    candidates = store.list_candidates(run_id, limit=2)

    assert [trial["strategy_name"] for trial in trials] == ["high", "mid"]
    assert [candidate["strategy_name"] for candidate in candidates] == ["high", "mid"]
    assert store.count_candidates(run_id) == 3


def test_research_store_lists_paper_candidates_before_higher_scoring_research_leads(tmp_path):
    store = ResearchStore(tmp_path / "research.sqlite3")
    run_id = store.create_run("XAUUSD", {"interval": "1day"}, status="finished")
    watchlist = _evaluation("high_score_watchlist", passed=False, promotion_tier="watchlist", robustness_score=95)
    paper = _evaluation("lower_score_paper", passed=True, robustness_score=55)

    store.save_candidate(run_id, "XAUUSD", watchlist)
    store.save_candidate(run_id, "XAUUSD", paper)

    candidates = store.list_candidates(run_id)

    assert [candidate["strategy_name"] for candidate in candidates] == ["lower_score_paper", "high_score_watchlist"]
    assert candidates[0]["promotion_tier"] == "paper_candidate"


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
        candidate=ProbabilityCandidate(name, ("fixture",), {"market_id": "NAS100"}, [0.1, 0.9]),
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
