from __future__ import annotations

from app.backtesting import BacktestResult
from app.research_critic import ResearchCritic
from app.research_lab import CandidateEvaluation
from app.research_metrics import ClassificationMetrics
from app.research_store import ResearchStore
from app.research_strategies import ProbabilityCandidate


def test_research_critic_keeps_eodhd_candidates_on_watchlist_only(tmp_path):
    store = ResearchStore(tmp_path / "research.sqlite3")
    run_id = store.create_run("NAS100", {"interval": "1h"}, status="finished")
    evaluation = _evaluation("accepted", passed=True)
    store.save_trial(run_id, evaluation)
    store.save_candidate(run_id, "NAS100", evaluation)

    report = ResearchCritic.default().critique(
        store.get_run(run_id),
        store.list_trials(run_id),
        store.list_candidates(run_id),
    )

    assert report.decision == "watchlist_only"
    assert any(finding.code == "eodhd_only_validation" for finding in report.findings)
    assert all(finding.code != "no_trial_audit" for finding in report.findings)


def test_research_critic_flags_high_auc_as_possible_leakage():
    report = ResearchCritic.default().critique(
        {
            "id": 1,
            "market_id": "XAUUSD",
            "data_source": "ig",
            "trial_count": 1,
            "passed_count": 1,
        },
        [
            {
                "id": 1,
                "run_id": 1,
                "strategy_name": "too_good",
                "passed": True,
                "robustness_score": 90,
                "metrics": {},
                "warnings": [],
            }
        ],
        [
            {
                "strategy_name": "too_good",
                "audit": {
                    "metrics": {
                        "roc_auc": 0.96,
                        "pr_auc": 0.8,
                        "positive_rate": 0.4,
                        "precision_at_top_quantile": 0.75,
                    },
                    "backtest": {"trade_count": 60, "sharpe": 1.1, "net_profit": 1000, "turnover": 60},
                    "fold_results": [{"net_profit": 100}, {"net_profit": 120}, {"net_profit": 80}],
                    "warnings": [],
                },
            }
        ],
    )

    assert any(finding.code == "possible_leakage_or_overfit" for finding in report.findings)


def test_research_critic_reports_full_counts_for_sampled_dashboard_context():
    report = ResearchCritic.default().critique(
        {
            "id": 1,
            "market_id": "NAS100",
            "data_source": "eodhd",
            "trial_count": 500,
            "passed_count": 0,
            "candidate_count": 35,
            "critique_sampled": True,
            "critique_trial_limit": 80,
            "critique_candidate_limit": 24,
        },
        [
            {
                "id": index,
                "run_id": 1,
                "strategy_name": f"sample_{index}",
                "passed": False,
                "robustness_score": 10,
                "metrics": {},
                "warnings": [],
            }
            for index in range(80)
        ],
        [],
    )

    codes = {finding.code for finding in report.findings}
    assert report.trial_count == 500
    assert report.candidate_count == 35
    assert "sampled_trial_audit" in codes
    assert "trial_count_mismatch" not in codes


def _evaluation(name: str, passed: bool) -> CandidateEvaluation:
    return CandidateEvaluation(
        candidate=ProbabilityCandidate(name, ("fixture",), {}, [0.1, 0.9]),
        metrics=ClassificationMetrics(0.7, 0.65, 0.01, 0.1, 0.7, 0.5, 2),
        backtest=BacktestResult(100, 1.0, 10, 0.6, 60, 0.3, 60, 60, 40),
        fold_results=(
            BacktestResult(10, 0.8, 1, 0.6, 5, 0.2, 1, 6, 4),
            BacktestResult(20, 0.9, 1, 0.6, 5, 0.2, 1, 8, 12),
        ),
        robustness_score=75.0,
        passed=passed,
        warnings=(),
    )
