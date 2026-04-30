from __future__ import annotations

from app.payload_compaction import FOLD_LIMIT, _compact_candidate, _compact_trial


def test_compacts_trial_folds_to_summary_metrics():
    trial = {"folds": [_fold(index) for index in range(FOLD_LIMIT + 3)]}

    compacted = _compact_trial(trial)

    assert len(compacted["folds"]) == FOLD_LIMIT
    assert compacted["folds"][0] == {
        "net_profit": 1,
        "gross_profit": 2,
        "sharpe": 3,
        "max_drawdown": 4,
        "win_rate": 5,
        "trade_count": 6,
        "total_cost": 7,
        "cost_confidence": "fixture",
    }
    assert "equity_curve" not in compacted["folds"][0]
    assert "drawdown_curve" not in compacted["folds"][0]


def test_compacts_candidate_audit_folds_without_mutating_source():
    candidate = {"audit": {"fold_results": [_fold(0)]}}

    compacted = _compact_candidate(candidate)

    assert "equity_curve" in candidate["audit"]["fold_results"][0]
    assert "equity_curve" not in compacted["audit"]["fold_results"][0]


def _fold(index: int) -> dict[str, object]:
    return {
        "net_profit": index + 1,
        "gross_profit": index + 2,
        "sharpe": index + 3,
        "max_drawdown": index + 4,
        "win_rate": index + 5,
        "trade_count": index + 6,
        "total_cost": index + 7,
        "cost_confidence": "fixture",
        "equity_curve": list(range(500)),
        "drawdown_curve": list(range(500)),
    }
