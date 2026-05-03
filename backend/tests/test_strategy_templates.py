from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

import app.main as main
from app.research_store import ResearchStore


def test_template_summary_and_status_endpoints(tmp_path, monkeypatch):
    store = ResearchStore(tmp_path / "research.sqlite3")
    monkeypatch.setattr(main, "research_store", store)

    saved = main.save_strategy_template(
        main.StrategyTemplatePayload(
            name="frozen_gold_trend",
            market_id="XAUUSD",
            interval="1day",
            strategy_family="intraday_trend",
            target_regime="trend_up",
            readiness_status="ready_for_paper",
            promotion_tier="paper_candidate",
            robustness_score=81.0,
            payload={
                "source_template": {
                    "parameters": {"lookback": 20, "threshold_bps": 12},
                },
                "warnings": [],
            },
        )
    )

    summary = main.templates_summary(limit=100)
    archived = main.update_strategy_template_status(saved["id"], main.StrategyTemplateStatusPayload(status="archived"))
    hidden_summary = main.templates_summary(limit=100)
    full_summary = main.templates_summary(include_inactive=True, limit=100)

    assert summary["counts"]["active"] == 1
    assert summary["counts"]["frozen"] == 1
    assert summary["counts"]["paper_ready"] == 1
    assert summary["templates"][0]["name"] == "frozen_gold_trend"
    assert archived["status"] == "archived"
    assert hidden_summary["templates"] == []
    assert full_summary["templates"][0]["status"] == "archived"
