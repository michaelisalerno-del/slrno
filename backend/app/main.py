from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .backtesting import BacktestConfig
from .config import allowed_origins
from .market_plugins import get_market_plugin, list_market_plugins
from .market_registry import MarketMapping, MarketRegistry
from .providers.fmp import FMPProvider
from .providers.ig import IGDemoProvider
from .research_critic import ResearchCritic
from .research_lab import ResearchStack
from .research_store import ResearchStore
from .settings_store import SettingsStore

app = FastAPI(title="slrno Trading Bot", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
settings = SettingsStore()
markets = MarketRegistry()
markets.seed_defaults()
research_store = ResearchStore()
research_critic = ResearchCritic.default()


class FMPSettings(BaseModel):
    api_key: str = Field(min_length=1)


class IGSettings(BaseModel):
    api_key: str = Field(min_length=1)
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)
    environment: str = "demo"


class MarketPayload(BaseModel):
    market_id: str
    name: str
    asset_class: str
    fmp_symbol: str
    ig_epic: str = ""
    enabled: bool = True
    plugin_id: str = ""
    ig_name: str = ""
    ig_search_terms: str = ""
    default_timeframe: str = "1h"
    spread_bps: float = 2.0
    slippage_bps: float = 1.0
    min_backtest_bars: int = 750


class ResearchRunPayload(BaseModel):
    market_id: str
    start: str
    end: str
    interval: str | None = None


class ResearchSchedulePayload(BaseModel):
    name: str
    cadence: str
    enabled: bool = True
    market_ids: list[str] = Field(default_factory=list)
    interval: str = "1h"


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "mode": "paper"}


@app.get("/settings/status")
def settings_status() -> list[dict[str, object]]:
    return [status.__dict__ for status in settings.statuses()]


@app.post("/settings/fmp")
async def save_fmp(payload: FMPSettings) -> dict[str, str]:
    settings.set_secret("fmp", "api_key", payload.api_key)
    provider = FMPProvider(payload.api_key)
    try:
        await provider.validate()
    except Exception as exc:
        settings.set_status("fmp", "error", str(exc))
        raise HTTPException(status_code=400, detail=f"FMP validation failed: {exc}") from exc
    settings.set_status("fmp", "connected")
    return {"status": "connected"}


@app.post("/settings/ig")
async def save_ig(payload: IGSettings) -> dict[str, str]:
    if payload.environment != "demo":
        raise HTTPException(status_code=400, detail="Only IG demo mode is supported in v1")
    settings.set_secret("ig", "api_key", payload.api_key)
    settings.set_secret("ig", "username", payload.username)
    settings.set_secret("ig", "password", payload.password)
    provider = IGDemoProvider(payload.api_key, payload.username, payload.password)
    try:
        await provider.account_status()
    except Exception as exc:
        settings.set_status("ig", "error", str(exc))
        raise HTTPException(status_code=400, detail=f"IG validation failed: {exc}") from exc
    settings.set_status("ig", "connected")
    return {"status": "connected", "environment": "demo"}


@app.get("/markets")
def list_markets() -> list[dict[str, object]]:
    return [market.__dict__ for market in markets.list()]


@app.get("/market-plugins")
def list_plugins() -> list[dict[str, object]]:
    return [plugin.as_dict() for plugin in list_market_plugins()]


@app.post("/market-plugins/{plugin_id}/install")
def install_plugin(plugin_id: str) -> dict[str, str]:
    plugin = get_market_plugin(plugin_id)
    if plugin is None:
        raise HTTPException(status_code=404, detail="Market plugin not found")
    markets.upsert(plugin.to_mapping())
    return {"status": "installed", "market_id": plugin.market_id}


@app.post("/markets")
def upsert_market(payload: MarketPayload) -> dict[str, str]:
    markets.upsert(
        MarketMapping(
            payload.market_id,
            payload.name,
            payload.asset_class,
            payload.fmp_symbol,
            payload.ig_epic,
            payload.enabled,
            payload.plugin_id,
            payload.ig_name,
            payload.ig_search_terms,
            payload.default_timeframe,
            payload.spread_bps,
            payload.slippage_bps,
            payload.min_backtest_bars,
        )
    )
    return {"status": "saved"}


@app.post("/research/runs")
async def create_research_run(payload: ResearchRunPayload) -> dict[str, object]:
    market = markets.get(payload.market_id)
    if market is None:
        raise HTTPException(status_code=404, detail="Market not found")
    if not market.enabled:
        raise HTTPException(status_code=400, detail="Market is disabled")
    api_key = settings.get_secret("fmp", "api_key")
    if api_key is None:
        raise HTTPException(status_code=400, detail="FMP API key is required before launching research")

    interval = payload.interval or market.default_timeframe
    run_id = research_store.create_run(
        market_id=market.market_id,
        data_source="fmp",
        status="running",
        config={
            "start": payload.start,
            "end": payload.end,
            "interval": interval,
            "research_only": True,
            "ig_validation_required": True,
        },
    )
    try:
        provider = FMPProvider(api_key)
        bars = await provider.historical_bars(market.fmp_symbol, interval, payload.start, payload.end)
        if len(bars) < market.min_backtest_bars:
            raise ValueError(f"Need at least {market.min_backtest_bars} bars; received {len(bars)}")
        evaluations = ResearchStack.default().evaluate(
            bars,
            BacktestConfig(spread_bps=market.spread_bps, slippage_bps=market.slippage_bps),
        )
        for evaluation in evaluations:
            research_store.save_trial(run_id, evaluation)
            research_store.save_candidate(run_id, market.market_id, evaluation)
        research_store.update_run_status(run_id, "finished")
    except Exception as exc:
        research_store.update_run_status(run_id, "error")
        raise HTTPException(status_code=400, detail=f"Research run failed: {exc}") from exc

    passed = [evaluation for evaluation in evaluations if evaluation.passed]
    return {
        "run_id": run_id,
        "status": "finished",
        "market_id": market.market_id,
        "trial_count": len(evaluations),
        "candidate_count": len(passed),
        "best_score": max((evaluation.robustness_score for evaluation in evaluations), default=0),
    }


@app.get("/research/runs")
def list_research_runs() -> list[dict[str, object]]:
    return research_store.list_runs()


@app.get("/research/critique")
def critique_latest_research() -> dict[str, object]:
    runs = research_store.list_runs()
    if not runs:
        return research_critic.critique(None, [], []).as_dict()
    latest = research_store.get_run(int(runs[0]["id"]))
    run_id = int(runs[0]["id"])
    return research_critic.critique(
        latest,
        research_store.list_trials(run_id),
        research_store.list_candidates(run_id),
    ).as_dict()


@app.get("/research/runs/{run_id}/critique")
def critique_research_run(run_id: int) -> dict[str, object]:
    run = research_store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Research run not found")
    return research_critic.critique(
        run,
        research_store.list_trials(run_id),
        research_store.list_candidates(run_id),
    ).as_dict()


@app.get("/research/candidates")
def list_research_candidates() -> list[dict[str, object]]:
    return research_store.list_candidates()


@app.post("/research/schedules")
def save_research_schedule(payload: ResearchSchedulePayload) -> dict[str, object]:
    schedule_id = research_store.save_schedule(
        payload.name,
        payload.cadence,
        payload.enabled,
        {"market_ids": payload.market_ids, "interval": payload.interval, "data_source": "fmp"},
    )
    return {"status": "saved", "schedule_id": schedule_id}
