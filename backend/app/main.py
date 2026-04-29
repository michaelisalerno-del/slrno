from __future__ import annotations

import re

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .adaptive_research import AdaptiveSearchConfig, available_research_engines, run_adaptive_search
from .backtesting import BacktestConfig
from .config import allowed_origins
from .ig_costs import IGCostProfile, backtest_config_from_profile, profile_from_ig_market, public_ig_cost_profile
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
    account_id: str = ""
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
    default_timeframe: str = "5min"
    spread_bps: float = 2.0
    slippage_bps: float = 1.0
    min_backtest_bars: int = 750


class ResearchRunPayload(BaseModel):
    market_id: str = "NAS100"
    market_ids: list[str] = Field(default_factory=list)
    start: str
    end: str
    interval: str | None = None
    engine: str = "adaptive_ig_v1"
    search_preset: str = "balanced"
    trading_style: str = "find_anything_robust"
    objective: str = "balanced"
    search_budget: int | None = None
    risk_profile: str = "balanced"
    strategy_families: list[str] = Field(default_factory=list)
    product_mode: str = "spread_bet"


class ResearchSchedulePayload(BaseModel):
    name: str
    cadence: str
    enabled: bool = True
    market_ids: list[str] = Field(default_factory=list)
    interval: str = "5min"


class IGCostSyncPayload(BaseModel):
    market_ids: list[str] = Field(default_factory=list)


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
        detail = _public_error(exc)
        settings.set_status("fmp", "error", detail)
        raise HTTPException(status_code=400, detail=f"FMP validation failed: {detail}") from exc
    settings.set_status("fmp", "connected")
    return {"status": "connected"}


@app.post("/settings/ig")
async def save_ig(payload: IGSettings) -> dict[str, str]:
    if payload.environment != "demo":
        raise HTTPException(status_code=400, detail="Only IG demo mode is supported in v1")
    settings.set_secret("ig", "api_key", payload.api_key)
    settings.set_secret("ig", "username", payload.username)
    settings.set_secret("ig", "password", payload.password)
    if payload.account_id.strip():
        settings.set_secret("ig", "account_id", payload.account_id.strip())
    provider = IGDemoProvider(payload.api_key, payload.username, payload.password, payload.account_id)
    try:
        account = await provider.account_status()
    except Exception as exc:
        detail = _public_error(exc)
        settings.set_status("ig", "error", detail)
        raise HTTPException(status_code=400, detail=f"IG validation failed: {detail}") from exc
    if account.account_id:
        settings.set_secret("ig", "account_id", account.account_id)
    settings.set_status("ig", "connected")
    return {"status": "connected", "environment": "demo", "account_id": account.account_id}


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


@app.get("/research/engines")
def research_engines() -> list[dict[str, object]]:
    return available_research_engines()


@app.post("/ig/markets/sync-costs")
async def sync_ig_market_costs(payload: IGCostSyncPayload) -> dict[str, object]:
    selected = _selected_markets(payload.market_ids)
    provider = _ig_provider_from_settings()
    account_currency = "GBP"
    if provider is not None:
        try:
            account_currency = (await provider.account_status()).currency or "GBP"
        except Exception:
            provider = None
    profiles: list[dict[str, object]] = []
    for market in selected:
        profile = public_ig_cost_profile(market, account_currency)
        if provider is not None and market.ig_epic:
            try:
                profile = profile_from_ig_market(market, await provider.market_details(market.ig_epic), account_currency)
            except Exception as exc:
                fallback = profile.as_dict()
                fallback["notes"] = list(fallback.get("notes", [])) + [f"IG sync failed: {_public_error(exc)}"]
                profile = IGCostProfile(**{key: value for key, value in fallback.items() if key in IGCostProfile.__dataclass_fields__})
        research_store.save_cost_profile(profile)
        profiles.append(profile.as_dict())
    return {"status": "synced", "profile_count": len(profiles), "profiles": profiles}


@app.get("/ig/markets/{market_id}/cost-profile")
def get_ig_cost_profile(market_id: str) -> dict[str, object]:
    market = markets.get(market_id)
    if market is None:
        raise HTTPException(status_code=404, detail="Market not found")
    stored = research_store.get_cost_profile(market_id)
    if stored is not None:
        return stored
    profile = public_ig_cost_profile(market)
    research_store.save_cost_profile(profile)
    return profile.as_dict()


@app.post("/research/runs")
async def create_research_run(payload: ResearchRunPayload) -> dict[str, object]:
    engine_ids = {engine["id"] for engine in available_research_engines()}
    if payload.engine not in engine_ids:
        raise HTTPException(status_code=400, detail="Unknown research engine")
    api_key = settings.get_secret("fmp", "api_key")
    if api_key is None:
        raise HTTPException(status_code=400, detail="FMP API key is required before launching research")

    selected_markets = _selected_markets(payload.market_ids or [payload.market_id])
    run_market_id = selected_markets[0].market_id if len(selected_markets) == 1 else "MULTI"
    run_id = research_store.create_run(
        market_id=run_market_id,
        data_source="fmp_with_ig_cost_model",
        status="running",
        config={
            "start": payload.start,
            "end": payload.end,
            "interval": payload.interval,
            "engine": payload.engine,
            "market_ids": [market.market_id for market in selected_markets],
            "search_preset": payload.search_preset,
            "trading_style": payload.trading_style,
            "objective": payload.objective,
            "search_budget": payload.search_budget,
            "risk_profile": payload.risk_profile,
            "strategy_families": payload.strategy_families,
            "product_mode": payload.product_mode,
            "research_only": True,
            "ig_validation_required": True,
        },
    )
    evaluations = []
    try:
        provider = FMPProvider(api_key)
        for market in selected_markets:
            if not market.enabled:
                raise ValueError(f"Market {market.market_id} is disabled")
            interval = payload.interval or market.default_timeframe
            bars = await provider.historical_bars(market.fmp_symbol, interval, payload.start, payload.end)
            if len(bars) < market.min_backtest_bars:
                raise ValueError(f"{market.market_id}: need at least {market.min_backtest_bars} bars; received {len(bars)}")
            if payload.engine == "adaptive_ig_v1":
                cost_profile = _cost_profile_for_market(market)
                result = run_adaptive_search(
                    bars,
                    market.market_id,
                    interval,
                    cost_profile,
                    AdaptiveSearchConfig(
                        preset=payload.search_preset,
                        trading_style=payload.trading_style,
                        objective=payload.objective,
                        search_budget=payload.search_budget,
                        risk_profile=payload.risk_profile,
                        strategy_families=tuple(payload.strategy_families),
                    ),
                )
                market_evaluations = list(result.evaluations)
            else:
                cost_profile = _cost_profile_for_market(market)
                market_evaluations = ResearchStack.default().evaluate(bars, backtest_config_from_profile(cost_profile))
            for evaluation in market_evaluations:
                evaluations.append(evaluation)
                research_store.save_trial(run_id, evaluation)
                research_store.save_candidate(run_id, market.market_id, evaluation)
        research_store.update_run_status(run_id, "finished")
    except Exception as exc:
        research_store.update_run_status(run_id, "error")
        raise HTTPException(status_code=400, detail=f"Research run failed: {_public_error(exc)}") from exc

    passed = [evaluation for evaluation in evaluations if evaluation.passed]
    return {
        "run_id": run_id,
        "status": "finished",
        "market_id": run_market_id,
        "trial_count": len(evaluations),
        "candidate_count": len(passed),
        "best_score": max((evaluation.robustness_score for evaluation in evaluations), default=0),
        "pareto": research_store.list_pareto(run_id),
    }


@app.get("/research/runs")
def list_research_runs() -> list[dict[str, object]]:
    return research_store.list_runs()


@app.get("/research/runs/{run_id}")
def get_research_run(run_id: int) -> dict[str, object]:
    run = research_store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Research run not found")
    return {
        **run,
        "trials": research_store.list_trials(run_id)[:25],
        "candidates": research_store.list_candidates(run_id),
        "pareto": research_store.list_pareto(run_id),
    }


@app.get("/research/runs/{run_id}/trials")
def list_research_trials(run_id: int) -> list[dict[str, object]]:
    if research_store.get_run(run_id) is None:
        raise HTTPException(status_code=404, detail="Research run not found")
    return research_store.list_trials(run_id)


@app.get("/research/runs/{run_id}/pareto")
def get_research_pareto(run_id: int) -> list[dict[str, object]]:
    if research_store.get_run(run_id) is None:
        raise HTTPException(status_code=404, detail="Research run not found")
    return research_store.list_pareto(run_id)


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


@app.get("/research/candidates/{candidate_id}")
def get_research_candidate(candidate_id: int) -> dict[str, object]:
    candidate = research_store.get_candidate(candidate_id)
    if candidate is None:
        raise HTTPException(status_code=404, detail="Research candidate not found")
    return candidate


@app.post("/research/schedules")
def save_research_schedule(payload: ResearchSchedulePayload) -> dict[str, object]:
    schedule_id = research_store.save_schedule(
        payload.name,
        payload.cadence,
        payload.enabled,
        {"market_ids": payload.market_ids, "interval": payload.interval, "data_source": "fmp"},
    )
    return {"status": "saved", "schedule_id": schedule_id}


def _selected_markets(market_ids: list[str]) -> list[MarketMapping]:
    selected: list[MarketMapping] = []
    ids = market_ids or [market.market_id for market in markets.list(enabled_only=True)]
    for market_id in ids:
        market = markets.get(market_id)
        if market is None:
            raise HTTPException(status_code=404, detail=f"Market {market_id} not found")
        selected.append(market)
    return selected


def _cost_profile_for_market(market: MarketMapping) -> IGCostProfile:
    stored = research_store.get_cost_profile(market.market_id)
    if stored is not None:
        stored.pop("updated_at", None)
        return IGCostProfile(**{key: value for key, value in stored.items() if key in IGCostProfile.__dataclass_fields__})
    profile = public_ig_cost_profile(market)
    research_store.save_cost_profile(profile)
    return profile


def _ig_provider_from_settings() -> IGDemoProvider | None:
    api_key = settings.get_secret("ig", "api_key")
    username = settings.get_secret("ig", "username")
    password = settings.get_secret("ig", "password")
    account_id = settings.get_secret("ig", "account_id") or ""
    if not api_key or not username or not password:
        return None
    return IGDemoProvider(api_key, username, password, account_id)


def _public_error(exc: Exception) -> str:
    text = str(exc)
    text = re.sub(r"apikey=[^&'\"\s]+", "apikey=***", text, flags=re.IGNORECASE)
    text = re.sub(r"api[-_ ]?key[^,;\n]*", "API key hidden", text, flags=re.IGNORECASE)
    return text
