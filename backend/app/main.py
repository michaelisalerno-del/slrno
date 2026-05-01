from __future__ import annotations

import asyncio
import re

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .adaptive_research import AdaptiveSearchConfig, available_research_engines, run_adaptive_search
from .config import allowed_origins
from .ig_costs import IGCostProfile, backtest_config_from_profile, profile_from_ig_market, public_ig_cost_profile
from .ig_spread_bet_engines import list_spread_bet_engines
from .market_data_cache import MarketDataCache
from .market_plugins import get_market_plugin, list_market_plugins
from .market_registry import MarketMapping, MarketRegistry
from .providers.eodhd import EODHDProvider
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

CRITIQUE_TRIAL_SAMPLE_LIMIT = 80
CRITIQUE_CANDIDATE_SAMPLE_LIMIT = 24


class EODHDSettings(BaseModel):
    api_token: str = Field(min_length=1)


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
    eodhd_symbol: str = ""
    fmp_symbol: str = ""
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
    cost_stress_multiplier: float = 2.0


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


@app.post("/settings/eodhd")
async def save_eodhd(payload: EODHDSettings) -> dict[str, str]:
    settings.set_secret("eodhd", "api_token", payload.api_token)
    provider = EODHDProvider(payload.api_token)
    try:
        await provider.validate()
    except Exception as exc:
        detail = _public_error(exc)
        settings.set_status("eodhd", "error", detail)
        raise HTTPException(status_code=400, detail=f"EODHD validation failed: {detail}") from exc
    settings.set_status("eodhd", "connected")
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
    return [_market_response(market) for market in markets.list()]


@app.get("/market-data/cache")
def market_data_cache_status() -> dict[str, object]:
    cache = MarketDataCache()
    return {
        "stats": cache.stats().as_dict(),
        "namespaces": cache.namespace_stats(),
        "recent_entries": cache.recent_entries(limit=10),
        "policy": {
            "quote_ttl_seconds": EODHDProvider.QUOTE_TTL_SECONDS,
            "live_history_ttl_seconds": EODHDProvider.LIVE_HISTORY_TTL_SECONDS,
            "closed_history_ttl_seconds": EODHDProvider.CLOSED_HISTORY_TTL_SECONDS,
            "provider_error_ttl_seconds": EODHDProvider.NEGATIVE_ERROR_TTL_SECONDS,
            "recent_history_refresh_days": 3,
        },
    }


@app.post("/market-data/cache/prune")
def prune_market_data_cache() -> dict[str, object]:
    cache = MarketDataCache()
    deleted = cache.prune_expired()
    return {"status": "pruned", "deleted": deleted, "stats": cache.stats().as_dict()}


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
            payload.eodhd_symbol or payload.fmp_symbol,
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


@app.get("/ig/spread-bet/engines")
def ig_spread_bet_engines() -> list[dict[str, object]]:
    return list_spread_bet_engines()


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
async def create_research_run(payload: ResearchRunPayload, background_tasks: BackgroundTasks) -> dict[str, object]:
    engine_ids = {engine["id"] for engine in available_research_engines()}
    if payload.engine not in engine_ids:
        raise HTTPException(status_code=400, detail="Unknown research engine")
    api_token = settings.get_secret("eodhd", "api_token")
    if api_token is None:
        raise HTTPException(status_code=400, detail="EODHD API token is required before launching research")

    selected_markets = _selected_markets(payload.market_ids or [payload.market_id])
    run_market_id = selected_markets[0].market_id if len(selected_markets) == 1 else "MULTI"
    run_id = research_store.create_run(
        market_id=run_market_id,
        data_source="eodhd_with_ig_cost_model",
        status="running",
        config=_research_run_config(payload, selected_markets),
    )
    background_tasks.add_task(_run_research_job, run_id, payload.model_dump(), api_token)
    return {
        "run_id": run_id,
        "status": "running",
        "market_id": run_market_id,
        "trial_count": 0,
        "candidate_count": 0,
        "best_score": 0,
        "pareto": [],
    }


def _run_research_job(run_id: int, payload_data: dict[str, object], api_token: str) -> None:
    try:
        asyncio.run(_execute_research_run(run_id, ResearchRunPayload(**payload_data), api_token))
    except Exception as exc:
        research_store.update_run_status(run_id, "error", _public_error(exc))


async def _execute_research_run(run_id: int, payload: ResearchRunPayload, api_token: str) -> None:
    selected_markets = _selected_markets(payload.market_ids or [payload.market_id])
    provider = EODHDProvider(api_token)
    market_statuses: list[dict[str, object]] = []
    market_failures: list[dict[str, object]] = []
    saved_trials = 0

    def persist_status() -> None:
        research_store.update_run_config(
            run_id,
            _research_run_config(payload, selected_markets, market_statuses=market_statuses, market_failures=market_failures),
        )

    for market in selected_markets:
        interval = payload.interval or market.default_timeframe
        market_status: dict[str, object] = {
            "market_id": market.market_id,
            "name": market.name,
            "eodhd_symbol": market.eodhd_symbol,
            "interval": interval,
            "status": "loading",
            "data_source_status": "eodhd_primary_symbol",
            "cost_source_status": "ig_cost_model",
        }
        market_statuses.append(market_status)
        persist_status()
        if not market.enabled:
            _mark_market_failed(market_status, market_failures, market, f"Market {market.market_id} is disabled")
            persist_status()
            continue
        try:
            bars = await provider.historical_bars(market.eodhd_symbol, interval, payload.start, payload.end)
        except Exception as exc:
            _mark_market_failed(
                market_status,
                market_failures,
                market,
                f"{market.market_id} skipped: {market.eodhd_symbol} EODHD data load failed: {_public_error(exc)}",
            )
            persist_status()
            continue
        if len(bars) < market.min_backtest_bars:
            _mark_market_failed(
                market_status,
                market_failures,
                market,
                f"{market.market_id} skipped: need at least {market.min_backtest_bars} bars; received {len(bars)}",
                bar_count=len(bars),
            )
            persist_status()
            continue
        market_status.update({"status": "evaluating", "bar_count": len(bars)})
        persist_status()
        cost_profile = _cost_profile_for_market(market)
        try:
            if payload.engine == "adaptive_ig_v1":
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
                        cost_stress_multiplier=max(1.0, payload.cost_stress_multiplier),
                    ),
                )
                market_evaluations = list(result.evaluations)
            else:
                market_evaluations = ResearchStack.default().evaluate(bars, backtest_config_from_profile(cost_profile))
        except Exception as exc:
            _mark_market_failed(
                market_status,
                market_failures,
                market,
                f"{market.market_id} skipped: research evaluation failed: {_public_error(exc)}",
                bar_count=len(bars),
            )
            persist_status()
            continue
        trial_count = 0
        for evaluation in market_evaluations:
            research_store.save_trial(run_id, evaluation)
            research_store.save_candidate(run_id, market.market_id, evaluation)
            saved_trials += 1
            trial_count += 1
        market_status.update({"status": "completed", "bar_count": len(bars), "trial_count": trial_count})
        persist_status()

    if saved_trials == 0:
        error = _market_failure_summary(market_failures) or "No valid trials were produced for the selected markets"
        research_store.update_run_status(run_id, "error", error)
        return
    if market_failures:
        research_store.update_run_status(run_id, "finished_with_warnings", _market_failure_summary(market_failures))
        return
    research_store.update_run_status(run_id, "finished")


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
        "trials": research_store.list_trials(run_id, limit=25),
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


@app.delete("/research/runs/{run_id}")
def delete_research_run(run_id: int) -> dict[str, object]:
    run = research_store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Research run not found")
    if run["status"] in {"created", "running"}:
        raise HTTPException(status_code=409, detail="Running research runs cannot be deleted")
    result = research_store.delete_run(run_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Research run not found")
    return {"status": "deleted", **result}


@app.get("/research/critique")
def critique_latest_research() -> dict[str, object]:
    runs = research_store.list_runs()
    if not runs:
        return research_critic.critique(None, [], []).as_dict()
    latest = research_store.get_run(int(runs[0]["id"]))
    return _critique_research_run(latest).as_dict()


@app.get("/research/runs/{run_id}/critique")
def critique_research_run(run_id: int) -> dict[str, object]:
    run = research_store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Research run not found")
    return _critique_research_run(run).as_dict()


def _critique_research_run(run: dict[str, object] | None):
    if run is None:
        return research_critic.critique(None, [], [])
    run_id = int(run["id"])
    trials = research_store.list_trials(run_id, limit=CRITIQUE_TRIAL_SAMPLE_LIMIT)
    candidates = research_store.list_candidates(run_id, limit=CRITIQUE_CANDIDATE_SAMPLE_LIMIT)
    stored_candidate_count = research_store.count_candidates(run_id)
    candidate_count = max(stored_candidate_count, len(candidates))
    trial_count = int(run.get("trial_count") or len(trials))
    critique_run = {
        **run,
        "candidate_count": candidate_count,
        "critique_sampled": trial_count > len(trials) or candidate_count > len(candidates),
        "critique_trial_sample_size": len(trials),
        "critique_candidate_sample_size": len(candidates),
        "critique_trial_limit": CRITIQUE_TRIAL_SAMPLE_LIMIT,
        "critique_candidate_limit": CRITIQUE_CANDIDATE_SAMPLE_LIMIT,
    }
    return research_critic.critique(critique_run, trials, candidates)


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
        {"market_ids": payload.market_ids, "interval": payload.interval, "data_source": "eodhd"},
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


def _research_run_config(
    payload: ResearchRunPayload,
    selected_markets: list[MarketMapping],
    market_statuses: list[dict[str, object]] | None = None,
    market_failures: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
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
        "cost_stress_multiplier": payload.cost_stress_multiplier,
        "product_mode": payload.product_mode,
        "research_only": True,
        "ig_validation_required": True,
        "data_source_policy": "eodhd_primary_symbol_no_silent_proxy",
        "market_statuses": market_statuses or [],
        "market_failures": market_failures or [],
    }


def _mark_market_failed(
    market_status: dict[str, object],
    market_failures: list[dict[str, object]],
    market: MarketMapping,
    error: str,
    bar_count: int | None = None,
) -> None:
    failure: dict[str, object] = {
        "market_id": market.market_id,
        "name": market.name,
        "eodhd_symbol": market.eodhd_symbol,
        "status": "failed",
        "error": error,
    }
    if bar_count is not None:
        failure["bar_count"] = bar_count
    market_status.update(failure)
    market_failures.append(failure)


def _market_failure_summary(market_failures: list[dict[str, object]]) -> str:
    return "; ".join(str(item.get("error", "")) for item in market_failures if item.get("error"))


def _market_response(market: MarketMapping) -> dict[str, object]:
    payload = dict(market.__dict__)
    payload["estimated_spread_bps"] = market.spread_bps
    payload["estimated_slippage_bps"] = market.slippage_bps
    return payload


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
    text = re.sub(r"api_token=[^&'\"\s]+", "api_token=***", text, flags=re.IGNORECASE)
    text = re.sub(r"api[-_ ]?key[^,;\n]*", "API key hidden", text, flags=re.IGNORECASE)
    return text
