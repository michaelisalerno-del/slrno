from __future__ import annotations

import asyncio
import re
from datetime import date

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .adaptive_research import SEARCH_PRESETS, AdaptiveSearchConfig, available_research_engines, run_adaptive_search
from .bar_patterns import analyze_market_regimes
from .broker_preview import broker_order_preview
from .capital import (
    CAPITAL_SCENARIOS_GBP,
    DAILY_LOSS_FRACTION,
    RISK_PER_TRADE_FRACTION,
    WORKING_ACCOUNT_SIZE_GBP,
    capital_scenarios,
    capital_summary,
    scenario_account_sizes,
)
from .config import allowed_origins
from .evidence_export import build_research_export_zip
from .ig_costs import IGCostProfile, backtest_config_from_profile, profile_from_ig_market, public_ig_cost_profile, select_ig_market_candidate
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
INTRADAY_INTERVALS = {"1min", "1m", "5min", "5m", "15min", "15m", "30min", "30m", "1hour", "1h", "60min", "60m"}
DAILY_FALLBACK_ASSET_CLASSES = {"index", "share", "commodity"}
MULTI_MARKET_TOTAL_TRIAL_CAPS = {
    "quick": 96,
    "balanced": 216,
    "deep": 480,
}
MULTI_MARKET_MIN_TRIALS_PER_MARKET = {
    "quick": 6,
    "balanced": 9,
    "deep": 12,
}


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
    include_regime_scans: bool = True
    regime_scan_budget_per_regime: int | None = Field(default=None, ge=1, le=96)
    target_regime: str | None = None
    excluded_months: list[str] = Field(default_factory=list)
    repair_mode: str = "standard"
    account_size: float = Field(default=WORKING_ACCOUNT_SIZE_GBP, gt=0)
    source_template: dict[str, object] = Field(default_factory=dict)


class ResearchSchedulePayload(BaseModel):
    name: str
    cadence: str
    enabled: bool = True
    market_ids: list[str] = Field(default_factory=list)
    interval: str = "5min"


class StrategyTemplatePayload(BaseModel):
    name: str
    market_id: str
    interval: str = "5min"
    strategy_family: str = ""
    style: str = "find_anything_robust"
    target_regime: str = ""
    status: str = "active"
    source_run_id: int | None = None
    source_trial_id: int | None = None
    source_candidate_id: int | None = None
    source_kind: str = ""
    promotion_tier: str = "research_candidate"
    readiness_status: str = "blocked"
    robustness_score: float = 0
    testing_account_size: float = Field(default=WORKING_ACCOUNT_SIZE_GBP, gt=0)
    payload: dict[str, object] = Field(default_factory=dict)


class StrategyTemplateStatusPayload(BaseModel):
    status: str


class IGCostSyncPayload(BaseModel):
    market_ids: list[str] = Field(default_factory=list)


class BrokerOrderPreviewPayload(BaseModel):
    market_id: str
    side: str = "BUY"
    stake: float = Field(default=1.0, gt=0)
    account_size: float = Field(default=WORKING_ACCOUNT_SIZE_GBP, gt=0)
    entry_price: float | None = Field(default=None, gt=0)
    stop: float | None = Field(default=None, gt=0)
    limit: float | None = Field(default=None, gt=0)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "mode": "paper"}


@app.get("/cockpit/summary")
def cockpit_summary() -> dict[str, object]:
    runs = research_store.list_runs()
    running = [run for run in runs if run["status"] in {"created", "running"}]
    latest = runs[0] if runs else None
    return {
        "mode": "paper",
        "live_ordering_enabled": False,
        "providers": [status.__dict__ for status in settings.statuses()],
        "runs": {
            "total_visible": len(runs),
            "running": len(running),
            "latest": latest,
            "recent": runs[:5],
        },
        "risk": _risk_summary(),
        "next_actions": _cockpit_next_actions(runs),
    }


@app.get("/research/summary")
def research_summary(limit: int = Query(default=24, ge=1, le=80), include_critique: bool = False) -> dict[str, object]:
    candidates = [_candidate_summary_payload(_candidate_with_capital(candidate)) for candidate in research_store.list_candidates(limit=limit)]
    return {
        "queue": _candidate_queue_summary(candidates),
        "candidates": candidates,
        "critique": critique_latest_research() if include_critique else None,
    }


@app.get("/backtests/summary")
def backtests_summary(include_archived: bool = False) -> dict[str, object]:
    return {
        "runs": research_store.list_runs(include_archived=include_archived),
        "engines": available_research_engines(),
        "spread_bet_engines": list_spread_bet_engines(),
    }


@app.get("/templates/summary")
def templates_summary(include_inactive: bool = False, limit: int = Query(default=100, ge=1, le=500)) -> dict[str, object]:
    templates = research_store.list_templates(include_inactive=include_inactive, limit=limit)
    active = [item for item in templates if item["status"] == "active"]
    frozen = [item for item in templates if (item.get("source_template") or {}).get("parameters")]
    paper_ready = [
        item
        for item in templates
        if item.get("readiness_status") == "ready_for_paper" or item.get("promotion_tier") in {"paper_candidate", "validated_candidate"}
    ]
    blocked = [item for item in templates if item.get("readiness_status") == "blocked"]
    return {
        "templates": templates,
        "counts": {
            "visible": len(templates),
            "active": len(active),
            "frozen": len(frozen),
            "paper_ready": len(paper_ready),
            "blocked": len(blocked),
            "markets": len({str(item.get("market_id") or "") for item in templates if item.get("market_id")}),
            "target_regimes": len({str(item.get("target_regime") or "") for item in templates if item.get("target_regime")}),
        },
    }


@app.post("/templates")
def save_strategy_template(payload: StrategyTemplatePayload) -> dict[str, object]:
    return research_store.save_template(payload.model_dump())


@app.patch("/templates/{template_id}/status")
def update_strategy_template_status(template_id: int, payload: StrategyTemplateStatusPayload) -> dict[str, object]:
    if payload.status not in {"active", "paused", "archived"}:
        raise HTTPException(status_code=400, detail="Template status must be active, paused, or archived")
    template = research_store.update_template_status(template_id, payload.status)
    if template is None:
        raise HTTPException(status_code=404, detail="Template not found")
    return template


@app.get("/paper/summary")
def paper_summary() -> dict[str, object]:
    tracked_candidates = _paper_track_candidates()
    return {
        "status": "ready_queue" if tracked_candidates else "not_started",
        "live_ordering_enabled": False,
        "protocol": "30-day paper review with regime gate before any live trading work",
        "tracked_candidates": tracked_candidates,
    }


@app.get("/broker/summary")
def broker_summary() -> dict[str, object]:
    return {
        "live_ordering_enabled": False,
        "order_placement": "disabled",
        "providers": [status.__dict__ for status in settings.statuses()],
        "mode": "demo_read_only",
        "preview_policy": {
            "enabled": True,
            "places_orders": False,
            "default_account_size": WORKING_ACCOUNT_SIZE_GBP,
            "capital_scenarios": list(CAPITAL_SCENARIOS_GBP),
            "checks": [
                "IG minimum deal size",
                "margin estimate",
                "stop/limit distance",
                "1% planned risk per trade",
                "5% daily loss envelope",
            ],
        },
    }


@app.get("/risk/summary")
def risk_summary() -> dict[str, object]:
    return _risk_summary()


@app.get("/settings/summary")
def settings_summary() -> dict[str, object]:
    cache = MarketDataCache()
    return {
        "providers": [status.__dict__ for status in settings.statuses()],
        "cache": {
            "stats": cache.stats().as_dict(),
            "namespaces": cache.namespace_stats(),
            "recent_entries": cache.recent_entries(limit=10),
        },
    }


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
        resolved_market = market
        resolution_note = ""
        if provider is not None and not resolved_market.ig_epic:
            resolved = await _resolve_ig_market(provider, resolved_market)
            if resolved is not None:
                resolved_market, resolution_note = resolved
                markets.upsert(resolved_market)
        if provider is not None and resolved_market.ig_epic:
            try:
                market_details = await provider.market_details(resolved_market.ig_epic)
                profile = profile_from_ig_market(resolved_market, market_details, account_currency)
                if profile.confidence == "ig_live_epic_rules_no_spread":
                    try:
                        recent_price = await provider.recent_price_snapshot(resolved_market.ig_epic)
                    except Exception:
                        recent_price = None
                    if recent_price is not None:
                        profile = profile_from_ig_market(resolved_market, market_details, account_currency, recent_price=recent_price)
                if resolution_note:
                    payload = profile.as_dict()
                    payload["notes"] = list(payload.get("notes", [])) + [resolution_note]
                    profile = IGCostProfile(**{key: value for key, value in payload.items() if key in IGCostProfile.__dataclass_fields__})
            except Exception as exc:
                fallback = profile.as_dict()
                fallback["notes"] = list(fallback.get("notes", [])) + [f"IG sync failed: {_public_error(exc)}"]
                profile = IGCostProfile(**{key: value for key, value in fallback.items() if key in IGCostProfile.__dataclass_fields__})
        research_store.save_cost_profile(profile)
        profiles.append(profile.as_dict())
    return {"status": "synced", "profile_count": len(profiles), "profiles": profiles}


async def _resolve_ig_market(provider: IGDemoProvider, market: MarketMapping) -> tuple[MarketMapping, str] | None:
    queries = [
        market.ig_name,
        *(part.strip() for part in market.ig_search_terms.split(",")),
        market.name,
        market.market_id,
    ]
    seen: set[str] = set()
    for query in queries:
        search_term = str(query or "").strip()
        if not search_term or search_term.lower() in seen:
            continue
        seen.add(search_term.lower())
        try:
            matches = await provider.find_market(search_term)
        except Exception:
            continue
        selected = select_ig_market_candidate(market, matches)
        if selected is None:
            continue
        epic = str(selected.get("epic") or "").strip()
        if not epic:
            continue
        name = str(selected.get("name") or market.ig_name or market.name)
        resolved = MarketMapping(
            market.market_id,
            market.name,
            market.asset_class,
            market.eodhd_symbol,
            epic,
            market.enabled,
            market.plugin_id,
            name,
            market.ig_search_terms,
            market.default_timeframe,
            market.spread_bps,
            market.slippage_bps,
            market.min_backtest_bars,
        )
        return resolved, f"IG EPIC auto-bound from search term '{search_term}' to {epic} ({name})."
    return None


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


@app.post("/broker/order-preview")
def preview_broker_order(payload: BrokerOrderPreviewPayload) -> dict[str, object]:
    market = markets.get(payload.market_id)
    if market is None:
        raise HTTPException(status_code=404, detail="Market not found")
    profile = research_store.get_cost_profile(market.market_id)
    if profile is None:
        profile = public_ig_cost_profile(market).as_dict()
    return broker_order_preview(
        _market_response(market),
        profile,
        payload.side,
        payload.stake,
        payload.account_size,
        entry_price=payload.entry_price,
        stop=payload.stop,
        limit=payload.limit,
    )


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
    effective_search_budget = _effective_search_budget(payload, len(selected_markets))
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
        interval = _run_interval_for_market(payload, market)
        start = _run_start_for_market(payload, market, interval)
        market_status: dict[str, object] = {
            "market_id": market.market_id,
            "name": market.name,
            "eodhd_symbol": market.eodhd_symbol,
            "interval": interval,
            "start": start,
            "end": payload.end,
            "status": "loading",
            "data_source_status": "eodhd_primary_symbol",
            "cost_source_status": "ig_cost_model",
            "effective_search_budget": effective_search_budget or _preset_budget(payload.search_preset),
        }
        if start != payload.start:
            market_status["requested_start"] = payload.start
            market_status["history_expanded"] = True
        market_statuses.append(market_status)
        persist_status()
        if not market.enabled:
            _mark_market_failed(market_status, market_failures, market, f"Market {market.market_id} is disabled")
            persist_status()
            continue
        try:
            bars = await provider.historical_bars(market.eodhd_symbol, interval, start, payload.end)
        except Exception as exc:
            _mark_market_failed(
                market_status,
                market_failures,
                market,
                f"{market.market_id} skipped: {market.eodhd_symbol} EODHD data load failed: {_public_error(exc)}",
            )
            persist_status()
            continue
        required_bars = _minimum_bars_for_interval(market, interval)
        if _should_try_daily_fallback(market, interval, len(bars), required_bars):
            fallback_interval = "1day"
            fallback_start = _run_start_for_market(payload, market, fallback_interval)
            try:
                fallback_bars = await provider.historical_bars(market.eodhd_symbol, fallback_interval, fallback_start, payload.end)
            except Exception as exc:
                market_status["fallback_error"] = _public_error(exc)
            else:
                fallback_required = _minimum_bars_for_interval(market, fallback_interval)
                if _should_use_daily_fallback(len(bars), required_bars, len(fallback_bars), fallback_required):
                    market_status.update(
                        {
                            "requested_interval": interval,
                            "requested_start": market_status.get("requested_start", start),
                            "interval": fallback_interval,
                            "start": fallback_start,
                            "data_source_status": "eodhd_daily_fallback",
                            "fallback_reason": (
                                f"EODHD returned {len(bars)} {interval} bars; "
                                f"using {len(fallback_bars)} {fallback_interval} bars"
                            ),
                            "intraday_bar_count": len(bars),
                            "fallback_bar_count": len(fallback_bars),
                        }
                    )
                    interval = fallback_interval
                    start = fallback_start
                    bars = fallback_bars
                    required_bars = fallback_required
        excluded_months = _normalized_excluded_months(payload.excluded_months)
        if excluded_months:
            original_bar_count = len(bars)
            bars = [bar for bar in bars if _bar_month_key(bar) not in excluded_months]
            market_status.update(
                {
                    "excluded_months": sorted(excluded_months),
                    "excluded_bar_count": original_bar_count - len(bars),
                    "bar_count_before_exclusions": original_bar_count,
                }
            )
            persist_status()
        if len(bars) < required_bars:
            _mark_market_failed(
                market_status,
                market_failures,
                market,
                f"{market.market_id} skipped: need at least {required_bars} {interval} bars; received {len(bars)}",
                bar_count=len(bars),
            )
            persist_status()
            continue
        snapshot = research_store.save_bar_snapshot(
            run_id,
            market.market_id,
            interval,
            "eodhd_primary_symbol",
            start,
            payload.end,
            bars,
        )
        market_status.update({"status": "evaluating", "bar_count": len(bars), "bar_regime": analyze_market_regimes(bars)})
        market_status["bar_snapshot"] = snapshot
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
                        search_budget=effective_search_budget,
                        risk_profile=payload.risk_profile,
                        strategy_families=tuple(payload.strategy_families),
                        cost_stress_multiplier=max(1.0, payload.cost_stress_multiplier),
                        include_regime_scans=payload.include_regime_scans,
                        regime_scan_budget_per_regime=payload.regime_scan_budget_per_regime,
                        target_regime=payload.target_regime,
                        repair_mode=payload.repair_mode,
                        account_size=payload.account_size,
                        source_template=payload.source_template,
                    ),
                )
                market_evaluations = list(result.evaluations)
                regime_scan = getattr(result, "regime_scan", {}) or {}
                market_status["regime_scan_enabled"] = bool(regime_scan.get("enabled"))
                market_status["eligible_regimes"] = regime_scan.get("eligible_regimes", [])
                market_status["regime_scan_trial_count"] = regime_scan.get("trial_count", 0)
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
def list_research_runs(include_archived: bool = False) -> list[dict[str, object]]:
    return research_store.list_runs(include_archived=include_archived)


@app.get("/research/runs/{run_id}")
def get_research_run(run_id: int) -> dict[str, object]:
    run = research_store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Research run not found")
    return {
        **run,
        "trials": [_trial_with_capital(trial) for trial in research_store.list_trials(run_id, limit=25)],
        "candidates": [_candidate_with_capital(candidate) for candidate in research_store.list_candidates(run_id, limit=24)],
        "pareto": research_store.list_pareto(run_id),
        "regime_picks": research_store.list_regime_picks(run_id),
        "bar_snapshots": research_store.list_bar_snapshots(run_id, include_payload=False),
    }


@app.get("/research/runs/{run_id}/trials")
def list_research_trials(run_id: int, limit: int = Query(default=100, ge=1, le=1000)) -> list[dict[str, object]]:
    if research_store.get_run(run_id) is None:
        raise HTTPException(status_code=404, detail="Research run not found")
    return [_trial_with_capital(trial) for trial in research_store.list_trials(run_id, limit=limit)]


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


@app.post("/research/runs/{run_id}/archive")
def archive_research_run(run_id: int) -> dict[str, object]:
    run = research_store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Research run not found")
    if run["status"] in {"created", "running"}:
        raise HTTPException(status_code=409, detail="Running research runs cannot be archived")
    result = research_store.archive_run(run_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Research run not found")
    return {"status": "archived", **result}


@app.get("/research/runs/{run_id}/export")
def export_research_run(run_id: int, include_bars: bool = True) -> Response:
    if research_store.get_run(run_id) is None:
        raise HTTPException(status_code=404, detail="Research run not found")
    try:
        payload = build_research_export_zip(research_store, run_id, include_bars=include_bars)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return Response(
        content=payload,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="slrno-run-{run_id}-evidence.zip"'},
    )


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
def list_research_candidates(limit: int | None = Query(default=None, ge=1, le=500)) -> list[dict[str, object]]:
    return [_candidate_with_capital(candidate) for candidate in research_store.list_candidates(limit=limit)]


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


def _effective_search_budget(payload: ResearchRunPayload, market_count: int) -> int | None:
    if payload.search_budget is not None:
        if _is_frozen_validation_payload(payload):
            return 1
        return max(6, min(500, int(payload.search_budget)))
    if market_count <= 1:
        return None
    preset_budget = _preset_budget(payload.search_preset)
    total_cap = MULTI_MARKET_TOTAL_TRIAL_CAPS.get(payload.search_preset, MULTI_MARKET_TOTAL_TRIAL_CAPS["balanced"])
    minimum = MULTI_MARKET_MIN_TRIALS_PER_MARKET.get(payload.search_preset, MULTI_MARKET_MIN_TRIALS_PER_MARKET["balanced"])
    capped = max(minimum, total_cap // max(1, market_count))
    return min(preset_budget, capped)


def _is_frozen_validation_payload(payload: ResearchRunPayload) -> bool:
    source_template = payload.source_template if isinstance(payload.source_template, dict) else {}
    parameters = source_template.get("parameters")
    return payload.repair_mode == "frozen_validation" and isinstance(parameters, dict) and bool(parameters)


def _preset_budget(search_preset: str) -> int:
    return SEARCH_PRESETS.get(search_preset, SEARCH_PRESETS["balanced"])


def _normalized_excluded_months(months: list[str] | tuple[str, ...] | None) -> set[str]:
    output: set[str] = set()
    for month in months or []:
        key = str(month or "").strip()
        if re.fullmatch(r"\d{4}-\d{2}", key):
            output.add(key)
    return output


def _bar_month_key(bar: object) -> str:
    timestamp = getattr(bar, "timestamp", None)
    if timestamp is None:
        return ""
    return timestamp.strftime("%Y-%m")


def _run_interval_for_market(payload: ResearchRunPayload, market: MarketMapping) -> str:
    requested = str(payload.interval or "").strip()
    if _is_eodhd_monthly_commodity(market):
        return "1month"
    if not requested or requested == "market_default":
        if market.asset_class == "forex":
            return "1hour"
        if market.asset_class == "commodity" and market.default_timeframe in {"1min", "1m", "5min", "5m", "15min", "15m", "30min", "30m", "1hour", "1h"}:
            return "1day"
        return market.default_timeframe
    return requested


def _should_try_daily_fallback(market: MarketMapping, interval: str, bar_count: int, required_bars: int) -> bool:
    if interval not in INTRADAY_INTERVALS:
        return False
    if market.asset_class not in DAILY_FALLBACK_ASSET_CLASSES:
        return False
    if _is_eodhd_monthly_commodity(market):
        return False
    return bar_count < required_bars


def _should_use_daily_fallback(original_count: int, original_required: int, fallback_count: int, fallback_required: int) -> bool:
    if original_count >= original_required or fallback_count <= 0:
        return False
    if fallback_count >= fallback_required:
        return True
    return original_count == 0


def _is_eodhd_monthly_commodity(market: MarketMapping) -> bool:
    if not market.eodhd_symbol.startswith("COMMODITY:"):
        return False
    code = market.eodhd_symbol.removeprefix("COMMODITY:")
    return code in EODHDProvider._MONTHLY_COMMODITIES


def _run_start_for_market(payload: ResearchRunPayload, market: MarketMapping, interval: str) -> str:
    requested_interval = str(payload.interval or "").strip()
    if interval not in {"1month", "1mo", "monthly"}:
        return payload.start
    if requested_interval != "market_default" and not _is_eodhd_monthly_commodity(market):
        return payload.start
    return min(payload.start, _years_before(payload.end, 6))


def _years_before(value: str, years: int) -> str:
    try:
        end = date.fromisoformat(value[:10])
    except ValueError:
        return value
    year = max(1, end.year - years)
    day = end.day
    while day > 28:
        try:
            return end.replace(year=year, day=day).isoformat()
        except ValueError:
            day -= 1
    return end.replace(year=year, day=day).isoformat()


def _minimum_bars_for_interval(market: MarketMapping, interval: str) -> int:
    if interval in {"1day", "1d", "day", "daily"}:
        return min(market.min_backtest_bars, 250)
    if interval in {"1week", "1w", "weekly"}:
        return min(market.min_backtest_bars, 80)
    if interval in {"1month", "1mo", "monthly"}:
        return min(market.min_backtest_bars, 36)
    return market.min_backtest_bars


def _research_run_config(
    payload: ResearchRunPayload,
    selected_markets: list[MarketMapping],
    market_statuses: list[dict[str, object]] | None = None,
    market_failures: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    effective_search_budget = _effective_search_budget(payload, len(selected_markets))
    preset_budget = _preset_budget(payload.search_preset)
    search_budget_mode = "manual" if payload.search_budget is not None else "preset"
    if payload.search_budget is None and len(selected_markets) > 1 and (effective_search_budget or preset_budget) < preset_budget:
        search_budget_mode = "auto_multi_market_cap"
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
        "effective_search_budget": effective_search_budget or preset_budget,
        "search_budget_mode": search_budget_mode,
        "risk_profile": payload.risk_profile,
        "strategy_families": payload.strategy_families,
        "cost_stress_multiplier": payload.cost_stress_multiplier,
        "include_regime_scans": payload.include_regime_scans,
        "regime_scan_budget_per_regime": payload.regime_scan_budget_per_regime,
        "target_regime": payload.target_regime,
        "excluded_months": sorted(_normalized_excluded_months(payload.excluded_months)),
        "repair_mode": payload.repair_mode,
        "account_size": payload.account_size,
        "source_template": _compact_source_template(payload.source_template),
        "product_mode": payload.product_mode,
        "research_only": True,
        "ig_validation_required": True,
        "data_source_policy": "eodhd_primary_symbol_no_silent_proxy",
        "market_statuses": market_statuses or [],
        "market_failures": market_failures or [],
    }


def _compact_source_template(source_template: dict[str, object]) -> dict[str, object]:
    if not isinstance(source_template, dict) or not source_template:
        return {}
    parameters = source_template.get("parameters") if isinstance(source_template.get("parameters"), dict) else {}
    return {
        "name": str(source_template.get("name") or ""),
        "source_id": source_template.get("source_id"),
        "market_id": str(source_template.get("market_id") or parameters.get("market_id") or ""),
        "family": str(source_template.get("family") or parameters.get("family") or ""),
        "style": str(source_template.get("style") or parameters.get("style") or ""),
        "interval": str(source_template.get("interval") or parameters.get("timeframe") or ""),
        "target_regime": str(source_template.get("target_regime") or parameters.get("target_regime") or ""),
        "parameters": {
            str(key): value
            for key, value in parameters.items()
            if key
            in {
                "confidence_quantile",
                "direction",
                "false_breakout_filter",
                "lookback",
                "max_hold_bars",
                "min_hold_bars",
                "min_trade_spacing",
                "month_end_window",
                "month_start_window",
                "position_size",
                "previous_day_filter",
                "regime_filter",
                "stop_loss_bps",
                "take_profit_bps",
                "threshold_bps",
                "volatility_multiplier",
                "weekday",
                "z_threshold",
            }
        },
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


def _trial_with_capital(trial: dict[str, object]) -> dict[str, object]:
    parameters = trial.get("parameters") if isinstance(trial.get("parameters"), dict) else {}
    backtest = trial.get("backtest") if isinstance(trial.get("backtest"), dict) else {}
    market_id = str(parameters.get("market_id") or "")
    profile = research_store.get_cost_profile(market_id) if market_id else None
    scenarios = capital_scenarios(backtest, parameters, profile, account_sizes=_scenario_sizes_for_parameters(parameters))
    return {**trial, "capital_scenarios": scenarios, "capital_summary": capital_summary(scenarios)}


def _candidate_with_capital(candidate: dict[str, object]) -> dict[str, object]:
    audit = candidate.get("audit") if isinstance(candidate.get("audit"), dict) else {}
    candidate_payload = audit.get("candidate") if isinstance(audit.get("candidate"), dict) else {}
    parameters = candidate_payload.get("parameters") if isinstance(candidate_payload.get("parameters"), dict) else {}
    backtest = audit.get("backtest") if isinstance(audit.get("backtest"), dict) else {}
    market_id = str(candidate.get("market_id") or parameters.get("market_id") or "")
    profile = research_store.get_cost_profile(market_id) if market_id else None
    scenarios = capital_scenarios(backtest, parameters, profile, account_sizes=_scenario_sizes_for_parameters(parameters))
    return {**candidate, "capital_scenarios": scenarios, "capital_summary": capital_summary(scenarios)}


def _scenario_sizes_for_parameters(parameters: dict[str, object]) -> tuple[float, ...]:
    search_audit = parameters.get("search_audit") if isinstance(parameters.get("search_audit"), dict) else {}
    return scenario_account_sizes(parameters.get("testing_account_size") or search_audit.get("testing_account_size"))


def _candidate_summary_payload(candidate: dict[str, object]) -> dict[str, object]:
    payload = dict(candidate)
    audit = dict(payload.get("audit") if isinstance(payload.get("audit"), dict) else {})
    candidate_payload = dict(audit.get("candidate") if isinstance(audit.get("candidate"), dict) else {})
    candidate_payload.pop("probability_sample", None)
    candidate_payload.pop("probabilities", None)
    audit["candidate"] = candidate_payload
    audit["backtest"] = _summary_backtest(audit.get("backtest") if isinstance(audit.get("backtest"), dict) else {})
    audit.pop("fold_results", None)
    audit.pop("metrics", None)
    payload["audit"] = audit
    return payload


def _summary_backtest(backtest: dict[str, object]) -> dict[str, object]:
    keep = {
        "cost_confidence",
        "cost_to_gross_ratio",
        "compounded_projection_final_equity",
        "compounded_projection_max_drawdown",
        "compounded_projection_return_pct",
        "compounded_position_sizing",
        "daily_pnl_sample_sharpe",
        "daily_pnl_sharpe",
        "estimated_slippage_bps",
        "estimated_spread_bps",
        "expectancy_per_trade",
        "gross_profit",
        "max_drawdown",
        "net_cost_ratio",
        "net_profit",
        "sample_calendar_days",
        "sample_trading_days",
        "sharpe",
        "sharpe_annualization_note",
        "sharpe_observations",
        "test_profit",
        "total_cost",
        "trade_count",
    }
    return {key: value for key, value in backtest.items() if key in keep}


def _risk_summary() -> dict[str, object]:
    return {
        "capital_scenarios": list(CAPITAL_SCENARIOS_GBP),
        "working_account_size": WORKING_ACCOUNT_SIZE_GBP,
        "risk_per_trade_fraction": RISK_PER_TRADE_FRACTION,
        "daily_loss_fraction": DAILY_LOSS_FRACTION,
        "live_ordering_enabled": False,
        "kill_switch_enabled": True,
        "policy": "paper/demo only; live order placement disabled",
    }


def _paper_track_candidates(limit: int = 50) -> list[dict[str, object]]:
    tracked: list[dict[str, object]] = []
    for candidate in (_candidate_with_capital(item) for item in research_store.list_candidates(limit=limit)):
        audit = candidate.get("audit") if isinstance(candidate.get("audit"), dict) else {}
        readiness = audit.get("promotion_readiness") if isinstance(audit.get("promotion_readiness"), dict) else {}
        if readiness.get("status") != "ready_for_paper":
            continue
        candidate_payload = audit.get("candidate") if isinstance(audit.get("candidate"), dict) else {}
        parameters = candidate_payload.get("parameters") if isinstance(candidate_payload.get("parameters"), dict) else {}
        pattern = parameters.get("bar_pattern_analysis") if isinstance(parameters.get("bar_pattern_analysis"), dict) else {}
        tracked.append(
            {
                "id": candidate.get("id"),
                "run_id": candidate.get("run_id"),
                "strategy_name": candidate.get("strategy_name"),
                "market_id": candidate.get("market_id"),
                "promotion_tier": candidate.get("promotion_tier"),
                "allowed_regimes": pattern.get("allowed_regimes", []),
                "blocked_regimes": pattern.get("blocked_regimes", []),
                "current_regime": (pattern.get("market_regime") or {}).get("current_regime") if isinstance(pattern.get("market_regime"), dict) else None,
                "dominant_profit_regime": (pattern.get("dominant_profit_regime") or {}).get("key") if isinstance(pattern.get("dominant_profit_regime"), dict) else None,
                "capital_summary": candidate.get("capital_summary"),
                "testing_account_size": _testing_account_size(parameters),
                "next_action": readiness.get("next_action"),
            }
        )
    return tracked


def _candidate_queue_summary(candidates: list[dict[str, object]]) -> dict[str, int]:
    summary = {"blocked": 0, "needs_fresh_run": 0, "needs_ig_validation": 0, "paper_ready": 0, "capital_infeasible": 0}
    for candidate in candidates:
        audit = candidate.get("audit") if isinstance(candidate.get("audit"), dict) else {}
        readiness = audit.get("promotion_readiness") if isinstance(audit.get("promotion_readiness"), dict) else {}
        status = str(readiness.get("status") or "blocked")
        blockers = list(readiness.get("blockers") or [])
        if status == "ready_for_paper":
            summary["paper_ready"] += 1
        elif status == "needs_ig_validation":
            summary["needs_ig_validation"] += 1
        else:
            summary["blocked"] += 1
        if any(warning in blockers for warning in ("legacy_sharpe_diagnostics", "missing_cost_profile", "missing_spread_slippage", "short_sharpe_sample", "limited_sharpe_sample")):
            summary["needs_fresh_run"] += 1
        candidate_payload = audit.get("candidate") if isinstance(audit.get("candidate"), dict) else {}
        parameters = candidate_payload.get("parameters") if isinstance(candidate_payload.get("parameters"), dict) else {}
        testing_account = _testing_account_size(parameters)
        capital = candidate.get("capital_summary") if isinstance(candidate.get("capital_summary"), dict) else {}
        if testing_account in (capital.get("blocked_accounts") or []):
            summary["capital_infeasible"] += 1
    return summary


def _testing_account_size(parameters: dict[str, object]) -> float:
    search_audit = parameters.get("search_audit") if isinstance(parameters.get("search_audit"), dict) else {}
    try:
        return float(parameters.get("testing_account_size") or search_audit.get("testing_account_size") or WORKING_ACCOUNT_SIZE_GBP)
    except (TypeError, ValueError):
        return WORKING_ACCOUNT_SIZE_GBP


def _cockpit_next_actions(runs: list[dict[str, object]]) -> list[dict[str, str]]:
    actions: list[dict[str, str]] = []
    if any(run["status"] in {"created", "running"} for run in runs):
        actions.append({"kind": "run_active", "label": "Research run active", "detail": "Check Backtests for progress before starting another run."})
    if not runs:
        actions.append({"kind": "no_runs", "label": "No research runs", "detail": "Start in Backtests when ready to generate evidence."})
    actions.append({"kind": "live_disabled", "label": "Live trading locked", "detail": "Order placement remains disabled; use paper/demo review only."})
    return actions


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
