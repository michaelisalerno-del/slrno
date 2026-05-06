from __future__ import annotations

import asyncio
import re
from dataclasses import asdict
from datetime import date, timedelta
from math import isfinite

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .adaptive_research import (
    FROZEN_PARAMETER_KEYS,
    SEARCH_PRESETS,
    AdaptiveSearchConfig,
    apply_frozen_template_rules,
    available_research_engines,
    run_adaptive_search,
)
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
from .ig_costs import (
    IGCostProfile,
    backtest_config_from_profile,
    normalized_cost_profile_payload,
    profile_from_ig_market,
    public_ig_cost_profile,
    select_ig_market_candidate,
)
from .ig_spread_bet_engines import list_spread_bet_engines
from .market_context import summarize_economic_calendar, unavailable_market_context
from .market_data_cache import MarketDataCache
from .market_discovery import (
    DEFAULT_MAX_MARKET_CAP,
    DEFAULT_MAX_SPREAD_BPS,
    DEFAULT_MIN_MARKET_CAP,
    DEFAULT_MIN_VOLUME,
    MidcapDiscoveryCandidate,
    MidcapDiscoveryCriteria,
    build_midcap_candidates,
    country_exchange_hint,
    fallback_midcap_rows,
)
from .market_plugins import get_market_plugin, list_market_plugins
from .market_registry import MarketMapping, MarketRegistry
from .providers.eodhd import EODHDProvider
from .providers.fmp import FMPProvider
from .providers.fred import FREDProvider
from .providers.ig import IGDemoProvider
from .promotion_readiness import PRICE_VALIDATED_COST_CONFIDENCES
from .research_critic import ResearchCritic
from .research_lab import ResearchStack
from .research_store import ResearchStore
from .settings_store import SettingsStore
from .share_spread_betting import share_spread_bet_model

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
SETTINGS_PROVIDERS = ("eodhd", "fmp", "ig", "ig_accounts")
EODHD_MONTHLY_COMMODITIES = set(EODHDProvider._MONTHLY_COMMODITIES)
FMP_DAILY_BAR_SYMBOLS = {
    "FTSE100": "^FTSE",
}
PRODUCT_MODES = {"spread_bet", "cfd"}
TWO_VCPU_MIDCAP_DISCOVERY_LIMIT = 24
TWO_VCPU_MIDCAP_MARKET_CAP = 3
TWO_VCPU_MIDCAP_SEARCH_BUDGET = 36
TWO_VCPU_MIDCAP_REGIME_BUDGET = 6
TWO_VCPU_MIDCAP_DIAGNOSTIC_LIMIT = 18
GUIDED_AUTO_FREEZE_TRIAL_LIMIT = 250
GUIDED_AUTO_FREEZE_MIN_TRADES = 20
GUIDED_AUTO_FREEZE_MIN_OOS_TRADES = 8
GUIDED_AUTO_FREEZE_TERMINAL_WARNINGS = {
    "diagnostics_deferred_fast_scan",
    "ig_minimum_margin_too_large_for_account",
    "ig_minimum_risk_too_large_for_account",
    "day_trade_forbidden_overnight_family",
    "day_trade_held_overnight",
    "day_trade_missing_flat_policy",
    "day_trade_requires_intraday_bars",
}
GUIDED_AUTO_FREEZE_STRICT_WARNINGS = {
    "best_trades_dominate",
    "costs_overwhelm_edge",
    "fails_higher_slippage",
    "headline_sharpe_not_regime_robust",
    "low_oos_trades",
    "multiple_testing_haircut",
    "needs_ig_price_validation",
    "negative_after_costs",
    "one_fold_dependency",
    "profit_concentrated_single_month",
    "profit_concentrated_single_regime",
    "profits_not_consistent_across_folds",
    "regime_gated_backtest_negative",
    "regime_gated_oos_negative",
    "target_regime_low_oos_trades",
    "too_few_trades",
    "weak_net_cost_efficiency",
    "weak_oos_evidence",
}
MANUAL_TRADER_PLAYBOOKS: dict[str, dict[str, object]] = {
    "opening_range_breakout": {
        "id": "opening_range_breakout",
        "label": "Opening range breakout",
        "description": "Trade only when the frozen signal also breaks the first 30-minute range with VWAP and volume behind it.",
        "families": ["breakout", "volatility_expansion"],
        "min_relative_volume": 0.9,
        "max_spread_bps": 65.0,
        "min_opening_break_bps": 2.0,
        "min_trend_bps": 0.0,
        "require_vwap_alignment": True,
    },
    "vwap_trend_pullback": {
        "id": "vwap_trend_pullback",
        "label": "VWAP trend pullback",
        "description": "Trade with the session trend when price has reclaimed or held VWAP and today has enough participation.",
        "families": ["intraday_trend", "mean_reversion"],
        "min_relative_volume": 0.75,
        "max_spread_bps": 65.0,
        "min_trend_bps": 8.0,
        "max_vwap_distance_against_bps": 12.0,
        "require_vwap_alignment": True,
    },
    "failed_breakout_reversal": {
        "id": "failed_breakout_reversal",
        "label": "Failed breakout reversal",
        "description": "Trade a frozen reversal only after the session has rejected one side of the opening range and moved back through VWAP.",
        "families": ["liquidity_sweep_reversal"],
        "min_relative_volume": 0.8,
        "max_spread_bps": 70.0,
        "min_sweep_bps": 1.0,
        "require_vwap_alignment": True,
    },
    "high_relative_volume_trend": {
        "id": "high_relative_volume_trend",
        "label": "High relative-volume trend",
        "description": "Allow the frozen signal only when current volume is meaningfully ahead of normal and price is moving with VWAP.",
        "families": ["volatility_expansion", "scalping"],
        "min_relative_volume": 1.1,
        "max_spread_bps": 60.0,
        "min_trend_bps": 10.0,
        "require_vwap_alignment": True,
    },
    "frozen_signal_confirmation": {
        "id": "frozen_signal_confirmation",
        "label": "Frozen signal confirmation",
        "description": "Fallback manual gate for frozen templates: fresh same-day bars, acceptable spread, some volume, and no obvious tape conflict.",
        "families": [],
        "min_relative_volume": 0.5,
        "max_spread_bps": 75.0,
        "max_vwap_distance_against_bps": 25.0,
        "require_vwap_alignment": False,
    },
}
MIDCAP_TEMPLATE_DESIGNS: dict[str, dict[str, object]] = {
    "liquid_uk_midcap_trend_pullback": {
        "id": "liquid_uk_midcap_trend_pullback",
        "label": "Liquid UK midcap trend pullback",
        "market_type": "share",
        "country": "UK",
        "behaviour": "Liquid mid-cap shares in a trend-up or orderly pullback regime.",
        "template_goal": "Find reusable intraday/no-overnight pullback rules that can later be repaired, saved, and Freeze validated.",
        "strategy_families": ["intraday_trend", "mean_reversion", "liquidity_sweep_reversal"],
        "market_filters": {
            "min_market_cap": DEFAULT_MIN_MARKET_CAP,
            "max_market_cap": DEFAULT_MAX_MARKET_CAP,
            "min_volume": DEFAULT_MIN_VOLUME,
            "max_spread_bps": DEFAULT_MAX_SPREAD_BPS,
        },
        "run_defaults": {
            "interval": "5min",
            "search_preset": "balanced",
            "search_budget": TWO_VCPU_MIDCAP_SEARCH_BUDGET,
            "regime_scan_budget_per_regime": TWO_VCPU_MIDCAP_REGIME_BUDGET,
            "objective": "profit_first",
            "risk_profile": "conservative",
            "cost_stress_multiplier": 2.5,
        },
        "session_rules": {
            "holding_period": "intraday",
            "force_flat_before_close": True,
            "no_overnight": True,
        },
        "promotion_contract": [
            "Discovery run can create leads, not live rules.",
            "Make Tradeable / Repair Remaining must clear IG, cost, stop, margin, OOS, fold, and regime blockers.",
            "Freeze Validate must retest exact parameters before the template can enter daily paper scanning.",
        ],
    },
    "liquid_uk_midcap_breakout": {
        "id": "liquid_uk_midcap_breakout",
        "label": "Liquid UK midcap breakout",
        "market_type": "share",
        "country": "UK",
        "behaviour": "Liquid mid-cap shares with volatility expansion, range breaks, or opening continuation.",
        "template_goal": "Find intraday breakout templates that still survive realistic spread, slippage, and small-account margin checks.",
        "strategy_families": ["breakout", "volatility_expansion", "intraday_trend"],
        "market_filters": {
            "min_market_cap": DEFAULT_MIN_MARKET_CAP,
            "max_market_cap": DEFAULT_MAX_MARKET_CAP,
            "min_volume": DEFAULT_MIN_VOLUME,
            "max_spread_bps": DEFAULT_MAX_SPREAD_BPS,
        },
        "run_defaults": {
            "interval": "5min",
            "search_preset": "balanced",
            "search_budget": TWO_VCPU_MIDCAP_SEARCH_BUDGET,
            "regime_scan_budget_per_regime": TWO_VCPU_MIDCAP_REGIME_BUDGET,
            "objective": "profit_first",
            "risk_profile": "conservative",
            "cost_stress_multiplier": 2.75,
        },
        "session_rules": {
            "holding_period": "intraday",
            "force_flat_before_close": True,
            "no_overnight": True,
        },
        "promotion_contract": [
            "Discovery run can create leads, not live rules.",
            "Breakout candidates must prove net OOS profit after spread/slippage and avoid one-session dependence.",
            "Freeze Validate must retest exact parameters before the template can enter daily paper scanning.",
        ],
    },
    "liquid_us_midcap_trend_pullback": {
        "id": "liquid_us_midcap_trend_pullback",
        "label": "Liquid US midcap trend pullback",
        "market_type": "share",
        "country": "US",
        "behaviour": "Liquid US mid-cap shares in a trend-up or orderly pullback regime.",
        "template_goal": "Find reusable intraday/no-overnight US pullback rules with FX, spread, margin, and IG catalogue gates explicit.",
        "strategy_families": ["intraday_trend", "mean_reversion", "liquidity_sweep_reversal"],
        "market_filters": {
            "min_market_cap": DEFAULT_MIN_MARKET_CAP,
            "max_market_cap": DEFAULT_MAX_MARKET_CAP,
            "min_volume": DEFAULT_MIN_VOLUME,
            "max_spread_bps": DEFAULT_MAX_SPREAD_BPS,
        },
        "run_defaults": {
            "interval": "5min",
            "search_preset": "balanced",
            "search_budget": TWO_VCPU_MIDCAP_SEARCH_BUDGET,
            "regime_scan_budget_per_regime": TWO_VCPU_MIDCAP_REGIME_BUDGET,
            "objective": "profit_first",
            "risk_profile": "conservative",
            "cost_stress_multiplier": 3.0,
        },
        "session_rules": {
            "holding_period": "intraday",
            "force_flat_before_close": True,
            "no_overnight": True,
        },
        "promotion_contract": [
            "Discovery run can create leads, not live rules.",
            "Make Tradeable / Repair Remaining must clear IG, cost, stop, margin, OOS, fold, FX, and regime blockers.",
            "Freeze Validate must retest exact parameters before the template can enter daily paper scanning.",
        ],
    },
    "liquid_us_midcap_breakout": {
        "id": "liquid_us_midcap_breakout",
        "label": "Liquid US midcap breakout",
        "market_type": "share",
        "country": "US",
        "behaviour": "Liquid US mid-cap shares with volatility expansion, range breaks, or opening continuation.",
        "template_goal": "Find intraday US breakout templates that survive realistic spread, slippage, FX, and small-account margin checks.",
        "strategy_families": ["breakout", "volatility_expansion", "intraday_trend"],
        "market_filters": {
            "min_market_cap": DEFAULT_MIN_MARKET_CAP,
            "max_market_cap": DEFAULT_MAX_MARKET_CAP,
            "min_volume": DEFAULT_MIN_VOLUME,
            "max_spread_bps": DEFAULT_MAX_SPREAD_BPS,
        },
        "run_defaults": {
            "interval": "5min",
            "search_preset": "balanced",
            "search_budget": TWO_VCPU_MIDCAP_SEARCH_BUDGET,
            "regime_scan_budget_per_regime": TWO_VCPU_MIDCAP_REGIME_BUDGET,
            "objective": "profit_first",
            "risk_profile": "conservative",
            "cost_stress_multiplier": 3.25,
        },
        "session_rules": {
            "holding_period": "intraday",
            "force_flat_before_close": True,
            "no_overnight": True,
        },
        "promotion_contract": [
            "Discovery run can create leads, not live rules.",
            "Breakout candidates must prove net OOS profit after spread/slippage/FX and avoid one-session dependence.",
            "Freeze Validate must retest exact parameters before the template can enter daily paper scanning.",
        ],
    },
    "liquid_us_midcap_intraday": {
        "id": "liquid_us_midcap_intraday",
        "label": "Liquid US midcap multi-setup",
        "market_type": "share",
        "country": "US",
        "behaviour": "Liquid US mid-cap shares with intraday continuation, pullback, or reversal setups.",
        "template_goal": "Create no-overnight US share templates while keeping account-size, FX, and IG catalogue gates explicit.",
        "strategy_families": ["intraday_trend", "breakout", "mean_reversion", "liquidity_sweep_reversal"],
        "market_filters": {
            "min_market_cap": DEFAULT_MIN_MARKET_CAP,
            "max_market_cap": DEFAULT_MAX_MARKET_CAP,
            "min_volume": DEFAULT_MIN_VOLUME,
            "max_spread_bps": DEFAULT_MAX_SPREAD_BPS,
        },
        "run_defaults": {
            "interval": "5min",
            "search_preset": "balanced",
            "search_budget": TWO_VCPU_MIDCAP_SEARCH_BUDGET,
            "regime_scan_budget_per_regime": TWO_VCPU_MIDCAP_REGIME_BUDGET,
            "objective": "profit_first",
            "risk_profile": "conservative",
            "cost_stress_multiplier": 3.0,
        },
        "session_rules": {
            "holding_period": "intraday",
            "force_flat_before_close": True,
            "no_overnight": True,
        },
        "promotion_contract": [
            "Discovery run can create leads, not live rules.",
            "US share candidates must keep FX, spread, and minimum-margin pressure visible for the GBP account.",
            "Freeze Validate must retest exact parameters before the template can enter daily paper scanning.",
        ],
    },
}


class EODHDSettings(BaseModel):
    api_token: str = Field(min_length=1)


class FMPSettings(BaseModel):
    api_key: str = Field(min_length=1)


class IGSettings(BaseModel):
    api_key: str = Field(min_length=1)
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)
    account_id: str = ""
    environment: str = "demo"


class IGAccountRolesPayload(BaseModel):
    spread_bet_account_id: str = ""
    cfd_account_id: str = ""
    default_product_mode: str = "spread_bet"


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
    diagnostic_limit: int | None = Field(default=None, ge=1, le=500)
    include_market_context: bool = True
    target_regime: str | None = None
    excluded_months: list[str] = Field(default_factory=list)
    repair_mode: str = "standard"
    account_size: float = Field(default=WORKING_ACCOUNT_SIZE_GBP, gt=0)
    source_template: dict[str, object] = Field(default_factory=dict)
    pipeline: dict[str, object] = Field(default_factory=dict)
    day_trading_mode: bool = False
    force_flat_before_close: bool = False
    paper_queue_limit: int = Field(default=3, ge=1, le=5)
    review_queue_limit: int = Field(default=10, ge=1, le=20)


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
    product_mode: str = "default"
    skip_account_status: bool = False


class BrokerOrderPreviewPayload(BaseModel):
    market_id: str
    side: str = "BUY"
    stake: float = Field(default=1.0, gt=0)
    account_size: float = Field(default=WORKING_ACCOUNT_SIZE_GBP, gt=0)
    entry_price: float | None = Field(default=None, gt=0)
    stop: float | None = Field(default=None, gt=0)
    limit: float | None = Field(default=None, gt=0)


class DailyTemplateScannerPayload(BaseModel):
    trading_date: str | None = None
    market_ids: list[str] = Field(default_factory=list)
    product_mode: str = "spread_bet"
    account_size: float = Field(default=WORKING_ACCOUNT_SIZE_GBP, gt=0)
    paper_limit: int = Field(default=3, ge=1, le=5)
    review_limit: int = Field(default=10, ge=1, le=20)
    lookback_days: int = Field(default=10, ge=1, le=30)
    max_markets: int = Field(default=24, ge=1, le=120)


class DailyTemplateAfterClosePayload(BaseModel):
    results: dict[str, object] = Field(default_factory=dict)
    status: str = "reviewed"


class MidcapTemplatePipelinePayload(BaseModel):
    design_id: str = "liquid_uk_midcap_trend_pullback"
    country: str = "UK"
    product_mode: str = "spread_bet"
    account_size: float = Field(default=WORKING_ACCOUNT_SIZE_GBP, gt=0)
    limit: int = Field(default=TWO_VCPU_MIDCAP_DISCOVERY_LIMIT, ge=1, le=120)
    max_markets: int = Field(default=TWO_VCPU_MIDCAP_MARKET_CAP, ge=1, le=20)
    min_market_cap: float = Field(default=DEFAULT_MIN_MARKET_CAP, ge=0)
    max_market_cap: float = Field(default=DEFAULT_MAX_MARKET_CAP, ge=0)
    min_volume: float = Field(default=DEFAULT_MIN_VOLUME, ge=0)
    max_spread_bps: float = Field(default=DEFAULT_MAX_SPREAD_BPS, ge=1)
    start: str = "2025-01-01"
    end: str = Field(default_factory=lambda: date.today().isoformat())
    auto_install: bool = True
    auto_sync_costs: bool = True
    auto_start_run: bool = True


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "mode": "paper"}


def _settings_provider_statuses() -> list[dict[str, object]]:
    statuses = {status.provider: status.__dict__ for status in settings.statuses()}
    for provider in SETTINGS_PROVIDERS:
        statuses.setdefault(
            provider,
            {
                "provider": provider,
                "configured": False,
                "last_status": "not_configured",
                "last_error": None,
            },
        )
    return [statuses[provider] for provider in SETTINGS_PROVIDERS] + [
        status for provider, status in sorted(statuses.items()) if provider not in SETTINGS_PROVIDERS
    ]


async def _market_context_summary_for_range(start: str, end: str, market_id: str = "", limit: int = 12) -> dict[str, object]:
    api_key = settings.get_secret("fmp", "api_key")
    if not api_key:
        return _market_context_unavailable("FMP API key is not configured", start, end, market_id)
    provider = FMPProvider(api_key)
    try:
        events = await provider.economic_calendar(start, end)
    except Exception as exc:
        detail = _public_error(exc)
        fallback = await _partial_fmp_calendar_summary(provider, start, end, market_id, limit, detail)
        if fallback is not None:
            return fallback
        return _market_context_unavailable(detail, start, end, market_id)
    summary = summarize_economic_calendar(events, start, end, market_id=market_id, limit=limit)
    return _calendar_coverage(summary, start, end, start, end, "full")


async def _partial_fmp_calendar_summary(
    provider: FMPProvider,
    start: str,
    end: str,
    market_id: str,
    limit: int,
    reason: str,
) -> dict[str, object] | None:
    if not _is_fmp_calendar_plan_limit(reason):
        return None
    requested_start = _parse_iso_date(start)
    requested_end = _parse_iso_date(end)
    if requested_start is None or requested_end is None:
        return None
    fallback_start = max(date(requested_end.year, 1, 1), requested_start)
    if fallback_start <= requested_start or fallback_start > requested_end:
        return None
    try:
        events = await provider.economic_calendar(fallback_start.isoformat(), requested_end.isoformat())
    except Exception:
        return None
    summary = summarize_economic_calendar(events, fallback_start.isoformat(), requested_end.isoformat(), market_id=market_id, limit=limit)
    summary = _calendar_coverage(
        summary,
        start,
        end,
        fallback_start.isoformat(),
        requested_end.isoformat(),
        "partial_recent",
    )
    summary["reason"] = "FMP plan only allows recent calendar history; using supported recent calendar window."
    completeness = summary.get("data_completeness") if isinstance(summary.get("data_completeness"), dict) else {}
    summary["data_completeness"] = {
        **completeness,
        "partial_reason": reason,
    }
    return summary


def _calendar_coverage(
    summary: dict[str, object],
    requested_start: str,
    requested_end: str,
    coverage_start: str,
    coverage_end: str,
    status: str,
) -> dict[str, object]:
    completeness = summary.get("data_completeness") if isinstance(summary.get("data_completeness"), dict) else {}
    summary.update(
        {
            "requested_start": requested_start,
            "requested_end": requested_end,
            "coverage_start": coverage_start,
            "coverage_end": coverage_end,
            "coverage_status": status,
        }
    )
    summary["data_completeness"] = {
        **completeness,
        "events_exact_for_full_range": status == "full",
        "requested_start": requested_start,
        "requested_end": requested_end,
        "coverage_start": coverage_start,
        "coverage_end": coverage_end,
    }
    return summary


def _is_fmp_calendar_plan_limit(detail: str) -> bool:
    normalized = detail.lower()
    return "http 402" in normalized or "not available under your current subscription" in normalized or "premium query parameter" in normalized


def _parse_iso_date(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _market_context_unavailable(reason: str, start: str, end: str, market_id: str = "") -> dict[str, object]:
    summary = unavailable_market_context(reason)
    summary.update({"start": start, "end": end, "market_id": market_id})
    return summary


async def _fred_context_summary_for_range(start: str, end: str) -> dict[str, object]:
    provider = FREDProvider()
    vix = await _safe_fred_series(provider, "VIXCLS", start, end)
    high_yield = await _safe_fred_series(provider, "BAMLH0A0HYM2", start, end)
    yield_curve = await _safe_fred_series(provider, "T10Y2Y", start, end)
    return {
        "volatility": _vix_context(vix),
        "macro": {
            "provider": "fred_public_csv",
            "available": bool(high_yield.get("available") or yield_curve.get("available")),
            "high_yield_spread": _spread_context(high_yield),
            "yield_curve_10y2y": _yield_curve_context(yield_curve),
        },
    }


async def _safe_fred_series(provider: FREDProvider, series_id: str, start: str, end: str) -> dict[str, object]:
    try:
        rows = await provider.series(series_id, start=start, end=end)
    except Exception as exc:
        return {"series_id": series_id, "available": False, "reason": _public_error(exc), "rows": []}
    return {"series_id": series_id, "available": bool(rows), "rows": rows}


def _vix_context(series: dict[str, object]) -> dict[str, object]:
    payload = _series_stats(series)
    latest = float(payload.get("latest_value") or 0.0)
    avg20 = float(payload.get("average_20") or 0.0)
    if not payload.get("available"):
        regime = "unavailable"
    elif latest >= 25 or (avg20 > 0 and latest > avg20 * 1.25):
        regime = "high_volatility"
    elif latest >= 20 or (avg20 > 0 and latest > avg20 * 1.1):
        regime = "elevated_volatility"
    elif latest <= 15 and (avg20 <= 0 or latest <= avg20):
        regime = "low_volatility"
    else:
        regime = "normal_volatility"
    return {"provider": "fred_public_csv", "name": "VIXCLS", "regime": regime, **payload}


def _spread_context(series: dict[str, object]) -> dict[str, object]:
    payload = _series_stats(series)
    latest = float(payload.get("latest_value") or 0.0)
    change20 = float(payload.get("change_20") or 0.0)
    if not payload.get("available"):
        risk = "unavailable"
    elif latest >= 6 or change20 >= 0.5:
        risk = "credit_stress"
    elif latest >= 4 or change20 >= 0.25:
        risk = "credit_widening"
    else:
        risk = "credit_stable"
    return {"name": "BAMLH0A0HYM2", "risk": risk, **payload}


def _yield_curve_context(series: dict[str, object]) -> dict[str, object]:
    payload = _series_stats(series)
    latest = float(payload.get("latest_value") or 0.0)
    if not payload.get("available"):
        regime = "unavailable"
    elif latest < 0:
        regime = "inverted"
    elif latest < 0.5:
        regime = "flat"
    else:
        regime = "normal"
    return {"name": "T10Y2Y", "regime": regime, **payload}


def _series_stats(series: dict[str, object]) -> dict[str, object]:
    rows = series.get("rows") if isinstance(series.get("rows"), list) else []
    values = [float(row["value"]) for row in rows if isinstance(row, dict) and _is_number(row.get("value"))]
    latest_row = next((row for row in reversed(rows) if isinstance(row, dict) and _is_number(row.get("value"))), None)
    if not values or latest_row is None:
        return {"available": False, "series_id": series.get("series_id"), "reason": series.get("reason"), "observation_count": 0}
    window = values[-20:]
    previous = values[-21] if len(values) > 20 else values[0]
    latest = float(latest_row["value"])
    return {
        "available": True,
        "series_id": series.get("series_id"),
        "latest_date": latest_row.get("date"),
        "latest_value": round(latest, 6),
        "average_20": round(sum(window) / len(window), 6) if window else 0.0,
        "change_20": round(latest - previous, 6),
        "observation_count": len(values),
    }


def _is_number(value: object) -> bool:
    try:
        return isfinite(float(value))
    except (TypeError, ValueError):
        return False


@app.get("/cockpit/summary")
def cockpit_summary() -> dict[str, object]:
    runs = research_store.list_runs()
    running = [run for run in runs if run["status"] in {"created", "running"}]
    latest = runs[0] if runs else None
    return {
        "mode": "paper",
        "live_ordering_enabled": False,
        "providers": _settings_provider_statuses(),
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


@app.get("/day-trading/factory/summary")
def day_trading_factory_summary(
    account_size: float = Query(default=WORKING_ACCOUNT_SIZE_GBP, gt=0),
    paper_limit: int = Query(default=3, ge=1, le=5),
    review_limit: int = Query(default=10, ge=1, le=20),
) -> dict[str, object]:
    templates = research_store.list_templates(limit=250)
    candidates = [_candidate_with_capital(candidate) for candidate in research_store.list_candidates(limit=250)]
    day_templates = [template for template in templates if _is_day_trading_template(template)]
    frozen_day_templates = [template for template in day_templates if _is_frozen_template(template)]
    overnight_templates = [template for template in templates if _is_overnight_template(template)]
    daily_templates = [_day_trading_template_payload(template, account_size) for template in frozen_day_templates]
    daily_templates = [template for template in daily_templates if template is not None]
    unsuitable = [template for template in daily_templates if template["unsuitable"]]
    eligible = [template for template in daily_templates if not template["unsuitable"] and template["eligible_for_review"]]
    paper_ready = [template for template in eligible if template["paper_ready"]]
    review_signals = sorted(eligible, key=_day_trading_signal_rank, reverse=True)[:review_limit]
    paper_queue = sorted(paper_ready, key=_day_trading_signal_rank, reverse=True)[:paper_limit]
    discovery_leads = [_day_trading_signal_payload(candidate, account_size) for candidate in candidates if _is_day_trading_source(candidate)]
    discovery_leads = [candidate for candidate in discovery_leads if candidate is not None]
    non_frozen_day_templates = [template for template in day_templates if not _is_frozen_template(template)]
    latest_scan = research_store.latest_day_trading_scan() if hasattr(research_store, "latest_day_trading_scan") else None
    latest_daily_queue = latest_scan.get("daily_paper_queue", []) if isinstance(latest_scan, dict) else []
    latest_review_signals = latest_scan.get("review_signals", []) if isinstance(latest_scan, dict) else []
    latest_unsuitable = latest_scan.get("unsuitable", []) if isinstance(latest_scan, dict) else []
    latest_config = latest_scan.get("config", {}) if isinstance(latest_scan, dict) and isinstance(latest_scan.get("config"), dict) else {}
    latest_no_setup = latest_config.get("no_setup_sample", []) if isinstance(latest_config.get("no_setup_sample"), list) else []
    return {
        "schema": "day_trading_template_factory_v2",
        "mode": "manual",
        "phase": "daily_template_matcher",
        "account_size": account_size,
        "live_ordering_enabled": False,
        "order_placement": "disabled",
        "strategy_generation_allowed": False,
        "daily_mode_source": "active_frozen_template_library_only",
        "policy": _day_trading_policy(paper_limit, review_limit, account_size),
        "counts": {
            "day_trading_candidates": len(discovery_leads),
            "discovery_leads_needing_freeze": len(discovery_leads),
            "day_trading_templates": len(day_templates),
            "frozen_day_templates": len(frozen_day_templates),
            "non_frozen_day_templates": len(non_frozen_day_templates),
            "template_ready_for_scan": len(paper_queue),
            "eligible_review_signals": len(latest_review_signals),
            "daily_paper_queue": len(latest_daily_queue),
            "unsuitable": len(latest_unsuitable) if latest_scan else len(unsuitable),
            "no_setup": len(latest_no_setup),
            "today_filter_blockers": len(latest_config.get("today_filter_blocker_counts", {})) if isinstance(latest_config.get("today_filter_blocker_counts"), dict) else 0,
            "overnight_or_swing_templates": len(overnight_templates),
        },
        "daily_paper_queue": latest_daily_queue,
        "review_signals": latest_review_signals,
        "unsuitable": (latest_unsuitable if latest_scan else unsuitable)[:review_limit],
        "no_setup_sample": latest_no_setup[:review_limit],
        "template_ready_without_scan": paper_queue,
        "latest_scan": latest_scan,
        "template_library": {
            "day_trading_templates": [_template_queue_payload(template) for template in day_templates[:review_limit]],
            "needs_freeze_validation": [_template_queue_payload(template) for template in non_frozen_day_templates[:review_limit]],
            "overnight_or_swing_templates": [_template_queue_payload(template) for template in overnight_templates[:review_limit]],
        },
        "manual_playbooks": [_manual_playbook_payload(playbook) for playbook in MANUAL_TRADER_PLAYBOOKS.values()],
        "discovery_leads_not_live": sorted(discovery_leads, key=_day_trading_signal_rank, reverse=True)[:review_limit],
        "next_actions": [
            "Use Discovery mode to find or repair ideas, then save and Freeze validate them.",
            "Keep only active frozen intraday/no-overnight templates in the daily queue.",
            "At market open, match frozen templates to today's tape: relative volume, VWAP, opening range, spread, and broker-safe capital fit.",
            "After close, review expected versus actual; do not change live/paper rules without another validation cycle.",
        ],
    }


@app.post("/day-trading/scanner/start")
async def start_daily_template_scanner(payload: DailyTemplateScannerPayload) -> dict[str, object]:
    api_token = settings.get_secret("eodhd", "api_token")
    if api_token is None:
        raise HTTPException(status_code=400, detail="EODHD API token is required before starting the daily template scanner")
    return await _run_daily_template_scanner(payload, api_token)


@app.get("/day-trading/scanner/latest")
def latest_daily_template_scan() -> dict[str, object]:
    latest = research_store.latest_day_trading_scan()
    if latest is None:
        return {"status": "not_started", "latest_scan": None}
    return {"status": latest.get("status", "unknown"), "latest_scan": latest}


@app.post("/day-trading/scanner/{scan_id}/after-close")
def record_daily_template_after_close(scan_id: int, payload: DailyTemplateAfterClosePayload) -> dict[str, object]:
    if payload.status not in {"reviewed", "closed", "error"}:
        raise HTTPException(status_code=400, detail="After-close status must be reviewed, closed, or error")
    scan = research_store.update_day_trading_scan_results(scan_id, payload.results, payload.status)
    if scan is None:
        raise HTTPException(status_code=404, detail="Daily template scan not found")
    return scan


@app.get("/day-trading/template-designs")
def day_trading_template_designs() -> dict[str, object]:
    return {
        "schema": "day_trading_template_designs_v1",
        "designs": [_template_design_payload(design) for design in MIDCAP_TEMPLATE_DESIGNS.values()],
        "manual_playbooks": [_manual_playbook_payload(playbook) for playbook in MANUAL_TRADER_PLAYBOOKS.values()],
        "policy": {
            "daily_mode_source": "active_frozen_template_library_only",
            "strategy_generation_allowed_in_daily_mode": False,
            "design_mode": "research_discovery_only",
            "live_ordering_enabled": False,
            "order_placement": "disabled",
        },
    }


@app.get("/day-trading/manual-playbooks")
def day_trading_manual_playbooks() -> dict[str, object]:
    return {
        "schema": "manual_trader_playbooks_v1",
        "daily_mode_source": "active_frozen_template_library_only",
        "strategy_generation_allowed": False,
        "playbooks": [_manual_playbook_payload(playbook) for playbook in MANUAL_TRADER_PLAYBOOKS.values()],
        "today_filters": [
            "same-day 5-minute bars",
            "relative volume versus recent sessions",
            "opening range break or rejection",
            "VWAP alignment",
            "session trend",
            "current spread/slippage and broker-safe preview",
            "IG minimum stake, stop, margin, and £3k capital fit",
        ],
    }


@app.post("/day-trading/midcap-template-pipeline/start")
async def start_midcap_template_pipeline(
    payload: MidcapTemplatePipelinePayload,
    background_tasks: BackgroundTasks,
) -> dict[str, object]:
    api_token = settings.get_secret("eodhd", "api_token")
    if payload.auto_start_run and api_token is None:
        raise HTTPException(status_code=400, detail="EODHD API token is required before starting a midcap template-design run")
    return await _run_midcap_template_pipeline(payload, background_tasks, api_token or "")


@app.get("/broker/summary")
def broker_summary() -> dict[str, object]:
    return {
        "live_ordering_enabled": False,
        "order_placement": "disabled",
        "providers": _settings_provider_statuses(),
        "mode": "demo_read_only",
        "ig_account_roles": _ig_account_roles_summary(),
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
        "providers": _settings_provider_statuses(),
        "ig_account_roles": _ig_account_roles_summary(),
        "cache": {
            "stats": cache.stats().as_dict(),
            "namespaces": cache.namespace_stats(),
            "recent_entries": cache.recent_entries(limit=10),
        },
    }


@app.get("/settings/status")
def settings_status() -> list[dict[str, object]]:
    return _settings_provider_statuses()


@app.get("/market-context/summary")
async def market_context_summary(
    start: date | None = Query(default=None),
    end: date | None = Query(default=None),
    market_id: str = "",
) -> dict[str, object]:
    today = date.today()
    start_date = start or today - timedelta(days=7)
    end_date = end or today + timedelta(days=21)
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="Market context end date must be on or after start date")
    return await _market_context_summary_for_range(start_date.isoformat(), end_date.isoformat(), market_id=market_id, limit=16)


@app.get("/market-context/stack")
async def market_context_stack(
    start: date | None = Query(default=None),
    end: date | None = Query(default=None),
    market_id: str = "",
) -> dict[str, object]:
    today = date.today()
    start_date = start or today - timedelta(days=90)
    end_date = end or today
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="Market context end date must be on or after start date")
    calendar = await _market_context_summary_for_range(
        (today - timedelta(days=7)).isoformat(),
        (today + timedelta(days=21)).isoformat(),
        market_id=market_id,
        limit=12,
    )
    macro = await _fred_context_summary_for_range(start_date.isoformat(), end_date.isoformat())
    return {
        "schema": "market_context_stack_v1",
        "market_id": market_id,
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "calendar": calendar,
        "volatility": macro["volatility"],
        "macro": macro["macro"],
        "positioning": {
            "provider": "cftc_cot",
            "available": False,
            "status": "planned",
            "next_use": "COT positioning filter for index, FX, metals, and oil templates.",
        },
        "breadth": {
            "provider": "market_breadth",
            "available": False,
            "status": "planned",
            "next_use": "Breadth confirmation for S&P/Nasdaq trend and breakout templates.",
        },
        "tick_quote": {
            "provider": "shortlisted_market_quote_data",
            "available": False,
            "status": "shortlist_only",
            "next_use": "Use only after a template survives bars, calendar, macro, costs, OOS, and paper evidence.",
        },
    }


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


@app.post("/settings/ig/accounts")
async def save_ig_account_roles(payload: IGAccountRolesPayload) -> dict[str, object]:
    product_mode = _normalize_product_mode(payload.default_product_mode)
    resolved_roles = await _resolve_ig_account_roles(payload)
    _store_ig_account_role("spread_bet", resolved_roles["spread_bet"])
    _store_ig_account_role("cfd", resolved_roles["cfd"])
    settings.set_secret("ig_accounts", "default_product_mode", product_mode)
    settings.set_status("ig_accounts", "saved")
    return {"status": "saved", "ig_account_roles": _ig_account_roles_summary()}


@app.get("/markets")
def list_markets() -> list[dict[str, object]]:
    return [_market_response(market) for market in markets.list()]


@app.get("/markets/discovery/midcaps")
async def discover_midcap_markets(
    country: str = "UK",
    product_mode: str = "spread_bet",
    limit: int = Query(default=40, ge=1, le=120),
    min_market_cap: float = Query(default=DEFAULT_MIN_MARKET_CAP, ge=0),
    max_market_cap: float = Query(default=DEFAULT_MAX_MARKET_CAP, ge=0),
    min_volume: float = Query(default=DEFAULT_MIN_VOLUME, ge=0),
    max_spread_bps: float = Query(default=DEFAULT_MAX_SPREAD_BPS, ge=1),
    account_size: float = Query(default=WORKING_ACCOUNT_SIZE_GBP, gt=0),
    verify_ig: bool = True,
    require_ig_catalogue: bool = True,
) -> dict[str, object]:
    criteria = MidcapDiscoveryCriteria(
        country=country,
        product_mode=_normalize_product_mode(product_mode),
        min_market_cap=min_market_cap,
        max_market_cap=max_market_cap,
        min_volume=min_volume,
        max_spread_bps=max_spread_bps,
        account_size=account_size,
    )
    source = "eodhd_stock_screener"
    rows: list[dict[str, object]] = []
    source_errors: list[dict[str, str]] = []
    eodhd_error = ""
    fmp_error = ""
    exchange_hint, country_hint = country_exchange_hint(country)
    exchanges = ["NASDAQ", "NYSE", "AMEX"] if country_hint == "US" else [exchange_hint]
    eodhd_api_token = settings.get_secret("eodhd", "api_token")
    if eodhd_api_token:
        provider = EODHDProvider(eodhd_api_token)
        per_exchange_row_target = max(limit, 40)
        for exchange in [item for item in exchanges if item]:
            try:
                exchange_row_count = 0
                for offset in range(0, 1000, 100):
                    page = await provider.stock_screener(
                        exchange=exchange,
                        market_cap_more_than=min_market_cap,
                        market_cap_lower_than=max_market_cap,
                        min_volume=min_volume,
                        limit=100,
                        offset=offset,
                    )
                    filtered_rows = _eodhd_midcap_rows(page, criteria, country_hint)
                    rows.extend(filtered_rows)
                    exchange_row_count += len(filtered_rows)
                    if exchange_row_count >= per_exchange_row_target or len(page) < 100:
                        break
            except Exception as exc:
                eodhd_error = _public_error(exc)
                source_errors.append({"provider": "eodhd", "operation": "stock_screener", "detail": eodhd_error})
                if rows:
                    break
            if country_hint != "US" and len(rows) >= limit:
                break
    fmp_api_key = settings.get_secret("fmp", "api_key")
    if not rows and fmp_api_key:
        source = "fmp_company_screener"
        provider = FMPProvider(fmp_api_key)
        for exchange in exchanges:
            try:
                rows.extend(
                    await provider.company_screener(
                        exchange=exchange,
                        country=country_hint,
                        market_cap_more_than=min_market_cap,
                        market_cap_lower_than=max_market_cap,
                        min_volume=min_volume,
                        limit=limit,
                    )
                )
            except Exception as exc:
                fmp_error = _public_error(exc)
                source_errors.append({"provider": "fmp", "operation": "company_screener", "detail": fmp_error})
                if rows:
                    break
    if not rows:
        rows = fallback_midcap_rows(country)
        source = "built_in_uk_midcap_starter" if not source_errors else "built_in_fallback_after_provider_error"
        if fmp_api_key and rows:
            try:
                rows = await _enrich_midcap_fallback_rows_with_fmp_quotes(FMPProvider(fmp_api_key), rows)
                source = "built_in_midcap_starter_with_fmp_quotes"
            except Exception as exc:
                fmp_error = f"{fmp_error}; FMP batch quote fallback failed: {_public_error(exc)}" if fmp_error else _public_error(exc)
                source_errors.append({"provider": "fmp", "operation": "batch_quote_fallback", "detail": _public_error(exc)})
    candidates = build_midcap_candidates(rows, criteria, source)[:limit]
    ig_status = "not_checked"
    if require_ig_catalogue and not verify_ig:
        ig_status = "ig_required_not_checked"
        candidates = [
            candidate.with_ig_blocker("not_checked", "ig_catalogue_not_checked", "ig_catalogue_required")
            for candidate in candidates
        ]
    elif verify_ig:
        provider = _ig_provider_from_settings(criteria.product_mode)
        if provider is None:
            ig_status = "ig_not_configured"
            if require_ig_catalogue:
                blocker, warning = _ig_provider_blocker(criteria.product_mode)
                candidates = [
                    candidate.with_ig_blocker("ig_not_configured", blocker, warning)
                    for candidate in candidates
                ]
        else:
            candidates = await _verify_midcap_candidates_with_ig(provider, candidates)
            ig_status = "checked"
    candidate_payloads = [candidate.as_dict() for candidate in candidates]
    return {
        "schema": "midcap_discovery_v1",
        "country": country,
        "product_mode": criteria.product_mode,
        "account_size": criteria.account_size,
        "data_source": source,
        "eodhd_error": eodhd_error,
        "fmp_error": fmp_error,
        "source_errors": source_errors,
        "ig_status": ig_status,
        "ig_catalogue_required": require_ig_catalogue,
        "criteria": criteria.as_dict(),
        "candidate_count": len(candidates),
        "eligible_count": sum(1 for candidate in candidates if candidate.eligible),
        "blocked_count": sum(1 for candidate in candidates if not candidate.eligible),
        "blocker_counts": _candidate_issue_counts(candidate_payloads, "blockers"),
        "warning_counts": _candidate_issue_counts(candidate_payloads, "warnings"),
        "candidates": candidate_payloads,
        "screening_pipeline": [
            "EODHD stock screener for market cap, volume, and exchange filters.",
            "FMP company-screener only as a secondary fallback.",
            "FMP batch-quote enrichment for the starter universe when the screener is unavailable.",
            "IG /markets search catalogue match as the hard eligibility gate.",
            "Installed markets still need IG cost sync and normal paper-readiness checks before promotion.",
        ],
        "notes": [
            "Discovery only installs a market mapping; candidates still need full backtest, IG cost sync, OOS, fold, regime, and paper gates.",
            "Eligible means the candidate passed the financial filters and was found in the selected IG account catalogue.",
            "CFD account roles can be saved now, but the current research engine still uses the spread-bet cost model unless a future CFD cost model is added.",
        ],
    }


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
    product_mode = None if payload.product_mode == "default" else _normalize_product_mode(payload.product_mode)
    provider = _ig_provider_from_settings(product_mode)
    account_currency = "GBP"
    provider_warning = ""
    account_status_rate_limited = False
    price_validation_rate_limited = False
    if provider is not None and not payload.skip_account_status:
        try:
            account_currency = (await provider.account_status()).currency or "GBP"
        except Exception as exc:
            status_error = _public_error(exc)
            account_status_rate_limited = _looks_like_ig_rate_limit(status_error)
            provider_warning = f"IG account status check failed, using GBP account currency fallback before EPIC price validation: {status_error}"
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
                    price_errors: list[str] = []
                    recent_price = await _recent_ig_price_snapshot(provider, resolved_market, price_errors)
                    if recent_price is not None:
                        profile = profile_from_ig_market(resolved_market, market_details, account_currency, recent_price=recent_price)
                    elif any(_looks_like_ig_rate_limit(error) for error in price_errors):
                        price_validation_rate_limited = True
                        profile = _cost_profile_with_notes(
                            profile,
                            ["IG /prices history validation was rate-limited, so this EPIC still needs price validation before research."],
                        )
                if resolution_note:
                    profile = _cost_profile_with_notes(profile, [resolution_note])
            except Exception as exc:
                sync_error = _public_error(exc)
                price_errors: list[str] = []
                recent_price = await _recent_ig_price_snapshot(provider, resolved_market, price_errors)
                if recent_price is not None:
                    profile = profile_from_ig_market(
                        resolved_market,
                        {
                            "instrument": {
                                "epic": resolved_market.ig_epic,
                                "name": resolved_market.ig_name or resolved_market.name,
                                "type": resolved_market.asset_class,
                            },
                            "snapshot": {},
                            "dealingRules": {},
                        },
                        account_currency,
                        recent_price=recent_price,
                    )
                    profile = _cost_profile_with_notes(
                        profile,
                        [f"IG market detail sync failed ({sync_error}), but recent IG /prices history validated the EPIC reference price."],
                    )
                else:
                    profile = _cost_profile_with_notes(profile, [f"IG sync failed: {sync_error}"])
                    if _looks_like_ig_rate_limit(sync_error) or any(_looks_like_ig_rate_limit(error) for error in price_errors):
                        price_validation_rate_limited = True
                        profile = _cost_profile_with_notes(
                            profile,
                            ["IG price validation was rate-limited; retry after the IG API allowance cools down."],
                        )
        if provider_warning:
            profile = _cost_profile_with_notes(profile, [provider_warning])
        research_store.save_cost_profile(profile)
        profiles.append(profile.as_dict())
    price_validated_count = sum(1 for profile in profiles if str(profile.get("confidence") or "") in PRICE_VALIDATED_COST_CONFIDENCES)
    ig_rate_limited = price_validation_rate_limited and price_validated_count == 0
    status = (
        "synced"
        if price_validated_count == len(profiles)
        else "ig_rate_limited"
        if ig_rate_limited
        else "synced_needs_price_validation"
    )
    return {
        "status": status,
        "profile_count": len(profiles),
        "price_validated_count": price_validated_count,
        "ig_rate_limited": ig_rate_limited,
        "price_validation_rate_limited": price_validation_rate_limited,
        "account_status_rate_limited": account_status_rate_limited,
        "profiles": profiles,
    }


def _cost_profile_with_notes(profile: IGCostProfile, notes: list[str]) -> IGCostProfile:
    payload = profile.as_dict()
    existing_notes = list(payload.get("notes", []))
    for note in notes:
        if note and note not in existing_notes:
            existing_notes.append(note)
    payload["notes"] = existing_notes
    return IGCostProfile(**{key: value for key, value in payload.items() if key in IGCostProfile.__dataclass_fields__})


def _looks_like_ig_rate_limit(text: str) -> bool:
    lowered = str(text or "").lower()
    return "exceeded" in lowered and ("api key" in lowered or "rate" in lowered or "allowance" in lowered or "api-key" in lowered)


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


async def _recent_ig_price_snapshot(
    provider: IGDemoProvider,
    market: MarketMapping,
    errors: list[str] | None = None,
) -> dict[str, object] | None:
    resolutions = ("MINUTE_5", "MINUTE", "HOUR", "DAY")
    if market.asset_class == "share" or ".DAILY." in market.ig_epic.upper():
        resolutions = ("MINUTE_5", "DAY", "HOUR", "MINUTE")
    for resolution in resolutions:
        try:
            snapshot = await provider.recent_price_snapshot(market.ig_epic, resolution=resolution)
        except Exception as exc:
            if errors is not None:
                errors.append(_public_error(exc))
            continue
        if snapshot is not None:
            return snapshot
    return None


@app.get("/ig/markets/{market_id}/cost-profile")
def get_ig_cost_profile(market_id: str) -> dict[str, object]:
    market = markets.get(market_id)
    if market is None:
        raise HTTPException(status_code=404, detail="Market not found")
    stored = _cost_profile_payload_for_market(market)
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
    fmp_api_key = settings.get_secret("fmp", "api_key")
    fmp_provider = FMPProvider(fmp_api_key) if fmp_api_key else None
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
        bar_source = "eodhd_primary_symbol"
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
        if payload.include_market_context:
            market_status["market_context"] = await _market_context_summary_for_range(start, payload.end, market_id=market.market_id, limit=8)
        else:
            market_status["market_context"] = _market_context_unavailable(
                "Skipped by the fast 2 vCPU guided scan profile.",
                start,
                payload.end,
                market_id=market.market_id,
            )
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
        if _should_try_fmp_daily_fallback(market, interval, len(bars), required_bars):
            fallback_symbol = _fmp_daily_symbol_for_market(market)
            fallback_interval = "1day"
            fallback_start = _run_start_for_market(payload, market, fallback_interval)
            market_status["fmp_symbol"] = fallback_symbol
            if fmp_provider is None:
                market_status["fmp_fallback_error"] = "FMP API key is not configured"
            else:
                try:
                    fmp_bars = await fmp_provider.historical_bars(fallback_symbol, fallback_interval, fallback_start, payload.end)
                except Exception as exc:
                    market_status["fmp_fallback_error"] = _public_error(exc)
                else:
                    fmp_required = _minimum_bars_for_interval(market, fallback_interval)
                    market_status["fmp_fallback_bar_count"] = len(fmp_bars)
                    if len(fmp_bars) >= fmp_required:
                        market_status.update(
                            {
                                "requested_interval": market_status.get("requested_interval", interval),
                                "requested_start": market_status.get("requested_start", start),
                                "interval": fallback_interval,
                                "start": fallback_start,
                                "data_source_status": "fmp_historical_fallback",
                                "fallback_reason": (
                                    f"EODHD returned {len(bars)} {interval} bars for {market.eodhd_symbol}; "
                                    f"using {len(fmp_bars)} {fallback_interval} FMP bars from {fallback_symbol}"
                                ),
                                "eodhd_bar_count": len(bars),
                            }
                        )
                        interval = fallback_interval
                        start = fallback_start
                        bars = fmp_bars
                        required_bars = fmp_required
                        bar_source = "fmp_historical_fallback"
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
            bar_source,
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
                        diagnostic_limit=payload.diagnostic_limit,
                        target_regime=payload.target_regime,
                        repair_mode=payload.repair_mode,
                        account_size=payload.account_size,
                        source_template=payload.source_template,
                        market_context=market_status.get("market_context") if isinstance(market_status.get("market_context"), dict) else {},
                        day_trading_mode=payload.day_trading_mode,
                        force_flat_before_close=payload.force_flat_before_close or payload.day_trading_mode,
                        paper_queue_limit=payload.paper_queue_limit,
                        review_queue_limit=payload.review_queue_limit,
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
        _record_guided_auto_freeze_status(
            run_id,
            payload,
            selected_markets,
            market_statuses,
            market_failures,
            {
                "status": "skipped",
                "reason": "no_trials",
                "detail": "The guided design run produced no trials to freeze.",
            },
        )
        return
    final_status = "finished_with_warnings" if market_failures else "finished"
    final_error = _market_failure_summary(market_failures) if market_failures else ""
    research_store.update_run_status(run_id, final_status, final_error)
    try:
        await _maybe_auto_freeze_guided_pipeline(run_id, payload, api_token, selected_markets, market_statuses, market_failures)
    except Exception as exc:
        _record_guided_auto_freeze_status(
            run_id,
            payload,
            selected_markets,
            market_statuses,
            market_failures,
            {
                "status": "error",
                "detail": "The design run finished, but automatic freeze validation could not be started.",
                "error": _public_error(exc),
            },
        )


@app.get("/research/runs")
def list_research_runs(include_archived: bool = False) -> list[dict[str, object]]:
    return research_store.list_runs(include_archived=include_archived)


@app.get("/research/runs/{run_id}")
def get_research_run(run_id: int) -> dict[str, object]:
    run = research_store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Research run not found")
    run = _compact_research_run(run)
    return {
        **run,
        "trials": [_trial_with_capital(trial) for trial in research_store.list_trials(run_id, limit=25)],
        "candidates": [_candidate_with_capital(candidate) for candidate in research_store.list_candidates(run_id, limit=24)],
        "pareto": research_store.list_pareto(run_id),
        "regime_picks": research_store.list_regime_picks(run_id),
        "bar_snapshots": research_store.list_bar_snapshots(run_id, include_payload=False),
    }


def _compact_research_run(run: dict[str, object]) -> dict[str, object]:
    compacted = dict(run)
    config = compacted.get("config")
    if isinstance(config, dict):
        compacted["config"] = _compact_research_config(config)
    return compacted


def _compact_research_config(config: dict[str, object]) -> dict[str, object]:
    compacted = dict(config)
    statuses = compacted.get("market_statuses")
    if isinstance(statuses, list):
        compacted["market_statuses"] = [_compact_market_status(status) for status in statuses if isinstance(status, dict)]
    return compacted


def _compact_market_status(status: dict[str, object]) -> dict[str, object]:
    compacted = dict(status)
    bar_regime = compacted.get("bar_regime")
    if isinstance(bar_regime, dict):
        keys = ("schema", "bar_count", "current_regime", "regime_counts", "start", "end")
        compacted["bar_regime"] = {key: bar_regime[key] for key in keys if key in bar_regime}
    return compacted


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
    if research_store.run_has_move_forward_candidate(run_id):
        raise HTTPException(status_code=409, detail="Paper-ready runs cannot be deleted. Archive them to keep the evidence bundle.")
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


def _is_guided_midcap_design_payload(payload: ResearchRunPayload) -> bool:
    pipeline = payload.pipeline if isinstance(payload.pipeline, dict) else {}
    return (
        payload.repair_mode == "standard"
        and str(pipeline.get("schema") or "") == "midcap_template_pipeline_v1"
        and bool(pipeline.get("auto_freeze", {}).get("enabled", False) if isinstance(pipeline.get("auto_freeze"), dict) else False)
    )


async def _maybe_auto_freeze_guided_pipeline(
    run_id: int,
    payload: ResearchRunPayload,
    api_token: str,
    selected_markets: list[MarketMapping],
    market_statuses: list[dict[str, object]],
    market_failures: list[dict[str, object]],
) -> None:
    if not _is_guided_midcap_design_payload(payload):
        return
    _record_guided_auto_freeze_status(
        run_id,
        payload,
        selected_markets,
        market_statuses,
        market_failures,
        {
            "status": "selecting",
            "detail": "Selecting only IG-validated, OOS-positive intraday evidence that is strong enough to freeze automatically.",
        },
    )
    trials = research_store.list_trials(run_id, limit=GUIDED_AUTO_FREEZE_TRIAL_LIMIT)
    selected_trial, skip_summary = _select_guided_auto_freeze_trial(trials)
    if selected_trial is None:
        _record_guided_auto_freeze_status(
            run_id,
            payload,
            selected_markets,
            market_statuses,
            market_failures,
            {
                "status": "skipped",
                "reason": "no_freezeable_trial",
                "detail": "No design result was safe to freeze automatically.",
                "trial_count": len(trials),
                "skip_summary": skip_summary,
            },
        )
        return

    source_template = _guided_auto_freeze_source_template(selected_trial)
    if not source_template.get("parameters"):
        _record_guided_auto_freeze_status(
            run_id,
            payload,
            selected_markets,
            market_statuses,
            market_failures,
            {
                "status": "skipped",
                "reason": "missing_frozen_parameters",
                "detail": "The selected design result did not expose exact frozen parameters.",
                "source_trial_id": selected_trial.get("id"),
            },
        )
        return

    saved_template = research_store.save_template(
        _guided_auto_freeze_template_payload(
            selected_trial,
            payload,
            source_template,
            source_kind="guided_pipeline_auto_saved_source",
        )
    )
    freeze_payload = _guided_auto_freeze_validation_payload(payload, selected_trial, source_template, saved_template)
    freeze_markets = _selected_markets(freeze_payload.market_ids or [freeze_payload.market_id])
    freeze_run_id = research_store.create_run(
        market_id=freeze_markets[0].market_id,
        data_source="eodhd_with_ig_cost_model",
        status="running",
        config=_research_run_config(freeze_payload, freeze_markets),
    )
    _record_guided_auto_freeze_status(
        run_id,
        payload,
        selected_markets,
        market_statuses,
        market_failures,
        {
            "status": "running_freeze_validation",
            "detail": "Saved the best eligible lead as a frozen template and started a no-search validation run.",
            "source_trial_id": selected_trial.get("id"),
            "template_id": saved_template.get("id"),
            "template_name": saved_template.get("name"),
            "freeze_run_id": freeze_run_id,
        },
    )
    try:
        await _execute_research_run(freeze_run_id, freeze_payload, api_token)
    except Exception as exc:
        research_store.update_run_status(freeze_run_id, "error", _public_error(exc))
        _record_guided_auto_freeze_status(
            run_id,
            payload,
            selected_markets,
            market_statuses,
            market_failures,
            {
                "status": "error",
                "detail": "Frozen validation failed before it could produce evidence.",
                "source_trial_id": selected_trial.get("id"),
                "template_id": saved_template.get("id"),
                "freeze_run_id": freeze_run_id,
                "error": _public_error(exc),
            },
        )
        return

    freeze_run = research_store.get_run(freeze_run_id)
    validation_trial = _best_guided_freeze_validation_trial(freeze_run_id)
    validation_status = str(freeze_run.get("status") if freeze_run else "unknown")
    readiness_status = ""
    if validation_trial is not None:
        readiness = validation_trial.get("promotion_readiness") if isinstance(validation_trial.get("promotion_readiness"), dict) else {}
        readiness_status = str(readiness.get("status") or validation_trial.get("readiness_status") or "blocked")
        saved_template = research_store.save_template(
            _guided_auto_freeze_template_payload(
                selected_trial,
                payload,
                source_template,
                source_kind="guided_pipeline_auto_freeze_validated",
                validation_trial=validation_trial,
                validation_run_id=freeze_run_id,
            )
        )
    status = "freeze_validated_ready" if readiness_status == "ready_for_paper" else "freeze_validated_blocked"
    if validation_trial is None:
        status = "freeze_validation_error" if validation_status == "error" else "freeze_validation_finished_without_trial"
    _record_guided_auto_freeze_status(
        run_id,
        payload,
        selected_markets,
        market_statuses,
        market_failures,
        {
            "status": status,
            "detail": _guided_auto_freeze_completion_detail(status, readiness_status),
            "source_trial_id": selected_trial.get("id"),
            "template_id": saved_template.get("id"),
            "template_name": saved_template.get("name"),
            "freeze_run_id": freeze_run_id,
            "freeze_run_status": validation_status,
            "validation_trial_id": validation_trial.get("id") if validation_trial else None,
            "readiness_status": readiness_status,
        },
    )


def _record_guided_auto_freeze_status(
    run_id: int,
    payload: ResearchRunPayload,
    selected_markets: list[MarketMapping],
    market_statuses: list[dict[str, object]],
    market_failures: list[dict[str, object]],
    auto_freeze: dict[str, object],
) -> None:
    if not _is_guided_midcap_design_payload(payload):
        return
    pipeline = dict(payload.pipeline if isinstance(payload.pipeline, dict) else {})
    existing = pipeline.get("auto_freeze") if isinstance(pipeline.get("auto_freeze"), dict) else {}
    pipeline["auto_freeze"] = {
        **existing,
        "enabled": True,
        "policy": "save_best_oos_positive_ig_validated_intraday_lead_then_one_trial_frozen_validation",
        "strategy_generation_allowed": False,
        **auto_freeze,
    }
    payload.pipeline = pipeline
    research_store.update_run_config(
        run_id,
        _research_run_config(payload, selected_markets, market_statuses=market_statuses, market_failures=market_failures),
    )


def _select_guided_auto_freeze_trial(trials: list[dict[str, object]]) -> tuple[dict[str, object] | None, dict[str, int]]:
    skip_counts: dict[str, int] = {}
    ranked = sorted(trials, key=_guided_auto_freeze_trial_rank, reverse=True)
    for trial in ranked:
        rejection = _guided_auto_freeze_rejection(trial)
        if rejection:
            skip_counts[rejection] = skip_counts.get(rejection, 0) + 1
            continue
        return trial, skip_counts
    return None, skip_counts


def _guided_auto_freeze_rejection(trial: dict[str, object]) -> str:
    parameters = trial.get("parameters") if isinstance(trial.get("parameters"), dict) else {}
    backtest = trial.get("backtest") if isinstance(trial.get("backtest"), dict) else {}
    pattern = parameters.get("bar_pattern_analysis") if isinstance(parameters.get("bar_pattern_analysis"), dict) else {}
    gated = pattern.get("regime_gated_backtest") if isinstance(pattern.get("regime_gated_backtest"), dict) else {}
    evidence = parameters.get("evidence_profile") if isinstance(parameters.get("evidence_profile"), dict) else {}
    readiness = trial.get("promotion_readiness") if isinstance(trial.get("promotion_readiness"), dict) else {}
    warnings = set(str(item) for item in (trial.get("warnings") or []) if item)
    warnings.update(str(item) for item in (readiness.get("blockers") or []) if item)
    warnings.update(str(item) for item in (readiness.get("validation_warnings") or []) if item)
    if warnings & GUIDED_AUTO_FREEZE_TERMINAL_WARNINGS:
        return sorted(warnings & GUIDED_AUTO_FREEZE_TERMINAL_WARNINGS)[0]
    if warnings & GUIDED_AUTO_FREEZE_STRICT_WARNINGS:
        return sorted(warnings & GUIDED_AUTO_FREEZE_STRICT_WARNINGS)[0]
    if not str(parameters.get("market_id") or trial.get("market_id") or "").strip():
        return "missing_market_id"
    if not _freezeable_parameter_subset(parameters):
        return "missing_frozen_parameters"
    if not bool(parameters.get("day_trading_mode")):
        return "not_day_trading"
    if not bool(parameters.get("force_flat_before_close")) or not bool(parameters.get("no_overnight")):
        return "missing_intraday_flat_contract"
    if int(_safe_number(backtest.get("trade_count"))) < GUIDED_AUTO_FREEZE_MIN_TRADES:
        return "too_few_trades_to_freeze"
    if int(_safe_number(evidence.get("oos_trade_count"), backtest.get("oos_trade_count"))) < GUIDED_AUTO_FREEZE_MIN_OOS_TRADES:
        return "too_few_oos_trades_to_freeze"
    if _safe_number(backtest.get("net_profit")) <= 0 or _safe_number(backtest.get("test_profit")) <= 0:
        return "non_positive_net_or_oos_profit"
    if "regime_gated_backtest" in pattern and _safe_number(gated.get("test_profit")) <= 0:
        return "non_positive_regime_gated_oos_profit"
    if "stress_net_profit" in parameters and _safe_number(parameters.get("stress_net_profit")) <= 0:
        return "negative_stress_net_profit"
    return ""


def _guided_auto_freeze_trial_rank(trial: dict[str, object]) -> tuple[float, ...]:
    tier_rank = {
        "validated_candidate": 5,
        "paper_candidate": 4,
        "research_candidate": 3,
        "incubator": 2,
        "watchlist": 1,
        "reject": 0,
    }.get(str(trial.get("promotion_tier") or "reject"), 0)
    readiness = trial.get("promotion_readiness") if isinstance(trial.get("promotion_readiness"), dict) else {}
    readiness_rank = {"ready_for_paper": 3, "needs_ig_validation": 2, "blocked": 1}.get(str(readiness.get("status") or "blocked"), 0)
    backtest = trial.get("backtest") if isinstance(trial.get("backtest"), dict) else {}
    parameters = trial.get("parameters") if isinstance(trial.get("parameters"), dict) else {}
    evidence = parameters.get("evidence_profile") if isinstance(parameters.get("evidence_profile"), dict) else {}
    return (
        float(tier_rank),
        float(readiness_rank),
        1.0 if _guided_auto_freeze_rejection(trial) == "" else 0.0,
        _safe_number(evidence.get("oos_net_profit"), backtest.get("test_profit")),
        _safe_number(parameters.get("stress_net_profit")),
        _safe_number(backtest.get("net_profit")),
        _safe_number(trial.get("robustness_score")),
        _safe_number(backtest.get("net_cost_ratio")),
        -_safe_number(backtest.get("max_drawdown")),
        -_safe_number(trial.get("id")),
    )


def _freezeable_parameter_subset(parameters: dict[str, object]) -> dict[str, object]:
    return {
        str(key): value
        for key, value in parameters.items()
        if str(key) in FROZEN_PARAMETER_KEYS and value not in (None, "")
    }


def _guided_auto_freeze_source_template(trial: dict[str, object]) -> dict[str, object]:
    parameters = trial.get("parameters") if isinstance(trial.get("parameters"), dict) else {}
    pattern = parameters.get("bar_pattern_analysis") if isinstance(parameters.get("bar_pattern_analysis"), dict) else {}
    market_id = str(trial.get("market_id") or parameters.get("market_id") or "").strip()
    family = str(trial.get("strategy_family") or parameters.get("family") or "").strip()
    interval = str(parameters.get("timeframe") or parameters.get("interval") or "5min").strip()
    target_regime = str(parameters.get("target_regime") or pattern.get("target_regime") or "").strip()
    return {
        "name": str(trial.get("strategy_name") or f"{market_id} {family}").strip(),
        "source_id": trial.get("id"),
        "market_id": market_id,
        "family": family,
        "style": str(parameters.get("style") or trial.get("style") or "intraday_only"),
        "interval": interval,
        "target_regime": target_regime,
        "repair_attempt_count": _safe_int((parameters.get("search_audit") or {}).get("repair_attempt_count"))
        if isinstance(parameters.get("search_audit"), dict)
        else 0,
        "holding_period": "intraday",
        "force_flat_before_close": True,
        "no_overnight": True,
        "parameters": _freezeable_parameter_subset(parameters),
    }


def _guided_auto_freeze_template_payload(
    source_trial: dict[str, object],
    parent_payload: ResearchRunPayload,
    source_template: dict[str, object],
    *,
    source_kind: str,
    validation_trial: dict[str, object] | None = None,
    validation_run_id: int | None = None,
) -> dict[str, object]:
    evidence_trial = validation_trial or source_trial
    parameters = evidence_trial.get("parameters") if isinstance(evidence_trial.get("parameters"), dict) else {}
    backtest = evidence_trial.get("backtest") if isinstance(evidence_trial.get("backtest"), dict) else {}
    readiness = evidence_trial.get("promotion_readiness") if isinstance(evidence_trial.get("promotion_readiness"), dict) else {}
    pattern = parameters.get("bar_pattern_analysis") if isinstance(parameters.get("bar_pattern_analysis"), dict) else {}
    evidence = parameters.get("evidence_profile") if isinstance(parameters.get("evidence_profile"), dict) else {}
    search_audit = parameters.get("search_audit") if isinstance(parameters.get("search_audit"), dict) else {}
    capital_source = _trial_with_capital(evidence_trial)
    validated = validation_run_id is not None
    stored_readiness = dict(readiness)
    if not validated:
        stored_readiness["status"] = "blocked"
        blockers = list(stored_readiness.get("blockers") or [])
        if "pending_frozen_validation" not in blockers:
            blockers.append("pending_frozen_validation")
        stored_readiness["blockers"] = blockers
    validation: dict[str, object] = {}
    if validation_run_id is not None:
        validation = {
            "run_id": validation_run_id,
            "trial_id": evidence_trial.get("id"),
            "repair_mode": "frozen_validation",
            "parameter_hunting": False,
        }
    return {
        "name": source_template.get("name") or source_trial.get("strategy_name") or "Guided frozen template",
        "market_id": source_template.get("market_id") or parameters.get("market_id") or source_trial.get("market_id") or parent_payload.market_id,
        "interval": source_template.get("interval") or parameters.get("timeframe") or parent_payload.interval or "5min",
        "strategy_family": source_template.get("family") or parameters.get("family") or evidence_trial.get("strategy_family") or "",
        "style": source_template.get("style") or parameters.get("style") or parent_payload.trading_style,
        "target_regime": source_template.get("target_regime") or parameters.get("target_regime") or pattern.get("target_regime") or "",
        "status": "active" if validated else "paused",
        "source_run_id": source_trial.get("run_id"),
        "source_trial_id": source_trial.get("id"),
        "source_candidate_id": None,
        "source_kind": source_kind,
        "promotion_tier": evidence_trial.get("promotion_tier") if validated else "research_candidate",
        "readiness_status": stored_readiness.get("status") or "blocked",
        "robustness_score": _safe_number(evidence_trial.get("robustness_score")),
        "testing_account_size": parent_payload.account_size,
        "payload": {
            "source_template": source_template,
            "parameters": parameters,
            "backtest": backtest,
            "pattern": pattern,
            "evidence": evidence,
            "readiness": stored_readiness,
            "warnings": list(evidence_trial.get("warnings") or []) + ([] if validated else ["pending_frozen_validation"]),
            "search_audit": search_audit,
            "capital_scenarios": capital_source.get("capital_scenarios", []),
            "source_kind": source_kind,
            "validation": validation,
            "pipeline": {
                "schema": "guided_midcap_auto_freeze_template_v1",
                "parent_run_id": source_trial.get("run_id"),
                "validation_run_id": validation_run_id,
                "strategy_generation_allowed": False,
            },
        },
    }


def _guided_auto_freeze_validation_payload(
    parent_payload: ResearchRunPayload,
    source_trial: dict[str, object],
    source_template: dict[str, object],
    saved_template: dict[str, object],
) -> ResearchRunPayload:
    market_id = str(source_template.get("market_id") or source_trial.get("market_id") or parent_payload.market_id)
    family = str(source_template.get("family") or source_trial.get("strategy_family") or "")
    target_regime = str(source_template.get("target_regime") or "")
    return ResearchRunPayload(
        market_id=market_id,
        market_ids=[market_id],
        start=_guided_auto_freeze_validation_start(parent_payload.start, source_template),
        end=parent_payload.end,
        interval=str(source_template.get("interval") or parent_payload.interval or "5min"),
        engine=parent_payload.engine,
        search_preset="balanced",
        trading_style=str(source_template.get("style") or parent_payload.trading_style or "intraday_only"),
        objective="profit_first",
        search_budget=1,
        risk_profile=parent_payload.risk_profile,
        strategy_families=[family] if family else [],
        product_mode=parent_payload.product_mode,
        cost_stress_multiplier=max(2.5, parent_payload.cost_stress_multiplier),
        include_regime_scans=False,
        regime_scan_budget_per_regime=None,
        diagnostic_limit=None,
        include_market_context=False,
        target_regime=target_regime or None,
        excluded_months=[],
        repair_mode="frozen_validation",
        account_size=parent_payload.account_size,
        source_template=source_template,
        pipeline={
            "schema": "guided_midcap_auto_freeze_validation_v1",
            "parent_run_id": source_trial.get("run_id"),
            "source_trial_id": source_trial.get("id"),
            "template_id": saved_template.get("id"),
            "daily_mode_source": "active_frozen_template_library_only",
            "strategy_generation_allowed": False,
        },
        day_trading_mode=True,
        force_flat_before_close=True,
        paper_queue_limit=parent_payload.paper_queue_limit,
        review_queue_limit=parent_payload.review_queue_limit,
    )


def _guided_auto_freeze_validation_start(current_start: str, source_template: dict[str, object]) -> str:
    family = str(source_template.get("family") or "")
    interval = str(source_template.get("interval") or "")
    target = "2020-01-01" if interval == "1day" or family in {"calendar_turnaround_tuesday", "month_end_seasonality", "everyday_long"} else "2024-01-01"
    current = str(current_start or "").strip()
    return current if current and current < target else target


def _best_guided_freeze_validation_trial(run_id: int) -> dict[str, object] | None:
    trials = research_store.list_trials(run_id, limit=5)
    if not trials:
        return None
    return sorted(trials, key=_guided_auto_freeze_trial_rank, reverse=True)[0]


def _guided_auto_freeze_completion_detail(status: str, readiness_status: str) -> str:
    if status == "freeze_validated_ready":
        return "Frozen validation passed; the template can be considered for daily paper scans."
    if status == "freeze_validated_blocked":
        readiness = readiness_status or "blocked"
        return f"Frozen validation finished, but readiness is {readiness}; keep it out of daily paper until repaired."
    if status == "freeze_validation_error":
        return "Frozen validation could not produce a validation trial; the saved template remains paused."
    return "Frozen validation finished, but no validation trial was saved."


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


def _should_try_fmp_daily_fallback(market: MarketMapping, interval: str, bar_count: int, required_bars: int) -> bool:
    if bar_count >= required_bars:
        return False
    if not _fmp_daily_symbol_for_market(market):
        return False
    return interval in INTRADAY_INTERVALS or interval in {"1day", "1d", "day", "daily"}


def _fmp_daily_symbol_for_market(market: MarketMapping) -> str:
    configured = FMP_DAILY_BAR_SYMBOLS.get(market.market_id, "")
    if configured:
        return configured
    if market.asset_class != "share":
        return ""
    symbol = market.eodhd_symbol.strip()
    if symbol.endswith(".US"):
        return symbol.removesuffix(".US")
    if symbol.endswith(".LSE"):
        return f"{symbol.removesuffix('.LSE')}.L"
    return symbol


def _is_eodhd_monthly_commodity(market: MarketMapping) -> bool:
    if not market.eodhd_symbol.startswith("COMMODITY:"):
        return False
    code = market.eodhd_symbol.removeprefix("COMMODITY:")
    return code in EODHD_MONTHLY_COMMODITIES


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
        "diagnostic_limit": payload.diagnostic_limit,
        "include_market_context": payload.include_market_context,
        "target_regime": payload.target_regime,
        "excluded_months": sorted(_normalized_excluded_months(payload.excluded_months)),
        "repair_mode": payload.repair_mode,
        "account_size": payload.account_size,
        "source_template": _compact_source_template(payload.source_template),
        "pipeline": payload.pipeline if isinstance(payload.pipeline, dict) else {},
        "product_mode": payload.product_mode,
        "day_trading_mode": payload.day_trading_mode,
        "force_flat_before_close": payload.force_flat_before_close or payload.day_trading_mode,
        "paper_queue_limit": payload.paper_queue_limit,
        "review_queue_limit": payload.review_queue_limit,
        "daily_factory_policy": _day_trading_policy(payload.paper_queue_limit, payload.review_queue_limit, payload.account_size)
        if payload.day_trading_mode
        else {},
        "research_only": True,
        "ig_validation_required": True,
        "data_source_policy": "eodhd_primary_symbol_with_explicit_fmp_daily_fallback",
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
        "repair_attempt_count": _safe_int(source_template.get("repair_attempt_count")),
        "holding_period": str(source_template.get("holding_period") or parameters.get("holding_period") or ""),
        "force_flat_before_close": bool(source_template.get("force_flat_before_close") or parameters.get("force_flat_before_close")),
        "no_overnight": bool(source_template.get("no_overnight") or parameters.get("no_overnight")),
        "parameters": {
            str(key): value
            for key, value in parameters.items()
            if str(key) in FROZEN_PARAMETER_KEYS
        },
    }


def _safe_int(value: object) -> int:
    try:
        return max(0, int(float(value or 0)))
    except (TypeError, ValueError):
        return 0


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
    share_model = share_spread_bet_model(market)
    if share_model is not None:
        payload["spread_bet_model"] = share_model.as_dict()
        payload["estimated_spread_bps"] = max(market.spread_bps, share_model.dealing_spread_bps)
        payload["estimated_slippage_bps"] = max(market.slippage_bps, share_model.slippage_bps)
    return payload


def _market_mapping_from_payload(payload: dict[str, object]) -> MarketMapping:
    return MarketMapping(
        str(payload.get("market_id") or ""),
        str(payload.get("name") or payload.get("market_id") or ""),
        str(payload.get("asset_class") or "share"),
        str(payload.get("eodhd_symbol") or payload.get("fmp_symbol") or ""),
        str(payload.get("ig_epic") or ""),
        bool(payload.get("enabled", True)),
        str(payload.get("plugin_id") or ""),
        str(payload.get("ig_name") or payload.get("name") or ""),
        str(payload.get("ig_search_terms") or ""),
        str(payload.get("default_timeframe") or "5min"),
        _safe_number(payload.get("spread_bps"), payload.get("estimated_spread_bps"), 20.0),
        _safe_number(payload.get("slippage_bps"), payload.get("estimated_slippage_bps"), 5.0),
        max(1, int(_safe_number(payload.get("min_backtest_bars"), 750))),
    )


def _cost_profile_for_market(market: MarketMapping) -> IGCostProfile:
    stored = _cost_profile_payload_for_market(market)
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
    profile = _cost_profile_payload_for_market_id(market_id) if market_id else None
    scenarios = capital_scenarios(backtest, parameters, profile, account_sizes=_scenario_sizes_for_parameters(parameters))
    return {**trial, "capital_scenarios": scenarios, "capital_summary": capital_summary(scenarios)}


def _candidate_with_capital(candidate: dict[str, object]) -> dict[str, object]:
    audit = candidate.get("audit") if isinstance(candidate.get("audit"), dict) else {}
    candidate_payload = audit.get("candidate") if isinstance(audit.get("candidate"), dict) else {}
    parameters = candidate_payload.get("parameters") if isinstance(candidate_payload.get("parameters"), dict) else {}
    backtest = audit.get("backtest") if isinstance(audit.get("backtest"), dict) else {}
    market_id = str(candidate.get("market_id") or parameters.get("market_id") or "")
    profile = _cost_profile_payload_for_market_id(market_id) if market_id else None
    scenarios = capital_scenarios(backtest, parameters, profile, account_sizes=_scenario_sizes_for_parameters(parameters))
    return {**candidate, "capital_scenarios": scenarios, "capital_summary": capital_summary(scenarios)}


def _scenario_sizes_for_parameters(parameters: dict[str, object]) -> tuple[float, ...]:
    search_audit = parameters.get("search_audit") if isinstance(parameters.get("search_audit"), dict) else {}
    return scenario_account_sizes(parameters.get("testing_account_size") or search_audit.get("testing_account_size"))


def _cost_profile_payload_for_market_id(market_id: str) -> dict[str, object] | None:
    market = markets.get(market_id)
    if market is None:
        return research_store.get_cost_profile(market_id)
    return _cost_profile_payload_for_market(market)


def _cost_profile_payload_for_market(market: MarketMapping) -> dict[str, object] | None:
    stored = research_store.get_cost_profile(market.market_id)
    if stored is None:
        return None
    normalized = normalized_cost_profile_payload(market, stored)
    if normalized != stored:
        research_store.save_cost_profile(normalized)
        normalized["updated_at"] = stored.get("updated_at", normalized.get("updated_at", ""))
    return normalized


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


def _day_trading_policy(paper_limit: int = 3, review_limit: int = 10, account_size: float = WORKING_ACCOUNT_SIZE_GBP) -> dict[str, object]:
    return {
        "start_mode": "manual_button",
        "automation_status": "planned_market_open_scheduler",
        "flow": {
            "discovery_mode": "Find markets, regimes, and possible ideas; repair, freeze, and validate before reuse.",
            "template_library": "Store frozen rules only: market type, regime, entry, stop, target, max hold, session rules, capital fit, and cost assumptions.",
            "daily_paper_mode": "Scan today's eligible markets against active frozen templates only; no parameter search or new strategy invention.",
            "review_mode": "Compare expected versus actual after close; evidence can trigger a new validation cycle but cannot silently change rules.",
        },
        "daily_mode_source": "active_frozen_template_library_only",
        "strategy_generation_allowed_in_daily_mode": False,
        "holding_period": "intraday_only",
        "force_flat_before_close": True,
        "overnight_positions_allowed": False,
        "live_ordering_enabled": False,
        "order_placement": "disabled",
        "paper_queue_limit": max(1, min(5, int(paper_limit))),
        "review_signal_limit": max(1, min(20, int(review_limit))),
        "library_shape": {
            "daily_active_paper_trades": "1-3",
            "daily_review_signals": "5-10",
            "per_regime_template_target": "1 primary plus 1 backup per major regime and market type",
            "long_term_template_range": "20-50 frozen templates, with most idle on any given day",
            "share_template_bias": "Prefer behavior templates such as liquid UK midcap trend-up pullback over one template per stock.",
        },
        "account_size": account_size,
        "capital_checks": [
            "IG tradability and EPIC match",
            "recent IG price validation",
            "spread and slippage",
            "minimum deal size",
            "margin percent and minimum margin",
            "stop distance and planned risk",
            "35% max margin use for the selected account",
        ],
        "execution_policy": "broker-safe previews and paper simulation only",
    }


def _template_design_payload(design: dict[str, object]) -> dict[str, object]:
    return {
        "id": str(design.get("id") or ""),
        "label": str(design.get("label") or ""),
        "market_type": str(design.get("market_type") or "share"),
        "country": str(design.get("country") or "UK"),
        "behaviour": str(design.get("behaviour") or ""),
        "template_goal": str(design.get("template_goal") or ""),
        "strategy_families": list(design.get("strategy_families") or []),
        "market_filters": dict(design.get("market_filters") if isinstance(design.get("market_filters"), dict) else {}),
        "run_defaults": dict(design.get("run_defaults") if isinstance(design.get("run_defaults"), dict) else {}),
        "session_rules": dict(design.get("session_rules") if isinstance(design.get("session_rules"), dict) else {}),
        "promotion_contract": list(design.get("promotion_contract") or []),
    }


def _manual_playbook_payload(playbook: dict[str, object]) -> dict[str, object]:
    return {
        "id": str(playbook.get("id") or ""),
        "label": str(playbook.get("label") or ""),
        "description": str(playbook.get("description") or ""),
        "families": list(playbook.get("families") or []),
        "today_filters": {
            "min_relative_volume": _safe_number(playbook.get("min_relative_volume")),
            "max_spread_bps": _safe_number(playbook.get("max_spread_bps")),
            "min_opening_break_bps": _safe_number(playbook.get("min_opening_break_bps")),
            "min_trend_bps": _safe_number(playbook.get("min_trend_bps")),
            "min_sweep_bps": _safe_number(playbook.get("min_sweep_bps")),
            "max_vwap_distance_against_bps": _safe_number(playbook.get("max_vwap_distance_against_bps")),
            "require_vwap_alignment": bool(playbook.get("require_vwap_alignment")),
        },
    }


def _midcap_template_design(design_id: str) -> dict[str, object]:
    key = str(design_id or "").strip()
    design = MIDCAP_TEMPLATE_DESIGNS.get(key)
    if design is None:
        valid = ", ".join(sorted(MIDCAP_TEMPLATE_DESIGNS))
        raise HTTPException(status_code=400, detail=f"Unknown template design. Choose one of: {valid}")
    return _template_design_payload(design)


def _midcap_design_country_code(country: object) -> str:
    _exchange, country_code = country_exchange_hint(str(country or ""))
    if country_code == "GB":
        return "UK"
    return country_code or str(country or "").strip().upper()


def _validate_midcap_design_country(design: dict[str, object], country: str) -> str:
    requested_country = str(country or design.get("country") or "UK")
    design_country = _midcap_design_country_code(design.get("country"))
    requested_country_code = _midcap_design_country_code(requested_country)
    if design_country and requested_country_code and design_country != requested_country_code:
        raise HTTPException(
            status_code=400,
            detail=(
                f"{design.get('label')} is a {design_country} template design. "
                f"Switch the market universe to {design_country} or choose a {requested_country_code} template design."
            ),
        )
    return requested_country


def _midcap_pipeline_market_cap(requested_max_markets: int) -> int:
    return max(1, min(TWO_VCPU_MIDCAP_MARKET_CAP, int(requested_max_markets or TWO_VCPU_MIDCAP_MARKET_CAP)))


def _midcap_pipeline_discovery_limit(requested_limit: int, run_market_cap: int) -> int:
    target = max(TWO_VCPU_MIDCAP_DISCOVERY_LIMIT, run_market_cap * 8)
    return max(run_market_cap, min(int(requested_limit or target), target))


def _midcap_pipeline_server_profile(
    requested_max_markets: int,
    requested_limit: int,
    run_market_cap: int,
    discovery_limit: int,
) -> dict[str, object]:
    return {
        "schema": "guided_midcap_2vcpu_profile_v1",
        "reason": "Keep guided scans responsive on the 2 vCPU server; use repair/freeze for deeper follow-up once a lead is worth it.",
        "requested_max_markets": requested_max_markets,
        "run_market_cap": run_market_cap,
        "requested_discovery_limit": requested_limit,
        "discovery_limit": discovery_limit,
        "search_budget_per_market": TWO_VCPU_MIDCAP_SEARCH_BUDGET,
        "regime_scan_budget_per_regime": TWO_VCPU_MIDCAP_REGIME_BUDGET,
        "diagnostic_limit_per_market": TWO_VCPU_MIDCAP_DIAGNOSTIC_LIMIT,
        "market_context": "skipped_for_guided_pilot",
    }


async def _run_midcap_template_pipeline(
    payload: MidcapTemplatePipelinePayload,
    background_tasks: BackgroundTasks,
    api_token: str,
) -> dict[str, object]:
    design = _midcap_template_design(payload.design_id)
    requested_country = _validate_midcap_design_country(design, payload.country)
    product_mode = _normalize_product_mode(payload.product_mode)
    run_market_cap = _midcap_pipeline_market_cap(payload.max_markets)
    discovery_limit = _midcap_pipeline_discovery_limit(payload.limit, run_market_cap)
    discovery = await discover_midcap_markets(
        country=requested_country,
        product_mode=product_mode,
        limit=discovery_limit,
        min_market_cap=payload.min_market_cap,
        max_market_cap=payload.max_market_cap,
        min_volume=payload.min_volume,
        max_spread_bps=payload.max_spread_bps,
        account_size=payload.account_size,
        verify_ig=True,
        require_ig_catalogue=True,
    )
    candidates = list(discovery.get("candidates") or [])
    eligible = [
        candidate
        for candidate in candidates
        if isinstance(candidate, dict)
        and candidate.get("eligible")
        and candidate.get("ig_status") == "ig_matched"
        and isinstance(candidate.get("market_mapping"), dict)
    ]
    eligible = sorted(eligible, key=lambda item: (_safe_number(item.get("score")), _safe_number(item.get("volume"))), reverse=True)
    selected_candidates = eligible[:run_market_cap]
    installed_markets: list[dict[str, object]] = []
    selected_market_ids: list[str] = []
    for candidate in selected_candidates:
        mapping_payload = candidate.get("market_mapping")
        if not isinstance(mapping_payload, dict):
            continue
        mapping = _market_mapping_from_payload(mapping_payload)
        if payload.auto_install:
            markets.upsert(mapping)
        elif markets.get(mapping.market_id) is None:
            continue
        selected_market_ids.append(mapping.market_id)
        installed_markets.append(
            {
                "market_id": mapping.market_id,
                "name": mapping.name,
                "score": candidate.get("score", 0),
                "market_cap": candidate.get("market_cap", 0),
                "volume": candidate.get("volume", 0),
                "estimated_spread_bps": candidate.get("estimated_spread_bps", mapping.spread_bps),
                "estimated_slippage_bps": candidate.get("estimated_slippage_bps", mapping.slippage_bps),
                "ig_epic": mapping.ig_epic,
                "market_mapping": _market_response(mapping),
            }
        )
    cost_sync: dict[str, object] = {"status": "skipped", "profile_count": 0, "price_validated_count": 0, "ig_rate_limited": False}
    run_ready_market_ids = list(selected_market_ids)
    if payload.auto_sync_costs and selected_market_ids:
        try:
            cost_sync_result = await sync_ig_market_costs(
                IGCostSyncPayload(market_ids=selected_market_ids, product_mode=product_mode, skip_account_status=True)
            )
            cost_sync = {
                "status": cost_sync_result.get("status", "synced"),
                "profile_count": cost_sync_result.get("profile_count", 0),
                "price_validated_count": cost_sync_result.get("price_validated_count", 0),
                "ig_rate_limited": cost_sync_result.get("ig_rate_limited", False),
                "profiles": [
                    {
                        "market_id": profile.get("market_id"),
                        "confidence": profile.get("confidence"),
                        "validation_status": profile.get("validation_status"),
                        "spread_bps": profile.get("spread_bps"),
                        "slippage_bps": profile.get("slippage_bps"),
                        "min_deal_size": profile.get("min_deal_size"),
                        "margin_percent": profile.get("margin_percent"),
                        "notes": list(profile.get("notes") or [])[-3:],
                    }
                    for profile in list(cost_sync_result.get("profiles") or [])[: payload.max_markets]
                    if isinstance(profile, dict)
                ],
            }
            validated_market_ids = _price_validated_cost_profile_market_ids(cost_sync_result)
            run_ready_market_ids = [market_id for market_id in selected_market_ids if market_id in validated_market_ids]
        except HTTPException as exc:
            cost_sync = {"status": "error", "profile_count": 0, "error": str(exc.detail)}
            run_ready_market_ids = []
        except Exception as exc:
            cost_sync = {"status": "error", "profile_count": 0, "error": _public_error(exc)}
            run_ready_market_ids = []
    research_run_id: int | None = None
    run_payload: ResearchRunPayload | None = None
    if payload.auto_start_run and run_ready_market_ids:
        run_payload = _midcap_template_research_payload(payload, design, run_ready_market_ids, product_mode, cost_sync)
        selected_markets = _selected_markets(run_ready_market_ids)
        run_market_id = selected_markets[0].market_id if len(selected_markets) == 1 else "MULTI"
        research_run_id = research_store.create_run(
            market_id=run_market_id,
            data_source="eodhd_with_ig_cost_model",
            status="running",
            config=_research_run_config(run_payload, selected_markets),
        )
        background_tasks.add_task(_run_research_job, research_run_id, run_payload.model_dump(), api_token)
    rejected = [
        {
            "market_id": candidate.get("market_id"),
            "name": candidate.get("name"),
            "ig_status": candidate.get("ig_status"),
            "blockers": candidate.get("blockers", []),
            "warnings": candidate.get("warnings", []),
            "score": candidate.get("score", 0),
        }
        for candidate in candidates
        if isinstance(candidate, dict) and candidate not in selected_candidates
    ][:8]
    status = (
        "running"
        if research_run_id is not None
        else "blocked_price_validation"
        if selected_market_ids and payload.auto_sync_costs and not run_ready_market_ids
        else "ready_without_run"
        if selected_market_ids
        else "blocked_no_eligible_midcaps"
    )
    return {
        "schema": "midcap_template_pipeline_v1",
        "status": status,
        "design": design,
        "account_size": payload.account_size,
        "product_mode": product_mode,
        "strategy_generation_allowed_in_daily_mode": False,
        "design_mode": "research_discovery_only",
        "server_profile": _midcap_pipeline_server_profile(payload.max_markets, payload.limit, run_market_cap, discovery_limit),
        "live_ordering_enabled": False,
        "order_placement": "disabled",
        "discovery": {
            "schema": discovery.get("schema"),
            "country": discovery.get("country"),
            "data_source": discovery.get("data_source"),
            "eodhd_error": discovery.get("eodhd_error"),
            "fmp_error": discovery.get("fmp_error"),
            "source_errors": discovery.get("source_errors", []),
            "ig_status": discovery.get("ig_status"),
            "candidate_count": discovery.get("candidate_count", 0),
            "eligible_count": discovery.get("eligible_count", 0),
            "blocked_count": discovery.get("blocked_count", 0),
            "blocker_counts": discovery.get("blocker_counts", {}),
            "criteria": discovery.get("criteria", {}),
        },
        "selected_markets": installed_markets,
        "run_ready_market_ids": run_ready_market_ids,
        "rejected_candidates": rejected,
        "cost_sync": cost_sync,
        "research_run_id": research_run_id,
        "research_run_payload": run_payload.model_dump() if run_payload is not None else None,
        "auto_freeze_policy": {
            "enabled": bool(research_run_id),
            "max_freeze_runs": 1,
            "selection": "best IG-price-validated intraday lead with positive OOS, robust costs, and no fragile-evidence blockers",
            "validation": "one frozen-validation run with search_budget=1 and no parameter hunting",
        },
        "promotion_pipeline": _midcap_template_promotion_pipeline(status, research_run_id, str(cost_sync.get("status") or "skipped")),
        "next_actions": [
            "Open the research run after it finishes to see the Auto Freeze status." if research_run_id else "Fix IG price validation before running template discovery.",
            "If Auto Freeze is blocked, use Make tradeable or Repair remaining on the best non-terminal lead.",
            "Daily paper mode still uses only active frozen templates; rejected discovery leads will not fire.",
        ],
    }


def _price_validated_cost_profile_market_ids(cost_sync_result: dict[str, object]) -> set[str]:
    output: set[str] = set()
    for profile in cost_sync_result.get("profiles") or []:
        if not isinstance(profile, dict):
            continue
        confidence = str(profile.get("confidence") or "")
        market_id = str(profile.get("market_id") or "").strip()
        if market_id and confidence in PRICE_VALIDATED_COST_CONFIDENCES:
            output.add(market_id)
    return output


def _midcap_template_research_payload(
    payload: MidcapTemplatePipelinePayload,
    design: dict[str, object],
    market_ids: list[str],
    product_mode: str,
    cost_sync: dict[str, object],
) -> ResearchRunPayload:
    defaults = design.get("run_defaults") if isinstance(design.get("run_defaults"), dict) else {}
    return ResearchRunPayload(
        market_id=market_ids[0],
        market_ids=market_ids,
        start=payload.start,
        end=payload.end,
        interval=str(defaults.get("interval") or "5min"),
        engine="adaptive_ig_v1",
        search_preset=str(defaults.get("search_preset") or "balanced"),
        trading_style="intraday_only",
        objective=str(defaults.get("objective") or "profit_first"),
        search_budget=max(6, min(500, int(_safe_number(defaults.get("search_budget"), 54)))),
        risk_profile=str(defaults.get("risk_profile") or "conservative"),
        strategy_families=[str(item) for item in list(design.get("strategy_families") or []) if item],
        product_mode=product_mode,
        cost_stress_multiplier=max(1.0, _safe_number(defaults.get("cost_stress_multiplier"), 2.5)),
        include_regime_scans=True,
        regime_scan_budget_per_regime=max(1, min(96, int(_safe_number(defaults.get("regime_scan_budget_per_regime"), 12)))),
        diagnostic_limit=TWO_VCPU_MIDCAP_DIAGNOSTIC_LIMIT,
        include_market_context=False,
        target_regime=None,
        excluded_months=[],
        repair_mode="standard",
        account_size=payload.account_size,
        source_template={},
        pipeline={
            "schema": "midcap_template_pipeline_v1",
            "design_id": design.get("id"),
            "design_label": design.get("label"),
            "country": _validate_midcap_design_country(design, payload.country),
            "product_mode": product_mode,
            "selected_market_ids": market_ids,
            "auto_install": payload.auto_install,
            "auto_sync_costs": payload.auto_sync_costs,
            "auto_start_run": payload.auto_start_run,
            "cost_sync_status": cost_sync.get("status"),
            "server_profile": "guided_midcap_2vcpu_profile_v1",
            "diagnostic_limit_per_market": TWO_VCPU_MIDCAP_DIAGNOSTIC_LIMIT,
            "auto_freeze": {
                "enabled": True,
                "status": "waiting_for_design_run",
                "policy": "save_best_oos_positive_ig_validated_intraday_lead_then_one_trial_frozen_validation",
                "max_freeze_runs": 1,
                "strategy_generation_allowed": False,
            },
            "promotion_required": ["auto_save_best_freezeable_lead", "freeze_validate_exact_rules", "manual_repair_if_blocked"],
            "daily_mode_source": "active_frozen_template_library_only",
        },
        day_trading_mode=True,
        force_flat_before_close=True,
        paper_queue_limit=3,
        review_queue_limit=10,
    )


def _midcap_template_promotion_pipeline(status: str, research_run_id: int | None, cost_sync_status: str) -> list[dict[str, object]]:
    no_eligible = status == "blocked_no_eligible_midcaps"
    price_blocked = status == "blocked_price_validation"
    return [
        {
            "step": "discover_ig_midcaps",
            "status": "completed" if not no_eligible else "blocked",
            "detail": "Provider midcap universe filtered through liquidity, turnover, account-fit, and selected IG demo account checks.",
        },
        {
            "step": "sync_ig_costs",
            "status": "blocked" if no_eligible or price_blocked else cost_sync_status,
            "detail": "Requires IG price-validated spread, slippage, minimum deal size, margin, and stop-distance assumptions before running.",
        },
        {
            "step": "run_intraday_design_search",
            "status": "running" if research_run_id is not None else "blocked" if no_eligible or price_blocked else "waiting",
            "detail": "Runs the selected behaviour design on 5-minute bars with no overnight holding.",
            "run_id": research_run_id,
        },
        {
            "step": "auto_save_best_freezeable_lead",
            "status": "waiting_for_finished_run" if research_run_id is not None else "waiting",
            "detail": "When the design run finishes, the backend only saves leads with positive OOS, robust costs, and no fragile-evidence blockers.",
        },
        {
            "step": "auto_freeze_validate_exact_rules",
            "status": "waiting_for_saved_template" if research_run_id is not None else "waiting",
            "detail": "Launches one no-search frozen-validation run; failed or blocked templates stay out of daily paper.",
        },
    ]


async def _run_daily_template_scanner(payload: DailyTemplateScannerPayload, api_token: str) -> dict[str, object]:
    trading_date = _scanner_trading_date(payload.trading_date)
    product_mode = _normalize_product_mode(payload.product_mode)
    selected_markets = _selected_markets(payload.market_ids) if payload.market_ids else []
    templates = [
        template
        for template in research_store.list_templates(limit=250)
        if template.get("status") == "active" and _is_day_trading_template(template) and _is_frozen_template(template)
    ]
    provider = EODHDProvider(api_token)
    bars_cache: dict[tuple[str, str], list[object]] = {}
    review_candidates: list[dict[str, object]] = []
    unsuitable: list[dict[str, object]] = []
    no_setup: list[dict[str, object]] = []
    scanned_pairs = 0
    seen_pairs: set[tuple[int, str]] = set()
    market_statuses: list[dict[str, object]] = []
    playbook_counts: dict[str, int] = {}
    tape_blocker_counts: dict[str, int] = {}
    for template in templates:
        for market in _scanner_markets_for_template(template, selected_markets, payload.max_markets):
            pair_key = (int(template.get("id") or 0), market.market_id)
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)
            scanned_pairs += 1
            row = await _evaluate_daily_template_match(
                provider,
                template,
                market,
                trading_date,
                payload.lookback_days,
                payload.account_size,
                product_mode,
                bars_cache,
            )
            playbook = row.get("manual_playbook") if isinstance(row.get("manual_playbook"), dict) else {}
            playbook_id = str(playbook.get("id") or "unknown")
            playbook_counts[playbook_id] = playbook_counts.get(playbook_id, 0) + 1
            for blocker in row.get("today_filter_blockers") or []:
                key = str(blocker or "")
                if key:
                    tape_blocker_counts[key] = tape_blocker_counts.get(key, 0) + 1
            market_statuses.append(
                {
                    "template_id": template.get("id"),
                    "template_name": template.get("name"),
                    "market_id": market.market_id,
                    "status": row.get("status"),
                    "bar_count": row.get("bar_count", 0),
                    "reason": row.get("unsuitable_reason") or row.get("no_setup_reason") or "",
                }
            )
            if row.get("unsuitable"):
                unsuitable.append(row)
            elif row.get("setup_detected"):
                review_candidates.append(row)
            else:
                no_setup.append(row)
    review_signals = sorted(review_candidates, key=_daily_scan_signal_rank, reverse=True)[: payload.review_limit]
    daily_paper_queue = [item for item in review_signals if item.get("paper_ready")][: payload.paper_limit]
    scan_status = "ready_queue" if daily_paper_queue else "no_paper_setups"
    config: dict[str, object] = {
        "schema": "daily_template_scanner_config_v1",
        "trading_date": trading_date.isoformat(),
        "market_ids": [market.market_id for market in selected_markets],
        "product_mode": product_mode,
        "account_size": payload.account_size,
        "paper_limit": payload.paper_limit,
        "review_limit": payload.review_limit,
        "lookback_days": payload.lookback_days,
        "max_markets": payload.max_markets,
        "template_count": len(templates),
        "scanned_template_market_pairs": scanned_pairs,
        "no_setup_count": len(no_setup),
        "manual_playbook_counts": playbook_counts,
        "today_filter_blocker_counts": tape_blocker_counts,
        "no_setup_sample": no_setup[: min(10, payload.review_limit)],
        "market_statuses": market_statuses,
        "strategy_generation_allowed": False,
    }
    saved = research_store.save_day_trading_scan(
        trading_date=trading_date.isoformat(),
        status=scan_status,
        account_size=payload.account_size,
        product_mode=product_mode,
        config=config,
        daily_paper_queue=daily_paper_queue,
        review_signals=review_signals,
        unsuitable=unsuitable[: payload.review_limit],
    )
    return {
        "schema": "daily_template_scanner_v1",
        "scan_id": saved["id"],
        "created_at": saved["created_at"],
        "trading_date": trading_date.isoformat(),
        "status": scan_status,
        "mode": "manual",
        "live_ordering_enabled": False,
        "order_placement": "disabled",
        "strategy_generation_allowed": False,
        "daily_mode_source": "active_frozen_template_library_only",
        "account_size": payload.account_size,
        "product_mode": product_mode,
        "counts": {
            "active_frozen_templates": len(templates),
            "scanned_template_market_pairs": scanned_pairs,
            "daily_paper_queue": len(daily_paper_queue),
            "review_signals": len(review_signals),
            "unsuitable": len(unsuitable),
            "no_setup": len(no_setup),
            "manual_playbooks": len([key for key, count in playbook_counts.items() if count > 0]),
            "today_filter_blockers": len(tape_blocker_counts),
        },
        "daily_paper_queue": daily_paper_queue,
        "review_signals": review_signals,
        "unsuitable": unsuitable[: payload.review_limit],
        "no_setup_sample": no_setup[: min(10, payload.review_limit)],
        "manual_playbooks": [_manual_playbook_payload(playbook) for playbook in MANUAL_TRADER_PLAYBOOKS.values()],
        "latest_scan": saved,
    }


def _scanner_trading_date(value: object) -> date:
    if value in (None, ""):
        return date.today()
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="trading_date must be YYYY-MM-DD") from exc


def _scanner_markets_for_template(
    template: dict[str, object],
    selected_markets: list[MarketMapping],
    max_markets: int,
) -> list[MarketMapping]:
    if selected_markets:
        candidates = selected_markets
    else:
        scope = _template_match_scope(template)
        market_type = _market_type_for_template(template)
        if scope == "share_behavior" and market_type == "share":
            candidates = [market for market in markets.list(enabled_only=True) if market.asset_class == "share"]
        else:
            market = markets.get(str(template.get("market_id") or _template_source_template(template).get("market_id") or ""))
            candidates = [market] if market is not None else []
    output: list[MarketMapping] = []
    seen: set[str] = set()
    for market in candidates:
        if market.market_id in seen or not market.enabled or not market.eodhd_symbol:
            continue
        seen.add(market.market_id)
        output.append(market)
        if len(output) >= max_markets:
            break
    return output


async def _evaluate_daily_template_match(
    provider: EODHDProvider,
    template: dict[str, object],
    market: MarketMapping,
    trading_date: date,
    lookback_days: int,
    account_size: float,
    product_mode: str,
    bars_cache: dict[tuple[str, str], list[object]],
) -> dict[str, object]:
    interval = "5min"
    base = _daily_scan_base_payload(template, market, trading_date, interval, product_mode)
    profile = _cost_profile_for_market(market)
    profile_payload = profile.as_dict()
    epic = str(market.ig_epic or profile_payload.get("epic") or "").strip()
    if not epic:
        return {**base, "status": "unsuitable", "unsuitable": True, "unsuitable_reason": "IG EPIC is missing; sync/bind IG costs before daily scanning."}
    cache_key = (market.market_id, interval)
    try:
        bars = bars_cache.get(cache_key)
        if bars is None:
            start = (trading_date - timedelta(days=lookback_days)).isoformat()
            bars = provider and await provider.historical_bars(market.eodhd_symbol, interval, start, trading_date.isoformat())
            bars_cache[cache_key] = bars
    except Exception as exc:
        return {**base, "status": "unsuitable", "unsuitable": True, "unsuitable_reason": f"EODHD latest 5min bars failed: {_public_error(exc)}"}
    typed_bars = [bar for bar in bars if hasattr(bar, "timestamp") and hasattr(bar, "close")]
    minimum_bars = _daily_scanner_minimum_bars(template)
    if len(typed_bars) < minimum_bars:
        return {
            **base,
            "status": "no_setup_today",
            "setup_detected": False,
            "bar_count": len(typed_bars),
            "no_setup_reason": f"Need at least {minimum_bars} latest 5min bars for this frozen template.",
        }
    try:
        signal_result = apply_frozen_template_rules(
            typed_bars,
            market.market_id,
            interval,
            profile,
            _template_source_template(template),
            family=str(template.get("strategy_family") or ""),
            risk_profile="conservative",
            target_regime=str(template.get("target_regime") or ""),
            account_size=account_size,
        )
    except Exception as exc:
        return {**base, "status": "unsuitable", "unsuitable": True, "bar_count": len(typed_bars), "unsuitable_reason": f"Frozen template application failed: {_public_error(exc)}"}
    latest_bar = typed_bars[-1]
    if signal_result.current_signal == 0:
        tape_gate = _manual_setup_gate(template, market, typed_bars, trading_date, 0, profile_payload)
        return {
            **base,
            "status": "no_setup_today",
            "setup_detected": False,
            "bar_count": len(typed_bars),
            "latest_bar": _bar_payload(latest_bar),
            "current_regime": signal_result.current_regime,
            "target_regime": signal_result.target_regime,
            "regime_allowed": signal_result.regime_allowed,
            "manual_playbook": tape_gate["manual_playbook"],
            "today_tape": tape_gate["today_tape"],
            "today_filter_checks": tape_gate["checks"],
            "today_filter_blockers": tape_gate["blockers"],
            "signal_explainer": _daily_signal_explainer(
                template,
                market,
                signal_result,
                tape_gate,
                "Frozen rules did not produce an active setup on the latest 5min bar.",
            ),
            "no_setup_reason": "Frozen rules did not produce an active setup on the latest 5min bar.",
        }
    side = "BUY" if signal_result.current_signal > 0 else "SELL"
    tape_gate = _manual_setup_gate(template, market, typed_bars, trading_date, signal_result.current_signal, profile_payload)
    if not tape_gate["passed"]:
        reason = _today_filter_rejection_reason(tape_gate)
        return {
            **base,
            "status": "no_setup_today",
            "setup_detected": False,
            "bar_count": len(typed_bars),
            "latest_bar": _bar_payload(latest_bar),
            "current_regime": signal_result.current_regime,
            "target_regime": signal_result.target_regime,
            "regime_allowed": signal_result.regime_allowed,
            "side": side,
            "signal": signal_result.current_signal,
            "signal_state": _daily_signal_state(signal_result.previous_signal, signal_result.current_signal),
            "signal_age_bars": signal_result.signal_age_bars,
            "manual_playbook": tape_gate["manual_playbook"],
            "today_tape": tape_gate["today_tape"],
            "today_filter_checks": tape_gate["checks"],
            "today_filter_blockers": tape_gate["blockers"],
            "manual_setup_score": tape_gate["score"],
            "signal_explainer": _daily_signal_explainer(template, market, signal_result, tape_gate, reason),
            "no_setup_reason": reason,
        }
    stop, limit = _daily_scanner_stop_limit(float(latest_bar.close), side, signal_result.parameters)
    stake = _safe_number(signal_result.parameters.get("position_size")) or 1.0
    preview = broker_order_preview(
        _market_response(market),
        profile_payload,
        side,
        stake,
        account_size,
        entry_price=float(latest_bar.close),
        stop=stop,
        limit=limit,
    )
    rule_violations = [str(item) for item in preview.get("rule_violations", []) if item]
    terminal = _has_terminal_capital_blocker(rule_violations, {"violations": rule_violations})
    if rule_violations:
        return {
            **base,
            "status": "unsuitable",
            "unsuitable": True,
            "setup_detected": True,
            "bar_count": len(typed_bars),
            "latest_bar": _bar_payload(latest_bar),
            "current_regime": signal_result.current_regime,
            "target_regime": signal_result.target_regime,
            "side": side,
            "signal_state": _daily_signal_state(signal_result.previous_signal, signal_result.current_signal),
            "manual_playbook": tape_gate["manual_playbook"],
            "today_tape": tape_gate["today_tape"],
            "today_filter_checks": tape_gate["checks"],
            "today_filter_blockers": tape_gate["blockers"],
            "manual_setup_score": tape_gate["score"],
            "signal_explainer": _daily_signal_explainer(template, market, signal_result, tape_gate, "Broker preview blocked the setup for this account."),
            "broker_preview": preview,
            "rule_violations": rule_violations,
            "unsuitable_reason": _unsuitable_reason(rule_violations, {"violations": rule_violations}) if terminal else f"Broker preview failed: {', '.join(rule_violations)}.",
        }
    search_audit = _template_parameters(template).get("search_audit")
    search_audit = search_audit if isinstance(search_audit, dict) else {}
    backtest_summary = _summary_backtest(asdict(signal_result.backtest))
    return {
        **base,
        "status": "paper_preview",
        "unsuitable": False,
        "setup_detected": True,
        "paper_ready": True,
        "eligible_for_review": True,
        "bar_count": len(typed_bars),
        "latest_bar": _bar_payload(latest_bar),
        "current_regime": signal_result.current_regime,
        "target_regime": signal_result.target_regime,
        "regime_allowed": signal_result.regime_allowed,
        "side": side,
        "signal": signal_result.current_signal,
        "signal_state": _daily_signal_state(signal_result.previous_signal, signal_result.current_signal),
        "signal_age_bars": signal_result.signal_age_bars,
        "manual_playbook": tape_gate["manual_playbook"],
        "today_tape": tape_gate["today_tape"],
        "today_filter_checks": tape_gate["checks"],
        "today_filter_blockers": tape_gate["blockers"],
        "manual_setup_score": tape_gate["score"],
        "signal_explainer": _daily_signal_explainer(template, market, signal_result, tape_gate, "Frozen template fired and today's tape filters passed."),
        "broker_preview": preview,
        "rule_violations": [],
        "paper_readiness_score": search_audit.get("paper_readiness_score", template.get("robustness_score", 0)),
        "net_profit": backtest_summary.get("net_profit", 0),
        "oos_net_profit": _safe_number(_template_evidence(template).get("oos_net_profit"), backtest_summary.get("test_profit")),
        "trade_count": int(_safe_number(backtest_summary.get("trade_count"))),
        "cost_to_gross_ratio": backtest_summary.get("cost_to_gross_ratio", 0),
        "backtest_window": backtest_summary,
    }


def _daily_scan_base_payload(
    template: dict[str, object],
    market: MarketMapping,
    trading_date: date,
    interval: str,
    product_mode: str,
) -> dict[str, object]:
    playbook = _manual_playbook_for_template(template)
    return {
        "source_type": "daily_frozen_template_scan",
        "strategy_generation_allowed": False,
        "live_ordering_enabled": False,
        "order_placement": "disabled",
        "broker_preview_only": True,
        "frozen_rules": True,
        "daily_rule": "apply_frozen_template_without_parameter_changes",
        "trading_date": trading_date.isoformat(),
        "template_id": template.get("id"),
        "strategy_name": template.get("name"),
        "market_id": market.market_id,
        "market_name": market.name,
        "market_type": market.asset_class,
        "match_scope": _template_match_scope(template),
        "interval": interval,
        "product_mode": product_mode,
        "strategy_family": template.get("strategy_family") or _template_source_template(template).get("family"),
        "promotion_tier": template.get("promotion_tier"),
        "readiness_status": template.get("readiness_status"),
        "robustness_score": template.get("robustness_score", 0),
        "warnings": _template_warning_codes(template),
        "manual_playbook": _manual_playbook_payload(playbook),
        "unsuitable": False,
        "setup_detected": False,
        "paper_ready": False,
        "eligible_for_review": False,
    }


def _daily_scanner_minimum_bars(template: dict[str, object]) -> int:
    parameters = _template_parameters(template)
    lookback = max(2, _safe_int(parameters.get("lookback")) or 2)
    max_hold = max(1, _safe_int(parameters.get("max_hold_bars")) or 1)
    return max(lookback + max_hold + 2, 20)


def _daily_scanner_stop_limit(entry: float, side: str, parameters: dict[str, object]) -> tuple[float | None, float | None]:
    stop_bps = _safe_number(parameters.get("stop_loss_bps")) or 50.0
    target_bps = _safe_number(parameters.get("take_profit_bps")) or 80.0
    if side == "SELL":
        return round(entry * (1 + stop_bps / 10_000), 8), round(entry * (1 - target_bps / 10_000), 8)
    return round(entry * (1 - stop_bps / 10_000), 8), round(entry * (1 + target_bps / 10_000), 8)


def _daily_signal_state(previous_signal: int, current_signal: int) -> str:
    if current_signal == 0:
        return "flat"
    if previous_signal == 0:
        return "new_entry"
    if previous_signal == current_signal:
        return "active_hold"
    return "reversal"


def _manual_setup_gate(
    template: dict[str, object],
    market: MarketMapping,
    bars: list[object],
    trading_date: date,
    signal: int,
    profile_payload: dict[str, object],
) -> dict[str, object]:
    playbook = _manual_playbook_for_template(template)
    tape = _today_tape_snapshot(bars, trading_date)
    side = "BUY" if signal > 0 else "SELL" if signal < 0 else "FLAT"
    checks: list[dict[str, object]] = []

    def add_check(code: str, passed: bool, detail: str, value: object = None, threshold: object = None) -> None:
        checks.append({"code": code, "passed": bool(passed), "detail": detail, "value": value, "threshold": threshold})

    if not tape.get("has_session_bars"):
        add_check("no_same_day_bars", False, "No same-day 5-minute bars are available for the scanner date.")
    elif str(tape.get("active_session_date") or "") != trading_date.isoformat():
        add_check(
            "stale_intraday_bars",
            False,
            f"Latest intraday bars are from {tape.get('active_session_date')}, not {trading_date.isoformat()}.",
            tape.get("active_session_date"),
            trading_date.isoformat(),
        )
    else:
        add_check("same_day_bars", True, "Latest 5-minute bars are from the scanner date.", tape.get("active_session_date"), trading_date.isoformat())

    spread_bps = _safe_number(profile_payload.get("spread_bps"), profile_payload.get("estimated_spread_bps"))
    max_spread = _safe_number(playbook.get("max_spread_bps")) or 75.0
    add_check(
        "spread_within_playbook",
        spread_bps <= max_spread,
        f"Spread {round(spread_bps, 2)} bps must be at or below {round(max_spread, 2)} bps for this playbook.",
        round(spread_bps, 4),
        round(max_spread, 4),
    )

    relative_volume = _safe_number(tape.get("relative_volume"))
    min_relative_volume = _safe_number(playbook.get("min_relative_volume")) or 0.5
    add_check(
        "relative_volume",
        relative_volume >= min_relative_volume,
        f"Relative volume {round(relative_volume, 2)}x must be at least {round(min_relative_volume, 2)}x.",
        round(relative_volume, 4),
        round(min_relative_volume, 4),
    )

    if signal == 0:
        add_check("frozen_signal_active", False, "Frozen template did not fire on the latest bar.")
    else:
        add_check("frozen_signal_active", True, f"Frozen template produced a {side} signal.")

    playbook_id = str(playbook.get("id") or "")
    if playbook_id == "opening_range_breakout":
        _add_opening_range_breakout_checks(checks, tape, side, playbook)
    elif playbook_id == "vwap_trend_pullback":
        _add_vwap_trend_checks(checks, tape, side, playbook)
    elif playbook_id == "failed_breakout_reversal":
        _add_failed_breakout_reversal_checks(checks, tape, side, playbook)
    elif playbook_id == "high_relative_volume_trend":
        _add_high_relative_volume_trend_checks(checks, tape, side, playbook)
    else:
        _add_frozen_signal_confirmation_checks(checks, tape, side, playbook)

    blockers = [str(check["code"]) for check in checks if not check.get("passed")]
    return {
        "passed": not blockers,
        "score": _manual_setup_score(checks, tape),
        "manual_playbook": _manual_playbook_payload(playbook),
        "today_tape": tape,
        "checks": checks,
        "blockers": blockers,
        "market_id": market.market_id,
    }


def _add_opening_range_breakout_checks(
    checks: list[dict[str, object]],
    tape: dict[str, object],
    side: str,
    playbook: dict[str, object],
) -> None:
    min_break = _safe_number(playbook.get("min_opening_break_bps"))
    break_bps = _safe_number(tape.get("opening_range_break_bps"))
    trend_bps = _safe_number(tape.get("session_trend_bps"))
    if side == "SELL":
        passed_break = break_bps <= -min_break
        passed_trend = trend_bps <= -_safe_number(playbook.get("min_trend_bps"))
        vwap_passed = _safe_number(tape.get("distance_from_vwap_bps")) <= 0
    else:
        passed_break = break_bps >= min_break
        passed_trend = trend_bps >= _safe_number(playbook.get("min_trend_bps"))
        vwap_passed = _safe_number(tape.get("distance_from_vwap_bps")) >= 0
    _append_gate_check(checks, "opening_range_break", passed_break, "Price must break the first 30-minute range in the trade direction.", round(break_bps, 4), round(min_break, 4))
    _append_gate_check(checks, "session_trend_alignment", passed_trend, "Session trend must point in the trade direction.", round(trend_bps, 4), round(_safe_number(playbook.get("min_trend_bps")), 4))
    _append_gate_check(checks, "vwap_alignment", vwap_passed, "Price must be on the correct side of VWAP.", round(_safe_number(tape.get("distance_from_vwap_bps")), 4), 0)


def _add_vwap_trend_checks(
    checks: list[dict[str, object]],
    tape: dict[str, object],
    side: str,
    playbook: dict[str, object],
) -> None:
    trend_bps = _safe_number(tape.get("session_trend_bps"))
    distance_bps = _safe_number(tape.get("distance_from_vwap_bps"))
    min_trend = _safe_number(playbook.get("min_trend_bps"))
    max_against = _safe_number(playbook.get("max_vwap_distance_against_bps")) or 12.0
    if side == "SELL":
        trend_passed = trend_bps <= -min_trend
        vwap_passed = distance_bps <= max_against
    else:
        trend_passed = trend_bps >= min_trend
        vwap_passed = distance_bps >= -max_against
    _append_gate_check(checks, "session_trend_alignment", trend_passed, "Session trend must support the frozen signal.", round(trend_bps, 4), round(min_trend, 4))
    _append_gate_check(checks, "vwap_reclaim_or_hold", vwap_passed, "Price must be near or on the correct side of VWAP.", round(distance_bps, 4), round(max_against, 4))


def _add_failed_breakout_reversal_checks(
    checks: list[dict[str, object]],
    tape: dict[str, object],
    side: str,
    playbook: dict[str, object],
) -> None:
    sweep_bps = _safe_number(playbook.get("min_sweep_bps")) or 1.0
    distance_bps = _safe_number(tape.get("distance_from_vwap_bps"))
    low_sweep = _safe_number(tape.get("session_low_vs_opening_low_bps")) <= -sweep_bps
    high_sweep = _safe_number(tape.get("session_high_vs_opening_high_bps")) >= sweep_bps
    if side == "SELL":
        sweep_passed = high_sweep
        vwap_passed = distance_bps <= 0
    else:
        sweep_passed = low_sweep
        vwap_passed = distance_bps >= 0
    _append_gate_check(checks, "opening_range_sweep", sweep_passed, "Session must reject one side of the opening range before the reversal fires.", True if sweep_passed else False, True)
    _append_gate_check(checks, "vwap_reversal_confirmation", vwap_passed, "Reversal must reclaim or lose VWAP in the trade direction.", round(distance_bps, 4), 0)


def _add_high_relative_volume_trend_checks(
    checks: list[dict[str, object]],
    tape: dict[str, object],
    side: str,
    playbook: dict[str, object],
) -> None:
    trend_bps = _safe_number(tape.get("session_trend_bps"))
    distance_bps = _safe_number(tape.get("distance_from_vwap_bps"))
    min_trend = _safe_number(playbook.get("min_trend_bps"))
    if side == "SELL":
        trend_passed = trend_bps <= -min_trend
        vwap_passed = distance_bps <= 0
    else:
        trend_passed = trend_bps >= min_trend
        vwap_passed = distance_bps >= 0
    _append_gate_check(checks, "high_volume_trend_alignment", trend_passed, "High-volume setups must move in the trade direction.", round(trend_bps, 4), round(min_trend, 4))
    _append_gate_check(checks, "vwap_alignment", vwap_passed, "Price must be on the correct side of VWAP.", round(distance_bps, 4), 0)


def _add_frozen_signal_confirmation_checks(
    checks: list[dict[str, object]],
    tape: dict[str, object],
    side: str,
    playbook: dict[str, object],
) -> None:
    if side == "FLAT":
        return
    distance_bps = _safe_number(tape.get("distance_from_vwap_bps"))
    max_against = _safe_number(playbook.get("max_vwap_distance_against_bps")) or 25.0
    if side == "SELL":
        passed = distance_bps <= max_against
    else:
        passed = distance_bps >= -max_against
    _append_gate_check(checks, "no_obvious_vwap_conflict", passed, "Fallback playbook rejects only obvious VWAP conflicts.", round(distance_bps, 4), round(max_against, 4))


def _append_gate_check(
    checks: list[dict[str, object]],
    code: str,
    passed: bool,
    detail: str,
    value: object = None,
    threshold: object = None,
) -> None:
    checks.append({"code": code, "passed": bool(passed), "detail": detail, "value": value, "threshold": threshold})


def _manual_setup_score(checks: list[dict[str, object]], tape: dict[str, object]) -> float:
    if not checks:
        return 0.0
    pass_rate = sum(1 for check in checks if check.get("passed")) / len(checks)
    relative_volume = min(2.0, max(0.0, _safe_number(tape.get("relative_volume")))) / 2.0
    vwap_strength = min(1.0, abs(_safe_number(tape.get("distance_from_vwap_bps"))) / 80.0)
    break_strength = min(1.0, abs(_safe_number(tape.get("opening_range_break_bps"))) / 80.0)
    return round(100 * (0.62 * pass_rate + 0.18 * relative_volume + 0.10 * vwap_strength + 0.10 * break_strength), 4)


def _today_filter_rejection_reason(tape_gate: dict[str, object]) -> str:
    playbook = tape_gate.get("manual_playbook") if isinstance(tape_gate.get("manual_playbook"), dict) else {}
    blockers = [readable for readable in (_today_filter_blocker_label(code) for code in tape_gate.get("blockers") or []) if readable]
    if not blockers:
        return "Today's tape filters did not confirm the frozen setup."
    return f"{playbook.get('label') or 'Manual playbook'} blocked this setup: {', '.join(blockers[:3])}."


def _today_filter_blocker_label(code: object) -> str:
    labels = {
        "no_same_day_bars": "no same-day bars",
        "stale_intraday_bars": "stale intraday bars",
        "spread_within_playbook": "spread too wide",
        "relative_volume": "relative volume too low",
        "frozen_signal_active": "frozen signal inactive",
        "opening_range_break": "opening range not broken",
        "session_trend_alignment": "session trend disagrees",
        "vwap_alignment": "VWAP disagrees",
        "vwap_reclaim_or_hold": "VWAP not reclaimed/held",
        "opening_range_sweep": "no opening range rejection",
        "vwap_reversal_confirmation": "VWAP did not confirm reversal",
        "high_volume_trend_alignment": "trend not strong enough",
        "no_obvious_vwap_conflict": "obvious VWAP conflict",
    }
    return labels.get(str(code or ""), str(code or "").replace("_", " "))


def _daily_signal_explainer(
    template: dict[str, object],
    market: MarketMapping,
    signal_result: object,
    tape_gate: dict[str, object],
    headline: str,
) -> dict[str, object]:
    playbook = tape_gate.get("manual_playbook") if isinstance(tape_gate.get("manual_playbook"), dict) else {}
    tape = tape_gate.get("today_tape") if isinstance(tape_gate.get("today_tape"), dict) else {}
    passed = [check for check in tape_gate.get("checks", []) if isinstance(check, dict) and check.get("passed")]
    failed = [check for check in tape_gate.get("checks", []) if isinstance(check, dict) and not check.get("passed")]
    side = "BUY" if getattr(signal_result, "current_signal", 0) > 0 else "SELL" if getattr(signal_result, "current_signal", 0) < 0 else "FLAT"
    return {
        "headline": headline,
        "playbook": playbook.get("label") or "",
        "market": market.market_id,
        "template": template.get("name"),
        "side": side,
        "why_it_passed": [str(check.get("detail") or check.get("code")) for check in passed[:5]],
        "why_it_failed": [str(check.get("detail") or check.get("code")) for check in failed[:5]],
        "today_context": [
            f"Relative volume {round(_safe_number(tape.get('relative_volume')), 2)}x",
            f"VWAP distance {round(_safe_number(tape.get('distance_from_vwap_bps')), 1)} bps",
            f"Opening range break {round(_safe_number(tape.get('opening_range_break_bps')), 1)} bps",
            f"Session trend {round(_safe_number(tape.get('session_trend_bps')), 1)} bps",
        ],
        "rule_change_allowed": False,
    }


def _today_tape_snapshot(bars: list[object], trading_date: date) -> dict[str, object]:
    dated_bars = sorted(
        [bar for bar in bars if _bar_session_date(bar) is not None],
        key=lambda item: getattr(item, "timestamp", None),
    )
    if not dated_bars:
        return {"schema": "today_tape_snapshot_v1", "has_session_bars": False, "requested_date": trading_date.isoformat()}
    sessions: dict[date, list[object]] = {}
    for bar in dated_bars:
        session_date = _bar_session_date(bar)
        if session_date is None:
            continue
        sessions.setdefault(session_date, []).append(bar)
    active_date = trading_date if trading_date in sessions else max((day for day in sessions if day <= trading_date), default=max(sessions))
    session_bars = sessions.get(active_date, [])
    if not session_bars:
        return {"schema": "today_tape_snapshot_v1", "has_session_bars": False, "requested_date": trading_date.isoformat()}
    previous_dates = sorted(day for day in sessions if day < active_date)
    previous_date = previous_dates[-1] if previous_dates else None
    previous_close = _safe_number(getattr(sessions[previous_date][-1], "close", None)) if previous_date is not None else 0.0
    opening_count = min(6, len(session_bars))
    opening_bars = session_bars[:opening_count]
    opening_high = max(_safe_number(getattr(bar, "high", None), getattr(bar, "close", None)) for bar in opening_bars)
    opening_low = min(_safe_number(getattr(bar, "low", None), getattr(bar, "close", None)) for bar in opening_bars)
    session_high = max(_safe_number(getattr(bar, "high", None), getattr(bar, "close", None)) for bar in session_bars)
    session_low = min(_safe_number(getattr(bar, "low", None), getattr(bar, "close", None)) for bar in session_bars)
    first_bar = session_bars[0]
    latest_bar = session_bars[-1]
    first_open = _safe_number(getattr(first_bar, "open", None), getattr(first_bar, "close", None))
    latest_close = _safe_number(getattr(latest_bar, "close", None))
    vwap = _session_vwap(session_bars)
    relative_volume, relative_volume_source = _relative_session_volume(sessions, active_date, len(session_bars))
    opening_break_bps = 0.0
    if latest_close > opening_high and opening_high > 0:
        opening_break_bps = ((latest_close / opening_high) - 1.0) * 10_000
    elif latest_close < opening_low and opening_low > 0:
        opening_break_bps = -((opening_low / latest_close) - 1.0) * 10_000
    return {
        "schema": "today_tape_snapshot_v1",
        "requested_date": trading_date.isoformat(),
        "active_session_date": active_date.isoformat(),
        "has_session_bars": bool(session_bars),
        "bar_count": len(session_bars),
        "opening_range_bars": opening_count,
        "opening_high": round(opening_high, 8),
        "opening_low": round(opening_low, 8),
        "session_high": round(session_high, 8),
        "session_low": round(session_low, 8),
        "latest_close": round(latest_close, 8),
        "vwap": round(vwap, 8),
        "distance_from_vwap_bps": round(((latest_close / vwap) - 1.0) * 10_000, 4) if vwap > 0 else 0.0,
        "opening_range_break_bps": round(opening_break_bps, 4),
        "session_trend_bps": round(((latest_close / first_open) - 1.0) * 10_000, 4) if first_open > 0 else 0.0,
        "gap_bps": round(((first_open / previous_close) - 1.0) * 10_000, 4) if previous_close > 0 else 0.0,
        "relative_volume": round(relative_volume, 4),
        "relative_volume_source": relative_volume_source,
        "session_volume": round(sum(_safe_number(getattr(bar, "volume", None)) for bar in session_bars), 4),
        "session_high_vs_opening_high_bps": round(((session_high / opening_high) - 1.0) * 10_000, 4) if opening_high > 0 else 0.0,
        "session_low_vs_opening_low_bps": round(((session_low / opening_low) - 1.0) * 10_000, 4) if opening_low > 0 else 0.0,
        "latest_bar": _bar_payload(latest_bar),
    }


def _bar_session_date(bar: object) -> date | None:
    timestamp = getattr(bar, "timestamp", None)
    if timestamp is None or not hasattr(timestamp, "date"):
        return None
    return timestamp.date()


def _session_vwap(bars: list[object]) -> float:
    weighted = 0.0
    volume_total = 0.0
    for bar in bars:
        volume = _safe_number(getattr(bar, "volume", None))
        high = _safe_number(getattr(bar, "high", None), getattr(bar, "close", None))
        low = _safe_number(getattr(bar, "low", None), getattr(bar, "close", None))
        close = _safe_number(getattr(bar, "close", None))
        typical = (high + low + close) / 3.0 if high and low and close else close
        weighted += typical * max(0.0, volume)
        volume_total += max(0.0, volume)
    if volume_total <= 0:
        closes = [_safe_number(getattr(bar, "close", None)) for bar in bars]
        return sum(closes) / max(1, len(closes))
    return weighted / volume_total


def _relative_session_volume(sessions: dict[date, list[object]], active_date: date, active_count: int) -> tuple[float, str]:
    active_bars = sessions.get(active_date, [])
    active_volume = sum(_safe_number(getattr(bar, "volume", None)) for bar in active_bars)
    comparison: list[float] = []
    for day in sorted(day for day in sessions if day < active_date)[-8:]:
        bars = sessions[day][:active_count]
        if bars:
            comparison.append(sum(_safe_number(getattr(bar, "volume", None)) for bar in bars))
    if not comparison:
        return 1.0, "fallback_no_prior_sessions"
    average = sum(comparison) / max(1, len(comparison))
    if average <= 0:
        return 1.0, "fallback_zero_prior_volume"
    return active_volume / average, f"{len(comparison)} prior sessions"


def _manual_playbook_for_template(template: dict[str, object]) -> dict[str, object]:
    parameters = _template_parameters(template)
    source_template = _template_source_template(template)
    requested = str(
        parameters.get("manual_playbook")
        or parameters.get("setup_playbook")
        or source_template.get("manual_playbook")
        or source_template.get("setup_playbook")
        or ""
    ).strip()
    if requested in MANUAL_TRADER_PLAYBOOKS:
        return MANUAL_TRADER_PLAYBOOKS[requested]
    family = str(template.get("strategy_family") or source_template.get("family") or parameters.get("family") or "").strip()
    name = str(template.get("name") or source_template.get("name") or "").lower()
    if family == "liquidity_sweep_reversal" or "failed" in name or "reversal" in name or "sweep" in name:
        return MANUAL_TRADER_PLAYBOOKS["failed_breakout_reversal"]
    if family in {"breakout"} or "breakout" in name or "opening range" in name:
        return MANUAL_TRADER_PLAYBOOKS["opening_range_breakout"]
    if family in {"volatility_expansion", "scalping"} or "relative volume" in name or "momentum" in name:
        return MANUAL_TRADER_PLAYBOOKS["high_relative_volume_trend"]
    if family in {"intraday_trend", "mean_reversion"} or "pullback" in name or "vwap" in name or "trend" in name:
        return MANUAL_TRADER_PLAYBOOKS["vwap_trend_pullback"]
    return MANUAL_TRADER_PLAYBOOKS["frozen_signal_confirmation"]


def _daily_scan_signal_rank(signal: dict[str, object]) -> tuple[float, ...]:
    state_rank = {"new_entry": 3, "reversal": 2, "active_hold": 1}.get(str(signal.get("signal_state") or ""), 0)
    preview = signal.get("broker_preview") if isinstance(signal.get("broker_preview"), dict) else {}
    margin = _safe_number(preview.get("estimated_margin"))
    account_size = _safe_number(preview.get("account_size")) or WORKING_ACCOUNT_SIZE_GBP
    margin_headroom = 1.0 - min(1.0, margin / max(1.0, account_size * 0.35))
    tape = signal.get("today_tape") if isinstance(signal.get("today_tape"), dict) else {}
    return (
        state_rank,
        _safe_number(signal.get("manual_setup_score")),
        _safe_number(tape.get("relative_volume")),
        _safe_number(signal.get("paper_readiness_score")),
        _safe_number(signal.get("oos_net_profit")),
        _safe_number(signal.get("robustness_score")),
        margin_headroom,
        abs(_safe_number(tape.get("opening_range_break_bps"))),
        -_safe_number(signal.get("cost_to_gross_ratio")),
        -_safe_number(signal.get("signal_age_bars")),
    )


def _bar_payload(bar: object) -> dict[str, object]:
    return {
        "timestamp": getattr(bar, "timestamp", None).isoformat() if getattr(bar, "timestamp", None) is not None else "",
        "open": round(_safe_number(getattr(bar, "open", None)), 8),
        "high": round(_safe_number(getattr(bar, "high", None)), 8),
        "low": round(_safe_number(getattr(bar, "low", None)), 8),
        "close": round(_safe_number(getattr(bar, "close", None)), 8),
        "volume": round(_safe_number(getattr(bar, "volume", None)), 4),
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


def _is_day_trading_source(candidate: dict[str, object]) -> bool:
    parameters = _candidate_parameters(candidate)
    search_audit = parameters.get("search_audit") if isinstance(parameters.get("search_audit"), dict) else {}
    return bool(
        parameters.get("day_trading_mode")
        or search_audit.get("day_trading_mode")
        or parameters.get("holding_period") == "intraday"
        or parameters.get("force_flat_before_close")
        or parameters.get("no_overnight")
    )


def _is_day_trading_template(template: dict[str, object]) -> bool:
    parameters = _template_parameters(template)
    source_template = _template_source_template(template)
    return bool(
        parameters.get("day_trading_mode")
        or parameters.get("holding_period") == "intraday"
        or parameters.get("force_flat_before_close")
        or parameters.get("no_overnight")
        or source_template.get("holding_period") == "intraday"
        or source_template.get("force_flat_before_close")
        or source_template.get("no_overnight")
    )


def _is_overnight_template(template: dict[str, object]) -> bool:
    backtest = _template_backtest(template)
    family = str(template.get("strategy_family") or "").strip()
    interval = _template_interval(template)
    funding_cost = _safe_number(backtest.get("funding_cost"))
    return family in {"swing_trend", "calendar_turnaround_tuesday", "month_end_seasonality", "everyday_long"} or interval in {"1day", "1d", "daily"} or funding_cost > 0


def _is_frozen_template(template: dict[str, object]) -> bool:
    source_template = _template_source_template(template)
    parameters = source_template.get("parameters") if isinstance(source_template.get("parameters"), dict) else {}
    return bool(parameters)


def _day_trading_signal_payload(candidate: dict[str, object], account_size: float) -> dict[str, object] | None:
    parameters = _candidate_parameters(candidate)
    backtest = _candidate_backtest(candidate)
    readiness = _candidate_readiness(candidate)
    warnings = _candidate_warning_codes(candidate)
    scenario = _scenario_for_account(candidate.get("capital_scenarios"), account_size)
    if not parameters or not backtest:
        return None
    terminal = _has_terminal_capital_blocker(warnings, scenario)
    tier = str(candidate.get("promotion_tier") or _candidate_audit(candidate).get("promotion_tier") or "watchlist")
    paper_ready = readiness.get("status") == "ready_for_paper" or tier in {"paper_candidate", "validated_candidate"}
    eligible_for_review = tier in {"watchlist", "incubator", "research_candidate", "paper_candidate", "validated_candidate"} or _safe_number(backtest.get("net_profit")) > 0
    evidence = parameters.get("evidence_profile") if isinstance(parameters.get("evidence_profile"), dict) else {}
    search_audit = parameters.get("search_audit") if isinstance(parameters.get("search_audit"), dict) else {}
    pattern = parameters.get("bar_pattern_analysis") if isinstance(parameters.get("bar_pattern_analysis"), dict) else {}
    return {
        "id": candidate.get("id"),
        "run_id": candidate.get("run_id"),
        "strategy_name": candidate.get("strategy_name"),
        "market_id": candidate.get("market_id") or parameters.get("market_id"),
        "interval": parameters.get("timeframe") or parameters.get("interval") or "5min",
        "strategy_family": parameters.get("family"),
        "target_regime": parameters.get("target_regime") or pattern.get("target_regime"),
        "promotion_tier": tier,
        "readiness_status": readiness.get("status", "blocked"),
        "robustness_score": candidate.get("robustness_score", 0),
        "paper_readiness_score": search_audit.get("paper_readiness_score", 0),
        "net_profit": round(_safe_number(backtest.get("net_profit")), 4),
        "oos_net_profit": round(_safe_number(evidence.get("oos_net_profit"), backtest.get("test_profit")), 4),
        "trade_count": int(_safe_number(backtest.get("trade_count"))),
        "oos_trade_count": int(_safe_number(evidence.get("oos_trade_count"))),
        "cost_to_gross_ratio": round(_safe_number(backtest.get("cost_to_gross_ratio")), 6),
        "funding_cost": round(_safe_number(backtest.get("funding_cost")), 4),
        "warnings": warnings,
        "capital_scenario": scenario,
        "paper_ready": paper_ready and not terminal,
        "eligible_for_review": eligible_for_review and not terminal,
        "unsuitable": terminal,
        "unsuitable_reason": _unsuitable_reason(warnings, scenario) if terminal else "",
        "broker_preview_only": True,
        "live_ordering_enabled": False,
        "order_placement": "disabled",
    }


def _day_trading_template_payload(template: dict[str, object], account_size: float) -> dict[str, object] | None:
    parameters = _template_parameters(template)
    backtest = _template_backtest(template)
    evidence = _template_evidence(template)
    readiness = _template_readiness(template)
    warnings = _template_warning_codes(template)
    scenario = _scenario_for_account(template.get("capital_scenarios"), account_size)
    if not parameters and not _template_source_template(template):
        return None
    terminal = _has_terminal_capital_blocker(warnings, scenario)
    tier = str(template.get("promotion_tier") or "research_candidate")
    readiness_status = str(template.get("readiness_status") or readiness.get("status") or "blocked")
    interval = _template_interval(template)
    intraday_interval = _is_intraday_interval(interval)
    frozen = _is_frozen_template(template)
    active = template.get("status") == "active"
    overnight = _is_overnight_template(template)
    has_flat_policy = _template_has_flat_policy(template)
    blockers: list[str] = []
    if not active:
        blockers.append("template_not_active")
    if not frozen:
        blockers.append("template_not_frozen")
    if not intraday_interval:
        blockers.append("day_trade_requires_intraday_bars")
    if overnight:
        blockers.append("overnight_or_swing_template")
    if not has_flat_policy:
        blockers.append("day_trade_missing_flat_policy")
    if terminal:
        blockers.extend([warning for warning in warnings if warning in {"ig_minimum_margin_too_large_for_account", "ig_minimum_risk_too_large_for_account"}])
    structural_ready = active and frozen and intraday_interval and not overnight and has_flat_policy and not terminal
    readiness_ready = readiness_status == "ready_for_paper" or tier in {"paper_candidate", "validated_candidate"}
    paper_ready = structural_ready and readiness_ready
    source_template = _template_source_template(template)
    search_audit = parameters.get("search_audit") if isinstance(parameters.get("search_audit"), dict) else {}
    return {
        "id": template.get("id"),
        "template_id": template.get("id"),
        "source_type": "frozen_template",
        "strategy_name": template.get("name"),
        "name": template.get("name"),
        "market_id": template.get("market_id") or source_template.get("market_id") or parameters.get("market_id"),
        "market_type": _market_type_for_template(template),
        "match_scope": _template_match_scope(template),
        "interval": interval,
        "strategy_family": template.get("strategy_family") or source_template.get("family") or parameters.get("family"),
        "target_regime": template.get("target_regime") or source_template.get("target_regime") or parameters.get("target_regime"),
        "promotion_tier": tier,
        "readiness_status": readiness_status,
        "robustness_score": template.get("robustness_score", 0),
        "paper_readiness_score": search_audit.get("paper_readiness_score", template.get("robustness_score", 0)),
        "net_profit": round(_safe_number(backtest.get("net_profit")), 4),
        "oos_net_profit": round(_safe_number(evidence.get("oos_net_profit"), backtest.get("test_profit")), 4),
        "trade_count": int(_safe_number(backtest.get("trade_count"))),
        "oos_trade_count": int(_safe_number(evidence.get("oos_trade_count"))),
        "cost_to_gross_ratio": round(_safe_number(backtest.get("cost_to_gross_ratio")), 6),
        "funding_cost": round(_safe_number(backtest.get("funding_cost")), 4),
        "warnings": list(dict.fromkeys([*warnings, *blockers])),
        "capital_scenario": scenario,
        "frozen_rules": True,
        "strategy_generation_allowed": False,
        "paper_ready": paper_ready,
        "eligible_for_review": paper_ready,
        "unsuitable": terminal,
        "unsuitable_reason": _unsuitable_reason(warnings, scenario) if terminal else "",
        "broker_preview_only": True,
        "live_ordering_enabled": False,
        "order_placement": "disabled",
        "daily_rule": "match_frozen_template_only",
    }


def _template_queue_payload(template: dict[str, object]) -> dict[str, object]:
    payload = template.get("payload") if isinstance(template.get("payload"), dict) else {}
    warnings = _template_warning_codes(template)
    return {
        "id": template.get("id"),
        "name": template.get("name"),
        "market_id": template.get("market_id"),
        "market_type": _market_type_for_template(template),
        "match_scope": _template_match_scope(template),
        "interval": _template_interval(template),
        "strategy_family": template.get("strategy_family"),
        "target_regime": template.get("target_regime"),
        "promotion_tier": template.get("promotion_tier"),
        "readiness_status": template.get("readiness_status"),
        "robustness_score": template.get("robustness_score", 0),
        "warnings": warnings,
        "frozen_rules": _is_frozen_template(template),
        "holding_period": "intraday" if _is_day_trading_template(template) else "overnight_or_swing",
        "live_ordering_enabled": False,
        "strategy_generation_allowed": False,
    }


def _day_trading_signal_rank(candidate: dict[str, object]) -> tuple[float, ...]:
    tier_rank = {
        "validated_candidate": 5,
        "paper_candidate": 4,
        "research_candidate": 3,
        "incubator": 2,
        "watchlist": 1,
    }.get(str(candidate.get("promotion_tier") or ""), 0)
    scenario = candidate.get("capital_scenario") if isinstance(candidate.get("capital_scenario"), dict) else {}
    capital_rank = 1.0 if scenario.get("feasible") else 0.0
    cost_penalty = 1.0 if _safe_number(candidate.get("cost_to_gross_ratio")) > 0.65 else 0.0
    return (
        tier_rank,
        capital_rank,
        _safe_number(candidate.get("paper_readiness_score")),
        _safe_number(candidate.get("oos_net_profit")),
        _safe_number(candidate.get("net_profit")),
        _safe_number(candidate.get("oos_trade_count")),
        _safe_number(candidate.get("robustness_score")),
        -cost_penalty,
    )


def _candidate_parameters(candidate: dict[str, object]) -> dict[str, object]:
    audit = _candidate_audit(candidate)
    candidate_payload = audit.get("candidate") if isinstance(audit.get("candidate"), dict) else {}
    parameters = candidate_payload.get("parameters") if isinstance(candidate_payload.get("parameters"), dict) else {}
    return parameters


def _candidate_backtest(candidate: dict[str, object]) -> dict[str, object]:
    audit = _candidate_audit(candidate)
    return audit.get("backtest") if isinstance(audit.get("backtest"), dict) else {}


def _candidate_readiness(candidate: dict[str, object]) -> dict[str, object]:
    audit = _candidate_audit(candidate)
    readiness = audit.get("promotion_readiness") if isinstance(audit.get("promotion_readiness"), dict) else {}
    return readiness


def _candidate_audit(candidate: dict[str, object]) -> dict[str, object]:
    return candidate.get("audit") if isinstance(candidate.get("audit"), dict) else {}


def _candidate_warning_codes(candidate: dict[str, object]) -> list[str]:
    audit = _candidate_audit(candidate)
    readiness = _candidate_readiness(candidate)
    parameters = _candidate_parameters(candidate)
    pattern = parameters.get("bar_pattern_analysis") if isinstance(parameters.get("bar_pattern_analysis"), dict) else {}
    warnings = [
        *list(candidate.get("warnings") or []),
        *list(audit.get("warnings") or []),
        *list(readiness.get("blockers") or []),
        *list(readiness.get("validation_warnings") or []),
        *list(pattern.get("warnings") or []),
    ]
    return list(dict.fromkeys(str(warning) for warning in warnings if warning))


def _template_source_template(template: dict[str, object]) -> dict[str, object]:
    source_template = template.get("source_template") if isinstance(template.get("source_template"), dict) else {}
    payload = template.get("payload") if isinstance(template.get("payload"), dict) else {}
    payload_source = payload.get("source_template") if isinstance(payload.get("source_template"), dict) else {}
    return {**payload_source, **source_template}


def _template_parameters(template: dict[str, object]) -> dict[str, object]:
    payload = template.get("payload") if isinstance(template.get("payload"), dict) else {}
    payload_parameters = payload.get("parameters") if isinstance(payload.get("parameters"), dict) else {}
    parameters = template.get("parameters") if isinstance(template.get("parameters"), dict) else {}
    source_template = _template_source_template(template)
    source_parameters = source_template.get("parameters") if isinstance(source_template.get("parameters"), dict) else {}
    return {**source_parameters, **payload_parameters, **parameters}


def _template_backtest(template: dict[str, object]) -> dict[str, object]:
    payload = template.get("payload") if isinstance(template.get("payload"), dict) else {}
    if isinstance(template.get("backtest"), dict):
        return template["backtest"]
    return payload.get("backtest") if isinstance(payload.get("backtest"), dict) else {}


def _template_evidence(template: dict[str, object]) -> dict[str, object]:
    payload = template.get("payload") if isinstance(template.get("payload"), dict) else {}
    return payload.get("evidence") if isinstance(payload.get("evidence"), dict) else {}


def _template_readiness(template: dict[str, object]) -> dict[str, object]:
    payload = template.get("payload") if isinstance(template.get("payload"), dict) else {}
    readiness = template.get("readiness") if isinstance(template.get("readiness"), dict) else {}
    payload_readiness = payload.get("readiness") if isinstance(payload.get("readiness"), dict) else {}
    return {**payload_readiness, **readiness}


def _template_warning_codes(template: dict[str, object]) -> list[str]:
    payload = template.get("payload") if isinstance(template.get("payload"), dict) else {}
    readiness = _template_readiness(template)
    parameters = _template_parameters(template)
    pattern = template.get("pattern") if isinstance(template.get("pattern"), dict) else payload.get("pattern") if isinstance(payload.get("pattern"), dict) else {}
    if not isinstance(pattern, dict):
        pattern = {}
    warnings = [
        *list(template.get("warnings") or []),
        *list(payload.get("warnings") or []),
        *list(readiness.get("blockers") or []),
        *list(readiness.get("validation_warnings") or []),
        *list(pattern.get("warnings") or []),
        *list(parameters.get("warnings") or []),
    ]
    return list(dict.fromkeys(str(warning) for warning in warnings if warning))


def _template_interval(template: dict[str, object]) -> str:
    parameters = _template_parameters(template)
    source_template = _template_source_template(template)
    return str(template.get("interval") or source_template.get("interval") or parameters.get("timeframe") or parameters.get("interval") or "5min")


def _template_has_flat_policy(template: dict[str, object]) -> bool:
    parameters = _template_parameters(template)
    source_template = _template_source_template(template)
    return bool(
        parameters.get("force_flat_before_close")
        or parameters.get("no_overnight")
        or source_template.get("force_flat_before_close")
        or source_template.get("no_overnight")
    )


def _is_intraday_interval(interval: object) -> bool:
    return str(interval or "").strip().lower().replace(" ", "") in INTRADAY_INTERVALS


def _market_type_for_template(template: dict[str, object]) -> str:
    market_id = str(template.get("market_id") or _template_source_template(template).get("market_id") or "").strip()
    if market_id:
        market = markets.get(market_id)
        if market is not None:
            return market.asset_class
    parameters = _template_parameters(template)
    return str(parameters.get("market_type") or parameters.get("asset_class") or "").strip()


def _template_match_scope(template: dict[str, object]) -> str:
    parameters = _template_parameters(template)
    source_template = _template_source_template(template)
    scope = str(parameters.get("template_scope") or source_template.get("template_scope") or "").strip()
    if scope:
        return scope
    market_type = _market_type_for_template(template)
    if market_type == "share":
        return "share_behavior"
    return "single_market"


def _scenario_for_account(scenarios: object, account_size: float) -> dict[str, object]:
    if not isinstance(scenarios, list):
        return {}
    selected = next((item for item in scenarios if isinstance(item, dict) and _safe_number(item.get("account_size")) == float(account_size)), None)
    if selected is None:
        selected = next((item for item in scenarios if isinstance(item, dict)), None)
    return selected or {}


def _has_terminal_capital_blocker(warnings: list[str], scenario: dict[str, object]) -> bool:
    violations = list(scenario.get("violations") or []) if isinstance(scenario, dict) else []
    codes = set(warnings) | {str(item) for item in violations if item}
    return bool(codes & {"ig_minimum_margin_too_large_for_account", "ig_minimum_risk_too_large_for_account"})


def _unsuitable_reason(warnings: list[str], scenario: dict[str, object]) -> str:
    violations = list(scenario.get("violations") or []) if isinstance(scenario, dict) else []
    codes = [*warnings, *(str(item) for item in violations if item)]
    if "ig_minimum_margin_too_large_for_account" in codes:
        return "IG minimum margin is too large for the selected account."
    if "ig_minimum_risk_too_large_for_account" in codes:
        return "IG minimum risk is too large for the selected account."
    return "Capital fit failed for the selected account."


def _safe_number(*values: object) -> float:
    for value in values:
        if value in (None, ""):
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if isfinite(number):
            return number
    return 0.0



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


async def _enrich_midcap_fallback_rows_with_fmp_quotes(
    provider: FMPProvider,
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    quote_rows = await provider.batch_quote([str(row.get("symbol") or "") for row in rows])
    quote_by_symbol = {str(row.get("symbol") or "").upper(): row for row in quote_rows}
    enriched: list[dict[str, object]] = []
    for row in rows:
        symbol = str(row.get("symbol") or "").upper()
        quote = quote_by_symbol.get(symbol)
        if not quote:
            enriched.append(row)
            continue
        enriched.append(
            {
                **row,
                "price": quote.get("price") or quote.get("previousClose") or row.get("price"),
                "volume": quote.get("volume") or quote.get("avgVolume") or quote.get("averageVolume") or row.get("volume"),
                "marketCap": quote.get("marketCap") or row.get("marketCap"),
                "exchangeShortName": quote.get("exchange") or quote.get("exchangeShortName") or row.get("exchangeShortName"),
                "currency": quote.get("currency") or row.get("currency"),
                "source": "built_in_midcap_starter_with_fmp_quotes",
            }
        )
    return enriched


async def _verify_midcap_candidates_with_ig(
    provider: IGDemoProvider,
    candidates: list[MidcapDiscoveryCandidate],
) -> list[MidcapDiscoveryCandidate]:
    verified: list[MidcapDiscoveryCandidate] = []
    for candidate in candidates:
        market = candidate.market_mapping()
        try:
            resolved = await _resolve_ig_market(provider, market)
        except Exception:
            resolved = None
        if resolved is None:
            verified.append(candidate.with_ig_match("", ""))
            continue
        resolved_market, _note = resolved
        verified.append(candidate.with_ig_match(resolved_market.ig_epic, resolved_market.ig_name))
    return sorted(verified, key=lambda item: (item.eligible, item.ig_status == "ig_matched", item.score), reverse=True)


def _ig_account_roles_summary() -> dict[str, object]:
    spread_bet_id = settings.get_secret("ig_accounts", "spread_bet_account_id") or ""
    cfd_id = settings.get_secret("ig_accounts", "cfd_account_id") or ""
    default_product_mode = _normalize_product_mode(settings.get_secret("ig_accounts", "default_product_mode") or "spread_bet")
    spread_bet_status = settings.get_secret("ig_accounts", "spread_bet_validation_status") or ("legacy_saved" if spread_bet_id else "missing")
    cfd_status = settings.get_secret("ig_accounts", "cfd_validation_status") or ("legacy_saved" if cfd_id else "missing")
    spread_bet_active = bool(spread_bet_id) and spread_bet_status in {"validated", "legacy_saved"}
    cfd_active = bool(cfd_id) and cfd_status in {"validated", "legacy_saved"}
    return {
        "default_product_mode": default_product_mode,
        "spread_bet": {
            "configured": bool(spread_bet_id),
            "active": spread_bet_active,
            "masked_account_id": _mask_account_id(spread_bet_id),
            "display_name": settings.get_secret("ig_accounts", "spread_bet_display_name") or "",
            "validation_status": spread_bet_status,
        },
        "cfd": {
            "configured": bool(cfd_id),
            "active": cfd_active,
            "masked_account_id": _mask_account_id(cfd_id),
            "display_name": settings.get_secret("ig_accounts", "cfd_display_name") or "",
            "validation_status": cfd_status,
        },
        "both_active": spread_bet_active and cfd_active,
        "live_ordering_enabled": False,
        "notes": [
            "Both demo account roles can be active under the same IG login/API key; order placement remains disabled.",
            "Backtests remain spread-bet cost model until a dedicated CFD cost model is added.",
        ],
    }


async def _resolve_ig_account_roles(payload: IGAccountRolesPayload) -> dict[str, dict[str, str]]:
    raw_roles = {
        "spread_bet": payload.spread_bet_account_id.strip(),
        "cfd": payload.cfd_account_id.strip(),
    }
    api_key = settings.get_secret("ig", "api_key")
    username = settings.get_secret("ig", "username")
    password = settings.get_secret("ig", "password")
    if not api_key or not username or not password:
        return {
            role: {
                "account_id": value,
                "display_name": "",
                "validation_status": "saved_unvalidated" if value else "missing",
            }
            for role, value in raw_roles.items()
        }
    provider = IGDemoProvider(api_key, username, password)
    try:
        accounts = await provider.accounts()
    except Exception as exc:
        detail = _public_error(exc)
        settings.set_status("ig_accounts", "error", detail)
        raise HTTPException(status_code=400, detail=f"IG account role validation failed: {detail}") from exc
    return {
        role: _resolve_ig_account_role(value, accounts, _product_label(role))
        for role, value in raw_roles.items()
    }


def _resolve_ig_account_role(value: str, accounts: list[dict[str, object]], role_label: str) -> dict[str, str]:
    if not value:
        return {"account_id": "", "display_name": "", "validation_status": "missing"}
    direct = [account for account in accounts if _account_field(account, "accountId") == value]
    if direct:
        return _resolved_account_payload(direct[0])
    normalized = _normalize_account_match_text(value)
    exact = [
        account
        for account in accounts
        if normalized and normalized in _normalized_account_match_values(account)
    ]
    if len(exact) == 1:
        return _resolved_account_payload(exact[0])
    if len(exact) > 1:
        raise HTTPException(status_code=400, detail=f"{role_label} account name '{value}' matched multiple IG demo accounts; enter the account code instead.")
    contains = [
        account
        for account in accounts
        if normalized
        and any(normalized in part or part in normalized for part in _normalized_account_match_values(account))
    ]
    if len(contains) == 1:
        return _resolved_account_payload(contains[0])
    if len(contains) > 1:
        raise HTTPException(status_code=400, detail=f"{role_label} account name '{value}' matched multiple IG demo accounts; enter the account code instead.")
    available = ", ".join(
        part
        for account in accounts
        if (part := _account_display_name(account))
    )
    raise HTTPException(
        status_code=400,
        detail=f"{role_label} account '{value}' was not found in IG demo accounts. Available accounts: {available or 'none returned'}",
    )


def _store_ig_account_role(role: str, resolved: dict[str, str]) -> None:
    settings.set_secret("ig_accounts", f"{role}_account_id", resolved.get("account_id", ""))
    settings.set_secret("ig_accounts", f"{role}_display_name", resolved.get("display_name", ""))
    settings.set_secret("ig_accounts", f"{role}_validation_status", resolved.get("validation_status", "missing"))


def _resolved_account_payload(account: dict[str, object]) -> dict[str, str]:
    return {
        "account_id": _account_field(account, "accountId"),
        "display_name": _account_display_name(account),
        "validation_status": "validated",
    }


def _account_match_values(account: dict[str, object]) -> list[str]:
    return [
        _account_field(account, "accountId"),
        _account_field(account, "accountName"),
        _account_field(account, "accountAlias"),
        _account_field(account, "name"),
        _account_field(account, "alias"),
        _account_field(account, "title"),
        _account_field(account, "accountType"),
    ]


def _normalized_account_match_values(account: dict[str, object]) -> set[str]:
    return {
        normalized
        for part in _account_match_values(account)
        if (normalized := _normalize_account_match_text(part))
    }


def _account_display_name(account: dict[str, object]) -> str:
    for key in ("accountName", "accountAlias", "name", "alias", "title", "accountType", "accountId"):
        value = _account_field(account, key)
        if value:
            return value
    return ""


def _account_field(account: dict[str, object], key: str) -> str:
    return str(account.get(key) or "").strip()


def _normalize_account_match_text(value: object) -> str:
    return " ".join("".join(character.lower() if character.isalnum() else " " for character in str(value or "")).split())


def _product_label(product_mode: str) -> str:
    return "CFD demo" if _normalize_product_mode(product_mode) == "cfd" else "Spread bet demo"


def _mask_account_id(account_id: str) -> str:
    value = str(account_id or "").strip()
    if not value:
        return ""
    if len(value) <= 4:
        return "*" * len(value)
    return f"{'*' * max(3, len(value) - 4)}{value[-4:]}"


def _normalize_product_mode(value: object) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_")
    return normalized if normalized in PRODUCT_MODES else "spread_bet"


def _account_id_for_product_mode(product_mode: str | None = None) -> str:
    mode = _normalize_product_mode(product_mode or settings.get_secret("ig_accounts", "default_product_mode") or "spread_bet")
    role_key = "cfd_account_id" if mode == "cfd" else "spread_bet_account_id"
    return settings.get_secret("ig_accounts", role_key) or ""


def _ig_provider_from_settings(product_mode: str | None = None) -> IGDemoProvider | None:
    api_key = settings.get_secret("ig", "api_key")
    username = settings.get_secret("ig", "username")
    password = settings.get_secret("ig", "password")
    if not api_key or not username or not password:
        return None
    explicit_product = product_mode is not None and _normalize_product_mode(product_mode) in PRODUCT_MODES
    role_account_id = _account_id_for_product_mode(product_mode)
    if explicit_product and not role_account_id:
        return None
    account_id = role_account_id or settings.get_secret("ig", "account_id") or ""
    return IGDemoProvider(api_key, username, password, account_id)


def _ig_provider_blocker(product_mode: str | None = None) -> tuple[str, str]:
    api_key = settings.get_secret("ig", "api_key")
    username = settings.get_secret("ig", "username")
    password = settings.get_secret("ig", "password")
    if not api_key or not username or not password:
        return "ig_credentials_required", "ig_catalogue_required"
    mode = _normalize_product_mode(product_mode or settings.get_secret("ig_accounts", "default_product_mode") or "spread_bet")
    if not _account_id_for_product_mode(mode):
        return f"ig_{mode}_demo_account_required", "ig_demo_account_role_required"
    return "ig_credentials_required", "ig_catalogue_required"


def _candidate_issue_counts(candidates: list[dict[str, object]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for candidate in candidates:
        for value in list(candidate.get(field) or []):
            key = str(value or "").strip()
            if key:
                counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _eodhd_midcap_rows(rows: list[dict[str, object]], criteria: MidcapDiscoveryCriteria, country_hint: str) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for row in rows:
        code = str(row.get("code") or row.get("Code") or row.get("symbol") or "").strip()
        if not code or not code[0].isalpha():
            continue
        market_cap = _safe_number(row.get("market_capitalization"), row.get("marketCap"), row.get("market_cap"))
        price = _safe_number(row.get("adjusted_close"), row.get("price"), row.get("close"))
        volume = _safe_number(row.get("avgvol_200d"), row.get("avgvol_1d"), row.get("volume"))
        if market_cap < criteria.min_market_cap or market_cap > criteria.max_market_cap:
            continue
        if price <= 0 or volume < criteria.min_volume:
            continue
        if country_hint == "GB":
            currency = str(row.get("currency_symbol") or row.get("currency") or "").strip().lower()
            if currency not in {"p", "gbp", "gbx", "£"}:
                continue
            if _eodhd_row_looks_like_fund(row):
                continue
        if country_hint == "US":
            currency = str(row.get("currency_symbol") or row.get("currency") or "").strip().lower()
            if currency and currency not in {"$", "usd"}:
                continue
        output.append(row)
    return output


def _eodhd_row_looks_like_fund(row: dict[str, object]) -> bool:
    text = " ".join(str(row.get(key) or "") for key in ("name", "Name", "industry", "sector")).lower()
    return any(term in text for term in (" etf", "ucits", " etn", " etc", "investment trust", "fund", "physical "))


def _public_error(exc: Exception) -> str:
    text = str(exc)
    text = re.sub(r"apikey=[^&'\"\s]+", "apikey=***", text, flags=re.IGNORECASE)
    text = re.sub(r"api_token=[^&'\"\s]+", "api_token=***", text, flags=re.IGNORECASE)
    text = re.sub(r"api[-_ ]?key[^,;\n]*", "API key hidden", text, flags=re.IGNORECASE)
    return text
