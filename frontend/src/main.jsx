import React from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  Archive,
  BarChart3,
  BookOpen,
  CalendarDays,
  Database,
  Download,
  Home,
  KeyRound,
  Library,
  LineChart,
  LockKeyhole,
  Plug,
  Plus,
  RefreshCw,
  Save,
  Search,
  ShieldCheck,
  SlidersHorizontal,
  Sparkles,
  Trash2,
  Wallet,
} from "lucide-react";
import {
  archiveResearchRun,
  createResearchRun,
  deleteResearchRun,
  discoverMidcaps,
  getBacktestsSummary,
  getBrokerSummary,
  getCockpitSummary,
  getDayTradingFactorySummary,
  getDayTradingTemplateDesigns,
  getIgCostProfile,
  getMarketContextStack,
  getMarketContextSummary,
  getMarketDataCacheStatus,
  getMarketPlugins,
  getMarkets,
  getPaperSummary,
  getResearchCritique,
  getResearchSummary,
  getResearchRun,
  getRiskSummary,
  getSettingsSummary,
  getStatus,
  getTemplatesSummary,
  installMarketPlugin,
  previewBrokerOrder,
  pruneMarketDataCache,
  researchRunExportUrl,
  saveEodhd,
  saveFmp,
  saveIg,
  saveIgAccountRoles,
  saveMarket,
  saveResearchSchedule,
  saveStrategyTemplate,
  startDailyTemplateScanner,
  startMidcapTemplatePipeline,
  syncIgCosts,
  updateStrategyTemplateStatus,
} from "./api";
import "./styles.css";

const WORKING_ACCOUNT_SIZE = 3000;
const MAX_MARGIN_FRACTION = 0.35;
const MAX_REPAIR_ATTEMPTS = 3;

const FALLBACK_ENGINES = [
  {
    id: "adaptive_ig_v1",
    label: "Adaptive IG-aware search",
    description: "Searches trading styles and risk settings after IG-style costs.",
  },
  {
    id: "probability_stack_v1",
    label: "Probability stack v1",
    description: "Triple-barrier labels with momentum, pullback, and breakout probability modules.",
  },
];

const CANDLE_INTERVALS = [
  { value: "market_default", label: "Market default" },
  { value: "5min", label: "5 minute" },
  { value: "15min", label: "15 minute" },
  { value: "30min", label: "30 minute" },
  { value: "1hour", label: "1 hour" },
  { value: "1day", label: "1 day" },
];

const SEARCH_PRESETS = [
  { id: "quick", label: "Quick", budget: 18 },
  { id: "balanced", label: "Balanced", budget: 54 },
  { id: "deep", label: "Deep", budget: 120 },
];
const MULTI_MARKET_TOTAL_TRIAL_CAPS = { quick: 96, balanced: 216, deep: 480 };
const MULTI_MARKET_MIN_TRIALS_PER_MARKET = { quick: 6, balanced: 9, deep: 12 };
const DAY_TRADING_FAMILIES = ["intraday_trend", "breakout", "liquidity_sweep_reversal", "mean_reversion", "volatility_expansion", "scalping"];

function normalizeDesignCountry(value) {
  const country = String(value || "").trim().toUpperCase();
  if (country === "GB" || country === "UK" || country === "UNITED KINGDOM") return "UK";
  if (country === "USA" || country === "US" || country === "UNITED STATES") return "US";
  return country;
}

function designMatchesCountry(design, country) {
  return normalizeDesignCountry(design?.country) === normalizeDesignCountry(country);
}

const STYLE_OPTIONS = [
  { id: "find_anything_robust", label: "Find anything robust" },
  { id: "everyday_long", label: "Everyday long" },
  { id: "research_ideas", label: "Known research ideas" },
  { id: "intraday_only", label: "Intraday only" },
  { id: "swing_trades", label: "Swing trades" },
  { id: "lower_drawdown", label: "Lower drawdown" },
  { id: "higher_profit", label: "Higher profit" },
];

const OBJECTIVES = [
  { id: "balanced", label: "Balanced" },
  { id: "sharpe_first", label: "Sharpe first" },
  { id: "profit_first", label: "Profit first" },
];

const CANDIDATE_FACTORY_MODES = [
  {
    id: "day_trading",
    label: "Intraday discovery",
    badge: "Discovery only",
    detail: "Searches for no-overnight ideas to repair, freeze, and validate. Daily paper mode uses saved frozen templates only.",
    preset: "balanced",
    budgetLabel: "54 base trials / market + capped regime specialists",
  },
  {
    id: "balanced",
    label: "Balanced factory",
    badge: "Recommended",
    detail: "Find-anything-robust, regime templates on, realistic costs, and enough breadth to create leads without overloading the 2 vCPU server.",
    preset: "balanced",
    budgetLabel: "54 base trials / market + capped regime specialists",
  },
  {
    id: "deep_one_market",
    label: "Deep one-market factory",
    badge: "Best for gold",
    detail: "Uses only the first selected market and spends the whole budget there. Best when you want templates for every regime on one instrument.",
    preset: "deep",
    budgetLabel: "120 base trials + 18 trials / eligible regime",
  },
  {
    id: "evidence_first",
    label: "Evidence-first factory",
    badge: "Cleaner leads",
    detail: "Keeps discovery broad but ranks harder on OOS, folds, costs, and selected-capital fit before you start repairing.",
    preset: "balanced",
    budgetLabel: "54 base trials / market with stricter evidence ranking",
  },
];

const MODULES = [
  ["cockpit", "Today", Home],
  ["broker", "Accounts", Wallet],
  ["backtests", "Build Templates", Sparkles],
  ["paper", "Daily Paper", LineChart],
  ["templates", "Library", Library],
  ["settings", "Settings", SlidersHorizontal],
];

function App() {
  const [status, setStatus] = React.useState([]);
  const [markets, setMarkets] = React.useState([]);
  const [plugins, setPlugins] = React.useState([]);
  const [cacheStatus, setCacheStatus] = React.useState(null);
  const [spreadBetEngines, setSpreadBetEngines] = React.useState([]);
  const [engines, setEngines] = React.useState(FALLBACK_ENGINES);
  const [researchRuns, setResearchRuns] = React.useState([]);
  const [templates, setTemplates] = React.useState(null);
  const [candidates, setCandidates] = React.useState([]);
  const [critique, setCritique] = React.useState(null);
  const [runDetail, setRunDetail] = React.useState(null);
  const [costProfiles, setCostProfiles] = React.useState({});
  const [refinementTemplate, setRefinementTemplate] = React.useState(null);
  const [message, setMessage] = React.useState("");
  const [eodhdKey, setEodhdKey] = React.useState("");
  const [fmpKey, setFmpKey] = React.useState("");
  const [ig, setIg] = React.useState({ apiKey: "", username: "", password: "", accountId: "" });
  const [igRoles, setIgRoles] = React.useState({ spreadBetAccountId: "", cfdAccountId: "", defaultProductMode: "spread_bet" });
  const [igAccountRoles, setIgAccountRoles] = React.useState(null);
  const [midcapSearch, setMidcapSearch] = React.useState({
    country: "UK",
    product_mode: "spread_bet",
    limit: "24",
    min_market_cap: "250000000",
    max_market_cap: "10000000000",
    min_volume: "100000",
    max_spread_bps: "60",
    account_size: String(WORKING_ACCOUNT_SIZE),
    verify_ig: true,
    require_ig_catalogue: true,
    design_id: "liquid_uk_midcap_trend_pullback",
    max_markets: "3",
  });
  const [midcapDiscovery, setMidcapDiscovery] = React.useState(null);
  const [midcapLoading, setMidcapLoading] = React.useState(false);
  const [templateDesigns, setTemplateDesigns] = React.useState([]);
  const [midcapPipeline, setMidcapPipeline] = React.useState(null);
  const [midcapPipelineState, setMidcapPipelineState] = React.useState({ status: "idle", detail: "" });
  const [activeModule, setActiveModule] = React.useState("cockpit");
  const [loadingModule, setLoadingModule] = React.useState("");
  const [cockpit, setCockpit] = React.useState(null);
  const [marketContext, setMarketContext] = React.useState(null);
  const [paper, setPaper] = React.useState(null);
  const [dayFactory, setDayFactory] = React.useState(null);
  const [dailyScannerState, setDailyScannerState] = React.useState({ status: "idle", detail: "" });
  const [broker, setBroker] = React.useState(null);
  const [risk, setRisk] = React.useState(null);
  const [selectedProductMode, setSelectedProductMode] = React.useState("spread_bet");
  const [includeArchivedRuns, setIncludeArchivedRuns] = React.useState(false);
  const [exportIncludeBars, setExportIncludeBars] = React.useState(true);
  const [market, setMarket] = React.useState({
    market_id: "GBPUSD",
    name: "GBP/USD",
    asset_class: "forex",
    eodhd_symbol: "GBPUSD.FOREX",
    ig_epic: "",
    ig_name: "GBP/USD",
    ig_search_terms: "GBP/USD,GBPUSD",
    default_timeframe: "1hour",
    spread_bps: 1.4,
    slippage_bps: 0.9,
    min_backtest_bars: 750,
    enabled: true,
  });
  const [activeTab, setActiveTab] = React.useState("factory");
  const [activeMarketIds, setActiveMarketIds] = React.useState(["NAS100"]);
  const [researchRun, setResearchRun] = React.useState({
    market_id: "NAS100",
    engine: "adaptive_ig_v1",
    start: "2025-01-01",
    end: "2026-04-01",
    interval: "market_default",
    search_preset: "balanced",
    trading_style: "find_anything_robust",
    objective: "profit_first",
    search_budget: "",
    risk_profile: "balanced",
    strategy_families: [],
    product_mode: "spread_bet",
    cost_stress_multiplier: 2.0,
    include_regime_scans: true,
    regime_scan_budget_per_regime: "",
    target_regime: "",
    excluded_months: [],
    repair_mode: "standard",
    account_size: String(WORKING_ACCOUNT_SIZE),
    source_template: {},
    day_trading_mode: false,
    force_flat_before_close: false,
    paper_queue_limit: "3",
    review_queue_limit: "10",
  });
  const [researchState, setResearchState] = React.useState({ status: "idle", detail: "Ready.", progress: 0 });
  const activePollRunIdRef = React.useRef(null);

  const eodhdStatus = providerStatus(status, "eodhd");
  const fmpStatus = providerStatus(status, "fmp");
  const igStatus = providerStatus(status, "ig");
  const enabledMarkets = markets.filter((item) => item.enabled);
  const disabledMarkets = markets.filter((item) => !item.enabled);
  const selectedMarkets = enabledMarkets.filter((item) => activeMarketIds.includes(item.market_id));
  const selectedEngine = engines.find((engine) => engine.id === researchRun.engine) ?? engines[0] ?? FALLBACK_ENGINES[0];
  const selectedPreset = SEARCH_PRESETS.find((preset) => preset.id === researchRun.search_preset) ?? SEARCH_PRESETS[1];
  const refinementRepairActions = refinementTemplate ? repairActionsForTemplate(refinementTemplate) : [];
  const autoRefinementPlan = refinementTemplate
    ? autoRefinementPlanForTemplate(refinementTemplate, researchRun, enabledMarkets, activeMarketIds)
    : null;
  const strategyTrialMinimum = researchRun.repair_mode === "frozen_validation" ? 1 : 6;

  const loadModule = React.useCallback(async (moduleId = activeModule) => {
    setLoadingModule(moduleId);
    try {
      if (moduleId === "cockpit") {
        const [summary, context, brokerSummary] = await Promise.all([
          getCockpitSummary(),
          getMarketContextStack()
            .catch(() => getMarketContextSummary())
            .catch((error) => ({
              available: false,
              calendar_risk: "unavailable",
              reason: error.message,
              events: [],
            })),
          getBrokerSummary().catch(() => null),
        ]);
        setCockpit(summary);
        setMarketContext(context);
        setStatus(summary.providers ?? []);
        setIgAccountRoles(brokerSummary?.ig_account_roles ?? null);
      } else if (moduleId === "research") {
        const summary = await getResearchSummary();
        setCandidates(summary.candidates ?? []);
        setCritique(summary.critique ?? null);
        getResearchCritique().then(setCritique).catch(() => undefined);
      } else if (moduleId === "backtests") {
        const [nextStatus, nextMarkets, nextPlugins, nextCacheStatus, summary, researchSummary, daySummary, designSummary, brokerSummary] = await Promise.all([
          getStatus(),
          getMarkets(),
          getMarketPlugins(),
          getMarketDataCacheStatus().catch(() => null),
          getBacktestsSummary(includeArchivedRuns),
          getResearchSummary(80).catch(() => ({ candidates: [] })),
          getDayTradingFactorySummary().catch(() => null),
          getDayTradingTemplateDesigns().catch(() => ({ designs: [] })),
          getBrokerSummary().catch(() => null),
        ]);
        setStatus(nextStatus);
        setMarkets(nextMarkets);
        setPlugins(nextPlugins);
        setCacheStatus(nextCacheStatus);
        setEngines((summary.engines ?? []).length ? summary.engines : FALLBACK_ENGINES);
        setSpreadBetEngines(summary.spread_bet_engines ?? []);
        setResearchRuns(summary.runs ?? []);
        setCandidates(researchSummary.candidates ?? []);
        setDayFactory(daySummary);
        setTemplateDesigns(designSummary.designs ?? []);
        setIgAccountRoles(brokerSummary?.ig_account_roles ?? null);
        resumeLatestActiveRun(summary.runs ?? []);
      } else if (moduleId === "templates") {
        setTemplates(await getTemplatesSummary());
      } else if (moduleId === "paper") {
        const [nextPaper, daySummary, brokerSummary] = await Promise.all([
          getPaperSummary(),
          getDayTradingFactorySummary().catch(() => null),
          getBrokerSummary().catch(() => null),
        ]);
        setPaper(nextPaper);
        setDayFactory(daySummary);
        setIgAccountRoles(brokerSummary?.ig_account_roles ?? null);
      } else if (moduleId === "broker") {
        const [summary, nextMarkets, nextRisk] = await Promise.all([
          getBrokerSummary(),
          getMarkets(),
          getRiskSummary().catch(() => null),
        ]);
        setBroker(summary);
        setRisk(nextRisk);
        setStatus(summary.providers ?? []);
        setIgAccountRoles(summary.ig_account_roles ?? null);
        setMarkets(nextMarkets);
      } else if (moduleId === "risk") {
        setRisk(await getRiskSummary());
      } else if (moduleId === "settings") {
        const summary = await getSettingsSummary();
        setStatus(summary.providers ?? []);
        setIgAccountRoles(summary.ig_account_roles ?? null);
        setCacheStatus(summary.cache ?? null);
      }
    } finally {
      setLoadingModule("");
    }
  }, [activeModule, includeArchivedRuns]);

  React.useEffect(() => {
    loadModule(activeModule).catch((error) => setMessage(error.message));
  }, [activeModule, includeArchivedRuns, loadModule]);

  React.useEffect(() => {
    if (activeModule === "backtests" && !["builder", "factory", "results"].includes(activeTab)) {
      setActiveTab("builder");
    }
  }, [activeModule, activeTab]);

  React.useEffect(() => {
    if (enabledMarkets.length > 0 && activeMarketIds.length === 0) {
      setActiveMarketIds([enabledMarkets[0].market_id]);
    }
  }, [enabledMarkets, activeMarketIds.length]);

  React.useEffect(() => {
    if (activeMarketIds.length > 0) {
      setResearchRun((current) => ({ ...current, market_id: activeMarketIds[0] }));
    }
  }, [activeMarketIds]);

  async function submitEodhd(event) {
    event.preventDefault();
    setMessage("Validating EODHD...");
    try {
      await saveEodhd(eodhdKey);
      setEodhdKey("");
      setMessage("EODHD connected.");
      await loadModule(activeModule);
    } catch (error) {
      setMessage(error.message);
      await loadModule(activeModule).catch(() => undefined);
    }
  }

  async function submitFmp(event) {
    event.preventDefault();
    setMessage("Validating FMP...");
    try {
      await saveFmp(fmpKey);
      setFmpKey("");
      setMessage("FMP connected.");
      await loadModule(activeModule);
    } catch (error) {
      setMessage(error.message);
      await loadModule(activeModule).catch(() => undefined);
    }
  }

  async function submitIg(event) {
    event.preventDefault();
    setMessage("Validating IG demo...");
    try {
      const result = await saveIg(ig);
      setIg({ apiKey: "", username: "", password: "", accountId: "" });
      setMessage(`IG demo connected${result.account_id ? `: ${result.account_id}` : "."}`);
      await loadModule(activeModule);
    } catch (error) {
      setMessage(error.message);
      await loadModule(activeModule).catch(() => undefined);
    }
  }

  async function submitIgAccountRoles(event) {
    event.preventDefault();
    setMessage("Saving IG demo account roles...");
    try {
      const result = await saveIgAccountRoles(igRoles);
      setIgRoles({ spreadBetAccountId: "", cfdAccountId: "", defaultProductMode: igRoles.defaultProductMode });
      setIgAccountRoles(result.ig_account_roles ?? null);
      setMessage(result.ig_account_roles?.both_active ? "Both IG demo accounts active." : "IG demo account roles saved.");
      await loadModule(activeModule);
    } catch (error) {
      setMessage(error.message);
    }
  }

  function chooseAccountMode(productMode) {
    const mode = normalizeProductMode(productMode);
    setSelectedProductMode(mode);
    setResearchRun((current) => ({ ...current, product_mode: mode }));
    setMidcapSearch((current) => ({ ...current, product_mode: mode }));
    setIgRoles((current) => ({ ...current, defaultProductMode: mode }));
    setMessage(`${productModeLabel(mode)} workspace selected. Live order placement remains disabled.`);
  }

  async function submitMarket(event) {
    event.preventDefault();
    await saveMarket(market);
    setMessage("Market mapping saved.");
    await loadModule("backtests");
  }

  async function installPlugin(pluginId) {
    await installMarketPlugin(pluginId);
    setMessage("Market plugin installed.");
    await loadModule("backtests");
  }

  async function syncCosts(marketIds = activeMarketIds, productMode = researchRun.product_mode || "spread_bet") {
    const market_ids = marketIds.length ? marketIds : enabledMarkets.map((item) => item.market_id);
    setMessage("Syncing IG cost profiles...");
    const result = await syncIgCosts({ market_ids, product_mode: productMode });
    const next = { ...costProfiles };
    for (const profile of result.profiles) {
      next[profile.market_id] = profile;
    }
    setCostProfiles(next);
    setMessage(`Synced ${result.profile_count} IG cost profiles.`);
  }

  async function runMidcapDiscovery(event) {
    event.preventDefault();
    setMidcapLoading(true);
    setMessage("Searching eligible mid-cap shares...");
    try {
      const result = await discoverMidcaps(midcapSearch);
      setMidcapDiscovery(result);
      setMessage(`Found ${result.eligible_count ?? 0} eligible mid-cap candidate${result.eligible_count === 1 ? "" : "s"}.`);
    } catch (error) {
      setMessage(error.message);
    } finally {
      setMidcapLoading(false);
    }
  }

  async function installDiscoveredMarket(candidate) {
    const mapping = candidate.market_mapping;
    if (!mapping) {
      setMessage("Discovery candidate is missing a market mapping.");
      return;
    }
    await saveMarket(mapping);
    setActiveMarketIds([mapping.market_id]);
    setResearchRun((current) => ({ ...current, market_id: mapping.market_id, interval: "market_default" }));
    setMessage(`${mapping.market_id} installed as a share market.`);
    await loadModule("backtests");
  }

  async function runMidcapTemplatePipeline() {
    const accountSize = optionalNumber(midcapSearch.account_size) ?? WORKING_ACCOUNT_SIZE;
    const maxMarkets = optionalNumber(midcapSearch.max_markets) ?? 3;
    setMidcapPipelineState({ status: "running", detail: "Finding liquid midcaps with EODHD/FMP first, then running research without spending IG calls. IG validation is reserved for the top finalists." });
    setMessage("Midcap template pipeline: research-first watchlist, conservative costs, then IG-validate only finalists before paper/freeze.");
    try {
      const result = await startMidcapTemplatePipeline({
        design_id: midcapSearch.design_id,
        country: midcapSearch.country,
        product_mode: midcapSearch.product_mode,
        account_size: accountSize,
        limit: optionalNumber(midcapSearch.limit) ?? 24,
        max_markets: maxMarkets,
        min_market_cap: optionalNumber(midcapSearch.min_market_cap) ?? 250000000,
        max_market_cap: optionalNumber(midcapSearch.max_market_cap) ?? 10000000000,
        min_volume: optionalNumber(midcapSearch.min_volume) ?? 100000,
        max_spread_bps: optionalNumber(midcapSearch.max_spread_bps) ?? 60,
        broker_validation_mode: "research_first",
        auto_install: true,
        auto_sync_costs: true,
        auto_start_run: true,
      });
      setMidcapPipeline(result);
      const marketIds = (result.run_ready_market_ids?.length ? result.run_ready_market_ids : result.selected_markets?.map((item) => item.market_id) ?? []).filter(Boolean);
      if (marketIds.length > 0) {
        setActiveMarketIds(marketIds);
      }
      if (result.research_run_payload) {
        setResearchRun((current) => ({
          ...current,
          ...result.research_run_payload,
          search_budget: String(result.research_run_payload.search_budget ?? ""),
          regime_scan_budget_per_regime: String(result.research_run_payload.regime_scan_budget_per_regime ?? ""),
          account_size: String(result.research_run_payload.account_size ?? accountSize),
          paper_queue_limit: String(result.research_run_payload.paper_queue_limit ?? 3),
          review_queue_limit: String(result.research_run_payload.review_queue_limit ?? 10),
        }));
      }
      setMidcapPipelineState({
        status: result.status ?? "started",
        detail: result.research_run_id
          ? result.status === "running_research_only"
            ? `Run ${result.research_run_id} started in research-only mode for ${marketIds.length} midcap market${marketIds.length === 1 ? "" : "s"}. IG broker validation is deferred until the best finalists are worth checking.`
            : `Run ${result.research_run_id} started for ${marketIds.length} midcap market${marketIds.length === 1 ? "" : "s"} using the fast 2 vCPU profile; Auto Freeze will save and validate the best eligible lead when it finishes.`
          : result.status === "blocked_no_actionable_midcaps"
          ? "No run started because the shortlisted midcaps did not have enough recent movement versus stressed costs for day trading. Try a wider or more active watchlist before changing score weights."
          : result.cost_sync?.ig_rate_limited
          ? "No run started because IG refused price validation for the unvalidated shortlist. Cached price-validated profiles were reused where available; let the IG API cool down before trying another fresh shortlist."
          : result.status === "blocked_price_validation"
          ? "No run started because IG did not return price-validated cost profiles for the shortlisted markets. Wait for the IG API cooldown or refresh credentials, then start again."
          : `No run started. ${result.discovery?.eligible_count ?? 0} eligible midcap candidate${result.discovery?.eligible_count === 1 ? "" : "s"} found.`,
      });
      setMessage(result.research_run_id
        ? result.status === "running_research_only"
          ? `Research-first midcap run ${result.research_run_id} started. Paper/freeze stays locked until top finalists pass IG validation.`
          : `Midcap template pipeline started run ${result.research_run_id}.`
        : result.cost_sync?.ig_rate_limited
        ? "IG price-validation cooldown hit; no cached price-ready midcaps were available for a run."
        : result.status === "blocked_price_validation"
        ? "Midcap template builder stopped before running because IG price validation is missing."
        : result.status === "blocked_no_actionable_midcaps"
        ? "Midcap template builder stopped because the shortlist was too flat or too expensive for day trading."
        : "Midcap template builder did not find enough eligible markets to start a run.");
      await loadModule("backtests").catch(() => undefined);
      if (result.research_run_id) {
        await loadRunDetail(result.research_run_id);
      }
    } catch (error) {
      setMidcapPipelineState({ status: "error", detail: error.message });
      setMessage(error.message);
    }
  }

  async function loadCostProfile(marketId) {
    const profile = await getIgCostProfile(marketId);
    setCostProfiles((current) => ({ ...current, [marketId]: profile }));
  }

  async function submitResearchRun(event) {
    event.preventDefault();
    await launchResearchRun(researchRun, activeMarketIds);
  }

  async function loadRunDetail(runId, resumeActive = true) {
    const detail = await getResearchRun(runId);
    const plannedTrials = plannedTrialsForRun(detail);
    setRunDetail(detail);
    setResearchState(researchStateFromRun(detail, plannedTrials));
    if (resumeActive && isActiveRun(detail) && activePollRunIdRef.current !== detail.id) {
      resumePollingRun(detail, plannedTrials);
    }
    return detail;
  }

  function resumeLatestActiveRun(runs = []) {
    const activeRun = runs.find((run) => isActiveRun(run));
    if (!activeRun || activePollRunIdRef.current === activeRun.id) {
      return;
    }
    loadRunDetail(activeRun.id).catch((error) => setMessage(error.message));
  }

  function resumePollingRun(detail, plannedTrials = plannedTrialsForRun(detail)) {
    const runId = detail.id;
    activePollRunIdRef.current = runId;
    setActiveTab("results");
    setResearchState(researchStateFromRun(detail, plannedTrials));
    pollResearchRun(runId, plannedTrials)
      .then(async (finishedDetail) => {
        setResearchState(researchStateFromRun(finishedDetail, plannedTrials));
        if (!isActiveRun(finishedDetail)) {
          setMessage(runCompletionMessage(runId, finishedDetail));
        }
        const summary = await getBacktestsSummary(includeArchivedRuns).catch(() => null);
        if (summary) {
          setResearchRuns(summary.runs ?? []);
        }
      })
      .catch((error) => setMessage(error.message))
      .finally(() => {
        if (activePollRunIdRef.current === runId) {
          activePollRunIdRef.current = null;
        }
      });
  }

  async function launchResearchRun(runConfig = researchRun, marketIdsOverride = activeMarketIds, launchMessage = "Launching adaptive IG-aware search...") {
    const market_ids = marketIdsOverride.length ? marketIdsOverride : [runConfig.market_id];
    const preset = SEARCH_PRESETS.find((item) => item.id === runConfig.search_preset) ?? selectedPreset;
    const engine = engines.find((item) => item.id === runConfig.engine) ?? selectedEngine;
    const manualBudget = runConfig.search_budget !== "";
    const budget = manualBudget ? Number(runConfig.search_budget) : preset.budget;
    const effectiveBudget = effectiveSearchBudget(preset.id, budget, market_ids.length, manualBudget);
    const plannedTrials = effectiveBudget * market_ids.length;
    const testingCapital = optionalNumber(runConfig.account_size) ?? WORKING_ACCOUNT_SIZE;
    const regimeScanNote = runConfig.include_regime_scans ? " plus per-regime template discovery" : "";
    const targetRegimeNote = runConfig.target_regime ? `, ${regimeLabel(runConfig.target_regime)} only` : "";
    const dayTradingNote = runConfig.day_trading_mode ? ", forced flat before close" : "";
    const speedNote = effectiveBudget < budget ? " (auto-capped for multi-market speed)" : "";
    setMessage(launchMessage);
    setResearchState({
      status: "running",
      detail: `${engine.label}: ${effectiveBudget} strategy trials per market, ${plannedTrials} base total${speedNote}${regimeScanNote}${targetRegimeNote}${dayTradingNote}, graded on ${accountSizeLabel(testingCapital)}.`,
      progress: 2,
    });
    try {
      const result = await createResearchRun({
        ...runConfig,
        market_id: market_ids[0],
        market_ids,
        search_budget: manualBudget ? budget : null,
        regime_scan_budget_per_regime: runConfig.regime_scan_budget_per_regime === "" ? null : Number(runConfig.regime_scan_budget_per_regime),
        target_regime: runConfig.target_regime || null,
        excluded_months: uniqueMonths(runConfig.excluded_months),
        repair_mode: runConfig.repair_mode || "standard",
        account_size: testingCapital,
        source_template: runConfig.source_template && Object.keys(runConfig.source_template).length ? runConfig.source_template : {},
        product_mode: runConfig.product_mode || "spread_bet",
        day_trading_mode: Boolean(runConfig.day_trading_mode),
        force_flat_before_close: Boolean(runConfig.force_flat_before_close || runConfig.day_trading_mode),
        paper_queue_limit: optionalNumber(runConfig.paper_queue_limit) ?? 3,
        review_queue_limit: optionalNumber(runConfig.review_queue_limit) ?? 10,
      });
      setActiveTab("results");
      setMessage(`Run ${result.run_id} started: ${budget} base trials per market${regimeScanNote}${dayTradingNote}.`);
      const detail = await pollResearchRun(result.run_id, plannedTrials);
      activePollRunIdRef.current = null;
      setResearchState({
        status: detail.status,
        detail: runStateDetail(detail, plannedTrials),
        progress: runProgress(detail, plannedTrials),
      });
      setMessage(runCompletionMessage(result.run_id, detail));
      await loadModule("backtests");
    } catch (error) {
      setResearchState({ status: "error", detail: error.message, progress: 100 });
      setMessage(error.message);
      await loadModule("backtests").catch(() => undefined);
    }
  }

  function stageAutoRefinement() {
    if (!autoRefinementPlan) {
      return;
    }
    if (autoRefinementPlan.stopReason) {
      setMessage(autoRefinementPlan.stopReason);
      return;
    }
    applyAutoRefinementPlan(autoRefinementPlan);
    setMessage(autoRefinementPlan.stageMessage);
  }

  async function runAutoRefinement() {
    if (!autoRefinementPlan) {
      return;
    }
    await launchAutoRefinementPlan(autoRefinementPlan, "Launching make-tradeable repair run...");
  }

  function stageCandidateFactory(mode = "balanced") {
    const plan = candidateFactoryPlan(mode, researchRun, enabledMarkets, activeMarketIds);
    if (plan.stopReason) {
      setMessage(plan.stopReason);
      return;
    }
    setRefinementTemplate(null);
    setActiveMarketIds(plan.marketIds);
    for (const marketId of plan.marketIds) {
      loadCostProfile(marketId).catch(() => undefined);
    }
    setResearchRun({ ...researchRun, ...plan.runPatch });
    setActiveTab("builder");
    setMessage(plan.stageMessage);
  }

  async function runCandidateFactory(mode = "balanced") {
    const plan = candidateFactoryPlan(mode, researchRun, enabledMarkets, activeMarketIds);
    if (plan.stopReason) {
      setMessage(plan.stopReason);
      return;
    }
    const runConfig = { ...researchRun, ...plan.runPatch };
    setRefinementTemplate(null);
    setActiveMarketIds(plan.marketIds);
    for (const marketId of plan.marketIds) {
      loadCostProfile(marketId).catch(() => undefined);
    }
    setResearchRun(runConfig);
    setActiveModule("backtests");
    setActiveTab("results");
    try {
      setMessage(plan.mode.id === "day_trading" ? "Intraday discovery: syncing IG cost profiles..." : "Candidate Factory: syncing IG cost profiles...");
      const result = await syncIgCosts({ market_ids: plan.marketIds, product_mode: runConfig.product_mode || "spread_bet" });
      setCostProfiles((current) => {
        const next = { ...current };
        for (const profile of result.profiles ?? []) {
          next[profile.market_id] = profile;
        }
        return next;
      });
      await launchResearchRun(runConfig, plan.marketIds, plan.launchMessage);
    } catch (error) {
      setResearchState({ status: "error", detail: error.message, progress: 100 });
      setMessage(error.message);
    }
  }

  async function runDailyTemplateScanner() {
    const testingCapital = optionalNumber(researchRun.account_size) ?? WORKING_ACCOUNT_SIZE;
    setDailyScannerState({ status: "running", detail: "Scanning active frozen templates against today's eligible 5-minute markets..." });
    setMessage("Daily Template Scanner: applying frozen rules only...");
    try {
      const result = await startDailyTemplateScanner({
        market_ids: activeMarketIds,
        product_mode: researchRun.product_mode || "spread_bet",
        account_size: testingCapital,
        paper_limit: optionalNumber(researchRun.paper_queue_limit) ?? 3,
        review_limit: optionalNumber(researchRun.review_queue_limit) ?? 10,
        lookback_days: 10,
        max_markets: 24,
      });
      setDayFactory((current) => ({
        ...(current ?? {}),
        daily_paper_queue: result.daily_paper_queue ?? [],
        review_signals: result.review_signals ?? [],
        unsuitable: result.unsuitable ?? [],
        no_setup_sample: result.no_setup_sample ?? [],
        manual_playbooks: result.manual_playbooks ?? current?.manual_playbooks ?? [],
        latest_scan: result.latest_scan ?? null,
        counts: { ...(current?.counts ?? {}), ...(result.counts ?? {}) },
      }));
      setDailyScannerState({
        status: result.status ?? "finished",
        detail: `Scan ${result.scan_id}: ${result.counts?.daily_paper_queue ?? 0} paper preview${result.counts?.daily_paper_queue === 1 ? "" : "s"}, ${result.counts?.review_signals ?? 0} review signal${result.counts?.review_signals === 1 ? "" : "s"}.`,
      });
      setMessage(`Daily Template Scanner finished: ${result.counts?.daily_paper_queue ?? 0} paper preview${result.counts?.daily_paper_queue === 1 ? "" : "s"}.`);
    } catch (error) {
      setDailyScannerState({ status: "error", detail: error.message });
      setMessage(error.message);
    }
  }

  async function makeTradeable(source) {
    const template = refinementTemplateFromSource(source, researchRun);
    const plan = autoRefinementPlanForTemplate(template, researchRun, enabledMarkets, template.market_id ? [template.market_id] : activeMarketIds, { mode: "make_tradeable" });
    setRefinementTemplate(template);
    await launchAutoRefinementPlan(plan, "Launching make-tradeable repair run...");
  }

  async function repairRemaining(source) {
    const template = refinementTemplateFromSource(source, researchRun);
    const plan = autoRefinementPlanForTemplate(template, researchRun, enabledMarkets, template.market_id ? [template.market_id] : activeMarketIds, { mode: "repair_remaining" });
    setRefinementTemplate(template);
    if (plan.stopReason) {
      setActiveModule("backtests");
      setActiveTab("builder");
      setMessage(plan.stopReason);
      return;
    }
    await launchAutoRefinementPlan(plan, `Launching repair attempt ${plan.repairAttempt}/${MAX_REPAIR_ATTEMPTS}...`);
  }

  async function freezeValidate(source) {
    const sourceForTemplate = source.strategy_name ? source : savedTemplateAsSource(source);
    const template = refinementTemplateFromSource(sourceForTemplate, researchRun);
    const plan = frozenValidationPlanForTemplate(template, researchRun, template.market_id ? [template.market_id] : activeMarketIds);
    setRefinementTemplate(template);
    if (plan.stopReason) {
      setActiveModule("backtests");
      setActiveTab("builder");
      setMessage(plan.stopReason);
      return;
    }
    await launchAutoRefinementPlan(plan, "Launching frozen validation run...");
  }

  async function launchAutoRefinementPlan(plan, launchMessage = "Launching auto-refine run...") {
    if (plan.stopReason) {
      setActiveModule("backtests");
      setActiveTab("builder");
      setMessage(plan.stopReason);
      return;
    }
    const runConfig = applyAutoRefinementPlan(plan);
    setActiveModule("backtests");
    setActiveTab("results");
    try {
      if (plan.syncCosts) {
        setMessage("Make tradeable: syncing IG cost profiles...");
        const result = await syncIgCosts({ market_ids: plan.marketIds, product_mode: runConfig.product_mode || "spread_bet" });
        setCostProfiles((current) => {
          const next = { ...current };
          for (const profile of result.profiles ?? []) {
            next[profile.market_id] = profile;
          }
          return next;
        });
      }
      await launchResearchRun(runConfig, plan.marketIds, launchMessage);
    } catch (error) {
      setResearchState({ status: "error", detail: error.message, progress: 100 });
      setMessage(error.message);
    }
  }

  function applyAutoRefinementPlan(plan) {
    const runConfig = { ...researchRun, ...plan.runPatch };
    setActiveMarketIds(plan.marketIds);
    for (const marketId of plan.marketIds) {
      loadCostProfile(marketId).catch(() => undefined);
    }
    setResearchRun(runConfig);
    return runConfig;
  }

  async function scheduleResearch() {
    const enabledIds = enabledMarkets.map((item) => item.market_id);
    const result = await saveResearchSchedule({
      name: "Nightly adaptive IG research",
      cadence: "nightly",
      enabled: true,
      market_ids: enabledIds,
      interval: researchRun.interval,
    });
    setMessage(`Research schedule ${result.schedule_id} saved.`);
  }

  function refineTemplate(source) {
    const template = refinementTemplateFromSource(source, researchRun);
    const marketId = template.market_id;
    setRefinementTemplate(template);
    if (marketId) {
      setActiveMarketIds([marketId]);
      loadCostProfile(marketId).catch(() => undefined);
    }
    setResearchRun((current) => ({
      ...current,
      market_id: marketId || current.market_id,
      interval: template.interval,
      trading_style: template.style,
      objective: template.objective,
      risk_profile: template.risk_profile,
      search_preset: "balanced",
      search_budget: current.search_budget || "54",
      strategy_families: template.family ? [template.family] : [],
      cost_stress_multiplier: current.cost_stress_multiplier || 2.0,
      target_regime: template.target_regime,
      source_template: frozenTemplatePayload(template),
      day_trading_mode: Boolean(template.day_trading_mode),
      force_flat_before_close: Boolean(template.day_trading_mode),
    }));
    setActiveTab("builder");
    setMessage(`Refining ${source.strategy_name} on ${marketId || "selected market"}.`);
  }

  async function refineFurther(source) {
    const template = refinementTemplateFromSource(source, researchRun);
    const plan = autoRefinementPlanForTemplate(template, researchRun, enabledMarkets, template.market_id ? [template.market_id] : activeMarketIds);
    if (plan.stopReason) {
      setRefinementTemplate(template);
      setActiveTab("builder");
      setMessage(plan.stopReason);
      return;
    }
    const runConfig = { ...researchRun, ...plan.runPatch };
    setRefinementTemplate(template);
    setActiveMarketIds(plan.marketIds);
    for (const marketId of plan.marketIds) {
      loadCostProfile(marketId).catch(() => undefined);
    }
    setResearchRun(runConfig);
    setActiveTab("builder");
    if (!plan.syncCosts) {
      setMessage(`Make tradeable staged: ${plan.steps.slice(0, 3).join("; ")}.`);
      return;
    }
    try {
      setMessage("Make tradeable: syncing IG cost profiles...");
      const result = await syncIgCosts({ market_ids: plan.marketIds, product_mode: runConfig.product_mode || "spread_bet" });
      setCostProfiles((current) => {
        const next = { ...current };
        for (const profile of result.profiles ?? []) {
          next[profile.market_id] = profile;
        }
        return next;
      });
      setMessage(`Make tradeable staged and synced ${result.profile_count ?? 0} IG cost profile${result.profile_count === 1 ? "" : "s"}.`);
    } catch (error) {
      setMessage(`Make tradeable staged, but IG validation still needs attention: ${error.message}`);
    }
  }

  async function saveTemplateFromSource(source) {
    const template = refinementTemplateFromSource(source, researchRun);
    const ids = templateSourceIds(source);
    const readiness = source.audit?.promotion_readiness ?? source.promotion_readiness ?? {};
    const payload = {
      name: template.name,
      market_id: template.market_id,
      interval: template.interval,
      strategy_family: template.family,
      style: template.style,
      target_regime: templateSpecificRegime(template),
      status: "active",
      source_run_id: source.run_id ?? null,
      source_trial_id: ids.trialId,
      source_candidate_id: ids.candidateId,
      source_kind: ids.kind,
      promotion_tier: source.promotion_tier ?? source.audit?.promotion_tier ?? "research_candidate",
      readiness_status: readiness.status ?? source.promotion_readiness?.status ?? "blocked",
      robustness_score: Number(source.robustness_score ?? 0),
      testing_account_size: testingAccountSizeForSource(source),
      payload: {
        source_template: frozenTemplatePayload(template),
        parameters: template.parameters,
        backtest: template.backtest,
        pattern: template.pattern,
        evidence: template.evidence,
        readiness,
        warnings: template.warnings,
        search_audit: template.parameters?.search_audit ?? {},
        capital_scenarios: source.capital_scenarios ?? [],
        source_kind: ids.kind,
      },
    };
    const saved = await saveStrategyTemplate(payload);
    setMessage(`Saved ${saved.name} to Template Library.`);
    if (activeModule === "templates") {
      await loadModule("templates");
    }
  }

  function useSavedTemplate(template) {
    const source = savedTemplateAsSource(template);
    refineTemplate(source);
    setActiveModule("backtests");
    setActiveTab("builder");
    setMessage(`Loaded ${template.name} into the Backtests builder.`);
  }

  async function makeSavedTemplateTradeable(template) {
    await makeTradeable(savedTemplateAsSource(template));
  }

  async function archiveSavedTemplate(template) {
    await updateStrategyTemplateStatus(template.id, "archived");
    setMessage(`Archived template ${template.name}.`);
    await loadModule("templates");
  }

  function refinementTemplateFromSource(source, currentRun = researchRun) {
    const parameters = source.audit?.candidate?.parameters ?? source.parameters ?? source.settings ?? {};
    const searchAudit = parameters.search_audit ?? {};
    const pattern = parameters.bar_pattern_analysis ?? {};
    const family = parameters.family ? String(parameters.family) : String(source.strategy_family || source.family || "");
    const marketId = String(source.market_id || parameters.market_id || currentRun.market_id);
    const targetRegime = String(parameters.target_regime || pattern.target_regime || "");
    return {
      id: source.id ?? source.trial_id ?? source.strategy_name,
      name: source.strategy_name,
      market_id: marketId,
      family,
      style: String(parameters.style || searchAudit.trading_style || "find_anything_robust"),
      objective: String(parameters.objective || searchAudit.objective || "profit_first"),
      interval: intervalValue(parameters.timeframe || currentRun.interval),
      risk_profile: String(searchAudit.risk_profile || currentRun.risk_profile),
      target_regime: targetRegime,
      repair_attempt_count: repairAttemptCountForSource(source),
      recipe: researchRecipeLabel(parameters.research_recipe),
      warnings: warningCodesForSource(source),
      readiness: source.audit?.promotion_readiness ?? null,
      pattern,
      evidence: evidenceProfileForSource(source),
      backtest: source.audit?.backtest ?? source.backtest ?? {},
      parameters,
      day_trading_mode: Boolean(parameters.day_trading_mode || searchAudit.day_trading_mode || parameters.holding_period === "intraday"),
    };
  }

  function clearRefinementTemplate() {
    setRefinementTemplate(null);
    setResearchRun((current) => ({ ...current, strategy_families: [], excluded_months: [], target_regime: "", source_template: {} }));
  }

  function applyRobustnessPreset(preset) {
    if (!refinementTemplate) {
      return;
    }
    const isDayTrading = Boolean(refinementTemplate.day_trading_mode || refinementTemplate.parameters?.day_trading_mode || researchRun.day_trading_mode);
    const family = refinementTemplate.family ? [refinementTemplate.family] : [];
    const sourceMarket = String(refinementTemplate.market_id || activeMarketIds[0] || researchRun.market_id || "").trim();
    const selectedMarket = sourceMarket ? [sourceMarket] : [];
    const allMarketIds = enabledMarkets.map((item) => item.market_id).filter(Boolean);
    const discoveryMarketIds = allMarketIds.filter((marketId) => marketId !== sourceMarket);
    const dominantMonth = refinementTemplate.pattern?.dominant_profit_month?.key;
    const templateTargetRegime = templateSpecificRegime(refinementTemplate);
    const regimeRepairTarget = regimeRefineTarget(refinementTemplate);
    const presetConfig = {
      focused: {
        marketIds: selectedMarket,
        budget: "54",
        stress: 2.0,
        start: researchRun.start,
        end: researchRun.end,
        interval: refinementTemplate.interval,
        targetRegime: templateTargetRegime,
        repairMode: "focused_retest",
        label: "Focused robustness run staged.",
      },
      evidence_first: {
        marketIds: selectedMarket,
        budget: "24",
        stress: 2.5,
        start: researchRun.start,
        end: researchRun.end,
        interval: refinementTemplate.interval,
        targetRegime: templateTargetRegime,
        repairMode: "evidence_first",
        label: "Evidence-first retest staged with a smaller locked search and stricter fold ranking.",
      },
      frozen_validation: {
        marketIds: selectedMarket,
        budget: "1",
        stress: 2.5,
        start: longEvidenceStartForTemplate(refinementTemplate),
        end: researchRun.end,
        interval: refinementTemplate.interval,
        targetRegime: templateTargetRegime,
        repairMode: "frozen_validation",
        sourceTemplate: frozenTemplatePayload(refinementTemplate),
        label: templateTargetRegime
          ? `Frozen validation staged: exact rules retest inside ${regimeLabel(templateTargetRegime)} with no parameter hunting.`
          : "Frozen validation staged: exact rules retest with no parameter hunting.",
      },
      capital_fit: {
        marketIds: selectedMarket,
        budget: "120",
        stress: 2.5,
        start: earlierDate(researchRun.start, "2024-01-01"),
        end: researchRun.end,
        interval: refinementTemplate.interval,
        targetRegime: templateTargetRegime,
        repairMode: "capital_fit",
        objective: "balanced",
        riskProfile: "conservative",
        accountSize: researchRun.account_size || String(WORKING_ACCOUNT_SIZE),
        label: `Capital-fit retest staged for ${accountSizeLabel(optionalNumber(researchRun.account_size) ?? WORKING_ACCOUNT_SIZE)} with smaller stakes, stops, and stricter drawdown ranking.`,
      },
      more_trades: {
        marketIds: selectedMarket,
        budget: "120",
        stress: 2.0,
        start: longEvidenceStartForTemplate(refinementTemplate),
        end: researchRun.end,
        interval: refinementTemplate.interval,
        targetRegime: regimeRepairTarget,
        repairMode: "more_trades",
        label: regimeRepairTarget
          ? `Target-regime OOS repair staged for ${regimeLabel(regimeRepairTarget)} with longer history and a deeper search.`
          : "More-trades repair run staged with longer history and a deeper search.",
      },
      higher_costs: {
        marketIds: selectedMarket,
        budget: "54",
        stress: 3.0,
        start: researchRun.start,
        end: researchRun.end,
        interval: refinementTemplate.interval,
        targetRegime: templateTargetRegime,
        repairMode: "cost_stress",
        label: "Higher-cost robustness run staged.",
      },
      cross_market: {
        marketIds: discoveryMarketIds.length ? discoveryMarketIds : selectedMarket,
        budget: "120",
        stress: 2.5,
        start: researchRun.start,
        end: researchRun.end,
        interval: "market_default",
        targetRegime: regimeRepairTarget,
        repairMode: "cross_market_discovery",
        label: regimeRepairTarget
          ? `Similar-edge discovery staged across other markets, scored inside ${regimeLabel(regimeRepairTarget)}. These become separate leads, not proof for this template.`
          : "Similar-edge discovery staged across other markets. These become separate leads, not proof for this template.",
      },
      regime_scan: {
        marketIds: selectedMarket,
        budget: "54",
        stress: 2.0,
        start: researchRun.start,
        end: researchRun.end,
        interval: refinementTemplate.interval,
        includeRegimeScans: !regimeRepairTarget,
        regimeScanBudget: "",
        targetRegime: regimeRepairTarget,
        repairMode: "regime_repair",
        label: regimeRepairTarget
          ? `Regime repair staged: scoring ${regimeLabel(regimeRepairTarget)} only and forcing flat outside it.`
          : "Regime repair run staged with capped specialist scans.",
      },
      exclude_best_month: {
        marketIds: selectedMarket,
        budget: "54",
        stress: 2.0,
        start: researchRun.start,
        end: researchRun.end,
        interval: refinementTemplate.interval,
        targetRegime: templateTargetRegime,
        excludedMonths: dominantMonth ? [dominantMonth] : [],
        repairMode: "month_exclusion",
        label: dominantMonth ? `Best-month exclusion run staged without ${dominantMonth}.` : "No dominant month found to exclude.",
      },
      longer_history: {
        marketIds: selectedMarket,
        budget: "120",
        stress: 2.0,
        start: longEvidenceStartForTemplate(refinementTemplate),
        end: researchRun.end,
        interval: refinementTemplate.interval,
        targetRegime: regimeRepairTarget,
        repairMode: "longer_history",
        label: regimeRepairTarget
          ? `Longer-history evidence staged inside ${regimeLabel(regimeRepairTarget)}.`
          : "Longer-history robustness run staged.",
      },
    }[preset];
    if (!presetConfig) {
      return;
    }
    setActiveMarketIds(presetConfig.marketIds);
    for (const marketId of presetConfig.marketIds) {
      loadCostProfile(marketId).catch(() => undefined);
    }
    setResearchRun((current) => ({
      ...current,
      market_id: presetConfig.marketIds[0] || current.market_id,
      interval: presetConfig.interval,
      start: presetConfig.start,
      end: presetConfig.end,
      trading_style: refinementTemplate.style,
      objective: presetConfig.objective ?? "profit_first",
      risk_profile: presetConfig.riskProfile ?? refinementTemplate.risk_profile,
      search_preset: presetConfig.budget === "120" ? "deep" : "balanced",
      search_budget: presetConfig.budget,
      strategy_families: family,
      cost_stress_multiplier: presetConfig.stress,
      include_regime_scans: Boolean(presetConfig.includeRegimeScans) && !presetConfig.targetRegime,
      regime_scan_budget_per_regime: presetConfig.regimeScanBudget ?? "",
      target_regime: presetConfig.targetRegime || "",
      excluded_months: presetConfig.excludedMonths ?? [],
      repair_mode: presetConfig.repairMode ?? "standard",
      account_size: presetConfig.accountSize ?? current.account_size,
      source_template: presetConfig.sourceTemplate ?? frozenTemplatePayload(refinementTemplate),
      day_trading_mode: isDayTrading,
      force_flat_before_close: isDayTrading,
    }));
    setMessage(presetConfig.label);
  }

  async function pollResearchRun(runId, plannedTrials) {
    let detail = await getResearchRun(runId);
    setRunDetail(detail);
    getMarketDataCacheStatus().then(setCacheStatus).catch(() => undefined);
    for (let attempt = 0; attempt < 720 && ["created", "running"].includes(detail.status); attempt += 1) {
      setResearchState({ status: "running", detail: runStateDetail(detail, plannedTrials), progress: runProgress(detail, plannedTrials) });
      await sleep(2000);
      const [nextDetail, nextCacheStatus] = await Promise.all([
        getResearchRun(runId),
        getMarketDataCacheStatus().catch(() => null),
      ]);
      detail = nextDetail;
      setRunDetail(detail);
      setResearchState({ status: detail.status, detail: runStateDetail(detail, plannedTrials), progress: runProgress(detail, plannedTrials) });
      if (nextCacheStatus) {
        setCacheStatus(nextCacheStatus);
      }
    }
    return detail;
  }

  async function pruneCache() {
    const result = await pruneMarketDataCache();
    setCacheStatus({ stats: result.stats, namespaces: cacheStatus?.namespaces ?? [], policy: cacheStatus?.policy ?? {} });
    setMessage(`Pruned ${result.deleted} expired cache entries.`);
    await loadModule(activeModule).catch(() => undefined);
  }

  async function deleteRun(run) {
    if (["created", "running"].includes(run.status)) {
      setMessage(`Run ${run.id} is still ${run.status}; stop waiting for it to finish before deleting it.`);
      return;
    }
    if (!window.confirm(`Delete Run ${run.id} and its saved trials/candidates?`)) {
      return;
    }
    const result = await deleteResearchRun(run.id);
    if (runDetail?.id === run.id) {
      setRunDetail(null);
    }
    setMessage(`Deleted Run ${run.id}: ${result.deleted_trials} trials and ${result.deleted_candidates} candidates removed.`);
    await loadModule("backtests");
  }

  async function archiveRun(run) {
    if (["created", "running"].includes(run.status)) {
      setMessage(`Run ${run.id} is still ${run.status}; archive it after it finishes.`);
      return;
    }
    const result = await archiveResearchRun(run.id);
    if (runDetail?.id === run.id) {
      setRunDetail(null);
    }
    setMessage(`Archived Run ${result.run_id}.`);
    await loadModule("backtests");
  }

  async function archiveRuns(runs) {
    const archivable = runs.filter((run) => !["created", "running"].includes(run.status) && !run.archived);
    if (archivable.length === 0) {
      setMessage("No finished unarchived runs selected.");
      return;
    }
    for (const run of archivable) {
      await archiveResearchRun(run.id);
    }
    if (runDetail && archivable.some((run) => run.id === runDetail.id)) {
      setRunDetail(null);
    }
    setMessage(`Archived ${archivable.length} runs.`);
    await loadModule("backtests");
  }

  async function deleteRuns(runs) {
    const deletable = runs.filter((run) => !["created", "running"].includes(run.status));
    if (deletable.length === 0) {
      setMessage("No finished runs selected for deletion.");
      return;
    }
    if (!window.confirm(`Delete ${deletable.length} finished runs and all of their saved trials/candidates?`)) {
      return;
    }
    let deletedTrials = 0;
    let deletedCandidates = 0;
    for (const run of deletable) {
      const result = await deleteResearchRun(run.id);
      deletedTrials += Number(result.deleted_trials ?? 0);
      deletedCandidates += Number(result.deleted_candidates ?? 0);
    }
    if (runDetail && deletable.some((run) => run.id === runDetail.id)) {
      setRunDetail(null);
    }
    setMessage(`Deleted ${deletable.length} runs: ${deletedTrials} trials and ${deletedCandidates} candidates removed.`);
    await loadModule("backtests");
  }

  function toggleMarket(marketId) {
    setActiveMarketIds((current) => {
      if (current.includes(marketId)) {
        return current.filter((item) => item !== marketId);
      }
      return [...current, marketId];
    });
    loadCostProfile(marketId).catch(() => undefined);
  }

  return (
    <main>
      <header className="topbar app-topbar">
        <div>
          <h1>slrno</h1>
          <p>One guided workflow for template design, frozen-rule paper trading, and account safety.</p>
        </div>
        <div className="topbar-actions">
          <AccountSwitcher
            accountRoles={igAccountRoles}
            selectedProductMode={selectedProductMode}
            onSelect={chooseAccountMode}
          />
          <div className="mode"><ShieldCheck size={18} /> Live orders locked</div>
        </div>
      </header>

      <nav className="module-nav" aria-label="Primary">
        {MODULES.map(([id, label, Icon]) => (
          <button className={activeModule === id ? "module-tab active" : "module-tab"} key={id} type="button" onClick={() => setActiveModule(id)}>
            <Icon size={16} /> {label}
          </button>
        ))}
      </nav>

      {message && <div className="notice">{message}</div>}
      {loadingModule && <div className="notice loading">Loading {moduleLabel(loadingModule)}...</div>}

      {activeModule === "cockpit" && <CockpitView summary={cockpit} marketContext={marketContext} setActiveModule={setActiveModule} />}
      {activeModule === "guide" && <GuideView setActiveModule={setActiveModule} />}
      {activeModule === "templates" && (
        <TemplateLibraryView
          summary={templates}
          onUseTemplate={useSavedTemplate}
          onMakeTradeable={makeSavedTemplateTradeable}
          onRepairRemaining={repairRemaining}
          onFreezeValidate={freezeValidate}
          onArchive={archiveSavedTemplate}
          onRefresh={() => loadModule("templates")}
        />
      )}

      {activeModule === "research" && (
        <section className="lab-shell">
          <div className="lab-header">
            <div>
              <h2><Sparkles size={20} /> Research Pipeline</h2>
              <p>Candidate readiness, capital feasibility, validation blockers, and next actions.</p>
            </div>
            <button type="button" className="secondary" onClick={() => loadModule("research")}><RefreshCw size={16} /> Refresh</button>
          </div>
          <CandidateView candidates={candidates} critique={critique} onRefineTemplate={refineTemplate} onRefineFurther={refineFurther} onMakeTradeable={makeTradeable} onRepairRemaining={repairRemaining} onFreezeValidate={freezeValidate} onSaveTemplate={saveTemplateFromSource} />
        </section>
      )}

      {activeModule === "backtests" && (
        <>
          <section className="lab-shell">
            <div className="lab-header">
              <div>
                <h2><Sparkles size={20} /> Build Templates</h2>
                <p>Find eligible markets, design intraday templates, repair blockers, and freeze exact rules before daily paper.</p>
              </div>
              <div className={`run-state ${researchState.status}`}>
                <strong>{researchState.status.toUpperCase()}</strong>
                <span>{researchState.detail}</span>
                <div className="progress-row">
                  <div className="progress-track" aria-label="Research run progress">
                    <div className={`progress-fill ${researchState.status}`} style={{ width: `${boundedProgress(researchState.progress)}%` }} />
                  </div>
                  <small>{Math.round(boundedProgress(researchState.progress))}%</small>
                </div>
              </div>
            </div>
            <AccountWorkspaceNotice productMode={selectedProductMode} accountRoles={igAccountRoles} />
            <div className="grid two build-start-grid">
              <MidcapDiscoveryPanel
                search={midcapSearch}
                setSearch={setMidcapSearch}
                result={midcapDiscovery}
                loading={midcapLoading}
                templateDesigns={templateDesigns}
                pipeline={midcapPipeline}
                pipelineState={midcapPipelineState}
                onSearch={runMidcapDiscovery}
                onInstall={installDiscoveredMarket}
                onRunPipeline={runMidcapTemplatePipeline}
              />
              <Panel icon={<ShieldCheck />} title="Template Workflow">
                <div className="factory-flow compact-flow">
                  {[
                    ["1", "Find", "Choose account and discover IG-eligible markets."],
                    ["2", "Design", "Run intraday/no-overnight template discovery."],
                    ["3", "Repair", "Clear cost, OOS, fold, regime, and capital blockers."],
                    ["4", "Freeze", "Retest exact parameters before reuse."],
                    ["5", "Paper", "Daily mode matches frozen templates only."],
                  ].map(([step, title, detail]) => (
                    <div className="factory-step" key={title}>
                      <span>{step}</span>
                      <strong>{title}</strong>
                      <small>{detail}</small>
                    </div>
                  ))}
                </div>
                <div className="status compact-status">
                  <strong>Daily rules are not invented here</strong>
                  <span>Discovery produces leads. Only saved and Freeze-validated intraday templates can enter Daily Paper.</span>
                </div>
              </Panel>
            </div>
            <div className="tabs">
              {[
                ["factory", "Repair & Freeze"],
                ["builder", "Advanced Run"],
                ["results", "Evidence Runs"],
              ].map(([id, label]) => (
                <button className={activeTab === id ? "tab active" : "tab"} key={id} type="button" onClick={() => setActiveTab(id)}>
                  {label}
                </button>
              ))}
              <label className="check compact-check">
                <input type="checkbox" checked={includeArchivedRuns} onChange={(event) => setIncludeArchivedRuns(event.target.checked)} />
                Include archived
              </label>
              <label className="check compact-check">
                <input type="checkbox" checked={exportIncludeBars} onChange={(event) => setExportIncludeBars(event.target.checked)} />
                Export bars
              </label>
            </div>

            {activeTab === "builder" && (
              <form onSubmit={submitResearchRun} className="lab-grid">
                {refinementTemplate && (
                  <section className="lab-section span-2 refinement-panel">
                    <div className="label-row table-heading">
                      <h3>Template Refinement</h3>
                      <button type="button" className="ghost" onClick={clearRefinementTemplate}>Clear</button>
                    </div>
                    <div className="status-list">
                      <div className="status">
                        <strong>{refinementTemplate.name}</strong>
                        <span>{refinementTemplate.market_id} · {strategyFamilyLabel(refinementTemplate.family)} · {normalizeInterval(refinementTemplate.interval)}</span>
                        {refinementTemplate.recipe && <small>{refinementTemplate.recipe}</small>}
                      </div>
                    </div>
                    <div className="mini-metrics refinement-metrics">
                      <Metric label="Lookback" value={refinementTemplate.parameters.lookback ?? "-"} />
                      <Metric label="Threshold" value={`${round(refinementTemplate.parameters.threshold_bps)} bps`} />
                      <Metric label="Stop" value={`${round(refinementTemplate.parameters.stop_loss_bps)} bps`} />
                      <Metric label="Take profit" value={`${round(refinementTemplate.parameters.take_profit_bps)} bps`} />
                      <Metric label="Hold bars" value={refinementTemplate.parameters.max_hold_bars ?? "-"} />
                      <Metric label="Direction" value={refinementTemplate.parameters.direction ?? "-"} />
                      <Metric label="Fold win" value={percent(refinementTemplate.evidence?.positive_fold_rate)} />
                      <Metric label="Fold share" value={percent(refinementTemplate.evidence?.single_fold_profit_share)} />
                      <Metric label="OOS net" value={formatMoney(refinementTemplate.evidence?.oos_net_profit)} />
                      <Metric label="OOS trades" value={refinementTemplate.evidence?.oos_trade_count ?? 0} />
                      <TemplateUtilizationMetrics backtest={refinementTemplate.backtest} pattern={refinementTemplate.pattern} />
                      <RegimeEvidenceMetrics pattern={refinementTemplate.pattern} />
                    </div>
                    <div className="warning-row">
                      <WarningChips warnings={refinementTemplate.warnings} limit={10} empty="No active blockers were attached to this template." />
                    </div>
                    {autoRefinementPlan && (
                      <div className="auto-refine-card">
                        <div className="auto-refine-heading">
                          <div>
                            <strong><ShieldCheck size={16} /> Make tradeable plan</strong>
                            <span>{autoRefinementPlan.summary}</span>
                          </div>
                          <div className="badge-group">
                            {autoRefinementPlan.targetRegime && <span className="badge market-badge">{regimeLabel(autoRefinementPlan.targetRegime)} only</span>}
                            <span className="badge muted-badge">{autoRefinementPlan.marketIds.length} market{autoRefinementPlan.marketIds.length === 1 ? "" : "s"}</span>
                          </div>
                        </div>
                        {autoRefinementPlan.targetRegime && (
                          <div className="auto-refine-target">
                            <strong>Tradeability target: {regimeLabel(autoRefinementPlan.targetRegime)} only</strong>
                            <span>Trades outside {regimeLabel(autoRefinementPlan.targetRegime)} are forced flat. The search is scored on the selected regime, with full-history gated evidence kept for context.</span>
                          </div>
                        )}
                        {autoRefinementPlan.crossMarketDiscovery && (
                          <div className="auto-refine-target">
                            <strong>Cross-market discovery is separate</strong>
                            <span>Make tradeable stays on this template's source market. If a winning regime is known, it stays gated to that regime. Use Find similar elsewhere to create independent leads; those scores are not blended into this template.</span>
                          </div>
                        )}
                        <div className="auto-refine-steps">
                          {autoRefinementPlan.steps.map((step) => (
                            <span key={step}>{step}</span>
                          ))}
                        </div>
                        {autoRefinementPlan.targets.length > 0 && (
                          <div className="tradeability-targets">
                            <strong>Promotion checks this run is trying to clear</strong>
                            <div>
                              {autoRefinementPlan.targets.map((target) => (
                                <span key={target}>{target}</span>
                              ))}
                            </div>
                          </div>
                        )}
                        <div className="button-row">
                          <button type="button" className="secondary" onClick={runAutoRefinement}><ShieldCheck size={16} /> Run next repair</button>
                          <button type="button" className="ghost" onClick={stageAutoRefinement}>Stage only</button>
                        </div>
                      </div>
                    )}
                    <div className="repair-plan">
                      {refinementRepairActions.map((action) => (
                        <div className="repair-action" key={action.id}>
                          <div>
                            <strong>{action.title}</strong>
                            <span>{action.detail}</span>
                          </div>
                          <button
                            type="button"
                            className={action.primary ? "secondary" : "ghost"}
                            disabled={action.preset === "exclude_best_month" && !refinementTemplate.pattern?.dominant_profit_month?.key}
                            onClick={() => (action.kind === "sync_costs" ? syncCosts() : applyRobustnessPreset(action.preset))}
                          >
                            {action.button}
                          </button>
                        </div>
                      ))}
                    </div>
                    <div className="button-row robustness-actions">
                      <button type="button" className="secondary" onClick={() => applyRobustnessPreset("focused")}><RefreshCw size={16} /> Same market</button>
                      <button type="button" className="ghost" onClick={() => applyRobustnessPreset("capital_fit")}>Capital fit</button>
                      <button type="button" className="ghost" onClick={() => applyRobustnessPreset("evidence_first")}>Evidence first</button>
                      <button type="button" className="ghost" onClick={() => applyRobustnessPreset("higher_costs")}>Higher costs</button>
                      <button type="button" className="ghost" onClick={() => applyRobustnessPreset("cross_market")}>Find similar elsewhere</button>
                      <button type="button" className="ghost" onClick={() => applyRobustnessPreset("longer_history")}>Longer history</button>
                      <button type="button" className="ghost" onClick={() => applyRobustnessPreset("regime_scan")}>Regime repair</button>
                      <button type="button" className="ghost" onClick={() => applyRobustnessPreset("frozen_validation")}>Freeze validate</button>
                    </div>
                  </section>
                )}
                <section className="lab-section span-2">
                  <h3>Search Mode</h3>
                  <div className="segmented">
                    {SEARCH_PRESETS.map((preset) => (
                      <button type="button" className={researchRun.search_preset === preset.id ? "segment active" : "segment"} key={preset.id} onClick={() => setResearchRun({ ...researchRun, search_preset: preset.id, search_budget: "" })}>
                        {preset.label}
                      </button>
                    ))}
                  </div>
                  <div className="segmented wrap">
                    {STYLE_OPTIONS.map((style) => (
                      <button type="button" className={researchRun.trading_style === style.id ? "segment active" : "segment"} key={style.id} onClick={() => setResearchRun({ ...researchRun, trading_style: style.id })}>
                        {style.label}
                      </button>
                    ))}
                  </div>
                  {refinementTemplate?.family && (
                    <div className="refinement-lock">
                      <span className="badge base">Family locked</span>
                      <strong>{strategyFamilyLabel(refinementTemplate.family)}</strong>
                    </div>
                  )}
                </section>

                <section className="lab-section span-2">
                  <h3>Markets</h3>
                  <div className="market-picker">
                    {enabledMarkets.map((item) => (
                      <button type="button" className={activeMarketIds.includes(item.market_id) ? "market-chip active" : "market-chip"} key={item.market_id} onClick={() => toggleMarket(item.market_id)}>
                        <strong>{item.market_id}</strong>
                        <span>{item.name}</span>
                        {marketSubline(item) && <small>{marketSubline(item)}</small>}
                      </button>
                    ))}
                    {disabledMarkets.map((item) => (
                      <button type="button" className="market-chip unavailable" key={item.market_id} disabled title={marketAvailabilityNote(item)}>
                        <strong>{item.market_id}</strong>
                        <span>{item.name}</span>
                        {marketSubline(item) && <small>{marketSubline(item)}</small>}
                        <span>{marketAvailabilityNote(item)}</span>
                      </button>
                    ))}
                  </div>
                </section>

                <section className="lab-section">
                  <h3>Inputs</h3>
                  <label>Engine</label>
                  <select value={researchRun.engine} onChange={(event) => setResearchRun({ ...researchRun, engine: event.target.value })}>
                    {engines.map((engine) => <option value={engine.id} key={engine.id}>{engine.label}</option>)}
                  </select>
                  <label>Candle timeframe</label>
                  <select value={researchRun.interval} onChange={(event) => setResearchRun({ ...researchRun, interval: event.target.value })}>
                    {CANDLE_INTERVALS.map((interval) => <option value={interval.value} key={interval.value}>{interval.label}</option>)}
                  </select>
                  <label>Objective</label>
                  <select value={researchRun.objective} onChange={(event) => setResearchRun({ ...researchRun, objective: event.target.value })}>
                    {OBJECTIVES.map((objective) => <option value={objective.id} key={objective.id}>{objective.label}</option>)}
                  </select>
                  <label>Risk profile</label>
                  <select value={researchRun.risk_profile} onChange={(event) => setResearchRun({ ...researchRun, risk_profile: event.target.value })}>
                    <option value="conservative">Conservative</option>
                    <option value="balanced">Balanced</option>
                    <option value="aggressive">Aggressive</option>
                  </select>
                  <label>Testing capital (£)</label>
                  <input
                    value={researchRun.account_size}
                    onChange={(event) => setResearchRun({ ...researchRun, account_size: event.target.value })}
                    type="number"
                    min="100"
                    max="1000000"
                    step="50"
                    placeholder={String(WORKING_ACCOUNT_SIZE)}
                  />
                  <label>Cost stress</label>
                  <input value={researchRun.cost_stress_multiplier} onChange={(event) => setResearchRun({ ...researchRun, cost_stress_multiplier: Number(event.target.value) })} type="number" min="1" max="5" step="0.25" />
                  <label className="checkbox-line">
                    <input
                      type="checkbox"
                      checked={researchRun.include_regime_scans}
                      onChange={(event) => setResearchRun({ ...researchRun, include_regime_scans: event.target.checked })}
                    />
                    Build regime templates
                  </label>
                  <label className="checkbox-line">
                    <input
                      type="checkbox"
                      checked={researchRun.day_trading_mode}
                      onChange={(event) => setResearchRun({
                        ...researchRun,
                        day_trading_mode: event.target.checked,
                        force_flat_before_close: event.target.checked,
                        trading_style: event.target.checked ? "intraday_only" : researchRun.trading_style,
                        interval: event.target.checked ? "5min" : researchRun.interval,
                      })}
                    />
                    Intraday discovery only
                  </label>
                  {researchRun.day_trading_mode && (
                    <div className="status compact-status">
                      <strong>Discovery output, not daily firing rules</strong>
                      <span>Search results must still be repaired, saved, and Freeze validated before the daily paper queue can use them.</span>
                    </div>
                  )}
                  {researchRun.include_regime_scans && (
                    <>
                      <label>Regime trials / regime</label>
                      <input
                        value={researchRun.regime_scan_budget_per_regime}
                        onChange={(event) => setResearchRun({ ...researchRun, regime_scan_budget_per_regime: event.target.value })}
                        placeholder={String(regimePresetBudget(researchRun.search_preset))}
                        type="number"
                        min="1"
                        max={regimePresetBudget(researchRun.search_preset)}
                      />
                    </>
                  )}
                  {researchRun.target_regime && (
                    <div className="status compact-status">
                      <strong>Target regime · {regimeLabel(researchRun.target_regime)}</strong>
                      <span>Signals are scored only inside this regime and forced flat outside it.</span>
                      <button type="button" className="ghost" onClick={() => setResearchRun({ ...researchRun, target_regime: "" })}>Clear target</button>
                    </div>
                  )}
                </section>

                <section className="lab-section">
                  <h3>Window</h3>
                  <label>Start</label>
                  <input value={researchRun.start} onChange={(event) => setResearchRun({ ...researchRun, start: event.target.value })} required />
                  <label>End</label>
                  <input value={researchRun.end} onChange={(event) => setResearchRun({ ...researchRun, end: event.target.value })} required />
                  <label>Strategy trials / market</label>
                  <input
                    value={researchRun.search_budget}
                    onChange={(event) => setResearchRun({ ...researchRun, search_budget: event.target.value })}
                    placeholder={`${researchRun.repair_mode === "frozen_validation" ? 1 : selectedPreset.budget}`}
                    type="number"
                    min={strategyTrialMinimum}
                    max="500"
                  />
                  <label>Repair mode</label>
                  <span className="badge muted-badge repair-mode-badge">{repairModeLabel(researchRun.repair_mode)}</span>
                  {researchRun.day_trading_mode && (
                    <>
                      <label>Daily paper slots after freeze</label>
                      <input
                        value={researchRun.paper_queue_limit}
                        onChange={(event) => setResearchRun({ ...researchRun, paper_queue_limit: event.target.value })}
                        type="number"
                        min="1"
                        max="5"
                        step="1"
                      />
                      <label>Review matches after freeze</label>
                      <input
                        value={researchRun.review_queue_limit}
                        onChange={(event) => setResearchRun({ ...researchRun, review_queue_limit: event.target.value })}
                        type="number"
                        min="1"
                        max="20"
                        step="1"
                      />
                    </>
                  )}
                  <label>Excluded months</label>
                  <div className="exclusion-row">
                    {uniqueMonths(researchRun.excluded_months).length > 0
                      ? uniqueMonths(researchRun.excluded_months).map((month) => <span className="badge muted-badge" key={month}>{month}</span>)
                      : <span className="muted">None</span>}
                    {uniqueMonths(researchRun.excluded_months).length > 0 && (
                      <button type="button" className="ghost compact-button" onClick={() => setResearchRun({ ...researchRun, excluded_months: [] })}>Clear</button>
                    )}
                  </div>
                  <div className="button-row">
                    <button type="button" className="secondary" onClick={syncCosts}><RefreshCw size={16} /> Sync costs</button>
                    <button disabled={researchState.status === "running" || activeMarketIds.length === 0}>{researchState.status === "running" ? "Running..." : "Run search"}</button>
                  </div>
                  <button type="button" className="ghost" onClick={scheduleResearch}>Save nightly schedule</button>
                </section>

                <section className="lab-section span-2">
                  <h3>Cost Profiles</h3>
                  <div className="cost-grid">
                    {selectedMarkets.map((item) => <CostProfile key={item.market_id} market={item} profile={costProfiles[item.market_id]} onLoad={() => loadCostProfile(item.market_id)} />)}
                    {selectedMarkets.length === 0 && <span className="muted">Choose at least one market.</span>}
                  </div>
                </section>
              </form>
            )}

            {activeTab === "factory" && (
              <CandidateFactoryView
                candidates={candidates}
                dayFactory={dayFactory}
                runDetail={runDetail}
                researchRuns={researchRuns}
                enabledMarkets={enabledMarkets}
                disabledMarkets={disabledMarkets}
                activeMarketIds={activeMarketIds}
                selectedMarkets={selectedMarkets}
                researchRun={researchRun}
                toggleMarket={toggleMarket}
                onRunFactory={runCandidateFactory}
                onStageFactory={stageCandidateFactory}
                onMakeTradeable={makeTradeable}
                onRepairRemaining={repairRemaining}
                onFreezeValidate={freezeValidate}
                onSaveTemplate={saveTemplateFromSource}
                onRefresh={() => loadModule("backtests")}
              />
            )}

            {activeTab === "results" && (
              <ResultsView
                runDetail={runDetail}
                researchRuns={researchRuns}
                loadRun={loadRunDetail}
                deleteRun={deleteRun}
                archiveRun={archiveRun}
                archiveRuns={archiveRuns}
                deleteRuns={deleteRuns}
                onRefineTemplate={refineTemplate}
                onRefineFurther={refineFurther}
                onMakeTradeable={makeTradeable}
                onRepairRemaining={repairRemaining}
                onFreezeValidate={freezeValidate}
                onSaveTemplate={saveTemplateFromSource}
                exportIncludeBars={exportIncludeBars}
              />
            )}
          </section>

          <section className="grid two lower-grid">
            <Panel icon={<Plug />} title="Market Plugins">
              <div className="plugin-list">
                {plugins.slice(0, 8).map((plugin) => (
                  <div className="plugin" key={plugin.plugin_id}>
                    <div>
                      <strong>{plugin.name}</strong>
                      <span>{plugin.ig_name} · {plugin.asset_class}</span>
                      <small>{plugin.ig_search_terms.join(", ")} · est {round(plugin.estimated_spread_bps)} / {round(plugin.estimated_slippage_bps)} bps</small>
                    </div>
                    <button type="button" onClick={() => installPlugin(plugin.plugin_id)}>Install</button>
                  </div>
                ))}
              </div>
            </Panel>

            <Panel icon={<Database />} title="Market Registry">
              <form onSubmit={submitMarket} className="compact">
                <input value={market.market_id} onChange={(event) => setMarket({ ...market, market_id: event.target.value })} placeholder="Market ID" required />
                <input value={market.name} onChange={(event) => setMarket({ ...market, name: event.target.value })} placeholder="Name" required />
                <input value={market.asset_class} onChange={(event) => setMarket({ ...market, asset_class: event.target.value })} placeholder="Asset class" required />
                <input value={market.eodhd_symbol} onChange={(event) => setMarket({ ...market, eodhd_symbol: event.target.value })} placeholder="EODHD symbol" required />
                <input value={market.ig_epic} onChange={(event) => setMarket({ ...market, ig_epic: event.target.value })} placeholder="IG EPIC" />
                <input value={market.ig_name} onChange={(event) => setMarket({ ...market, ig_name: event.target.value })} placeholder="IG market name" />
                <input value={market.ig_search_terms} onChange={(event) => setMarket({ ...market, ig_search_terms: event.target.value })} placeholder="IG search terms" />
                <input value={market.default_timeframe} onChange={(event) => setMarket({ ...market, default_timeframe: event.target.value })} placeholder="Default timeframe" />
                <input value={market.spread_bps} onChange={(event) => setMarket({ ...market, spread_bps: Number(event.target.value) })} type="number" min="0" step="0.1" placeholder="Spread bps" />
                <input value={market.slippage_bps} onChange={(event) => setMarket({ ...market, slippage_bps: Number(event.target.value) })} type="number" min="0" step="0.1" placeholder="Slippage bps" />
                <input value={market.min_backtest_bars} onChange={(event) => setMarket({ ...market, min_backtest_bars: Number(event.target.value) })} type="number" min="2" step="1" placeholder="Min bars" />
                <label className="check"><input type="checkbox" checked={market.enabled} onChange={(event) => setMarket({ ...market, enabled: event.target.checked })} />Enabled</label>
                <button>Save market</button>
              </form>
            </Panel>

            <Panel icon={<BarChart3 />} title="IG Spread Betting Engines">
              <div className="engine-list">
                {spreadBetEngines.map((engine) => (
                  <div className="engine" key={engine.engine_id}>
                    <div>
                      <strong>{engine.label}</strong>
                      <span>{engine.instrument_types.join(", ")}</span>
                      <small>{engine.notes}</small>
                    </div>
                    <span className={`badge ${engine.eligible_for_adaptive_backtest ? "good" : "base"}`}>{engine.eligible_for_adaptive_backtest ? "Backtest-ready" : "Needs product model"}</span>
                  </div>
                ))}
                {spreadBetEngines.length === 0 && <span className="muted">Engine registry unavailable.</span>}
              </div>
            </Panel>
          </section>

          <MarketTable markets={markets} />
        </>
      )}

      {activeModule === "paper" && (
        <DailyPaperView
          summary={paper}
          dayFactory={dayFactory}
          dailyScannerState={dailyScannerState}
          productMode={selectedProductMode}
          accountRoles={igAccountRoles}
          onRunDailyScanner={runDailyTemplateScanner}
          onRefresh={() => loadModule("paper")}
        />
      )}
      {activeModule === "broker" && (
        <BrokerView
          summary={broker}
          risk={risk}
          markets={markets}
          selectedProductMode={selectedProductMode}
          onSelectProductMode={chooseAccountMode}
        />
      )}
      {activeModule === "risk" && <RiskView summary={risk} />}

      {activeModule === "settings" && (
        <section className="lab-shell">
          <div className="lab-header">
            <div>
              <h2><SlidersHorizontal size={20} /> Settings</h2>
              <p>Provider credentials, connection state, and cache maintenance.</p>
            </div>
          </div>
          <SettingsView
            status={status}
            eodhdKey={eodhdKey}
            setEodhdKey={setEodhdKey}
            fmpKey={fmpKey}
            setFmpKey={setFmpKey}
            ig={ig}
            setIg={setIg}
            igRoles={igRoles}
            setIgRoles={setIgRoles}
            igAccountRoles={igAccountRoles}
            submitEodhd={submitEodhd}
            submitFmp={submitFmp}
            submitIg={submitIg}
            submitIgAccountRoles={submitIgAccountRoles}
            eodhdStatus={eodhdStatus}
            fmpStatus={fmpStatus}
            igStatus={igStatus}
            cacheStatus={cacheStatus}
            pruneCache={pruneCache}
          />
        </section>
      )}
    </main>
  );
}

function AccountSwitcher({ accountRoles, selectedProductMode, onSelect }) {
  const modes = [
    ["spread_bet", "Spread Bet"],
    ["cfd", "CFD"],
  ];
  return (
    <div className="account-switcher" aria-label="Account workspace">
      {modes.map(([mode, label]) => {
        const role = accountRoles?.[mode] ?? {};
        return (
          <button
            type="button"
            className={selectedProductMode === mode ? "account-switch active" : "account-switch"}
            key={mode}
            onClick={() => onSelect(mode)}
          >
            <span>{label}</span>
            <small>{role.active ? "active" : role.configured ? "saved" : "not set"}</small>
          </button>
        );
      })}
    </div>
  );
}

function AccountWorkspaceNotice({ productMode, accountRoles }) {
  const role = accountRoles?.[productMode] ?? {};
  return (
    <div className="account-workspace-notice">
      <div>
        <strong>{productModeLabel(productMode)} workspace</strong>
        <span>{role.active ? `${role.display_name || role.masked_account_id || "Demo account"} active` : "Set this IG demo account role in Settings before broker catalogue checks."}</span>
      </div>
      <div className="badge-group">
        <span className={`badge ${role.active ? "good" : "warn"}`}>{role.active ? "Account active" : "Needs account role"}</span>
        {productMode === "cfd" && <span className="badge warn">CFD cost model incomplete</span>}
        <span className="badge base">Live orders locked</span>
      </div>
    </div>
  );
}

function CockpitView({ summary, marketContext, setActiveModule }) {
  const runs = summary?.runs ?? {};
  const latest = runs.latest;
  const risk = summary?.risk ?? {};
  const calendarContext = marketContext?.calendar ?? marketContext ?? {};
  const contextEvents = calendarContext?.events ?? [];
  const volatility = marketContext?.volatility ?? {};
  const macro = marketContext?.macro ?? {};
  return (
    <section className="lab-shell cockpit">
      <div className="lab-header">
        <div>
          <h2><Home size={20} /> Today</h2>
          <p>System health, latest evidence, market context, and the safest next action.</p>
        </div>
        <span className="mode"><LockKeyhole size={16} /> {summary?.live_ordering_enabled ? "Live enabled" : "Live disabled"}</span>
      </div>
      <div className="metrics four">
        <Metric label="Mode" value={summary?.mode ?? "paper"} />
        <Metric label="Visible runs" value={runs.total_visible ?? 0} />
        <Metric label="Running" value={runs.running ?? 0} />
        <Metric label="Risk/trade" value={percent(risk.risk_per_trade_fraction)} />
      </div>
      <div className="grid two lower-grid">
        <Panel icon={<Activity />} title="Next Actions">
          <div className="status-list">
            {(summary?.next_actions ?? []).map((item) => (
              <button className="status action-status" type="button" key={item.kind} onClick={() => setActiveModule(item.kind === "providers" ? "settings" : "backtests")}>
                <strong>{item.label}</strong>
                <span>{item.detail}</span>
              </button>
            ))}
          </div>
        </Panel>
        <Panel icon={<ShieldCheck />} title="Latest Research">
          {latest ? (
            <div className="status-list">
              <div className="status">
                <strong>Run {latest.id} · {latest.status}</strong>
                <span>{latest.market_id} · {latest.trial_count} trials · best {round(latest.best_score)}</span>
              </div>
              <button className="secondary" type="button" onClick={() => setActiveModule("backtests")}>Build Templates</button>
            </div>
          ) : <span className="muted">No research runs yet.</span>}
        </Panel>
        <Panel icon={<CalendarDays />} title="Market Context">
          <div className="context-card">
            <div className="label-row">
              <div>
                <strong>{calendarContext?.available ? calendarRiskLabel(calendarContext.calendar_risk) : "FMP calendar unavailable"}</strong>
                <span>{calendarContext?.available ? `${calendarContext.high_impact_count ?? 0} high-impact events · ${calendarContext.major_event_count ?? 0} major` : calendarContext?.reason ?? "Connect FMP in Settings."}</span>
              </div>
              <span className={`badge ${calendarRiskClass(calendarContext?.calendar_risk)}`}>{calendarContext?.calendar_risk ?? "unknown"}</span>
            </div>
            {calendarContext?.next_major_event && (
              <div className="context-next">
                <span>Next major</span>
                <strong>{calendarContext.next_major_event.day} · {calendarContext.next_major_event.event}</strong>
              </div>
            )}
            {(volatility.available || macro.available) && (
              <div className="mini-metrics context-mini">
                <Metric label="VIX regime" value={volatility.regime ?? "n/a"} />
                <Metric label="VIX latest" value={round(volatility.latest_value)} />
                <Metric label="Credit" value={macro.high_yield_spread?.risk ?? "n/a"} />
                <Metric label="Yield curve" value={macro.yield_curve_10y2y?.regime ?? "n/a"} />
              </div>
            )}
            <div className="context-events">
              {contextEvents.slice(0, 5).map((event) => (
                <div className="context-event" key={`${event.day}-${event.time}-${event.event}`}>
                  <strong>{event.day}{event.time ? ` ${event.time}` : ""}</strong>
                  <span>{event.currency || event.country || "Global"} · {event.event}</span>
                </div>
              ))}
              {calendarContext?.available && contextEvents.length === 0 && <span className="muted">No major market-context events in the current window.</span>}
            </div>
            <button type="button" className="ghost" onClick={() => setActiveModule("settings")}>Open Settings</button>
          </div>
        </Panel>
      </div>
    </section>
  );
}

function GuideView({ setActiveModule }) {
  const workflow = [
    ["1", "Connect providers", "Use Settings for EODHD bars, FMP market context, and IG demo credentials. IG validation matters because costs, margin, minimum stake, and stop rules change the result."],
    ["2", "Check markets", "Use Backtests to confirm each market has the right symbol, timeframe, spread, slippage, minimum bars, and IG mapping."],
    ["3", "Run Candidate Factory", "Start with one market, Balanced factory, realistic dates, cost stress 2.0, and regime templates on. This creates leads without lowering paper gates."],
    ["4", "Read evidence first", "Focus on net profit after costs, compounded end balance, out-of-sample net, trade count, fold win rate, fold concentration, cost/gross, drawdown, capital fit, and warnings."],
    ["5", "Make it tradeable", "Use Best by market first, then click Make tradeable. The app chooses the next repair from the blockers, locks the market/family/regime, syncs IG costs when needed, and launches the repair run."],
    ["6", "Save and freeze", "Save promising repaired results to Templates, then use Freeze validate. This retests the exact rules with no parameter hunting, so the OOS period does not quietly become training data."],
    ["7", "Export evidence", "Download the evidence ZIP when something is worth offline review. Include bars when you want Codex-assisted analysis later."],
    ["8", "Paper only", "Only move forward after freshness, IG validation, capital, OOS, fold, cost, frozen validation, and regime gates are clear."],
  ];
  const modules = [
    ["Cockpit", "The home view for system status, provider health, current mode, and next actions."],
    ["Backtests", "Candidate Factory, run builder, run history, trial cards, regime evidence, Make tradeable repair workflow, archives, and exports."],
    ["Templates", "Saved strategy templates with frozen rules, market/timeframe/regime identity, readiness, and reuse actions."],
    ["Research", "Candidate readiness, blockers, validation warnings, capital feasibility, and paper queue status."],
    ["Broker", "Order previews only. Live order placement remains disabled."],
    ["Risk", "Capital scenarios, selected testing capital, compounded balance projections, 1% planned risk, and 5% daily loss envelope."],
  ];
  const metrics = [
    ["Net", "Profit after spread, slippage, funding, FX, and other modelled costs."],
    ["Daily Sharpe", "Annualized daily Sharpe. Useful, but only after sample size and robustness checks."],
    ["Days", "Daily observations used for Sharpe. Promotion normally needs at least 120."],
    ["DSR", "Deflated Sharpe probability, adjusted for repeated scans."],
    ["Testing capital fit", "Whether the candidate is feasible for the account size selected in the run. If blocked, the tile names the first sizing/risk reason."],
    ["Incubator", "A positive, cost-aware idea that is not paper-ready yet because OOS, fold, sample, or regime evidence is still too thin. Treat it as a research lead, not a trade."],
    ["Paper score", "A stricter score that weights capital fit, OOS trades, OOS profit, fold stability, cost stress, and Sharpe sample size before headline profit."],
    ["End balance", "Projected account balance after compounding from the selected account size."],
    ["Return", "Projected percentage return for that account scenario."],
    ["Edge active days", "Estimated days the best/target regime actually has capital at work. Sparse specialists can be useful portfolio slots if OOS and folds improve."],
    ["Capital use", "Active days divided by the available history. Low use means the capital can be scheduled elsewhere when this template is flat."],
    ["Net/active day", "Net profit divided by active days, useful for comparing small-window specialists without pretending they trade all year."],
    ["OOS net", "Walk-forward out-of-sample net profit after costs."],
    ["Fold win", "Share of trade-active walk-forward folds that made money. Idle folds are evidence about activity, not losing folds."],
    ["Fold share", "How much positive fold profit came from the best fold. High values mean fragility."],
    ["Gated net", "Profit after forcing the strategy flat outside allowed regimes. In target-regime refinements this is labelled Target net because the whole run is already gated."],
    ["Gated OOS", "Out-of-sample profit after the regime gate is applied. In target-regime refinements this can match the run OOS by design."],
    ["Cost/gross", "How much gross edge is consumed by trading friction. Under 25% is healthy, 25-40% needs care, and above 50-65% is usually fragile unless the OOS evidence is excellent."],
    ["Net/cost", "How much net profit remains for each pound of cost."],
    ["Warning colours", "Red blocks paper promotion, orange needs repair, blue is a specialist/regime identity, and grey is diagnostic."],
  ];
  const repairs = [
    ["Too few trades / low OOS", "Make tradeable runs a deeper longer-history retest. If a best regime exists, the retest grades that regime only and forces the rest flat."],
    ["Fragile folds", "If multiple-testing is also present, Make tradeable freezes the exact template first so the app stops hunting new parameters. Remaining fold warnings then need Longer history or rejection."],
    ["Single-month profit", "Use Exclude month. The run removes the dominant month from saved bars and retests."],
    ["Single-regime profit", "Use Regime repair. It retests full-history evidence and capped regime specialists."],
    ["Weak OOS evidence", "Use Evidence first or Longer history. Headline net is not enough if OOS is weak."],
    ["Missing IG validation", "Use Make tradeable or Sync costs. The app refreshes spread, slippage, margin, and minimum stake before rerunning."],
    ["Capital fit blocked", "Use Capital fit. It reruns the same market and family with conservative sizing, smaller stops, and ranking weighted toward the selected account size."],
    ["Multiple-testing haircut", "Make tradeable runs Freeze validate first when exact parameters are available, after IG costs are known. If not, use Evidence first. Use Find similar elsewhere only to create independent leads, not to upgrade the original score."],
    ["Costs overwhelm edge", "Use Higher costs. If the edge disappears, reject or redesign it."],
    ["Template ready for library", "Use Freeze validate before paper/demo. It keeps the exact parameters, target regime, market, and timeframe unchanged."],
  ];
  const gates = [
    "Fresh Sharpe days and no stale-data warnings.",
    "Realistic IG/EODHD costs, spread, slippage, and minimum stake assumptions.",
    "Positive net profit after costs and positive out-of-sample net.",
    "Enough trades and enough walk-forward evidence.",
    "No one fold, month, or rare regime carries the whole result.",
    "Regime-gated retest remains positive.",
    "Frozen validation passes after parameter changes have stopped.",
    "Selected testing capital scenario is feasible under margin, stop, and drawdown checks.",
    "Live trading remains locked; good candidates go to paper/demo review first.",
  ];
  return (
    <section className="lab-shell guide-shell">
      <div className="lab-header">
        <div>
          <h2><BookOpen size={20} /> Beginner Guide</h2>
          <p>A practical map of the trading cockpit, research workflow, robustness gates, and evidence exports.</p>
        </div>
        <div className="button-row">
          <button type="button" className="secondary" onClick={() => setActiveModule("backtests")}><Sparkles size={16} /> Build Templates</button>
          <button type="button" className="ghost" onClick={() => setActiveModule("paper")}><LineChart size={16} /> Daily Paper</button>
        </div>
      </div>

      <section className="guide-band">
        <h3>What This App Is</h3>
        <p>
          slrno is a research and preparation system. It runs cost-aware backtests, checks capital feasibility,
          splits results by market regime, exports evidence bundles, and keeps live order placement disabled while
          strategies are still being researched.
        </p>
      </section>

      <div className="guide-grid">
        <section className="lab-section">
          <h3>First Workflow</h3>
          <div className="guide-steps">
            {workflow.map(([number, title, detail]) => (
              <div className="guide-step" key={title}>
                <span>{number}</span>
                <div>
                  <strong>{title}</strong>
                  <p>{detail}</p>
                </div>
              </div>
            ))}
          </div>
        </section>

        <section className="lab-section">
          <h3>Main Areas</h3>
          <div className="guide-list">
            {modules.map(([term, detail]) => (
              <div className="guide-row" key={term}>
                <strong>{term}</strong>
                <span>{detail}</span>
              </div>
            ))}
          </div>
        </section>
      </div>

      <section className="lab-section">
        <h3>How To Read A Card</h3>
        <div className="guide-metric-grid">
          {metrics.map(([label, detail]) => (
            <div className="guide-metric" key={label}>
              <strong>{label}</strong>
              <span>{detail}</span>
            </div>
          ))}
        </div>
      </section>

      <div className="guide-grid">
        <section className="lab-section">
          <h3>Repair Actions</h3>
          <div className="guide-list">
            {repairs.map(([label, detail]) => (
              <div className="guide-row" key={label}>
                <strong>{label}</strong>
                <span>{detail}</span>
              </div>
            ))}
          </div>
        </section>

        <section className="lab-section">
          <h3>Promotion Gates</h3>
          <div className="guide-checklist">
            {gates.map((item) => (
              <div className="guide-check" key={item}>
                <ShieldCheck size={16} />
                <span>{item}</span>
              </div>
            ))}
          </div>
        </section>
      </div>

      <section className="lab-section">
        <h3>Regime Evidence</h3>
        <div className="guide-columns">
          <div>
            <strong>Full-period backtest</strong>
            <p>Shows the normal headline result over the selected date range.</p>
          </div>
          <div>
            <strong>Regime split</strong>
            <p>Shows which market conditions produced or damaged the result.</p>
          </div>
          <div>
            <strong>Regime-gated retest</strong>
            <p>Reruns the strategy while flat outside allowed regimes.</p>
          </div>
          <div>
            <strong>Specialist scan</strong>
            <p>Optional capped search for regime-specific research leads.</p>
          </div>
        </div>
      </section>

      <section className="lab-section">
        <h3>Simple Decision Rule</h3>
        <p className="guide-rule">
          Prefer candidates with positive out-of-sample net profit after realistic costs, feasible selected-account sizing,
          enough trades, fresh Sharpe days, stable folds, tolerable drawdown, and regime evidence that survives the
          gated retest. Treat headline Sharpe as supporting evidence, not permission to trade.
        </p>
      </section>
    </section>
  );
}

function TemplateLibraryView({ summary, onUseTemplate, onMakeTradeable, onRepairRemaining, onFreezeValidate, onArchive, onRefresh }) {
  const templates = summary?.templates ?? [];
  const counts = summary?.counts ?? {};
  const [marketFilter, setMarketFilter] = React.useState("all");
  const [statusFilter, setStatusFilter] = React.useState("active");
  const markets = [...new Set(templates.map((template) => template.market_id).filter(Boolean))].sort();
  const visibleTemplates = templates.filter((template) => {
    if (marketFilter !== "all" && template.market_id !== marketFilter) {
      return false;
    }
    if (statusFilter === "active") {
      return template.status === "active";
    }
    if (statusFilter === "paper") {
      return template.readiness_status === "ready_for_paper" || ["paper_candidate", "validated_candidate"].includes(template.promotion_tier);
    }
    if (statusFilter === "blocked") {
      return template.readiness_status === "blocked";
    }
    return true;
  });
  return (
    <section className="lab-shell">
      <div className="lab-header">
        <div>
          <h2><Library size={20} /> Template Library</h2>
          <p>Saved strategy templates, frozen rules, regime identity, and promotion evidence.</p>
        </div>
        <button type="button" className="secondary" onClick={onRefresh}><RefreshCw size={16} /> Refresh</button>
      </div>
      <div className="metrics four">
        <Metric label="Active" value={counts.active ?? 0} />
        <Metric label="Frozen rules" value={counts.frozen ?? 0} />
        <Metric label="Paper-ready" value={counts.paper_ready ?? 0} />
        <Metric label="Markets" value={counts.markets ?? 0} />
      </div>
      <div className="tabs template-filters">
        <div className="segmented compact-filter">
          {[
            ["active", "Active"],
            ["blocked", "Blocked"],
            ["paper", "Paper-ready"],
            ["all", "All"],
          ].map(([id, label]) => (
            <button className={statusFilter === id ? "segment active" : "segment"} key={id} type="button" onClick={() => setStatusFilter(id)}>
              {label}
            </button>
          ))}
        </div>
        {markets.length > 0 && (
          <div className="segmented compact-filter">
            <button className={marketFilter === "all" ? "segment active" : "segment"} type="button" onClick={() => setMarketFilter("all")}>All markets</button>
            {markets.slice(0, 8).map((marketId) => (
              <button className={marketFilter === marketId ? "segment active" : "segment"} key={marketId} type="button" onClick={() => setMarketFilter(marketId)}>
                {marketId}
              </button>
            ))}
          </div>
        )}
      </div>
      <div className="warning-legend">
        <span className="warning-chip blocker">Paper blocker</span>
        <span className="warning-chip repair">Needs repair</span>
        <span className="warning-chip specialist">Specialist identity</span>
        <span className="warning-chip info">Diagnostic</span>
      </div>
      <div className="template-grid">
        {visibleTemplates.map((template) => (
          <TemplateCard
            key={template.id}
            template={template}
            onUseTemplate={onUseTemplate}
            onMakeTradeable={onMakeTradeable}
            onRepairRemaining={onRepairRemaining}
            onFreezeValidate={onFreezeValidate}
            onArchive={onArchive}
          />
        ))}
        {templates.length === 0 && <span className="muted">No templates saved yet.</span>}
        {templates.length > 0 && visibleTemplates.length === 0 && <span className="muted">No templates match this filter.</span>}
      </div>
    </section>
  );
}

function TemplateCard({ template, onUseTemplate, onMakeTradeable, onRepairRemaining, onFreezeValidate, onArchive }) {
  const payload = template.payload ?? {};
  const backtest = template.backtest ?? payload.backtest ?? {};
  const pattern = template.pattern ?? payload.pattern ?? {};
  const evidence = payload.evidence ?? {};
  const searchAudit = payload.search_audit ?? {};
  const parameters = template.parameters ?? payload.parameters ?? {};
  const sourceTemplate = template.source_template ?? payload.source_template ?? {};
  const accountSize = template.testing_account_size || WORKING_ACCOUNT_SIZE;
  const frozenParameterCount = Object.keys(sourceTemplate?.parameters ?? {}).length;
  const capital = accountFeasibility(template.capital_scenarios ?? payload.capital_scenarios, accountSize);
  const intraday = Boolean(sourceTemplate.force_flat_before_close || sourceTemplate.no_overnight || parameters.force_flat_before_close || parameters.no_overnight || parameters.day_trading_mode);
  const showRepairRemaining = shouldShowRepairRemaining(template);
  const showFreezeValidate = shouldShowFreezeValidate(template);
  return (
    <article className="template-card">
      <div className="template-card-header">
        <div>
          <div className="label-row">
            <strong>{template.name}</strong>
            <div className="badge-group">
              <span className="badge market-badge">{template.market_id}</span>
              <span className={`badge ${tierBadgeClass(template.promotion_tier)}`}>{tierLabel(template.promotion_tier)}</span>
            </div>
          </div>
          <span>{[normalizeInterval(template.interval), strategyFamilyLabel(template.strategy_family), template.target_regime ? `${regimeLabel(template.target_regime)} only` : "", `score ${round(template.robustness_score)}`].filter(Boolean).join(" · ")}</span>
        </div>
        <div className="template-status">
          <small>{template.status}</small>
          <strong>{readinessLabel(template.readiness_status)}</strong>
        </div>
      </div>
      <div className="lifecycle-badges">
        <span className={`badge ${intraday ? "good" : "warn"}`}>{intraday ? "Intraday" : "Overnight / swing"}</span>
        <span className={`badge ${frozenParameterCount > 0 ? "good" : "warn"}`}>{frozenParameterCount > 0 ? "Frozen rules" : "Needs freeze"}</span>
        <span className={`badge ${String(capital).toLowerCase().includes("ok") ? "good" : "warn"}`}>{capital}</span>
        <span className="badge base">Broker preview only</span>
      </div>
      <div className="mini-metrics template-metrics">
        <Metric label="Frozen params" value={frozenParameterCount} />
        <Metric label="Paper score" value={round(searchAudit.paper_readiness_score)} />
        <Metric label={`${accountSizeLabel(accountSize)} fit`} value={capital} />
        <Metric label="Net" value={formatMoney(backtest.net_profit)} />
        <Metric label="OOS net" value={formatMoney(evidence.oos_net_profit ?? backtest.test_profit)} />
        <Metric label="OOS trades" value={evidence.oos_trade_count ?? 0} />
        <Metric label="Drawdown" value={formatMoney(backtest.max_drawdown)} />
        <Metric label="Trades" value={backtest.trade_count ?? 0} />
        <Metric label="Regime verdict" value={regimeVerdictLabel(pattern.regime_verdict)} />
        <Metric label="Best regime" value={regimeLabel(pattern.dominant_profit_regime?.key || template.target_regime)} />
      </div>
      <div className="warning-row">
        <WarningChips warnings={template.warnings ?? payload.warnings} limit={8} empty="Gate clear" />
      </div>
      <div className="button-row">
        {showRepairRemaining ? (
          <button type="button" className="secondary" onClick={() => onRepairRemaining(template)}><ShieldCheck size={16} /> Repair remaining</button>
        ) : (
          <button type="button" className="secondary" onClick={() => onMakeTradeable(template)}><ShieldCheck size={16} /> Make tradeable</button>
        )}
        {showFreezeValidate && (
          <button type="button" className="ghost" onClick={() => onFreezeValidate(template)}><LockKeyhole size={16} /> Freeze validate</button>
        )}
        <button type="button" className="ghost" onClick={() => onUseTemplate(template)}><RefreshCw size={16} /> Use</button>
        <button type="button" className="ghost" onClick={() => onArchive(template)}><Archive size={16} /> Archive</button>
      </div>
    </article>
  );
}

function DailyPaperView({ summary, dayFactory, dailyScannerState, productMode, accountRoles, onRunDailyScanner, onRefresh }) {
  const tracked = summary?.tracked_candidates ?? [];
  const counts = dayFactory?.counts ?? {};
  const dailyQueue = dayFactory?.daily_paper_queue ?? [];
  const reviewSignals = dayFactory?.review_signals ?? [];
  const unsuitableSignals = dayFactory?.unsuitable ?? [];
  const noSetupSignals = dayFactory?.no_setup_sample ?? dayFactory?.latest_scan?.config?.no_setup_sample ?? [];
  const manualPlaybooks = dayFactory?.manual_playbooks ?? [];
  const latestScan = dayFactory?.latest_scan;
  return (
    <section className="lab-shell">
      <div className="lab-header">
        <div>
          <h2><LineChart size={20} /> Daily Paper</h2>
          <p>Market-open scans use active frozen intraday templates plus today-specific tape gates. No parameter search, no live orders.</p>
        </div>
        <div className="button-row">
          <button type="button" onClick={onRunDailyScanner} disabled={dailyScannerState?.status === "running"}>
            <Search size={16} /> {dailyScannerState?.status === "running" ? "Scanning..." : "Start Daily Scan"}
          </button>
          <button type="button" className="secondary" onClick={onRefresh}><RefreshCw size={16} /> Refresh</button>
        </div>
      </div>
      <AccountWorkspaceNotice productMode={productMode} accountRoles={accountRoles} />
      <div className="metrics four">
        <Metric label="Frozen day templates" value={counts.frozen_day_templates ?? 0} />
        <Metric label="Paper queue" value={counts.daily_paper_queue ?? dailyQueue.length} />
        <Metric label="Review signals" value={counts.eligible_review_signals ?? reviewSignals.length} />
        <Metric label="Tape blockers" value={counts.today_filter_blockers ?? 0} />
        <Metric label="Order mode" value="disabled" />
      </div>
      <div className="status-list daily-status">
        <div className="status compact-status">
          <strong>Manual trader gates · frozen rules only</strong>
          <span>Discovery leads stay blocked until Make tradeable, Save template, and Freeze validate are complete. Daily mode then checks relative volume, VWAP, opening range, spread, and account fit before paper preview.</span>
          <small>{productMode === "cfd" ? "CFD account selection is available for catalogue checks, but CFD-specific cost validation still needs a dedicated model." : "Spread bet mode uses the current IG-style spread-bet cost model."}</small>
        </div>
        {manualPlaybooks.length > 0 && (
          <div className="status compact-status">
            <strong>Setup playbooks</strong>
            <span>{manualPlaybooks.slice(0, 5).map((playbook) => playbook.label).join(" · ")}</span>
            <small>These are confirmation gates around frozen templates, not new strategy generation.</small>
          </div>
        )}
        {dailyScannerState?.detail && (
          <div className="status compact-status">
            <strong>Scanner · {dailyScannerState.status}</strong>
            <span>{dailyScannerState.detail}</span>
          </div>
        )}
        {latestScan && (
          <div className="status compact-status">
            <strong>Latest scan · {latestScan.trading_date}</strong>
            <span>{latestScan.counts?.daily_paper_queue ?? 0} paper previews · {latestScan.counts?.review_signals ?? 0} review signals · {latestScan.status}</span>
            <small>Scan {latestScan.id} · strategy generation disabled</small>
          </div>
        )}
      </div>
      <div className="lab-grid daily-paper-grid">
        <section className="lab-section span-2">
          <h3>1-3 Paper Previews</h3>
          <div className="factory-lead-grid">
            {dailyQueue.map((lead) => <DayTradingQueueCard key={`${lead.id}-${lead.market_id}`} lead={lead} />)}
            {dailyQueue.length === 0 && <span className="muted">No active frozen intraday template has produced a paper preview yet.</span>}
          </div>
        </section>
        <section className="lab-section">
          <h3>Review Signals</h3>
          <div className="status-list">
            {reviewSignals.slice(0, 10).map((lead) => (
              <div className="status compact-status" key={`${lead.id}-${lead.market_id}-daily-review`}>
                <strong>{lead.market_id} · {lead.strategy_name}</strong>
                <span>{shortPlaybookLabel(lead.manual_playbook) || strategyFamilyLabel(lead.strategy_family)} · OOS {formatMoney(lead.oos_net_profit)} · RVol {formatRatio(lead.today_tape?.relative_volume ?? 0)}</span>
                <small>{lead.signal_explainer?.headline ?? `${lead.target_regime ? `${regimeLabel(lead.target_regime)} only · ` : ""}Preview only`}</small>
              </div>
            ))}
            {reviewSignals.length === 0 && <span className="muted">The review queue fills after frozen templates match today's eligible markets.</span>}
          </div>
        </section>
        <section className="lab-section">
          <h3>Unsuitable / Move On</h3>
          <div className="status-list">
            {unsuitableSignals.slice(0, 8).map((lead) => (
              <div className="status compact-status" key={`${lead.id}-${lead.market_id}-daily-unsuitable`}>
                <strong>{lead.market_id} · {lead.strategy_name}</strong>
                <span>{lead.unsuitable_reason || "Capital fit failed for this account."}</span>
                <small>The factory skips this so attention stays on feasible markets.</small>
              </div>
            ))}
            {unsuitableSignals.length === 0 && <span className="muted">No terminal account-fit blockers in the latest queue.</span>}
          </div>
        </section>
        <section className="lab-section">
          <h3>No Setup Today</h3>
          <div className="status-list">
            {noSetupSignals.slice(0, 8).map((lead) => (
              <div className="status compact-status" key={`${lead.template_id}-${lead.market_id}-daily-no-setup`}>
                <strong>{lead.market_id} · {lead.strategy_name}</strong>
                <span>{lead.no_setup_reason || lead.signal_explainer?.headline || "Today-specific filters did not confirm the setup."}</span>
                <small>{shortPlaybookLabel(lead.manual_playbook)} · RVol {formatRatio(lead.today_tape?.relative_volume ?? 0)} · VWAP {round(lead.today_tape?.distance_from_vwap_bps ?? 0)} bps</small>
              </div>
            ))}
            {noSetupSignals.length === 0 && <span className="muted">Frozen templates that do not fire today will appear here after a scan.</span>}
          </div>
        </section>
        <section className="lab-section span-2">
          <h3>30-Day Paper Review</h3>
          <div className="candidate-list">
            {tracked.map((candidate) => (
              <div className="candidate-card compact" key={candidate.id}>
                <div className="label-row">
                  <strong>{candidate.strategy_name}</strong>
                  <span className="badge good">Paper queue</span>
                </div>
                <span>{candidate.market_id} · current {regimeLabel(candidate.current_regime)}</span>
                <div className="mini-metrics">
                  <Metric label="Allowed" value={(candidate.allowed_regimes ?? []).map(regimeLabel).join(" / ") || "n/a"} />
                  <Metric label="Blocked" value={(candidate.blocked_regimes ?? []).map(regimeLabel).join(" / ") || "none"} />
                  <Metric label="Best regime" value={regimeLabel(candidate.dominant_profit_regime)} />
                  <Metric label={accountSizeLabel(candidate.testing_account_size)} value={(candidate.capital_summary?.feasible_accounts ?? []).includes(Number(candidate.testing_account_size ?? WORKING_ACCOUNT_SIZE)) ? "OK" : "Blocked"} />
                </div>
              </div>
            ))}
            {tracked.length === 0 && <span className="muted">No candidates have passed the freshness, cost, capital, and regime gates into longer paper review yet.</span>}
          </div>
        </section>
      </div>
    </section>
  );
}

function PaperView({ summary }) {
  const tracked = summary?.tracked_candidates ?? [];
  return (
    <section className="lab-shell">
      <div className="lab-header">
        <div>
          <h2><LineChart size={20} /> Paper Trading</h2>
          <p>Approved candidates will land here for 30-day paper review.</p>
        </div>
        <span className="mode"><ShieldCheck size={16} /> {summary?.status ?? "not started"}</span>
      </div>
      <div className="metrics four">
        <Metric label="Tracked" value={tracked.length} />
        <Metric label="Order mode" value={summary?.live_ordering_enabled ? "live" : "disabled"} />
        <Metric label="Protocol" value="30 days" />
        <Metric label="Review" value="regime-gated" />
      </div>
      <div className="candidate-list">
        {tracked.map((candidate) => (
          <div className="candidate-card compact" key={candidate.id}>
            <div className="label-row">
              <strong>{candidate.strategy_name}</strong>
              <span className="badge good">Paper queue</span>
            </div>
            <span>{candidate.market_id} · current {regimeLabel(candidate.current_regime)}</span>
            <div className="mini-metrics">
              <Metric label="Allowed" value={(candidate.allowed_regimes ?? []).map(regimeLabel).join(" / ") || "n/a"} />
              <Metric label="Blocked" value={(candidate.blocked_regimes ?? []).map(regimeLabel).join(" / ") || "none"} />
              <Metric label="Best regime" value={regimeLabel(candidate.dominant_profit_regime)} />
              <Metric label={accountSizeLabel(candidate.testing_account_size)} value={(candidate.capital_summary?.feasible_accounts ?? []).includes(Number(candidate.testing_account_size ?? WORKING_ACCOUNT_SIZE)) ? "OK" : "Blocked"} />
            </div>
          </div>
        ))}
        {tracked.length === 0 && <span className="muted">No candidates have passed the freshness, cost, capital, and regime gates yet.</span>}
      </div>
    </section>
  );
}

function AccountRoleCard({ mode, title, detail, role, active, onSelect }) {
  return (
    <button type="button" className={active ? "account-card active" : "account-card"} onClick={onSelect}>
      <div className="label-row">
        <strong>{title}</strong>
        <span className={`badge ${role?.active ? "good" : role?.configured ? "base" : "warn"}`}>
          {role?.active ? "Active" : role?.configured ? "Saved" : "Not set"}
        </span>
      </div>
      <span>{role?.display_name || role?.masked_account_id || detail}</span>
      <small>{mode === "cfd" ? "CFD templates must not be treated as ready until CFD-specific validation is complete." : "Default path for current £3k template discovery and daily paper."}</small>
    </button>
  );
}

function BrokerView({ summary, risk, markets, selectedProductMode, onSelectProductMode }) {
  const defaultMarket = markets[0]?.market_id ?? "NAS100";
  const [previewForm, setPreviewForm] = React.useState({
    market_id: defaultMarket,
    side: "BUY",
    stake: "1",
    account_size: String(WORKING_ACCOUNT_SIZE),
    entry_price: "",
    stop: "",
    limit: "",
  });
  const [preview, setPreview] = React.useState(null);
  const [previewError, setPreviewError] = React.useState("");

  React.useEffect(() => {
    if (!previewForm.market_id && defaultMarket) {
      setPreviewForm((current) => ({ ...current, market_id: defaultMarket }));
    }
  }, [defaultMarket, previewForm.market_id]);

  async function submitPreview(event) {
    event.preventDefault();
    setPreviewError("");
    try {
      const payload = {
        ...previewForm,
        stake: Number(previewForm.stake),
        account_size: Number(previewForm.account_size),
        entry_price: optionalNumber(previewForm.entry_price),
        stop: optionalNumber(previewForm.stop),
        limit: optionalNumber(previewForm.limit),
      };
      setPreview(await previewBrokerOrder(payload));
    } catch (error) {
      setPreviewError(error.message);
    }
  }

  return (
    <section className="lab-shell">
      <div className="lab-header">
        <div>
          <h2><Wallet size={20} /> Accounts</h2>
          <p>Two demo account workspaces with separate catalogue checks, capital view, and broker-safe previews.</p>
        </div>
        <span className="mode"><LockKeyhole size={16} /> {summary?.order_placement ?? "disabled"}</span>
      </div>
      <div className="account-grid">
        {[
          ["spread_bet", "Spread Bet Demo", "Current research and paper workflow uses the spread-bet cost model."],
          ["cfd", "CFD Demo", "Catalogue checks work, but dedicated CFD cost and margin modelling is still incomplete."],
        ].map(([mode, title, detail]) => (
          <AccountRoleCard
            key={mode}
            mode={mode}
            title={title}
            detail={detail}
            role={summary?.ig_account_roles?.[mode]}
            active={selectedProductMode === mode}
            onSelect={() => onSelectProductMode(mode)}
          />
        ))}
      </div>
      <div className="grid two">
        <Panel icon={<Activity />} title="Connection">
          <div className="status-list">
            {(summary?.providers ?? []).map((item) => (
              <div className="status" key={item.provider}>
                <strong>{item.provider.toUpperCase()}</strong>
                <span>{item.configured ? "saved" : "not saved"} · {item.last_status}</span>
              </div>
            ))}
          </div>
        </Panel>
        <Panel icon={<Database />} title="Rules Coverage">
          <div className="metrics four">
            <Metric label="Markets" value={markets.length} />
            <Metric label="EPICs" value={markets.filter((item) => item.ig_epic).length} />
            <Metric label="Enabled" value={markets.filter((item) => item.enabled).length} />
            <Metric label="Execution" value="off" />
          </div>
        </Panel>
        <Panel icon={<LockKeyhole />} title="Capital Guardrails">
          <div className="metrics four">
            <Metric label="Workspace" value={productModeLabel(selectedProductMode)} />
            <Metric label="Risk/trade" value={percent(risk?.risk_per_trade_fraction)} />
            <Metric label="Daily stop" value={percent(risk?.daily_loss_fraction)} />
            <Metric label="Live orders" value={risk?.live_ordering_enabled ? "on" : "off"} />
          </div>
        </Panel>
      </div>
      <div className="grid two lower-grid">
        <Panel icon={<Wallet />} title="Safe Order Preview">
          <form onSubmit={submitPreview} className="compact">
            <select value={previewForm.market_id} onChange={(event) => setPreviewForm({ ...previewForm, market_id: event.target.value })}>
              {markets.map((item) => <option key={item.market_id} value={item.market_id}>{item.market_id} · {item.name}</option>)}
            </select>
            <select value={previewForm.side} onChange={(event) => setPreviewForm({ ...previewForm, side: event.target.value })}>
              <option value="BUY">Buy</option>
              <option value="SELL">Sell</option>
            </select>
            <input value={previewForm.stake} onChange={(event) => setPreviewForm({ ...previewForm, stake: event.target.value })} type="number" min="0.01" step="0.01" placeholder="Stake" />
            <input value={previewForm.account_size} onChange={(event) => setPreviewForm({ ...previewForm, account_size: event.target.value })} type="number" min="1" step="1" placeholder="Account size" />
            <input value={previewForm.entry_price} onChange={(event) => setPreviewForm({ ...previewForm, entry_price: event.target.value })} type="number" min="0" step="0.0001" placeholder="Entry price optional" />
            <input value={previewForm.stop} onChange={(event) => setPreviewForm({ ...previewForm, stop: event.target.value })} type="number" min="0" step="0.0001" placeholder="Stop" />
            <input value={previewForm.limit} onChange={(event) => setPreviewForm({ ...previewForm, limit: event.target.value })} type="number" min="0" step="0.0001" placeholder="Limit optional" />
            <button>Preview only</button>
          </form>
          {previewError && <span className="muted">{previewError}</span>}
        </Panel>
        <Panel icon={<LockKeyhole />} title="Preview Result">
          {preview ? (
            <>
              <div className="metrics four">
                <Metric label="Stake" value={preview.effective_stake} />
                <Metric label="Entry" value={preview.entry_price} />
                <Metric label="Margin" value={formatMoney(preview.estimated_margin)} />
                <Metric label="Risk" value={formatMoney(preview.planned_risk)} />
              </div>
              <div className="warning-row">
                {(preview.rule_violations ?? []).length ? <WarningChips warnings={preview.rule_violations} /> : <span className="badge good">Preview clear</span>}
              </div>
              <small className="muted">Live order placement is disabled for this preview.</small>
            </>
          ) : (
            <span className="muted">Create a preview to check min stake, margin, stop distance, and risk before paper review.</span>
          )}
        </Panel>
      </div>
      <MarketTable markets={markets} />
    </section>
  );
}

function RiskView({ summary }) {
  return (
    <section className="lab-shell">
      <div className="lab-header">
        <div>
          <h2><LockKeyhole size={20} /> Risk</h2>
          <p>Capital scenarios, loss limits, and hard execution locks.</p>
        </div>
        <span className="mode"><ShieldCheck size={16} /> Kill switch on</span>
      </div>
      <div className="metrics four">
        <Metric label="Scenarios" value={(summary?.capital_scenarios ?? []).map(formatMoney).join(" / ")} />
        <Metric label="Risk/trade" value={percent(summary?.risk_per_trade_fraction)} />
        <Metric label="Daily stop" value={percent(summary?.daily_loss_fraction)} />
        <Metric label="Live orders" value={summary?.live_ordering_enabled ? "on" : "off"} />
      </div>
    </section>
  );
}

function MarketTable({ markets }) {
  return (
    <section className="market-table">
      <h2><SlidersHorizontal size={20} /> Markets</h2>
      <table>
        <thead>
          <tr><th>ID</th><th>Name</th><th>Class</th><th>EODHD</th><th>IG market</th><th>EPIC</th><th>Costs</th><th>Enabled</th></tr>
        </thead>
        <tbody>
          {markets.map((item) => (
            <tr key={item.market_id}>
              <td>{item.market_id}</td>
              <td>{item.name}</td>
              <td>{item.asset_class}</td>
              <td>{item.eodhd_symbol}</td>
              <td>{item.ig_name || "search required"}</td>
              <td>{item.ig_epic || "manual"}</td>
              <td>
                {normalizeInterval(item.default_timeframe)} · est {round(item.estimated_spread_bps ?? item.spread_bps)} / {round(item.estimated_slippage_bps ?? item.slippage_bps)} bps
                {marketSubline(item) && <small>{marketSubline(item)}</small>}
              </td>
              <td>{item.enabled ? "Yes" : marketAvailabilityNote(item)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}

function moduleLabel(id) {
  return MODULES.find((item) => item[0] === id)?.[1] ?? id;
}

function ResultsView({ runDetail, researchRuns, loadRun, deleteRun, archiveRun, archiveRuns, deleteRuns, onRefineTemplate, onRefineFurther, onMakeTradeable, onRepairRemaining, onFreezeValidate, onSaveTemplate, exportIncludeBars }) {
  const pareto = runDetail?.pareto ?? [];
  const trials = runDetail?.trials ?? [];
  const marketStatuses = runDetail?.config?.market_statuses ?? [];
  const marketFailures = runDetail?.config?.market_failures ?? [];
  const autoFreeze = runDetail?.config?.pipeline?.auto_freeze;
  const [trialTierFilter, setTrialTierFilter] = React.useState("active");
  const [trialScanFilter, setTrialScanFilter] = React.useState("all");
  const [trialMarketView, setTrialMarketView] = React.useState("overall");
  const [trialCostFilter, setTrialCostFilter] = React.useState("all");
  const [showAllRuns, setShowAllRuns] = React.useState(false);
  const [selectedRunIds, setSelectedRunIds] = React.useState([]);
  const qualitySummary = runQualitySummary(trials);
  const filteredTrials = trials
    .filter((trial) => tierMatchesFilter(trial.promotion_tier, trialTierFilter))
    .filter((trial) => trialScanMatchesFilter(trial, trialScanFilter))
    .filter((trial) => trialCostMatchesFilter(trial, trialCostFilter));
  const displayRankedTrials = sortTrialsForDisplay(filteredTrials);
  const rankedTrials = trialMarketView === "market_best"
    ? bestTrialsByMarket(displayRankedTrials, 3)
    : trialMarketView === "regime_best"
    ? bestTrialsByRegime(displayRankedTrials, 3)
    : displayRankedTrials;
  const visibleRuns = showAllRuns ? researchRuns : researchRuns.slice(0, 18);
  const selectedRuns = researchRuns.filter((run) => selectedRunIds.includes(run.id));
  const visibleDeletableRuns = visibleRuns.filter((run) => !["created", "running"].includes(run.status));
  const visibleNoisyRuns = visibleDeletableRuns.filter(isNoisyRun);
  const visibleTrials = rankedTrials.slice(0, 20);

  React.useEffect(() => {
    setSelectedRunIds((current) => current.filter((id) => researchRuns.some((run) => run.id === id)));
  }, [researchRuns]);

  function toggleSelectedRun(runId) {
    setSelectedRunIds((current) => (
      current.includes(runId) ? current.filter((id) => id !== runId) : [...current, runId]
    ));
  }

  function selectVisibleRuns() {
    setSelectedRunIds((current) => {
      const next = new Set(current);
      for (const run of visibleDeletableRuns) {
        next.add(run.id);
      }
      return [...next];
    });
  }

  function selectNoisyRuns() {
    setSelectedRunIds((current) => {
      const next = new Set(current);
      for (const run of visibleNoisyRuns) {
        next.add(run.id);
      }
      return [...next];
    });
  }

  return (
    <div className="lab-grid">
      <section className="lab-section span-2">
        <div className="label-row table-heading">
          <h3>Recent Runs</h3>
          <div className="button-row compact-actions">
            <button type="button" className="ghost" onClick={selectVisibleRuns} disabled={visibleDeletableRuns.length === 0}>Select visible</button>
            <button type="button" className="ghost" onClick={selectNoisyRuns} disabled={visibleNoisyRuns.length === 0}>Select noisy</button>
            <button type="button" className="ghost" onClick={() => setSelectedRunIds([])} disabled={selectedRunIds.length === 0}>Clear</button>
            <button type="button" className="secondary" onClick={() => archiveRuns(selectedRuns)} disabled={selectedRuns.length === 0}>
              Archive selected ({selectedRuns.length})
            </button>
            <button type="button" className="ghost" onClick={() => deleteRuns(selectedRuns)} disabled={selectedRuns.length === 0}>
              Delete selected ({selectedRuns.length})
            </button>
            {researchRuns.length > 18 && (
              <button type="button" className="ghost" onClick={() => setShowAllRuns((current) => !current)}>
                {showAllRuns ? "Show recent" : `Show all ${researchRuns.length}`}
              </button>
            )}
          </div>
        </div>
        <div className="run-list run-manager">
          {visibleRuns.map((run) => (
            <div className="run-item" key={run.id}>
              <label className="run-select" title={`Select Run ${run.id}`}>
                <input
                  type="checkbox"
                  checked={selectedRunIds.includes(run.id)}
                  onChange={() => toggleSelectedRun(run.id)}
                  disabled={["created", "running"].includes(run.status)}
                />
              </label>
              <button className={`run-pill ${runPurposeClass(run)}`} type="button" onClick={() => loadRun(run.id)} title={runPurposeDetail(run)}>
                <strong>Run {run.id}</strong>
                <span><span className="run-kind-label">{runPurposeLabel(run)}</span> · {run.market_id} · {run.status} · {run.trial_count} trials · best {round(run.best_score)}</span>
                {runPurposeSubLabel(run) && <small>{runPurposeSubLabel(run)}</small>}
              </button>
              <button
                className="icon-button danger"
                type="button"
                onClick={() => deleteRun(run)}
                disabled={["created", "running"].includes(run.status)}
                title={`Delete Run ${run.id}`}
              >
                <Trash2 size={16} />
              </button>
              <button
                className="icon-button"
                type="button"
                onClick={() => archiveRun(run)}
                disabled={["created", "running"].includes(run.status) || run.archived}
                title={`Archive Run ${run.id}`}
              >
                <Archive size={16} />
              </button>
              <a
                className="icon-button"
                href={researchRunExportUrl(run.id, exportIncludeBars)}
                title={`Download evidence bundle for Run ${run.id}`}
              >
                <Download size={16} />
              </a>
            </div>
          ))}
          {researchRuns.length === 0 && <span className="muted">No runs yet.</span>}
          {!showAllRuns && researchRuns.length > visibleRuns.length && (
            <span className="muted">Showing {visibleRuns.length} of {researchRuns.length} runs.</span>
          )}
        </div>
      </section>
      {(marketStatuses.length > 0 || marketFailures.length > 0 || runDetail?.error) && (
        <section className="lab-section span-2">
          <h3>Market Data Status</h3>
          <div className="status-list">
            {marketStatuses.map((item) => (
              <div className="status" key={`${item.market_id}-${item.status}`}>
                <strong>{item.market_id} · <span className={`badge ${statusBadgeClass(item.status)}`}>{item.status}</span></strong>
                <span>{item.eodhd_symbol} · {normalizeInterval(item.interval)} · {marketDataSourceLabel(item.data_source_status)}</span>
                {item.fallback_reason && <small>{item.fallback_reason}</small>}
                {item.bar_count !== undefined && <small>{marketStatusLine(item)}</small>}
                {item.error && <small>{item.error}</small>}
              </div>
            ))}
            {marketFailures.map((item) => (
              <div className="status" key={`${item.market_id}-${item.error}`}>
                <strong>{item.market_id} · <span className="badge warn">failed</span></strong>
                <span>{item.error}</span>
              </div>
            ))}
            {marketStatuses.length === 0 && runDetail?.error && <span className="muted">{runDetail.error}</span>}
          </div>
        </section>
      )}
      {autoFreeze && (
        <section className="lab-section span-2">
          <h3>Auto Freeze</h3>
          <div className="status-list">
            <div className="status compact-status">
              <strong>{readableSnake(autoFreeze.status ?? "waiting")}</strong>
              <span>{autoFreeze.detail ?? autoFreeze.reason ?? "Waiting for the guided design run to finish."}</span>
              {(autoFreeze.template_id || autoFreeze.freeze_run_id) && (
                <small>
                  {autoFreeze.template_id ? `Template ${autoFreeze.template_id}` : ""}
                  {autoFreeze.template_id && autoFreeze.freeze_run_id ? " · " : ""}
                  {autoFreeze.freeze_run_id ? `Freeze run ${autoFreeze.freeze_run_id}` : ""}
                </small>
              )}
              {autoFreeze.error && <small>{autoFreeze.error}</small>}
            </div>
          </div>
        </section>
      )}
      {runDetail && <RegimeEvidence runDetail={runDetail} trials={trials} />}
      {runDetail && (
        <RegimeTemplateShortlist
          runDetail={runDetail}
          onMakeTradeable={onMakeTradeable}
          onRepairRemaining={onRepairRemaining}
          onFreezeValidate={onFreezeValidate}
          onSaveTemplate={onSaveTemplate}
        />
      )}
      {runDetail && (
        <section className="lab-section span-2">
          <div className="label-row table-heading">
            <h3>Evidence Bundle</h3>
            <a className="secondary download-link" href={researchRunExportUrl(runDetail.id, exportIncludeBars)}>
              <Download size={16} /> Download Run {runDetail.id}
            </a>
          </div>
          <div className="status-list">
            {(runDetail.bar_snapshots ?? []).map((snapshot) => (
              <div className="status compact-status" key={`${snapshot.market_id}-${snapshot.interval}`}>
                <strong>{snapshot.market_id} · {snapshot.interval}</strong>
                <span>{snapshot.bar_count} exact bars · {String(snapshot.sha256).slice(0, 12)}</span>
              </div>
            ))}
            {(runDetail.bar_snapshots ?? []).length === 0 && <span className="muted">This older run has no exact saved bar snapshot.</span>}
          </div>
        </section>
      )}
      {trials.length > 0 && (
        <section className="lab-section span-2">
          <h3>Run Quality</h3>
          <div className="metrics four">
            <Metric label="Paper-ready" value={qualitySummary.paperReady} />
            <Metric label="Research/watch" value={qualitySummary.researchWatch} />
            <Metric label="Incubator" value={qualitySummary.incubator} />
            <Metric label="Cost fragile" value={qualitySummary.costFragile} />
          </div>
          <div className="status-list">
            {qualitySummary.topWarnings.map((item) => (
              <div className="status compact-status" key={item.warning}>
                <strong>{humanWarnings([item.warning])[0]}</strong>
                <span>{item.count} trials</span>
              </div>
            ))}
            {qualitySummary.topWarnings.length === 0 && <span className="muted">No warnings on saved trials.</span>}
          </div>
        </section>
      )}
      <section className="lab-section span-2">
        <h3>Pareto Picks</h3>
        <div className="pareto-grid">
          {pareto.map((item) => <ParetoCard key={`${item.kind}-${item.strategy_name}`} item={item} onRefineTemplate={onRefineTemplate} />)}
          {pareto.length === 0 && <span className="muted">Run an adaptive search to see balanced, Sharpe, and profit alternatives.</span>}
        </div>
      </section>
      <section className="lab-section span-2">
        <div className="label-row table-heading">
          <h3>Top Trials</h3>
          <div className="segmented compact-filter">
            {[
              ["active", "Active"],
              ["paper", "Paper"],
              ["research", "Research"],
              ["incubator", "Incubator"],
              ["rejected", "Rejected"],
              ["all", "All"],
            ].map(([id, label]) => (
              <button
                className={trialTierFilter === id ? "segment active" : "segment"}
                key={id}
                type="button"
                onClick={() => setTrialTierFilter(id)}
              >
                {label}
              </button>
            ))}
          </div>
          <div className="segmented compact-filter">
            {[
              ["all", "All scans"],
              ["full", "Full-period"],
              ["specialist", "Specialist"],
            ].map(([id, label]) => (
              <button
                className={trialScanFilter === id ? "segment active" : "segment"}
                key={id}
                type="button"
                onClick={() => setTrialScanFilter(id)}
              >
                {label}
              </button>
            ))}
          </div>
          <div className="segmented compact-filter">
            {[
              ["overall", "Best overall"],
              ["market_best", "Best by market"],
              ["regime_best", "Best by regime"],
            ].map(([id, label]) => (
              <button
                className={trialMarketView === id ? "segment active" : "segment"}
                key={id}
                type="button"
                onClick={() => setTrialMarketView(id)}
              >
                {label}
              </button>
            ))}
          </div>
          <div className="segmented compact-filter">
            {[
              ["all", "Any cost"],
              ["healthy", "Cost <25%"],
              ["ok", "Cost <40%"],
              ["survivable", "Cost <65%"],
            ].map(([id, label]) => (
              <button
                className={trialCostFilter === id ? "segment active" : "segment"}
                key={id}
                type="button"
                onClick={() => setTrialCostFilter(id)}
              >
                {label}
              </button>
            ))}
          </div>
        </div>
        <div className="warning-legend">
          <span className="warning-chip blocker">Paper blocker</span>
          <span className="warning-chip repair">Needs repair</span>
          <span className="warning-chip specialist">Specialist identity</span>
          <span className="warning-chip info">Diagnostic</span>
        </div>
        <div className="trial-list">
          {visibleTrials.map((trial) => <TrialCard key={trial.id} trial={trial} onRefineTemplate={onRefineTemplate} onRefineFurther={onRefineFurther} onMakeTradeable={onMakeTradeable} onRepairRemaining={onRepairRemaining} onFreezeValidate={onFreezeValidate} onSaveTemplate={onSaveTemplate} />)}
        </div>
        {filteredTrials.length === 0 && trials.length > 0 && (
          <span className="muted">No trials in this filter. Rejected and fragile trials are still available under Rejected or All.</span>
        )}
        {rankedTrials.length > visibleTrials.length && (
          <span className="muted">Showing top {visibleTrials.length} of {rankedTrials.length} trials in this view.</span>
        )}
      </section>
    </div>
  );
}

function CandidateFactoryView({
  candidates,
  dayFactory,
  runDetail,
  researchRuns,
  enabledMarkets,
  disabledMarkets = [],
  activeMarketIds,
  selectedMarkets,
  researchRun,
  toggleMarket,
  onRunFactory,
  onStageFactory,
  onMakeTradeable,
  onRepairRemaining,
  onFreezeValidate,
  onSaveTemplate,
  onRefresh,
}) {
  const leads = factoryLeads(candidates, runDetail, activeMarketIds);
  const summary = factoryLeadSummary(leads);
  const coverage = factoryCoverageRows(leads);
  const gaps = factoryRegimeGaps(runDetail, leads, activeMarketIds);
  const latestRun = researchRuns[0];
  const unsuitableSignals = dayFactory?.unsuitable ?? [];
  const discoveryLeads = dayFactory?.discovery_leads_not_live ?? [];
  const needsFreezeTemplates = dayFactory?.template_library?.needs_freeze_validation ?? [];
  const dayCounts = dayFactory?.counts ?? {};
  const selectedMarketText = selectedMarkets.length
    ? selectedMarkets.map((market) => market.market_id).join(" / ")
    : "Choose markets";
  const topLeads = leads.slice(0, 8);
  return (
    <div className="lab-grid candidate-factory">
      <section className="lab-section span-2 factory-hero">
        <div>
          <h3><ShieldCheck size={18} /> Repair & Freeze Board</h3>
          <p>
            Turn discovery leads into reusable frozen templates. Daily paper scanning lives in Daily Paper and only uses
            active frozen intraday rules.
          </p>
        </div>
        <div className="button-row">
          <button type="button" className="secondary" onClick={onRefresh}><RefreshCw size={16} /> Refresh template queue</button>
        </div>
      </section>

      <section className="lab-section span-2">
        <h3>Factory Status</h3>
        <div className="metrics four">
          <Metric label="Frozen day templates" value={dayCounts.frozen_day_templates ?? 0} />
          <Metric label="Needs freeze" value={dayCounts.non_frozen_day_templates ?? needsFreezeTemplates.length} />
          <Metric label="Discovery leads" value={dayCounts.discovery_leads_needing_freeze ?? summary.researchLeads} />
          <Metric label="Account blockers" value={unsuitableSignals.length} />
        </div>
        <div className="factory-flow">
          {[
            ["1", "Discover", "Find IG-matched markets that fit the selected account."],
            ["2", "Freeze", "Repair and Freeze validate exact rules before reuse."],
            ["3", "Match", "Daily mode scans only active frozen templates."],
            ["4", "Queue", "Keep 1-3 paper previews and 5-10 review matches."],
            ["5", "Review", "Update evidence after close without silently changing rules."],
          ].map(([step, title, detail]) => (
            <div className="factory-step" key={title}>
              <span>{step}</span>
              <strong>{title}</strong>
              <small>{detail}</small>
            </div>
          ))}
        </div>
        <div className="status-list factory-policy">
          <div className="status compact-status">
            <strong>Daily mode source · frozen templates only</strong>
            <span>Research candidates are discovery leads until they are saved and Freeze validated. Market-open automation can come later.</span>
            <small>Intraday templates force flat before the next session; overnight/swing templates stay separate until funding and gap risk are explicitly proven.</small>
          </div>
        </div>
      </section>

      {(discoveryLeads.length > 0 || needsFreezeTemplates.length > 0) && (
        <section className="lab-section span-2">
          <h3>Discovery Leads Not Allowed To Fire Yet</h3>
          <div className="status-list">
            {needsFreezeTemplates.slice(0, 5).map((template) => (
              <div className="status compact-status" key={`${template.id}-${template.market_id}-needs-freeze`}>
                <strong>{template.market_id} · {template.name}</strong>
                <span>{strategyFamilyLabel(template.strategy_family)} · saved but not frozen</span>
                <small>Use Freeze validate before this can enter the daily paper queue.</small>
              </div>
            ))}
            {discoveryLeads.slice(0, 5).map((lead) => (
              <div className="status compact-status" key={`${lead.id}-${lead.market_id}-discovery`}>
                <strong>{lead.market_id} · {lead.strategy_name}</strong>
                <span>{strategyFamilyLabel(lead.strategy_family)} · {tierLabel(lead.promotion_tier)} · research/discovery only</span>
                <small>Repair, save to Templates, then Freeze validate. Daily mode will not alter or fire this lead directly.</small>
              </div>
            ))}
          </div>
        </section>
      )}

      {unsuitableSignals.length > 0 && (
        <section className="lab-section span-2">
          <h3>Unsuitable For Selected Account</h3>
          <div className="status-list">
            {unsuitableSignals.slice(0, 8).map((lead) => (
              <div className="status compact-status" key={`${lead.id}-${lead.market_id}-unsuitable`}>
                <strong>{lead.market_id} · {lead.strategy_name}</strong>
                <span>{lead.unsuitable_reason || "Capital fit failed for the selected account."}</span>
                <small>Marked unsuitable so the factory can move on.</small>
              </div>
            ))}
          </div>
        </section>
      )}

      <section className="lab-section span-2">
        <div className="label-row table-heading">
          <h3>Markets For Discovery</h3>
          <span className="badge market-badge">{selectedMarketText}</span>
        </div>
        <div className="market-picker">
          {enabledMarkets.map((item) => (
            <button type="button" className={activeMarketIds.includes(item.market_id) ? "market-chip active" : "market-chip"} key={item.market_id} onClick={() => toggleMarket(item.market_id)}>
              <strong>{item.market_id}</strong>
              <span>{item.name} · {normalizeInterval(item.default_timeframe)}</span>
              {marketSubline(item) && <small>{marketSubline(item)}</small>}
            </button>
          ))}
          {disabledMarkets.map((item) => (
            <button type="button" className="market-chip unavailable" key={item.market_id} disabled title={marketAvailabilityNote(item)}>
              <strong>{item.market_id}</strong>
              <span>{item.name} · {normalizeInterval(item.default_timeframe)}</span>
              {marketSubline(item) && <small>{marketSubline(item)}</small>}
              <span>{marketAvailabilityNote(item)}</span>
            </button>
          ))}
        </div>
        {selectedMarkets.length > 3 && (
          <small className="muted">For cleaner candidates, prefer one market at a time. Multi-market factory runs are capped for speed.</small>
        )}
      </section>

      <section className="lab-section span-2">
        <h3>Run Discovery Scans</h3>
        <div className="factory-mode-grid">
          {CANDIDATE_FACTORY_MODES.map((mode) => {
            const plan = candidateFactoryPlan(mode.id, researchRun, enabledMarkets, activeMarketIds);
            return (
              <div className="factory-mode-card" key={mode.id}>
                <div className="label-row">
                  <strong>{mode.label}</strong>
                  <span className="badge base">{mode.badge}</span>
                </div>
                <span>{mode.detail}</span>
                <div className="mini-metrics">
                  <Metric label="Preset" value={mode.preset} />
                  <Metric label="Budget" value={mode.budgetLabel} />
                  <Metric label="Markets" value={plan.marketIds.join(" / ") || "none"} />
                  <Metric label="Start" value={plan.runPatch.start ?? researchRun.start} />
                </div>
                <div className="button-row">
                  <button type="button" className="secondary" onClick={() => onRunFactory(mode.id)} disabled={Boolean(plan.stopReason)}>
                    <Sparkles size={16} /> Run
                  </button>
                  <button type="button" className="ghost" onClick={() => onStageFactory(mode.id)} disabled={Boolean(plan.stopReason)}>
                    Stage
                  </button>
                </div>
                {plan.stopReason && <small className="muted">{plan.stopReason}</small>}
              </div>
            );
          })}
        </div>
      </section>

      <section className="lab-section">
        <h3>Coverage Board</h3>
        <div className="status-list">
          {coverage.slice(0, 10).map((row) => (
            <div className="status compact-status" key={`${row.marketId}-${row.regime}`}>
              <strong>{row.marketId} · {regimeLabel(row.regime)}</strong>
              <span>{row.count} lead{row.count === 1 ? "" : "s"} · best {round(row.bestScore)} · {tierLabel(row.bestTier)}</span>
              <small>OOS {formatMoney(row.bestOos)} · trades {row.bestTrades} · cost/gross {percent(row.bestCostToGross)}</small>
            </div>
          ))}
          {coverage.length === 0 && <span className="muted">No market/regime leads yet. Run a discovery scan first.</span>}
        </div>
      </section>

      <section className="lab-section">
        <h3>Gaps To Hunt</h3>
        <div className="status-list">
          {gaps.slice(0, 10).map((gap) => (
            <div className="status compact-status" key={`${gap.marketId}-${gap.regime}`}>
              <strong>{gap.marketId} · {regimeLabel(gap.regime)}</strong>
              <span>{gap.tradingDays} regime days · no saved lead yet</span>
              <small>Run a discovery scan or deep one-market scan to search this specialist slot.</small>
            </div>
          ))}
          {gaps.length === 0 && (
            <span className="muted">
              {runDetail ? "No obvious regime gaps in the loaded run." : "Load a run in Results to see eligible regime gaps."}
            </span>
          )}
        </div>
      </section>

      <section className="lab-section span-2">
        <div className="label-row table-heading">
          <h3>Best Discovery Leads To Repair Or Freeze</h3>
          {latestRun && <span className="badge muted-badge">Latest run {latestRun.id} · {latestRun.status}</span>}
        </div>
        <div className="factory-lead-grid">
          {topLeads.map((lead) => (
            <FactoryLeadCard
              key={lead.key}
              lead={lead}
              onMakeTradeable={onMakeTradeable}
              onRepairRemaining={onRepairRemaining}
              onFreezeValidate={onFreezeValidate}
              onSaveTemplate={onSaveTemplate}
            />
          ))}
          {topLeads.length === 0 && <span className="muted">No leads yet. Start with a discovery scan on one market, then inspect the repair list.</span>}
        </div>
      </section>
    </div>
  );
}

function DayTradingQueueCard({ lead }) {
  return (
    <article className="factory-lead-card day-queue-card">
      <div className="label-row">
        <strong>{lead.strategy_name}</strong>
        <div className="badge-group">
          <span className="badge market-badge">{lead.market_id}</span>
          <span className="badge good">Paper preview</span>
        </div>
      </div>
      <span>{[normalizeInterval(lead.interval), strategyFamilyLabel(lead.strategy_family), lead.target_regime ? `${regimeLabel(lead.target_regime)} only` : ""].filter(Boolean).join(" · ")}</span>
      <div className="mini-metrics">
        <Metric label="Paper score" value={round(lead.paper_readiness_score)} />
        <Metric label="Side" value={lead.side ?? "-"} />
        <Metric label="Regime" value={regimeLabel(lead.current_regime)} />
        <Metric label="Setup" value={readableSnake(lead.signal_state)} />
        <Metric label="Playbook" value={shortPlaybookLabel(lead.manual_playbook) || "-"} />
        <Metric label="RVol" value={formatRatio(lead.today_tape?.relative_volume ?? 0)} />
        <Metric label="VWAP" value={`${round(lead.today_tape?.distance_from_vwap_bps ?? 0)} bps`} />
        <Metric label="Manual score" value={round(lead.manual_setup_score)} />
        <Metric label="OOS" value={formatMoney(lead.oos_net_profit)} />
        <Metric label="Order mode" value="Preview" />
      </div>
      {lead.signal_explainer?.headline && <small>{lead.signal_explainer.headline}</small>}
      <div className="warning-row">
        <WarningChips warnings={lead.warnings} limit={5} empty="Gate clear" />
      </div>
    </article>
  );
}

function FactoryLeadCard({ lead, onMakeTradeable, onRepairRemaining, onFreezeValidate, onSaveTemplate }) {
  const showRepairRemaining = shouldShowRepairRemaining(lead.source);
  const showFreezeValidate = shouldShowFreezeValidate(lead.source);
  return (
    <article className="factory-lead-card">
      <div className="label-row">
        <strong>{lead.name}</strong>
        <div className="badge-group">
          <span className="badge market-badge">{lead.marketId || "Market"}</span>
          {lead.regime && <span className="badge base">{regimeLabel(lead.regime)}</span>}
          <span className={`badge ${tierBadgeClass(lead.tier)}`}>{tierLabel(lead.tier)}</span>
        </div>
      </div>
      <span>{[lead.interval, strategyFamilyLabel(lead.family), `score ${round(lead.score)}`].filter(Boolean).join(" · ")}</span>
      <div className="mini-metrics">
        <Metric label="Net" value={formatMoney(lead.net)} />
        <Metric label="OOS" value={formatMoney(lead.oos)} />
        <Metric label="Trades" value={lead.trades} />
        <Metric label="Cost/gross" value={percent(lead.costToGross)} />
        <Metric label={`${accountSizeLabel(lead.accountSize)} fit`} value={lead.capitalFit} />
      </div>
      <div className="warning-row">
        <WarningChips warnings={lead.warnings} limit={5} empty="Gate clear" />
      </div>
      <div className="button-row compact-actions">
        {showRepairRemaining ? (
          <button type="button" className="secondary" onClick={() => onRepairRemaining(lead.source)}>
            <ShieldCheck size={16} /> Repair
          </button>
        ) : (
          <button type="button" className="secondary" onClick={() => onMakeTradeable(lead.source)}>
            <ShieldCheck size={16} /> Make tradeable
          </button>
        )}
        {showFreezeValidate && (
          <button type="button" className="ghost" onClick={() => onFreezeValidate(lead.source)}>
            <LockKeyhole size={16} /> Freeze
          </button>
        )}
        <button type="button" className="ghost" onClick={() => onSaveTemplate(lead.source)}>
          <Save size={16} /> Save
        </button>
      </div>
    </article>
  );
}

function RegimeEvidence({ runDetail, trials }) {
  const statuses = runDetail?.config?.market_statuses ?? [];
  const specialistCount = trials.filter((trial) => trial.parameters?.regime_scan).length;
  const topRegimeTrials = trials
    .filter((trial) => trial.parameters?.bar_pattern_analysis?.regime_gated_backtest)
    .slice(0, 4);
  return (
    <section className="lab-section span-2">
      <h3>Regime Evidence</h3>
      <div className="metrics four">
        <Metric label="Template scans" value={runDetail?.config?.include_regime_scans ? "On" : "Off"} />
        <Metric label="Specialist trials" value={specialistCount} />
        <Metric label="Eligible regimes" value={statuses.flatMap((item) => item.eligible_regimes ?? []).length} />
        <Metric label="Scan trials saved" value={statuses.reduce((total, item) => total + Number(item.regime_scan_trial_count ?? 0), 0)} />
      </div>
      <div className="status-list">
        {statuses.map((item) => (
          <div className="status compact-status" key={`${item.market_id}-regime`}>
            <strong>{item.market_id} · current {regimeLabel(item.bar_regime?.current_regime)}</strong>
            <span>{regimeCountsLabel(item.bar_regime?.regime_counts)}</span>
            {(item.eligible_regimes ?? []).length > 0 && (
              <small>{item.eligible_regimes.map((regime) => `${regimeLabel(regime.regime)} ${regime.trading_days}d`).join(" · ")}</small>
            )}
          </div>
        ))}
        {topRegimeTrials.map((trial) => {
          const pattern = trial.parameters?.bar_pattern_analysis ?? {};
          const gated = pattern.regime_gated_backtest ?? {};
          return (
            <div className="status compact-status" key={`${trial.id}-gated`}>
              <strong>{trial.strategy_name} · {regimeVerdictLabel(pattern.regime_verdict)}</strong>
              <span>
                Full {formatMoney(trial.backtest?.net_profit)} / gated {formatMoney(gated.net_profit)} / OOS {formatMoney(gated.test_profit)}
              </span>
              <small>Allowed {(pattern.allowed_regimes ?? []).map(regimeLabel).join(" / ") || "none"} · worst {regimeLabel(pattern.worst_regime?.key)}</small>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function RegimeTemplateShortlist({ runDetail, onMakeTradeable, onRepairRemaining, onFreezeValidate, onSaveTemplate }) {
  const groups = runDetail?.regime_picks ?? [];
  if (groups.length === 0) {
    return null;
  }
  return (
    <section className="lab-section span-2">
      <h3>Regime Template Shortlist</h3>
      <div className="regime-template-grid">
        {groups.map((group) => (
          <div className="regime-template-card" key={group.regime}>
            <span className="eyebrow">{regimeLabel(group.regime)}</span>
            <strong>{group.trials?.[0]?.market_id || runDetail.market_id || "Market"}</strong>
            <div className="status-list compact-regime-list">
              {(group.trials ?? []).map((trial) => (
                <div className="status regime-pick-card" key={trial.id}>
                  <strong>{trial.strategy_name}</strong>
                  <span>
                    {tierLabel(trial.promotion_tier)} · net {formatMoney(trial.net_profit)} · OOS {formatMoney(trial.oos_net_profit ?? trial.test_profit)}
                  </span>
                  <small>
                    In-regime {formatMoney(trial.in_regime_net_profit)} · {trial.regime_trading_days ?? 0}d · cost/gross {percent(trial.cost_to_gross_ratio)}
                  </small>
                  <div className="button-row compact-actions">
                    {shouldShowFreezeValidate(trial) && (
                      <button type="button" className="ghost" onClick={() => onFreezeValidate(trial)}>
                        <LockKeyhole size={16} /> Freeze
                      </button>
                    )}
                    {shouldShowRepairRemaining(trial) ? (
                      <button type="button" className="secondary" onClick={() => onRepairRemaining(trial)}>
                        <ShieldCheck size={16} /> Repair remaining
                      </button>
                    ) : (
                      <button type="button" className="secondary" onClick={() => onMakeTradeable(trial)}>
                        <ShieldCheck size={16} /> Make tradeable
                      </button>
                    )}
                    <button type="button" className="ghost" onClick={() => onSaveTemplate(trial)}>
                      <Save size={16} /> Save
                    </button>
                  </div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function RegimeEvidenceMetrics({ pattern }) {
  const evidence = pattern?.regime_trade_evidence ?? {};
  if (!evidence.available) {
    return null;
  }
  const inRegime = evidence.in_regime ?? {};
  return (
    <>
      <Metric label={evidence.is_targeted ? "Target regime" : "Edge regime"} value={regimeLabel(evidence.target_regime)} />
      <Metric label="In-regime net" value={formatMoney(inRegime.net_profit)} />
      <Metric label="In-regime OOS" value={formatMoney(inRegime.test_profit)} />
      <Metric label="In-regime OOS trades" value={inRegime.test_trade_count ?? 0} />
      <Metric label="In-regime Sharpe" value={round(inRegime.daily_pnl_sharpe)} />
      <Metric label="Regime days" value={`${evidence.regime_trading_days ?? 0} (${percent(evidence.regime_history_share)})`} />
      <Metric label="Regime episodes" value={evidence.regime_episodes ?? 0} />
      <Metric label="Outside trades" value={evidence.outside_trade_count ?? 0} />
    </>
  );
}

function CalendarEvidenceMetrics({ analysis }) {
  if (!analysis?.available) {
    return null;
  }
  const eventDay = analysis.event_day_summary ?? {};
  const eventWindow = analysis.event_window_summary ?? {};
  const normalDay = analysis.normal_day_summary ?? {};
  const avoidWindow = (analysis.policy_backtests ?? []).find((item) => item.policy === "avoid_event_window") ?? {};
  return (
    <>
      <Metric label="Calendar policy" value={calendarPolicyLabel(analysis.recommended_policy)} />
      <Metric label="Event-day net" value={formatMoney(eventDay.net_profit)} />
      <Metric label="Event-window net" value={formatMoney(eventWindow.net_profit)} />
      <Metric label="Normal-day net" value={formatMoney(normalDay.net_profit)} />
      <Metric label="Event trades" value={eventWindow.trade_count ?? 0} />
      <Metric label="Avoid-window net" value={formatMoney(avoidWindow.net_profit)} />
    </>
  );
}

function TemplateUtilizationMetrics({ backtest, pattern }) {
  const profile = templateUtilizationProfile(backtest, pattern);
  if (!profile.available) {
    return null;
  }
  const activeLabel = profile.mode === "target" ? "Target active days" : profile.mode === "edge" ? "Edge active days" : "Active days";
  return (
    <>
      <Metric label={activeLabel} value={profile.activeDays} />
      <Metric label="Idle days" value={profile.idleDays} />
      <Metric label="Capital use" value={percent(profile.activeShare)} />
      <Metric label="Net/active day" value={formatMoney(profile.netPerActiveDay)} />
    </>
  );
}

function TrialCard({ trial, onRefineTemplate, onRefineFurther, onMakeTradeable, onRepairRemaining, onFreezeValidate, onSaveTemplate }) {
  const backtest = trial.backtest ?? {};
  const accountSize = testingAccountSizeForSource(trial);
  const accountLabel = accountSizeLabel(accountSize);
  const capital = accountFeasibility(trial.capital_scenarios, accountSize);
  const accountScenario = accountScenarioFor(trial.capital_scenarios, accountSize);
  const pattern = trial.parameters?.bar_pattern_analysis ?? {};
  const calendarAnalysis = trial.parameters?.calendar_context_analysis ?? {};
  const gated = pattern.regime_gated_backtest ?? {};
  const evidence = evidenceProfileForSource(trial);
  const marketId = trialMarketId(trial);
  const interval = trialIntervalLabel(trial);
  const searchAudit = trial.parameters?.search_audit ?? {};
  const scoreBasis = scoreBasisLabel(searchAudit);
  const discoveryLabel = discoveryBadgeLabel(trial.parameters?.search_audit);
  const showRepairRemaining = shouldShowRepairRemaining(trial);
  const showFreezeValidate = shouldShowFreezeValidate(trial);
  return (
    <article className="trial-card">
      <div className="trial-summary">
        <div>
          <div className="label-row">
            <strong>{trial.strategy_name}</strong>
            <div className="badge-group">
              {marketId && <span className="badge market-badge">{marketId}</span>}
              {discoveryLabel && <span className="badge muted-badge">{discoveryLabel}</span>}
              <span className={`badge ${tierBadgeClass(trial.promotion_tier)}`}>{tierLabel(trial.promotion_tier)}</span>
            </div>
          </div>
          <span>{[interval, strategyFamilyLabel(trial.strategy_family || trial.style), `score ${round(trial.robustness_score)}`, scoreBasis].filter(Boolean).join(" · ")}</span>
        </div>
        <div className="button-row trial-actions">
          {showRepairRemaining ? (
            <button type="button" className="secondary" onClick={() => onRepairRemaining(trial)}>
              <ShieldCheck size={16} /> Repair remaining
            </button>
          ) : (
            <button type="button" className="secondary" onClick={() => onMakeTradeable(trial)}>
              <ShieldCheck size={16} /> Make tradeable
            </button>
          )}
          <button type="button" className="ghost" onClick={() => onRefineTemplate(trial)}>
            <RefreshCw size={16} /> Refine
          </button>
          {showFreezeValidate && (
            <button type="button" className="ghost" onClick={() => onFreezeValidate(trial)}>
              <LockKeyhole size={16} /> Freeze validate
            </button>
          )}
          <button type="button" className="ghost" onClick={() => onSaveTemplate(trial)}>
            <Save size={16} /> Save
          </button>
        </div>
        <div className="trial-net">
          <small>Net</small>
          <strong>{formatMoney(backtest.net_profit)}</strong>
        </div>
      </div>
      <div className="trial-metrics">
        <Metric label="Paper score" value={round(searchAudit.paper_readiness_score)} />
        <Metric label={`${accountLabel} score`} value={percent(searchAudit.working_capital_score)} />
        <Metric label={`${accountLabel} fit`} value={capital} />
        <Metric label="End balance" value={formatMoney(accountScenario?.projected_final_balance)} />
        <Metric label="Return" value={`${round(accountScenario?.projected_return_pct)}%`} />
        <TemplateUtilizationMetrics backtest={backtest} pattern={pattern} />
        <Metric label="Daily Sharpe" value={round(backtest.daily_pnl_sharpe)} />
        <Metric label="Days" value={backtest.sharpe_observations ?? 0} />
        <Metric label="DSR" value={percent(trial.parameters?.sharpe_diagnostics?.deflated_sharpe_probability)} />
        <Metric label="Drawdown" value={formatMoney(backtest.max_drawdown)} />
        <Metric label="Costs" value={formatMoney(backtest.total_cost)} />
        <Metric label="Expectancy" value={formatMoney(backtest.expectancy_per_trade)} />
        <Metric label="Net/cost" value={formatRatio(backtest.net_cost_ratio)} />
        <Metric label={gatedMetricLabel(pattern, "Gated net", "Target net")} value={formatMoney(gated.net_profit)} />
        <Metric label={gatedMetricLabel(pattern, "Gated OOS", "Target OOS")} value={formatMoney(gated.test_profit)} />
        <RegimeEvidenceMetrics pattern={pattern} />
        <CalendarEvidenceMetrics analysis={calendarAnalysis} />
        <Metric label="Fold win" value={percent(evidence.positive_fold_rate)} />
        <Metric label="Active folds" value={`${evidence.active_fold_count ?? 0}/${evidence.fold_count ?? 0}`} />
        <Metric label="Fold share" value={percent(evidence.single_fold_profit_share)} />
        <Metric label="OOS net" value={formatMoney(evidence.oos_net_profit)} />
        <Metric label="OOS trades" value={evidence.oos_trade_count ?? 0} />
        <Metric label="Regime verdict" value={regimeVerdictLabel(pattern.regime_verdict)} />
        <Metric label="Cost/gross" value={percent(backtest.cost_to_gross_ratio)} />
        <Metric label="Best regime" value={regimeLabel(pattern.dominant_profit_regime?.key)} />
        <Metric label="Worst regime" value={regimeLabel(pattern.worst_regime?.key)} />
        <Metric label="Best month" value={pattern.dominant_profit_month?.key ?? "n/a"} />
        <Metric label="Spread/slip" value={`${round(backtest.estimated_spread_bps)} / ${round(backtest.estimated_slippage_bps)} bps`} />
        <Metric label="Trades" value={backtest.trade_count ?? 0} />
      </div>
      <div className="warning-row">
        <WarningChips warnings={trial.warnings} limit={8} empty="Clear" />
      </div>
    </article>
  );
}

function ParetoCard({ item, onRefineTemplate }) {
  const recipe = researchRecipeLabel(item.settings?.research_recipe);
  return (
    <div className="pareto-card">
      <span className="eyebrow">{labelForKind(item.kind)}</span>
      <strong>{item.strategy_name}</strong>
      {recipe && <small>{recipe}</small>}
      <div className="mini-metrics">
        <Metric label="Score" value={round(item.robustness_score)} />
        <Metric label="Daily Sharpe (ann.)" value={round(item.daily_pnl_sharpe)} />
        <Metric label="Sharpe days" value={item.sharpe_observations ?? 0} />
        <Metric label="Bar Sharpe" value={round(item.sharpe)} />
        <Metric label="DSR" value={percent(item.deflated_sharpe_probability)} />
        <Metric label="Net" value={formatMoney(item.net_profit)} />
        <Metric label="Cost" value={formatMoney(item.total_cost)} />
        <Metric label="Expectancy" value={formatMoney(item.expectancy_per_trade)} />
        <Metric label="Net/cost" value={formatRatio(item.net_cost_ratio)} />
        <Metric label="Est spread/slip" value={`${round(item.estimated_spread_bps)} / ${round(item.estimated_slippage_bps)} bps`} />
      </div>
      <small>{humanWarnings(item.warnings).join(" · ") || "Ready for research review"}</small>
      <button type="button" className="ghost" onClick={() => onRefineTemplate(item)}>
        <RefreshCw size={16} /> Refine
      </button>
    </div>
  );
}

function CandidateView({ candidates, critique, onRefineTemplate, onRefineFurther, onMakeTradeable, onRepairRemaining, onFreezeValidate, onSaveTemplate }) {
  const visibleCandidates = candidates.slice(0, 24);
  const queue = candidateQueueSummary(candidates);
  return (
    <div className="lab-grid">
      <section className="lab-section span-2">
        <h3>Strategy Testing Queue</h3>
        <div className="metrics four">
          <Metric label="Blocked" value={queue.blocked} />
          <Metric label="Needs fresh run" value={queue.needsFreshRun} />
          <Metric label="Needs IG validation" value={queue.needsIgValidation} />
          <Metric label="Paper-ready" value={queue.paperReady} />
        </div>
        <div className="status-list">
          <div className="status compact-status">
            <strong>1 · Clear noisy history</strong>
            <span>Delete stale runs before comparing candidates.</span>
          </div>
          <div className="status compact-status">
            <strong>2 · Test known ideas</strong>
            <span>Use profit-first known research ideas after realistic IG/EODHD costs.</span>
          </div>
          <div className="status compact-status">
            <strong>3 · Gate promotion</strong>
            <span>Require fresh Sharpe days, spread/slippage, validation warnings, and no stale blockers.</span>
          </div>
          <div className="status compact-status">
            <strong>4 · Paper track</strong>
            <span>Only clear candidates move to 30-day live-paper review.</span>
          </div>
        </div>
      </section>
      <section className="lab-section span-2">
        <h3>Research Candidates</h3>
        <div className="candidate-grid">
          {visibleCandidates.map((candidate) => <CandidateCard candidate={candidate} key={candidate.id} onRefineTemplate={onRefineTemplate} onRefineFurther={onRefineFurther} onMakeTradeable={onMakeTradeable} onRepairRemaining={onRepairRemaining} onFreezeValidate={onFreezeValidate} onSaveTemplate={onSaveTemplate} />)}
          {candidates.length === 0 && <span className="muted">No saved research leads yet. Strong but flawed trials appear here with warnings.</span>}
          {candidates.length > visibleCandidates.length && (
            <span className="muted">Showing {visibleCandidates.length} of {candidates.length} candidates.</span>
          )}
        </div>
      </section>
      <section className="lab-section span-2">
        <h3>Research Critic</h3>
        {critique ? (
          <>
            <div className="metrics four">
              <Metric label="Decision" value={critique.decision} />
              <Metric label="Confidence" value={critique.confidence_score} />
              <Metric label="Trials" value={critique.trial_count} />
              <Metric label="Candidates" value={critique.candidate_count} />
            </div>
            <div className="status-list">
              {critique.findings.slice(0, 5).map((finding) => (
                <div className="status" key={`${finding.code}-${finding.message}`}>
                  <strong>{finding.severity.toUpperCase()} · {finding.code}</strong>
                  <span>{finding.message}</span>
                </div>
              ))}
            </div>
          </>
        ) : <span className="muted">No critique available yet.</span>}
      </section>
    </div>
  );
}

function CandidateCard({ candidate, onRefineTemplate, onRefineFurther, onMakeTradeable, onRepairRemaining, onFreezeValidate, onSaveTemplate }) {
  const readiness = candidateReadiness(candidate);
  const issues = readinessIssueCodes(readiness);
  const accountSize = testingAccountSizeForSource(candidate);
  const accountLabel = accountSizeLabel(accountSize);
  const capital = accountFeasibility(candidate.capital_scenarios, accountSize);
  const accountScenario = accountScenarioFor(candidate.capital_scenarios, accountSize);
  const pattern = candidate.audit?.candidate?.parameters?.bar_pattern_analysis ?? {};
  const calendarAnalysis = candidate.audit?.candidate?.parameters?.calendar_context_analysis ?? {};
  const gated = pattern.regime_gated_backtest ?? {};
  const evidence = evidenceProfileForSource(candidate);
  const searchAudit = candidate.audit?.candidate?.parameters?.search_audit ?? {};
  const scoreBasis = scoreBasisLabel(searchAudit);
  const discoveryLabel = discoveryBadgeLabel(candidate.audit?.candidate?.parameters?.search_audit);
  const showRepairRemaining = shouldShowRepairRemaining(candidate);
  const showFreezeValidate = shouldShowFreezeValidate(candidate);
  return (
    <div className="candidate-card">
      <div className="label-row">
        <span className="badge muted-badge">Research only</span>
        {discoveryLabel && <span className="badge muted-badge">{discoveryLabel}</span>}
        <span className={`badge ${tierBadgeClass(candidate.promotion_tier || candidate.audit?.promotion_tier)}`}>
          {tierLabel(candidate.promotion_tier || candidate.audit?.promotion_tier)}
        </span>
      </div>
      <div className="label-row">
        <strong>{candidate.strategy_name}</strong>
        <span className={`badge ${readinessBadgeClass(readiness.status)}`}>{readinessLabel(readiness.status)}</span>
      </div>
      <span>{[candidate.market_id, `score ${round(candidate.robustness_score)}`, scoreBasis].filter(Boolean).join(" · ")}</span>
      {researchRecipeLabel(candidate.audit?.candidate?.parameters?.research_recipe) && (
        <small>{researchRecipeLabel(candidate.audit?.candidate?.parameters?.research_recipe)}</small>
      )}
      <small>{nextActionLabel(readiness.next_action)}</small>
      <div className="warning-row">
        <WarningChips warnings={issues} limit={8} empty="Gate clear" />
      </div>
      <div className="button-row">
        {showRepairRemaining ? (
          <button type="button" className="secondary" onClick={() => onRepairRemaining(candidate)}>
            <ShieldCheck size={16} /> Repair remaining
          </button>
        ) : (
          <button type="button" className="secondary" onClick={() => onMakeTradeable(candidate)}>
            <ShieldCheck size={16} /> Make tradeable
          </button>
        )}
        <button type="button" className="ghost" onClick={() => onRefineTemplate(candidate)}>
          <RefreshCw size={16} /> Refine
        </button>
        {showFreezeValidate && (
          <button type="button" className="ghost" onClick={() => onFreezeValidate(candidate)}>
            <LockKeyhole size={16} /> Freeze validate
          </button>
        )}
        <button type="button" className="ghost" onClick={() => onSaveTemplate(candidate)}>
          <Save size={16} /> Save
        </button>
      </div>
      <div className="mini-metrics">
        <Metric label="Paper score" value={round(searchAudit.paper_readiness_score)} />
        <Metric label={`${accountLabel} score`} value={percent(searchAudit.working_capital_score)} />
        <Metric label={`${accountLabel} fit`} value={capital} />
        <Metric label="End balance" value={formatMoney(accountScenario?.projected_final_balance)} />
        <Metric label="Return" value={`${round(accountScenario?.projected_return_pct)}%`} />
        <TemplateUtilizationMetrics backtest={candidate.audit?.backtest} pattern={pattern} />
        <Metric label="Daily Sharpe (ann.)" value={round(candidate.audit?.backtest?.daily_pnl_sharpe)} />
        <Metric label="Sharpe days" value={candidate.audit?.backtest?.sharpe_observations ?? 0} />
        <Metric label="Bar Sharpe" value={round(candidate.audit?.backtest?.sharpe)} />
        <Metric label="DSR" value={percent(candidate.audit?.candidate?.parameters?.sharpe_diagnostics?.deflated_sharpe_probability)} />
        <Metric label="Stability" value={percent(candidate.audit?.candidate?.parameters?.parameter_stability_score)} />
        <Metric label="Net" value={formatMoney(candidate.audit?.backtest?.net_profit)} />
        <Metric label="Costs" value={formatMoney(candidate.audit?.backtest?.total_cost)} />
        <Metric label="Expectancy" value={formatMoney(candidate.audit?.backtest?.expectancy_per_trade)} />
        <Metric label="Net/cost" value={formatRatio(candidate.audit?.backtest?.net_cost_ratio)} />
        <Metric label={gatedMetricLabel(pattern, "Gated net", "Target net")} value={formatMoney(gated.net_profit)} />
        <Metric label={gatedMetricLabel(pattern, "Gated OOS", "Target OOS")} value={formatMoney(gated.test_profit)} />
        <RegimeEvidenceMetrics pattern={pattern} />
        <CalendarEvidenceMetrics analysis={calendarAnalysis} />
        <Metric label="Fold win" value={percent(evidence.positive_fold_rate)} />
        <Metric label="Active folds" value={`${evidence.active_fold_count ?? 0}/${evidence.fold_count ?? 0}`} />
        <Metric label="Fold share" value={percent(evidence.single_fold_profit_share)} />
        <Metric label="OOS net" value={formatMoney(evidence.oos_net_profit)} />
        <Metric label="OOS trades" value={evidence.oos_trade_count ?? 0} />
        <Metric label="Regime verdict" value={regimeVerdictLabel(pattern.regime_verdict)} />
        <Metric label="Best regime" value={regimeLabel(pattern.dominant_profit_regime?.key)} />
        <Metric label="Worst regime" value={regimeLabel(pattern.worst_regime?.key)} />
        <Metric label="Best month" value={pattern.dominant_profit_month?.key ?? "n/a"} />
        <Metric label="Spread/slip" value={`${round(candidate.audit?.backtest?.estimated_spread_bps)} / ${round(candidate.audit?.backtest?.estimated_slippage_bps)} bps`} />
        <Metric label="Trades" value={candidate.audit?.backtest?.trade_count ?? 0} />
      </div>
    </div>
  );
}

function MidcapDiscoveryPanel({ search, setSearch, result, loading, templateDesigns = [], pipeline, pipelineState, onSearch, onInstall, onRunPipeline }) {
  const candidates = result?.candidates ?? [];
  const compatibleDesigns = templateDesigns.filter((design) => designMatchesCountry(design, search.country));
  const designOptions = compatibleDesigns.length > 0 ? compatibleDesigns : templateDesigns;
  const selectedDesign = designOptions.find((design) => design.id === search.design_id) ?? designOptions[0];
  const sourceIssues = result?.source_errors ?? pipeline?.discovery?.source_errors ?? [];
  const dayTradingPreflight = pipeline?.day_trading_preflight;
  const preflightStatus = dayTradingPreflight?.status === "completed"
    ? `${dayTradingPreflight.passed_count ?? 0}/${dayTradingPreflight.checked_count ?? 0}${dayTradingPreflight.fallback_used ? " fallback" : ""}`
    : "skipped";
  const preflightBlockedPreview = dayTradingPreflight?.blocked_preview ?? [];
  React.useEffect(() => {
    if (!selectedDesign?.id || search.design_id === selectedDesign.id) return;
    setSearch((current) => (
      current.design_id === selectedDesign.id
        ? current
        : { ...current, design_id: selectedDesign.id }
    ));
  }, [selectedDesign?.id, search.design_id, setSearch]);
  const updateCountry = (country) => {
    const currentDesign = templateDesigns.find((design) => design.id === search.design_id);
    const nextDesign = currentDesign && designMatchesCountry(currentDesign, country)
      ? currentDesign
      : templateDesigns.find((design) => designMatchesCountry(design, country));
    setSearch({ ...search, country, design_id: nextDesign?.id ?? search.design_id });
  };
  return (
    <Panel icon={<Search />} title="Eligible Midcap Finder">
      <form className="compact midcap-search" onSubmit={onSearch}>
        <label>Market universe</label>
        <select value={search.country} onChange={(event) => updateCountry(event.target.value)}>
          <option value="UK">UK shares</option>
          <option value="US">US shares</option>
        </select>
        {designOptions.length > 0 && (
          <>
            <label>Template design</label>
            <select
              value={selectedDesign?.id ?? ""}
              onChange={(event) => {
                setSearch({ ...search, design_id: event.target.value });
              }}
            >
              {designOptions.map((design) => (
                <option value={design.id} key={design.id}>{design.label}</option>
              ))}
            </select>
          </>
        )}
        <label>Account role</label>
        <select value={search.product_mode} onChange={(event) => setSearch({ ...search, product_mode: event.target.value })}>
          <option value="spread_bet">Spread bet</option>
          <option value="cfd">CFD account</option>
        </select>
        <label>Account size (£)</label>
        <input value={search.account_size} onChange={(event) => setSearch({ ...search, account_size: event.target.value })} type="number" min="100" step="50" />
        <label>Max spread bps</label>
        <input value={search.max_spread_bps} onChange={(event) => setSearch({ ...search, max_spread_bps: event.target.value })} type="number" min="1" step="1" />
        <label>Min market cap</label>
        <input value={search.min_market_cap} onChange={(event) => setSearch({ ...search, min_market_cap: event.target.value })} type="number" min="0" step="10000000" />
        <label>Max market cap</label>
        <input value={search.max_market_cap} onChange={(event) => setSearch({ ...search, max_market_cap: event.target.value })} type="number" min="0" step="10000000" />
        <label>Min volume</label>
        <input value={search.min_volume} onChange={(event) => setSearch({ ...search, min_volume: event.target.value })} type="number" min="0" step="10000" />
        <label>Limit</label>
        <input value={search.limit} onChange={(event) => setSearch({ ...search, limit: event.target.value })} type="number" min="1" max="120" step="1" />
        <label>Run markets</label>
        <input value={search.max_markets} onChange={(event) => setSearch({ ...search, max_markets: event.target.value })} type="number" min="1" max="3" step="1" />
        <label className="check compact-check">
          <input type="checkbox" checked readOnly />
          Guided build defers IG validation
        </label>
        <div className="button-row midcap-actions">
          <button type="button" onClick={onRunPipeline} disabled={pipelineState?.status === "running"}>
            <ShieldCheck size={16} /> {pipelineState?.status === "running" ? "Building..." : "Start guided pipeline"}
          </button>
          <button type="submit" className="ghost" disabled={loading || pipelineState?.status === "running"}>
            <Search size={16} /> {loading ? "Previewing..." : "Preview candidates"}
          </button>
        </div>
      </form>
      {selectedDesign && (
        <div className="status compact-status">
          <strong>{selectedDesign.label}</strong>
          <span>{selectedDesign.behaviour}</span>
          <small>{(selectedDesign.strategy_families ?? []).map(strategyFamilyLabel).join(" / ")} · {normalizeInterval(selectedDesign.run_defaults?.interval ?? "5min")} · {selectedDesign.run_defaults?.search_budget ?? 36} pilot trials/market · no overnight · research first, IG-validate finalists</small>
        </div>
      )}
      {pipelineState?.detail && (
        <div className="status compact-status">
          <strong>Guided template build · {pipelineState.status}</strong>
          <span>{pipelineState.detail}</span>
          {pipeline?.research_run_id && <small>Research run {pipeline.research_run_id} · Auto Freeze saves one exact template and starts frozen validation after the run finishes.</small>}
        </div>
      )}
      {pipeline?.promotion_pipeline?.length > 0 && (
        <div className="pipeline-steps">
          {pipeline.promotion_pipeline.map((step) => (
            <div className="pipeline-step" key={step.step}>
              <div>
                <strong>{readableSnake(step.step)}</strong>
                <span>{readableSnake(step.status)}</span>
              </div>
              <small>{step.detail}</small>
            </div>
          ))}
        </div>
      )}
      {pipeline?.selected_markets?.length > 0 && (
        <div className="discovery-summary">
          <Metric label="Run ID" value={pipeline.research_run_id ?? "-"} />
          <Metric label="Shortlist" value={pipeline.selected_markets.length} />
          <Metric label="IG-ready" value={pipeline.run_ready_market_ids?.length ?? 0} />
          <Metric label="Mode" value={readableSnake(pipeline.broker_validation_mode ?? "research_first")} />
          <Metric label="Preflight" value={preflightStatus} />
          <Metric label="Universe" value={pipeline.discovery?.candidate_count ?? "-"} />
          <Metric label="Cost sync" value={pipeline.cost_sync?.status ?? "n/a"} />
        </div>
      )}
      {dayTradingPreflight?.status === "completed" && (
        <div className="status compact-status">
          <strong>Day-trading preflight</strong>
          <span>
            {dayTradingPreflight.passed_count ?? 0} of {dayTradingPreflight.checked_count ?? 0} candidates had enough recent action and movement versus stressed costs.
          </span>
          <small>{dayTradingPreflight.policy}</small>
          {preflightBlockedPreview.length > 0 && (
            <div className="warning-row">
              <WarningChips warnings={preflightBlockedPreview.flatMap((item) => item.blockers ?? [])} limit={6} />
            </div>
          )}
        </div>
      )}
      {result && (
        <div className="discovery-summary">
          <Metric label="Universe" value={result.candidate_count ?? candidates.length} />
          <Metric label="Eligible" value={result.eligible_count ?? 0} />
          <Metric label="Blocked" value={result.blocked_count ?? 0} />
          <Metric label="Source" value={result.data_source ?? "n/a"} />
          <Metric label="IG catalogue" value={result.ig_status ?? "n/a"} />
        </div>
      )}
      {sourceIssues.length > 0 && (
        <div className="status compact-status">
          <strong>Discovery source fallback</strong>
          <span>{sourceIssues.map((issue) => `${issue.provider}: ${issue.detail}`).join(" · ")}</span>
        </div>
      )}
      {result?.ig_status === "ig_not_configured" && (
        <div className="status compact-status">
          <strong>IG catalogue check required</strong>
          <span>Save IG credentials and account roles in Settings before installing discovered share markets.</span>
        </div>
      )}
      <div className="midcap-list">
        {candidates.slice(0, 8).map((candidate) => (
          <div className="midcap-card" key={candidate.market_id}>
            <div className="midcap-card-head">
              <div>
                <strong>{candidate.market_id}</strong>
                <span>{candidate.name}</span>
                <small>{candidate.eodhd_symbol} · {candidate.exchange || candidate.country}</small>
              </div>
              <span className={`badge ${candidate.eligible ? "good" : "warn"}`}>{candidate.eligible ? "Eligible" : "Blocked"}</span>
            </div>
            <div className="mini-metrics">
              <Metric label="Mkt cap" value={formatLargeMoney(candidate.market_cap)} />
              <Metric label="Price" value={round(candidate.price)} />
              <Metric label="Volume" value={formatLargeNumber(candidate.volume)} />
              <Metric label="Turnover" value={formatLargeMoney(candidate.turnover)} />
              <Metric label="£1/pt margin" value={formatMoney(candidate.estimated_margin_for_probe_stake)} />
              <Metric label="Spread/slip" value={`${round(candidate.estimated_spread_bps)} / ${round(candidate.estimated_slippage_bps)} bps`} />
              <Metric label="IG" value={candidate.ig_status?.replaceAll("_", " ") ?? "n/a"} />
            </div>
            {(candidate.blockers?.length > 0 || candidate.warnings?.length > 0) && (
              <div className="warning-row">
                <WarningChips warnings={[...(candidate.blockers ?? []), ...(candidate.warnings ?? [])]} limit={6} />
              </div>
            )}
            <button type="button" className="ghost" onClick={() => onInstall(candidate)} disabled={!candidate.eligible || candidate.ig_status !== "ig_matched"}>
              <Plus size={16} /> Install market
            </button>
          </div>
        ))}
        {result && candidates.length === 0 && <span className="muted">No mid-cap candidates matched these filters.</span>}
        {!result && <span className="muted">Search FMP/IG for share candidates before adding them to the backtest market list.</span>}
      </div>
    </Panel>
  );
}

function SettingsView({ eodhdKey, setEodhdKey, fmpKey, setFmpKey, ig, setIg, igRoles, setIgRoles, igAccountRoles, submitEodhd, submitFmp, submitIg, submitIgAccountRoles, eodhdStatus, fmpStatus, igStatus, cacheStatus, pruneCache }) {
  return (
    <div className="grid two">
      <Panel icon={<KeyRound />} title="Provider Settings">
        <form onSubmit={submitEodhd}>
          <div className="label-row">
            <label>EODHD API token</label>
            <SecretBadge status={eodhdStatus} />
          </div>
          <div className="row">
            <input value={eodhdKey} onChange={(event) => setEodhdKey(event.target.value)} type="password" required />
            <button>{eodhdStatus?.configured ? "Replace" : "Validate"}</button>
          </div>
        </form>
        <form onSubmit={submitFmp}>
          <div className="label-row">
            <label>FMP API key</label>
            <SecretBadge status={fmpStatus} />
          </div>
          <div className="row">
            <input value={fmpKey} onChange={(event) => setFmpKey(event.target.value)} type="password" required />
            <button>{fmpStatus?.configured ? "Replace FMP" : "Validate FMP"}</button>
          </div>
        </form>
        <form onSubmit={submitIg}>
          <div className="label-row">
            <label>IG demo credentials</label>
            <SecretBadge status={igStatus} />
          </div>
          <label>IG demo API key</label>
          <input value={ig.apiKey} onChange={(event) => setIg({ ...ig, apiKey: event.target.value })} type="password" required />
          <label>IG username</label>
          <input value={ig.username} onChange={(event) => setIg({ ...ig, username: event.target.value })} required />
          <label>IG password</label>
          <input value={ig.password} onChange={(event) => setIg({ ...ig, password: event.target.value })} type="password" required />
          <label>IG default account code</label>
          <input value={ig.accountId} onChange={(event) => setIg({ ...ig, accountId: event.target.value })} />
          <button>{igStatus?.configured ? "Replace IG demo" : "Validate IG demo"}</button>
        </form>
        <form onSubmit={submitIgAccountRoles}>
          <div className="label-row">
            <label>IG demo accounts</label>
            <span className={`secret-badge ${igAccountRoles?.both_active ? "connected" : "saved"}`}>{igAccountRoles?.both_active ? "Both active" : "Roles"}</span>
          </div>
          <label>Spread bet demo account name/code</label>
          <input
            value={igRoles.spreadBetAccountId}
            onChange={(event) => setIgRoles({ ...igRoles, spreadBetAccountId: event.target.value })}
            placeholder={igAccountRolePlaceholder(igAccountRoles?.spread_bet, "Spread bet demo account")}
          />
          <label>CFD demo account name/code</label>
          <input
            value={igRoles.cfdAccountId}
            onChange={(event) => setIgRoles({ ...igRoles, cfdAccountId: event.target.value })}
            placeholder={igAccountRolePlaceholder(igAccountRoles?.cfd, "CFD demo account")}
          />
          <label>Default form selection</label>
          <select value={igRoles.defaultProductMode} onChange={(event) => setIgRoles({ ...igRoles, defaultProductMode: event.target.value })}>
            <option value="spread_bet">Spread bet</option>
            <option value="cfd">CFD</option>
          </select>
          <button>Save demo accounts</button>
        </form>
      </Panel>
      <Panel icon={<Activity />} title="Connection Status">
        <div className="status-list">
          {[eodhdStatus, fmpStatus, igStatus].filter(Boolean).map((item) => (
            <div className="status" key={item.provider}>
              <strong>{item.provider.toUpperCase()}</strong>
              <span>{item.configured ? "saved on server" : "not saved"} · {item.last_status}</span>
              {item.last_error && <small>{item.last_error}</small>}
            </div>
          ))}
          {igAccountRoles && (
            <div className="status">
              <strong>IG DEMO ACCOUNTS</strong>
              <span>{igAccountRoles.both_active ? "Both demo accounts active" : "Set both demo account roles"} · default form {String(igAccountRoles.default_product_mode || "spread_bet").replace("_", " ")}</span>
              <small>Live order placement remains disabled.</small>
            </div>
          )}
          {igAccountRoles && (
            <IgAccountRoleStatus title="Spread Bet Demo" role={igAccountRoles.spread_bet} />
          )}
          {igAccountRoles && (
            <IgAccountRoleStatus title="CFD Demo" role={igAccountRoles.cfd} />
          )}
        </div>
      </Panel>
      <Panel icon={<Database />} title="Market Data Cache">
        <div className="metrics four">
          <Metric label="Cached API payloads" value={cacheStatus?.stats?.payload_entry_count ?? cacheStatus?.stats?.entry_count ?? 0} />
          <Metric label="Expired" value={cacheStatus?.stats?.expired_count ?? 0} />
          <Metric label="Provider errors" value={cacheStatus?.stats?.provider_error_count ?? 0} />
          <Metric label="Newest" value={shortDateTime(cacheStatus?.stats?.newest_created_at)} />
        </div>
        <div className="status-list">
          {(cacheStatus?.namespaces ?? []).slice(0, 5).map((item) => (
            <div className="status" key={item.namespace}>
              <strong>{item.namespace}</strong>
              <span>{item.entry_count} cached API payloads · {item.expired_count} expired</span>
            </div>
          ))}
          {(cacheStatus?.namespaces ?? []).length === 0 && <span className="muted">No cached provider payloads yet.</span>}
        </div>
        <div className="status-list">
          {(cacheStatus?.recent_entries ?? []).slice(0, 5).map((item) => (
            <div className="status" key={`${item.namespace}-${item.created_at}-${item.request_url}`}>
              <strong>{item.metadata?.symbol || item.namespace}</strong>
              <span>{item.namespace} · {item.metadata?.source_status || "cached"} · {item.expired ? "expired" : "fresh"}</span>
              <small>{shortDateTime(item.created_at)} · {item.payload_bytes ?? 0} bytes</small>
            </div>
          ))}
        </div>
        <button type="button" className="ghost" onClick={pruneCache}>Prune expired</button>
      </Panel>
    </div>
  );
}

function CostProfile({ market, profile, onLoad }) {
  const badge = costBadge(profile, market);
  return (
    <div className="cost-card">
      <div>
        <strong>{market.market_id}</strong>
        <span className={`badge ${badge.className}`}>{badge.label}</span>
      </div>
      <div className="mini-metrics">
        <Metric label="Spread" value={`${round(profile?.spread_bps ?? market.spread_bps)} bps`} />
        <Metric label="Slippage" value={`${round(profile?.slippage_bps ?? market.slippage_bps)} bps`} />
        <Metric label="Margin" value={`${round(profile?.margin_percent ?? market.spread_bet_model?.margin_percent ?? 5)}%`} />
        <Metric label="Point" value={`${round(profile?.contract_point_size ?? market.spread_bet_model?.contract_point_size ?? 1, 4)}`} />
        <Metric label="Funding" value={`${round((profile?.overnight_admin_fee_annual ?? 0.03) * 100)}%`} />
        <Metric label="FX" value={`${round(profile?.fx_conversion_bps ?? 80)} bps`} />
      </div>
      {market.spread_bet_model && <small className="muted">{shareSpreadBetLabel(market.spread_bet_model)}</small>}
      <button type="button" className="ghost" onClick={onLoad}>Load profile</button>
    </div>
  );
}

function Panel({ icon, title, children }) {
  return (
    <section className="panel">
      <h2>{React.cloneElement(icon, { size: 20 })}{title}</h2>
      {children}
    </section>
  );
}

function Metric({ label, value }) {
  return (
    <div className="metric">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function IgAccountRoleStatus({ title, role }) {
  const active = Boolean(role?.active);
  const label = role?.display_name || role?.masked_account_id || "not set";
  const validation = String(role?.validation_status || (role?.configured ? "saved" : "missing")).replaceAll("_", " ");
  return (
    <div className="status">
      <strong>{title}</strong>
      <span>{active ? "active" : role?.configured ? "saved" : "not set"} · {label}</span>
      <small>{validation}</small>
    </div>
  );
}

function igAccountRolePlaceholder(role, fallback) {
  return role?.display_name || role?.masked_account_id || fallback;
}

function WarningChips({ warnings = [], limit = 8, empty = "Clear" }) {
  const codes = arrayValue(warnings).filter(Boolean).map(String);
  if (codes.length === 0) {
    return <span className="muted">{empty}</span>;
  }
  return codes.slice(0, limit).map((warning) => (
    <span className={warningChipClass(warning)} key={warning} title={warningChipTitle(warning)}>
      {humanWarnings([warning])[0]}
    </span>
  ));
}

function SecretBadge({ status }) {
  if (!status?.configured) {
    return <span className="secret-badge empty">Not saved</span>;
  }
  return <span className={`secret-badge ${status.last_status === "connected" ? "connected" : "saved"}`}>Saved on server</span>;
}

function providerStatus(statuses, provider) {
  return statuses.find((item) => item.provider === provider);
}

function normalizeProductMode(value) {
  return String(value || "").replace("-", "_").toLowerCase() === "cfd" ? "cfd" : "spread_bet";
}

function productModeLabel(value) {
  return normalizeProductMode(value) === "cfd" ? "CFD Demo" : "Spread Bet Demo";
}

function isActiveRun(run) {
  return ["created", "running"].includes(run?.status);
}

function researchStateFromRun(detail, plannedTrials = plannedTrialsForRun(detail)) {
  return {
    status: isActiveRun(detail) ? "running" : detail?.status || "idle",
    detail: runStateDetail(detail, plannedTrials),
    progress: runProgress(detail, plannedTrials),
  };
}

function plannedTrialsForRun(detail) {
  const config = detail?.config ?? {};
  const preset = SEARCH_PRESETS.find((item) => item.id === config.search_preset) ?? SEARCH_PRESETS[1];
  const budget = Number(config.effective_search_budget || config.search_budget || preset?.budget || 0);
  const marketIds = Array.isArray(config.market_ids) ? config.market_ids.filter(Boolean) : [];
  const marketCount = Math.max(1, marketIds.length || (detail?.market_id && detail.market_id !== "MULTI" ? 1 : 0));
  return Math.max(1, budget) * marketCount;
}

function runProgress(detail, plannedTrials = 0) {
  if (!detail) {
    return 0;
  }
  if (["finished", "finished_with_warnings", "error"].includes(detail.status)) {
    return 100;
  }
  const statuses = detail.config?.market_statuses ?? [];
  const configuredMarkets = detail.config?.market_ids ?? [];
  const marketCount = Math.max(configuredMarkets.length, statuses.length, 1);
  if (statuses.length > 0) {
    const marketProgressTotal = statuses.reduce((total, item) => total + marketStatusProgress(item), 0);
    const missingMarkets = Math.max(0, marketCount - statuses.length);
    const marketProgress = marketProgressTotal / Math.max(1, statuses.length + missingMarkets);
    const trialProgress = plannedTrials > 0 ? Math.min(95, (Number(detail.trial_count ?? 0) / plannedTrials) * 95) : 0;
    return boundedProgress(Math.max(marketProgress, trialProgress), detail.status);
  }
  if (detail.status === "running") {
    const savedTrials = Number(detail.trial_count ?? 0);
    return plannedTrials > 0 ? boundedProgress((savedTrials / plannedTrials) * 95, detail.status) : 5;
  }
  return detail.status === "created" ? 2 : 0;
}

function marketStatusProgress(status) {
  if (status.status === "completed" || status.status === "failed") {
    return 100;
  }
  if (status.status === "evaluating") {
    return 68;
  }
  if (status.bar_snapshot || status.bar_count) {
    return 52;
  }
  if (status.status === "loading") {
    return 18;
  }
  return 5;
}

function marketStatusLine(item) {
  const bars = `${item.bar_count ?? 0} bars`;
  if (item.status === "completed") {
    return `${bars} · ${item.trial_count ?? 0} trials saved`;
  }
  if (item.status === "evaluating") {
    return `${bars} loaded · evaluating strategy trials`;
  }
  if (item.status === "failed") {
    return `${bars} loaded before failure`;
  }
  if (item.status === "loading") {
    return `${bars} loaded so far`;
  }
  return `${bars} loaded`;
}

function marketDataSourceLabel(value) {
  return {
    eodhd_primary_symbol: "EODHD primary symbol",
    eodhd_daily_fallback: "EODHD daily fallback",
  }[value] ?? value ?? "EODHD primary symbol";
}

function boundedProgress(value, status = "") {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return 0;
  }
  const upper = ["created", "running"].includes(status) ? 98 : 100;
  return Math.max(0, Math.min(upper, number));
}

function runStateDetail(detail, plannedTrials) {
  const savedTrials = detail?.trial_count ?? 0;
  if (["created", "running"].includes(detail?.status)) {
    return `Run ${detail.id}: ${savedTrials}/${plannedTrials} strategy trials saved.`;
  }
  if (detail?.status === "finished") {
    return `Run ${detail.id}: ${savedTrials} trials, ${detail.passed_count ?? 0} paper-ready, best score ${round(detail.best_score)}.`;
  }
  if (detail?.status === "finished_with_warnings") {
    return `Run ${detail.id}: ${savedTrials} trials saved with market data warnings.`;
  }
  return `Run ${detail?.id ?? ""}: ${detail?.error || detail?.status || "unknown status"}`;
}

function runCompletionMessage(runId, detail) {
  if (detail.status === "finished") {
    return `Run ${runId} finished after IG-style costs.`;
  }
  if (detail.status === "finished_with_warnings") {
    return `Run ${runId} finished with market data warnings.`;
  }
  return `Run ${runId} ${detail.status}: ${detail.error || "check run details"}`;
}

function sleep(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function shortDateTime(value) {
  if (!value) {
    return "none";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

function normalizeInterval(value) {
  if (value === "market_default") {
    return "Market default";
  }
  return intervalValue(value);
}

function intervalValue(value) {
  if (value === "1h") {
    return "1hour";
  }
  return value || "5min";
}

function marketAvailabilityNote(market = {}) {
  if (market.enabled) {
    return "Backtest-ready";
  }
  if (market.market_id === "SA40") {
    return "Broker-only: IG market found, bars unavailable";
  }
  if (market.ig_epic) {
    return "Broker-only: enable after bar data is mapped";
  }
  return "Disabled in market registry";
}

function marketSubline(market = {}) {
  if (market.spread_bet_model) {
    return shareSpreadBetLabel(market.spread_bet_model);
  }
  return "";
}

function shareSpreadBetLabel(model = {}) {
  const region = String(model.asset_region || "share").toUpperCase();
  const category = model.spread_category ? ` ${model.spread_category}` : "";
  const margin = model.margin_percent ? ` · ${round(model.margin_percent)}% margin` : "";
  const point = model.contract_point_size && Number(model.contract_point_size) !== 1 ? ` · point ${round(model.contract_point_size, 4)}` : "";
  return `${region}${category} share spread bet${margin}${point}`;
}

function trialMarketId(item) {
  return String(item?.market_id || item?.parameters?.market_id || "").trim();
}

function trialIntervalLabel(item) {
  const raw = item?.parameters?.timeframe || item?.parameters?.interval || item?.parameters?.bar_interval;
  return raw ? normalizeInterval(String(raw)) : "";
}

function costBadge(profile, market) {
  const confidence = profile?.confidence ?? "ig_public_spread_baseline";
  if (confidence === "ig_live_epic_cost_profile") {
    return { label: "IG live EPIC cost profile", className: "good" };
  }
  if (confidence === "ig_recent_epic_price_profile") {
    return { label: "IG recent EPIC price profile", className: "good" };
  }
  if (confidence === "eodhd_ig_cost_envelope") {
    return { label: "EODHD bars with IG cost envelope", className: "warn" };
  }
  if (confidence === "ig_public_spread_baseline") {
    return { label: "IG public spread baseline", className: "base" };
  }
  return { label: "Needs IG price validation", className: "warn" };
}

function statusBadgeClass(status) {
  if (status === "completed") {
    return "good";
  }
  if (status === "failed") {
    return "warn";
  }
  return "base";
}

function tierBadgeClass(tier) {
  if (tier === "validated_candidate" || tier === "paper_candidate") {
    return "good";
  }
  if (tier === "research_candidate" || tier === "incubator") {
    return "base";
  }
  if (tier === "watchlist") {
    return "warn";
  }
  return "muted-badge";
}

function readinessBadgeClass(status) {
  if (status === "ready_for_paper") {
    return "good";
  }
  if (status === "needs_ig_validation") {
    return "warn";
  }
  return "base";
}

function readinessLabel(status) {
  return {
    ready_for_paper: "Gate clear",
    needs_ig_validation: "Validate IG",
    blocked: "Blocked",
  }[status] ?? "Blocked";
}

function tierLabel(tier) {
  return {
    validated_candidate: "IG validated",
    paper_candidate: "Paper candidate",
    research_candidate: "Research",
    incubator: "Incubator",
    watchlist: "Watchlist",
    reject: "Reject",
  }[tier] ?? "Research";
}

function isNoisyRun(run) {
  if (run.status === "error" || run.status === "finished_with_warnings") {
    return true;
  }
  if (Number(run.trial_count ?? 0) === 0) {
    return true;
  }
  return Number(run.passed_count ?? 0) === 0 && Number(run.best_score ?? 0) < 25;
}

function tierMatchesFilter(tier, filter) {
  if (filter === "all") {
    return true;
  }
  if (filter === "paper") {
    return tier === "validated_candidate" || tier === "paper_candidate";
  }
  if (filter === "research") {
    return tier === "research_candidate" || tier === "incubator" || tier === "watchlist";
  }
  if (filter === "incubator") {
    return tier === "incubator";
  }
  if (filter === "rejected") {
    return tier === "reject";
  }
  return tier !== "reject";
}

function trialScanMatchesFilter(trial, filter) {
  if (filter === "all") {
    return true;
  }
  const specialist = Boolean(trial.parameters?.regime_scan);
  return filter === "specialist" ? specialist : !specialist;
}

function trialCostMatchesFilter(trial, filter) {
  if (filter === "all") {
    return true;
  }
  const costToGross = Number(trial.backtest?.cost_to_gross_ratio ?? 0);
  const thresholds = { healthy: 0.25, ok: 0.4, survivable: 0.65 };
  return costToGross > 0 && costToGross <= (thresholds[filter] ?? Number.POSITIVE_INFINITY);
}

function sortTrialsForDisplay(trials = []) {
  return [...trials].sort((left, right) => trialDisplayScore(right) - trialDisplayScore(left));
}

function trialDisplayScore(trial = {}) {
  const audit = trial.parameters?.search_audit ?? {};
  const capitalFeasible = audit.working_capital_feasible ? 1 : 0;
  const paperScore = Number(audit.paper_readiness_score ?? 0);
  const capitalScore = Number(audit.working_capital_score ?? 0) * 100;
  const evidence = evidenceProfileForSource(trial);
  const oosTrades = Math.min(18, Number(evidence.oos_trade_count ?? 0));
  const oosNet = Number(evidence.oos_net_profit ?? 0) > 0 ? 1 : 0;
  const tierScore = { validated_candidate: 400, paper_candidate: 350, research_candidate: 250, incubator: 200, watchlist: 150, reject: 0 }[trial.promotion_tier] ?? 0;
  return tierScore + capitalFeasible * 120 + paperScore * 2 + capitalScore + oosTrades * 2 + oosNet * 20 + Number(trial.robustness_score ?? 0);
}

function bestTrialsByMarket(trials = [], perMarket = 3) {
  const grouped = new Map();
  for (const trial of trials) {
    const marketId = trialMarketId(trial) || "Unknown";
    const group = grouped.get(marketId) ?? [];
    if (group.length < perMarket) {
      group.push(trial);
      grouped.set(marketId, group);
    }
  }
  return [...grouped.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .flatMap(([, group]) => group);
}

function bestTrialsByRegime(trials = [], perRegime = 3) {
  const grouped = new Map();
  for (const trial of trials) {
    const regime = trialRegimeKey(trial);
    if (!regime) {
      continue;
    }
    const group = grouped.get(regime) ?? [];
    if (group.length < perRegime) {
      group.push(trial);
      grouped.set(regime, group);
    }
  }
  return [...grouped.entries()]
    .sort(([left], [right]) => regimeLabel(left).localeCompare(regimeLabel(right)))
    .flatMap(([, group]) => group);
}

function trialRegimeKey(trial = {}) {
  const pattern = trial.parameters?.bar_pattern_analysis ?? {};
  return String(
    trial.parameters?.target_regime
      || pattern.target_regime
      || pattern.dominant_profit_regime?.key
      || ""
  ).trim();
}

function runQualitySummary(trials = []) {
  const warningCounts = new Map();
  let paperReady = 0;
  let researchWatch = 0;
  let incubator = 0;
  let rejected = 0;
  let costFragile = 0;
  for (const trial of trials) {
    const tier = trial.promotion_tier;
    if (tier === "validated_candidate" || tier === "paper_candidate") {
      paperReady += 1;
    } else if (tier === "incubator") {
      incubator += 1;
    } else if (tier === "research_candidate" || tier === "watchlist") {
      researchWatch += 1;
    } else {
      rejected += 1;
    }
    const warnings = trial.warnings ?? [];
    if (warnings.some((warning) => ["fails_higher_slippage", "costs_overwhelm_edge", "weak_net_cost_efficiency", "high_turnover_cost_drag"].includes(warning))) {
      costFragile += 1;
    }
    for (const warning of warnings) {
      warningCounts.set(warning, (warningCounts.get(warning) ?? 0) + 1);
    }
  }
  const topWarnings = [...warningCounts.entries()]
    .sort((left, right) => right[1] - left[1])
    .slice(0, 4)
    .map(([warning, count]) => ({ warning, count }));
  return { paperReady, researchWatch, incubator, rejected, costFragile, topWarnings };
}

function candidateFactoryPlan(modeId, researchRun, enabledMarkets = [], activeMarketIds = []) {
  const mode = CANDIDATE_FACTORY_MODES.find((item) => item.id === modeId) ?? CANDIDATE_FACTORY_MODES[0];
  const isDayTrading = mode.id === "day_trading";
  const enabledIds = new Set(enabledMarkets.map((market) => market.market_id));
  const selected = activeMarketIds.filter((marketId) => enabledIds.has(marketId));
  const fallbackMarket = (isDayTrading
    ? enabledMarkets.find((market) => market.asset_class === "share")?.market_id
    : enabledMarkets.find((market) => market.market_id === "XAUUSD")?.market_id)
    ?? enabledMarkets[0]?.market_id
    ?? researchRun.market_id;
  let marketIds = selected.length ? selected : [fallbackMarket].filter(Boolean);
  if (mode.id === "deep_one_market") {
    marketIds = marketIds.slice(0, 1);
  }
  if (marketIds.length === 0) {
    return { marketIds: [], runPatch: {}, stopReason: "Choose at least one enabled market before running the factory." };
  }
  const preset = mode.id === "deep_one_market" ? "deep" : "balanced";
  const budget = mode.id === "deep_one_market" ? "120" : mode.id === "evidence_first" ? "54" : "";
  const interval = isDayTrading ? "5min" : marketIds.length > 1 ? "market_default" : (researchRun.interval || "market_default");
  const start = factoryStartForMarkets(marketIds, enabledMarkets, interval, researchRun.start);
  const runPatch = {
    market_id: marketIds[0],
    interval,
    start,
    end: researchRun.end,
    trading_style: isDayTrading ? "intraday_only" : "find_anything_robust",
    objective: "profit_first",
    risk_profile: isDayTrading ? "conservative" : "balanced",
    search_preset: preset,
    search_budget: budget,
    strategy_families: isDayTrading ? DAY_TRADING_FAMILIES : [],
    cost_stress_multiplier: mode.id === "evidence_first" || isDayTrading ? 2.5 : 2.0,
    include_regime_scans: true,
    regime_scan_budget_per_regime: "",
    target_regime: "",
    excluded_months: [],
    repair_mode: mode.id === "evidence_first" ? "evidence_first" : "standard",
    account_size: researchRun.account_size || String(WORKING_ACCOUNT_SIZE),
    source_template: {},
    day_trading_mode: isDayTrading,
    force_flat_before_close: isDayTrading,
    paper_queue_limit: isDayTrading ? "3" : researchRun.paper_queue_limit || "3",
    review_queue_limit: isDayTrading ? "10" : researchRun.review_queue_limit || "10",
  };
  const specialistBudget = regimePresetBudget(preset);
  const trialPlan = mode.id === "deep_one_market"
    ? `120 base trials plus up to ${specialistBudget} per eligible regime`
    : `${preset === "balanced" ? 54 : 18} base trials per market plus up to ${specialistBudget} per eligible regime`;
  return {
    mode,
    marketIds,
    runPatch,
    launchMessage: isDayTrading
      ? `Launching intraday discovery: ${marketIds.join(" / ")} · ${trialPlan} · leads must be frozen before daily paper.`
      : `Launching Candidate Factory: ${marketIds.join(" / ")} · ${trialPlan}.`,
    stageMessage: isDayTrading
      ? `Intraday discovery staged for ${marketIds.join(" / ")}: no overnight, ${trialPlan}, then save and Freeze validate.`
      : `Candidate Factory staged for ${marketIds.join(" / ")}: find-anything-robust, regime templates on, ${trialPlan}.`,
  };
}

function factoryStartForMarkets(marketIds = [], enabledMarkets = [], interval, currentStart) {
  const selected = enabledMarkets.filter((market) => marketIds.includes(market.market_id));
  const requestedInterval = intervalValue(interval);
  const dailyRequested = requestedInterval === "1day";
  const allDailyMarkets = selected.length > 0 && selected.every((market) => intervalValue(market.default_timeframe) === "1day");
  const targetStart = dailyRequested || allDailyMarkets ? "2020-01-01" : "2024-01-01";
  return earlierDate(currentStart, targetStart);
}

function factoryLeads(candidates = [], runDetail = null, activeMarketIds = []) {
  const selected = new Set(activeMarketIds.filter(Boolean));
  const sources = [...arrayValue(candidates), ...arrayValue(runDetail?.trials)];
  const seen = new Set();
  const leads = [];
  for (const source of sources) {
    const lead = factoryLeadFromSource(source);
    if (!lead || (selected.size > 0 && lead.marketId && !selected.has(lead.marketId))) {
      continue;
    }
    if (!factoryViableLead(lead)) {
      continue;
    }
    if (seen.has(lead.dedupeKey)) {
      continue;
    }
    seen.add(lead.dedupeKey);
    leads.push(lead);
  }
  return leads.sort((left, right) => factoryLeadRank(right) - factoryLeadRank(left));
}

function factoryLeadFromSource(source = {}) {
  const parameters = source.audit?.candidate?.parameters ?? source.parameters ?? {};
  const backtest = source.audit?.backtest ?? source.backtest ?? {};
  const pattern = parameters.bar_pattern_analysis ?? {};
  const evidence = evidenceProfileForSource(source);
  const warnings = warningCodesForSource(source);
  const accountSize = testingAccountSizeForSource(source);
  const accountScenario = accountScenarioFor(source.capital_scenarios, accountSize);
  const marketId = String(source.market_id || parameters.market_id || "").trim();
  const regime = String(parameters.target_regime || pattern.target_regime || pattern.dominant_profit_regime?.key || "").trim();
  const name = String(source.strategy_name || source.name || "").trim();
  if (!name) {
    return null;
  }
  const tier = String(source.promotion_tier || source.audit?.promotion_tier || "watchlist");
  return {
    key: `${source.run_id ?? "x"}-${source.id ?? name}-${marketId}-${regime}`,
    dedupeKey: `${source.run_id ?? "x"}-${name}-${marketId}-${regime}`,
    source,
    id: source.id,
    runId: source.run_id,
    name,
    marketId,
    regime,
    tier,
    family: source.strategy_family || parameters.family || source.style || "",
    interval: normalizeInterval(parameters.timeframe || parameters.interval || ""),
    score: Number(source.robustness_score ?? 0),
    net: Number(backtest.net_profit ?? 0),
    oos: Number(evidence.oos_net_profit ?? backtest.test_profit ?? 0),
    trades: Number(backtest.trade_count ?? 0),
    oosTrades: Number(evidence.oos_trade_count ?? 0),
    costToGross: Number(backtest.cost_to_gross_ratio ?? 0),
    warnings,
    accountSize,
    accountScenario,
    capitalFeasible: accountScenario ? Boolean(accountScenario.feasible) : null,
    capitalFit: accountFeasibility(source.capital_scenarios, accountSize),
  };
}

function factoryViableLead(lead = {}) {
  if (hasTerminalCapitalWarning(lead.warnings)) {
    return false;
  }
  if (["paper_candidate", "validated_candidate", "research_candidate", "incubator", "watchlist"].includes(lead.tier)) {
    return lead.trades > 0 || lead.net > 0 || lead.oos > 0;
  }
  return lead.score >= 25 && lead.net > 0 && lead.trades >= 5 && (lead.oos > 0 || lead.oosTrades >= 6);
}

function hasTerminalCapitalWarning(warnings = []) {
  return arrayValue(warnings).some((warning) => ["ig_minimum_margin_too_large_for_account", "ig_minimum_risk_too_large_for_account"].includes(warning));
}

function factoryLeadRank(lead = {}) {
  const tierScore = { validated_candidate: 500, paper_candidate: 450, research_candidate: 320, incubator: 260, watchlist: 180, reject: 40 }[lead.tier] ?? 100;
  const oosBonus = lead.oos > 0 ? 60 : 0;
  const tradeBonus = Math.min(40, Number(lead.oosTrades || lead.trades || 0));
  const blockerPenalty = lead.warnings.filter((warning) => warningSeverity(warning) === "blocker").length * 18;
  const costPenalty = Number(lead.costToGross ?? 0) > 0.65 ? 40 : Number(lead.costToGross ?? 0) > 0.4 ? 16 : 0;
  return tierScore + Number(lead.score ?? 0) + oosBonus + tradeBonus - blockerPenalty - costPenalty;
}

function factoryLeadSummary(leads = []) {
  const coveredCells = new Set(leads.map((lead) => `${lead.marketId}-${lead.regime || "unknown"}`)).size;
  return {
    researchLeads: leads.filter((lead) => lead.tier !== "reject").length,
    paperReady: leads.filter((lead) => ["paper_candidate", "validated_candidate"].includes(lead.tier)).length,
    coveredCells,
  };
}

function factoryCoverageRows(leads = []) {
  const grouped = new Map();
  for (const lead of leads) {
    const key = `${lead.marketId || "Unknown"}-${lead.regime || "unknown"}`;
    const current = grouped.get(key) ?? {
      marketId: lead.marketId || "Unknown",
      regime: lead.regime || "unknown",
      count: 0,
      bestScore: 0,
      bestTier: "reject",
      bestOos: 0,
      bestTrades: 0,
      bestCostToGross: 0,
      rank: -Infinity,
    };
    const rank = factoryLeadRank(lead);
    current.count += 1;
    if (rank > current.rank) {
      current.rank = rank;
      current.bestScore = lead.score;
      current.bestTier = lead.tier;
      current.bestOos = lead.oos;
      current.bestTrades = lead.trades;
      current.bestCostToGross = lead.costToGross;
    }
    grouped.set(key, current);
  }
  return [...grouped.values()].sort((left, right) => right.rank - left.rank);
}

function factoryRegimeGaps(runDetail = null, leads = [], activeMarketIds = []) {
  const selected = new Set(activeMarketIds.filter(Boolean));
  const covered = new Set(leads.map((lead) => `${lead.marketId}-${lead.regime}`));
  const gaps = [];
  for (const status of arrayValue(runDetail?.config?.market_statuses)) {
    const marketId = String(status.market_id || "").trim();
    if (selected.size > 0 && marketId && !selected.has(marketId)) {
      continue;
    }
    for (const item of arrayValue(status.eligible_regimes)) {
      const regime = String(item.regime || "").trim();
      if (!regime || covered.has(`${marketId}-${regime}`)) {
        continue;
      }
      gaps.push({ marketId, regime, tradingDays: Number(item.trading_days ?? 0) });
    }
  }
  return gaps.sort((left, right) => right.tradingDays - left.tradingDays);
}

function candidateQueueSummary(candidates = []) {
  let blocked = 0;
  let needsFreshRun = 0;
  let needsIgValidation = 0;
  let paperReady = 0;
  for (const candidate of candidates) {
    const readiness = candidateReadiness(candidate);
    const blockers = readiness.blockers ?? [];
    if (readiness.status === "ready_for_paper") {
      paperReady += 1;
    }
    if (readiness.status === "needs_ig_validation") {
      needsIgValidation += 1;
    }
    if (readiness.status === "blocked") {
      blocked += 1;
    }
    if (blockers.some((warning) => ["legacy_sharpe_diagnostics", "missing_cost_profile", "missing_spread_slippage", "short_sharpe_sample", "limited_sharpe_sample"].includes(warning))) {
      needsFreshRun += 1;
    }
  }
  return { blocked, needsFreshRun, needsIgValidation, paperReady };
}

function candidateReadiness(candidate) {
  return candidate.audit?.promotion_readiness ?? {
    status: "blocked",
    blockers: candidate.audit?.warnings ?? [],
    validation_warnings: [],
    next_action: "rerun_with_fresh_diagnostics",
  };
}

function evidenceProfileForSource(source = {}) {
  const parameters = source.audit?.candidate?.parameters ?? source.parameters ?? source.settings ?? {};
  const stored = parameters.evidence_profile;
  if (stored && typeof stored === "object") {
    return {
      fold_count: numberOrZero(stored.fold_count),
      active_fold_count: numberOrZero(stored.active_fold_count),
      inactive_fold_count: numberOrZero(stored.inactive_fold_count),
      positive_fold_rate: numberOrZero(stored.positive_fold_rate),
      active_positive_fold_rate: numberOrZero(stored.active_positive_fold_rate),
      single_fold_profit_share: numberOrZero(stored.single_fold_profit_share),
      oos_net_profit: numberOrZero(stored.oos_net_profit),
      oos_trade_count: numberOrZero(stored.oos_trade_count),
      worst_fold_net_profit: numberOrZero(stored.worst_fold_net_profit),
      worst_active_fold_net_profit: numberOrZero(stored.worst_active_fold_net_profit),
    };
  }
  const folds = arrayValue(source.audit?.fold_results).length ? source.audit.fold_results : arrayValue(source.folds);
  if (folds.length === 0) {
    return { fold_count: 0, active_fold_count: 0, inactive_fold_count: 0, positive_fold_rate: 0, active_positive_fold_rate: 0, single_fold_profit_share: 0, oos_net_profit: 0, oos_trade_count: 0, worst_fold_net_profit: 0, worst_active_fold_net_profit: 0 };
  }
  const foldNet = folds.map((fold) => numberOrZero(fold.net_profit));
  const activeFolds = folds.filter((fold) => numberOrZero(fold.trade_count) > 0);
  const activeFoldNet = activeFolds.map((fold) => numberOrZero(fold.net_profit));
  const positive = foldNet.filter((value) => value > 0);
  const positiveTotal = positive.reduce((total, value) => total + value, 0);
  const activePositive = activeFoldNet.filter((value) => value > 0);
  return {
    fold_count: folds.length,
    active_fold_count: activeFolds.length,
    inactive_fold_count: Math.max(0, folds.length - activeFolds.length),
    positive_fold_rate: activeFolds.length ? activePositive.length / activeFolds.length : 0,
    active_positive_fold_rate: activeFolds.length ? activePositive.length / activeFolds.length : 0,
    single_fold_profit_share: positiveTotal > 0 ? Math.max(...positive) / positiveTotal : 0,
    oos_net_profit: foldNet.reduce((total, value) => total + value, 0),
    oos_trade_count: folds.reduce((total, fold) => total + numberOrZero(fold.trade_count), 0),
    worst_fold_net_profit: Math.min(...foldNet),
    worst_active_fold_net_profit: activeFoldNet.length ? Math.min(...activeFoldNet) : 0,
  };
}

function templateUtilizationProfile(backtest = {}, pattern = {}) {
  const regimeEvidence = pattern?.regime_trade_evidence ?? {};
  const inRegime = regimeEvidence?.in_regime ?? {};
  const targeted = Boolean(pattern?.target_regime || regimeEvidence?.is_targeted);
  const activeDaysFromRegime = numberOrZero(inRegime.active_days);
  const useRegimeWindow = Boolean(regimeEvidence?.available && activeDaysFromRegime > 0);
  const historyDays = Math.round(
    numberOrZero(regimeEvidence.history_trading_days)
      || numberOrZero(backtest.sample_trading_days)
      || numberOrZero(backtest.sharpe_observations)
  );
  const activeDays = Math.round(
    useRegimeWindow
      ? activeDaysFromRegime
      : numberOrZero(backtest.exposure) * historyDays
  );
  if (historyDays <= 0 || activeDays <= 0) {
    return { available: false };
  }
  const netProfit = useRegimeWindow ? numberOrZero(inRegime.net_profit) : numberOrZero(backtest.net_profit);
  return {
    available: true,
    targeted,
    mode: targeted ? "target" : useRegimeWindow ? "edge" : "full",
    activeDays,
    idleDays: Math.max(0, historyDays - activeDays),
    activeShare: historyDays > 0 ? activeDays / historyDays : 0,
    netPerActiveDay: netProfit / Math.max(1, activeDays),
  };
}

function numberOrZero(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

function warningCodesForSource(source = {}) {
  const parameters = source.audit?.candidate?.parameters ?? source.parameters ?? source.settings ?? {};
  const pattern = parameters.bar_pattern_analysis ?? {};
  const readiness = source.audit?.promotion_readiness ?? {};
  const warnings = [
    ...arrayValue(source.warnings),
    ...arrayValue(source.audit?.warnings),
    ...arrayValue(pattern.warnings),
    ...arrayValue(readiness.blockers),
    ...arrayValue(readiness.validation_warnings),
  ];
  return [...new Set(warnings.filter(Boolean).map(String))];
}

function arrayValue(value) {
  return Array.isArray(value) ? value : [];
}

function uniqueMonths(months = []) {
  return [...new Set(arrayValue(months).map((month) => String(month).trim()).filter((month) => /^\d{4}-\d{2}$/.test(month)))];
}

function frozenTemplatePayload(template = {}) {
  const parameters = template.parameters ?? {};
  const allowedKeys = [
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
  ];
  const frozenParameters = {};
  for (const key of allowedKeys) {
    if (parameters[key] !== undefined && parameters[key] !== null && parameters[key] !== "") {
      frozenParameters[key] = parameters[key];
    }
  }
  if (Object.keys(frozenParameters).length === 0) {
    return {};
  }
  return {
    name: template.name,
    source_id: template.id,
    market_id: template.market_id,
    family: template.family,
    style: template.style,
    interval: template.interval,
    target_regime: templateSpecificRegime(template),
    repair_attempt_count: repairAttemptCountForTemplate(template),
    holding_period: template.day_trading_mode || parameters.day_trading_mode ? "intraday" : parameters.holding_period,
    force_flat_before_close: Boolean(template.day_trading_mode || parameters.force_flat_before_close),
    no_overnight: Boolean(template.day_trading_mode || parameters.no_overnight),
    parameters: frozenParameters,
  };
}

function templateSpecificRegime(template) {
  return String(template?.target_regime || template?.parameters?.target_regime || template?.pattern?.target_regime || "").trim();
}

function hasFrozenTemplateParameters(template) {
  return Object.keys(frozenTemplatePayload(template).parameters ?? {}).length > 0;
}

function repairAttemptCountForTemplate(template = {}) {
  return repairAttemptCountForSource({
    ...template,
    parameters: template.parameters ?? {
      ...(template.payload?.parameters ?? {}),
      source_template: template.source_template ?? template.payload?.source_template ?? {},
      search_audit: template.payload?.search_audit ?? template.payload?.parameters?.search_audit ?? {},
    },
  });
}

function repairAttemptCountForSource(source = {}) {
  const parameters = source.audit?.candidate?.parameters ?? source.parameters ?? source.settings ?? {};
  const searchAudit = parameters.search_audit ?? source.search_audit ?? source.payload?.search_audit ?? {};
  const sourceTemplate = parameters.source_template ?? source.source_template ?? source.payload?.source_template ?? {};
  return Math.max(
    0,
    Math.floor(Number(
      searchAudit.repair_attempt_count
        ?? sourceTemplate.repair_attempt_count
        ?? source.repair_attempt_count
        ?? 0
    ) || 0)
  );
}

function shouldShowRepairRemaining(source = {}) {
  const attempts = repairAttemptCountForSource(source);
  const readiness = source.audit?.promotion_readiness ?? source.promotion_readiness ?? source.readiness ?? {};
  const readinessStatus = readiness.status ?? source.readiness_status;
  const tier = source.promotion_tier ?? source.audit?.promotion_tier;
  return attempts > 0 && readinessStatus !== "ready_for_paper" && !["paper_candidate", "validated_candidate"].includes(tier);
}

function shouldShowFreezeValidate(source = {}) {
  const template = source.strategy_name ? source : savedTemplateAsSource(source);
  const parameters = template.audit?.candidate?.parameters ?? template.parameters ?? template.settings ?? {};
  const searchAudit = parameters.search_audit ?? source.payload?.search_audit ?? {};
  if (searchAudit.frozen_validation || parameters.frozen_template_validation || source.repair_mode === "frozen_validation") {
    return false;
  }
  return hasFrozenTemplateParameters(refinementTemplateLike(template));
}

function refinementTemplateLike(source = {}) {
  const parameters = source.audit?.candidate?.parameters ?? source.parameters ?? source.settings ?? {};
  const searchAudit = parameters.search_audit ?? source.search_audit ?? source.payload?.search_audit ?? {};
  const pattern = parameters.bar_pattern_analysis ?? source.pattern ?? source.payload?.pattern ?? {};
  return {
    id: source.id ?? source.trial_id ?? source.source_trial_id ?? source.strategy_name ?? source.name,
    name: source.strategy_name ?? source.name,
    market_id: source.market_id ?? parameters.market_id,
    family: parameters.family ?? source.strategy_family ?? source.family,
    style: parameters.style ?? searchAudit.trading_style ?? source.style,
    interval: parameters.timeframe ?? source.interval,
    target_regime: parameters.target_regime ?? pattern.target_regime ?? source.target_regime,
    repair_attempt_count: repairAttemptCountForSource(source),
    parameters,
    pattern,
  };
}

function templateSourceIds(source = {}) {
  const numericId = Number(source.id);
  const candidateId = source.audit && numericId > 0 ? numericId : null;
  const trialId = source.trial_id
    ?? source.audit?.derived_from_trial_id
    ?? (source.audit && numericId < 0 ? Math.abs(numericId) : null)
    ?? (source.parameters && numericId > 0 ? numericId : null);
  const kind = candidateId ? "candidate" : trialId ? "trial" : "source";
  return { candidateId, trialId, kind };
}

function savedTemplateAsSource(template = {}) {
  const payload = template.payload ?? {};
  const sourceTemplate = template.source_template ?? payload.source_template ?? {};
  const sourceParameters = sourceTemplate.parameters ?? {};
  const parameters = {
    ...(payload.parameters ?? {}),
    ...sourceParameters,
    market_id: template.market_id,
    family: template.strategy_family,
    style: template.style,
    timeframe: template.interval,
    target_regime: template.target_regime,
    bar_pattern_analysis: payload.pattern ?? payload.parameters?.bar_pattern_analysis ?? {},
    search_audit: payload.search_audit ?? payload.parameters?.search_audit ?? {},
    testing_account_size: template.testing_account_size,
  };
  return {
    id: template.source_candidate_id ?? (template.source_trial_id ? -Math.abs(Number(template.source_trial_id)) : template.id),
    strategy_name: template.name,
    market_id: template.market_id,
    strategy_family: template.strategy_family,
    style: template.style,
    robustness_score: template.robustness_score,
    promotion_tier: template.promotion_tier,
    warnings: template.warnings ?? payload.warnings ?? [],
    capital_scenarios: template.capital_scenarios ?? payload.capital_scenarios ?? [],
    audit: {
      candidate: { parameters },
      backtest: template.backtest ?? payload.backtest ?? {},
      warnings: template.warnings ?? payload.warnings ?? [],
      promotion_readiness: template.readiness ?? payload.readiness ?? {},
      promotion_tier: template.promotion_tier,
      derived_from_trial_id: template.source_trial_id,
    },
  };
}

function regimeRefineTarget(template) {
  const specific = templateSpecificRegime(template);
  if (specific) {
    return specific;
  }
  const pattern = template?.pattern ?? {};
  return String(pattern.dominant_profit_regime?.key || "").trim();
}

function repairActionsForTemplate(template) {
  const warnings = new Set(template.warnings ?? []);
  const pattern = template.pattern ?? {};
  const dominantMonth = pattern.dominant_profit_month?.key;
  const dominantRegime = pattern.dominant_profit_regime?.key;
  const dominantMonthShare = Number(pattern.dominant_profit_month?.positive_profit_share ?? 0);
  const dominantRegimeShare = Number(pattern.dominant_profit_regime?.positive_profit_share ?? 0);
  const verdict = pattern.regime_verdict;
  const targetRegime = regimeRefineTarget(template);
  const canFreezeValidate = hasFrozenTemplateParameters(template);
  const hasAny = (...codes) => codes.some((code) => warnings.has(code));
  const hardRegimeWarning =
    hasAny(
      "headline_sharpe_not_regime_robust",
      "regime_gated_backtest_negative",
      "regime_gated_oos_negative",
      "insufficient_regime_sample",
      "fails_normal_volatility_regime",
      "shock_regime_dependency",
    ) ||
    ["headline_only", "thin_regime_sample"].includes(verdict);
  const regimeIdentity =
    hasAny("profit_concentrated_single_regime", "high_volatility_only_edge") ||
    dominantRegimeShare >= 0.5 ||
    verdict === "regime_specific";
  const needsRegimeRepair = hardRegimeWarning || (regimeIdentity && !targetRegime);
  const needsCostSync = hasAny("needs_ig_price_validation", "missing_cost_profile", "missing_spread_slippage");
  const needsFrozenValidation = hasAny("multiple_testing_haircut") && canFreezeValidate && !needsCostSync;
  const actions = [];
  const add = (action) => {
    if (!actions.some((item) => item.id === action.id)) {
      actions.push(action);
    }
  };

  if (needsCostSync) {
    add({
      id: "sync-costs",
      kind: "sync_costs",
      title: "Fix IG validation",
      detail: "Refresh the spread, slippage, margin, and minimum stake rules before trusting the result.",
      button: "Sync costs",
      primary: true,
    });
  }
  if (needsFrozenValidation) {
    add({
      id: "freeze-validation",
      preset: "frozen_validation",
      title: "Freeze validation required",
      detail: "Retest the exact market, regime, timeframe, and parameters with no new search before trying any further repairs.",
      button: "Freeze validate",
      primary: actions.length === 0,
    });
  }
  if (hasAny("risk_budget_exceeded", "historical_drawdown_too_large", "historical_daily_loss_stop_breached", "margin_too_large", "insufficient_account_for_margin", "below_ig_min_deal_size", "missing_reference_price", "drawdown_too_high", "ig_minimum_risk_too_large_for_account", "ig_minimum_margin_too_large_for_account")) {
    add({
      id: "capital-fit",
      preset: "capital_fit",
      title: "Fix capital fit",
      detail: "Rerun this market and family with smaller stakes, tighter stops, and ranking weighted toward the selected account size.",
      button: "Capital fit",
      primary: actions.length === 0,
    });
  }
  if (hasAny("too_few_trades", "high_sharpe_low_trade_count", "low_oos_trades", "target_regime_low_oos_trades", "calendar_effect_needs_longer_history")) {
    add({
      id: "more-trades",
      preset: "more_trades",
      title: dominantRegime ? "Fix target OOS" : "Fix too few trades",
      detail: dominantRegime
        ? `Extend history and grade only ${regimeLabel(dominantRegime)} so the specialist has more out-of-sample chances.`
        : "Use longer history and a deeper locked-family retest so the edge has more chances to prove itself.",
      button: dominantRegime ? "Target OOS" : "Stage retest",
      primary: actions.length === 0,
    });
  }
  if (hasAny("profit_concentrated_single_month", "best_trades_dominate") || dominantMonthShare >= 0.45) {
    add({
      id: "exclude-month",
      preset: "exclude_best_month",
      title: "Check month dependence",
      detail: dominantMonth ? `Rerun with ${dominantMonth} removed to see whether the edge survives.` : "Rerun after removing the dominant profit month when one is available.",
      button: "Exclude month",
      primary: actions.length === 0,
    });
  }
  if (needsRegimeRepair) {
    add({
      id: "regime-repair",
      preset: "regime_scan",
      title: "Check regime dependence",
      detail: dominantRegime ? `Run gated specialists around ${regimeLabel(dominantRegime)} and compare full-period evidence.` : "Run capped regime specialists and keep full-period gates active.",
      button: "Regime repair",
      primary: actions.length === 0,
    });
  }
  if (hasAny("profits_not_consistent_across_folds", "high_sharpe_weak_folds", "unstable_folds", "weak_oos_economics", "weak_oos_evidence", "no_walk_forward_folds", "one_fold_dependency")) {
    add({
      id: "fold-repair",
      preset: "longer_history",
      title: "Fix fragile folds",
      detail: dominantRegime
        ? `Extend the window inside ${regimeLabel(dominantRegime)} and keep the strategy family locked so fold evidence has more calendar variety.`
        : "Extend the window and keep the strategy family locked so fold evidence has more calendar variety.",
      button: "Longer history",
      primary: actions.length === 0,
    });
  }
  if (hasAny("isolated_parameter_peak") || (hasAny("multiple_testing_haircut") && !canFreezeValidate)) {
    add({
      id: "evidence-first",
      preset: "evidence_first",
      title: "Reduce scan bias",
      detail: "Rerank a smaller locked search around fold strength, OOS profit, and concentration before trusting the headline.",
      button: "Evidence first",
      primary: actions.length === 0,
    });
  }
  if (hasAny("known_edge_needs_cross_market_validation")) {
    add({
      id: "scan-bias",
      preset: "cross_market",
      title: "Find similar edges elsewhere",
      detail: "Run a separate discovery search across other enabled markets. Any winners become new leads and do not validate or improve this template's score.",
      button: "Find elsewhere",
      primary: actions.length === 0,
    });
  }
  if (hasAny("negative_after_costs", "costs_overwhelm_edge", "negative_expectancy_after_costs", "high_turnover_cost_drag")) {
    add({
      id: "cost-stress",
      preset: "higher_costs",
      title: "Stress costs",
      detail: "Rerun with harsher costs so weak net-profit edges are filtered before paper tracking.",
      button: "Higher costs",
      primary: actions.length === 0,
    });
  }
  if (actions.length === 0) {
    add({
      id: "focused",
      preset: "focused",
      title: "Confirm template",
      detail: "Rerun the same market with the strategy family locked before changing anything else.",
      button: "Same market",
      primary: true,
    });
  }
  return actions.slice(0, 5);
}

function autoRefinementPlanForTemplate(template, researchRun, enabledMarkets = [], activeMarketIds = [], options = {}) {
  const warnings = new Set(template.warnings ?? []);
  const mode = options.mode || "make_tradeable";
  const isDayTrading = Boolean(template.day_trading_mode || template.parameters?.day_trading_mode || researchRun.day_trading_mode);
  const priorRepairAttempt = repairAttemptCountForTemplate(template);
  const repairAttempt = Math.min(MAX_REPAIR_ATTEMPTS, priorRepairAttempt + 1);
  const pattern = template.pattern ?? {};
  const dominantMonth = pattern.dominant_profit_month?.key;
  const dominantRegime = pattern.dominant_profit_regime?.key;
  const dominantMonthShare = Number(pattern.dominant_profit_month?.positive_profit_share ?? 0);
  const dominantRegimeShare = Number(pattern.dominant_profit_regime?.positive_profit_share ?? 0);
  const verdict = pattern.regime_verdict;
  const targetRegime = regimeRefineTarget(template);
  const family = template.family ? [template.family] : [];
  const sourceMarket = String(template.market_id || activeMarketIds[0] || researchRun.market_id || "").trim();
  const selectedMarket = sourceMarket ? [sourceMarket] : [];
  const allMarketIds = enabledMarkets.map((item) => item.market_id).filter(Boolean);
  const discoveryMarketCount = allMarketIds.filter((marketId) => marketId !== sourceMarket).length;
  const steps = [];
  const addStep = (step) => {
    if (!steps.includes(step)) {
      steps.push(step);
    }
  };
  const targets = [];
  const addTarget = (target) => {
    if (!targets.includes(target)) {
      targets.push(target);
    }
  };
  const hasAny = (...codes) => codes.some((code) => warnings.has(code));
  const tooFewTrades = hasAny("too_few_trades", "high_sharpe_low_trade_count", "low_oos_trades", "target_regime_low_oos_trades", "calendar_effect_needs_longer_history");
  const hardRegimeRepair =
    hasAny(
      "headline_sharpe_not_regime_robust",
      "regime_gated_backtest_negative",
      "regime_gated_oos_negative",
      "insufficient_regime_sample",
      "fails_normal_volatility_regime",
      "shock_regime_dependency",
    ) ||
    ["headline_only", "thin_regime_sample"].includes(verdict);
  const regimeDependent =
    hasAny(
      "profit_concentrated_single_regime",
      "headline_sharpe_not_regime_robust",
      "regime_gated_backtest_negative",
      "regime_gated_oos_negative",
      "insufficient_regime_sample",
      "high_volatility_only_edge",
      "fails_normal_volatility_regime",
      "shock_regime_dependency",
    ) ||
    dominantRegimeShare >= 0.5 ||
    ["headline_only", "regime_specific", "thin_regime_sample"].includes(verdict);
  const monthDependent = hasAny("profit_concentrated_single_month", "best_trades_dominate") || dominantMonthShare >= 0.45;
  const fragileFolds = hasAny("profits_not_consistent_across_folds", "high_sharpe_weak_folds", "unstable_folds", "weak_oos_economics", "weak_oos_evidence", "no_walk_forward_folds", "one_fold_dependency");
  const scanBias = hasAny("multiple_testing_haircut", "isolated_parameter_peak");
  const crossMarket = hasAny("known_edge_needs_cross_market_validation");
  const crossMarketDiscovery = crossMarket && discoveryMarketCount > 0;
  const costStress = hasAny("negative_after_costs", "costs_overwhelm_edge", "negative_expectancy_after_costs", "high_turnover_cost_drag");
  const syncCosts = hasAny("needs_ig_price_validation", "missing_cost_profile", "missing_spread_slippage");
  const capitalBlocked = hasAny("risk_budget_exceeded", "historical_drawdown_too_large", "historical_daily_loss_stop_breached", "margin_too_large", "insufficient_account_for_margin", "below_ig_min_deal_size", "missing_reference_price", "drawdown_too_high", "ig_minimum_risk_too_large_for_account", "ig_minimum_margin_too_large_for_account");
  const terminalCapitalBlocked = hasAny("ig_minimum_margin_too_large_for_account", "ig_minimum_risk_too_large_for_account");
  const sourceTemplate = {
    ...frozenTemplatePayload(template),
    repair_attempt_count: repairAttempt,
  };
  const canFreezeValidate = Object.keys(sourceTemplate.parameters ?? {}).length > 0;
  const regimeNeedsRepair = hardRegimeRepair || (regimeDependent && !targetRegime);
  const shouldFreezeValidate =
    canFreezeValidate &&
    hasAny("multiple_testing_haircut") &&
    !syncCosts;
  const shouldConfirmFrozenValidation =
    canFreezeValidate &&
    !scanBias &&
    !tooFewTrades &&
    !capitalBlocked &&
    !syncCosts &&
    !costStress &&
    !fragileFolds &&
    !monthDependent &&
    !crossMarket &&
    !regimeNeedsRepair;

  let marketIds = selectedMarket;
  let budget = 54;
  let start = researchRun.start;
  let stress = 2.0;
  let objective = "profit_first";
  let riskProfile = template.risk_profile;
  let repairMode = "auto_refine";
  let includeRegimeScans = false;
  let runTargetRegime = targetRegime;
  let regimeScanBudget = "";
  const excludedMonths = [];

  if (mode === "repair_remaining" && priorRepairAttempt >= MAX_REPAIR_ATTEMPTS) {
    return {
      marketIds,
      runPatch: {},
      steps: [],
      syncCosts: false,
      summary: "Repair limit reached. Freeze validate, incubate, or reject this template instead of continuing to search around the same failures.",
      targetRegime,
      crossMarketDiscovery: false,
      targets: [],
      stageMessage: "Repair limit reached.",
      repairAttempt: priorRepairAttempt,
      priorRepairAttempt,
      stopReason: `Repair limit reached (${priorRepairAttempt}/${MAX_REPAIR_ATTEMPTS}). Freeze validate, incubate, or reject it rather than looping again.`,
    };
  }
  if (mode === "repair_remaining" && warnings.size === 0) {
    return {
      marketIds,
      runPatch: {},
      steps: [],
      syncCosts: false,
      summary: "No remaining blockers were attached to this result.",
      targetRegime,
      crossMarketDiscovery: false,
      targets: [],
      stageMessage: "No remaining blockers.",
      repairAttempt: priorRepairAttempt,
      priorRepairAttempt,
      stopReason: "No remaining blockers were attached. Use Freeze validate or save the template instead of searching again.",
    };
  }
  if (terminalCapitalBlocked) {
    const accountLabel = accountSizeLabel(optionalNumber(researchRun.account_size) ?? WORKING_ACCOUNT_SIZE);
    const issue = hasAny("ig_minimum_margin_too_large_for_account") ? "margin" : "risk";
    return {
      marketIds,
      runPatch: {},
      steps: [],
      syncCosts: false,
      summary: `IG minimum ${issue} is too large for ${accountLabel}; move on to the next market/template or raise testing capital.`,
      targetRegime,
      crossMarketDiscovery: false,
      targets: [],
      stageMessage: `Skipped: IG minimum ${issue} too large for selected capital.`,
      repairAttempt: priorRepairAttempt,
      priorRepairAttempt,
      stopReason: `Skip this one for ${accountLabel}: IG minimum ${issue} is too large, so smaller search parameters cannot make it fit. Move on to the next lead.`,
    };
  }

  addStep(`Repair attempt ${repairAttempt}/${MAX_REPAIR_ATTEMPTS}`);

  if (runTargetRegime) {
    addStep(`Make tradeable target: ${regimeLabel(runTargetRegime)} only`);
    addStep(`Force flat outside ${regimeLabel(runTargetRegime)}`);
    addTarget(`Prove the edge inside ${regimeLabel(runTargetRegime)}`);
  }
  if (syncCosts) {
    addStep("Refresh IG costs, spread, slippage, margin, and minimum stake");
    addTarget("Clear IG validation");
  }
  if (capitalBlocked && !shouldFreezeValidate) {
    budget = 120;
    stress = Math.max(stress, 2.5);
    start = earlierDate(start, longEvidenceStartForTemplate(template));
    objective = "balanced";
    riskProfile = "conservative";
    repairMode = "capital_fit";
    addStep(`Rank by ${accountSizeLabel(optionalNumber(researchRun.account_size) ?? WORKING_ACCOUNT_SIZE)} capital fit first`);
    addStep("Search smaller stakes and tighter stops before profit");
    addTarget(`Fit ${accountSizeLabel(optionalNumber(researchRun.account_size) ?? WORKING_ACCOUNT_SIZE)} risk and IG minimums`);
  }
  if (tooFewTrades && !shouldFreezeValidate) {
    budget = 120;
    start = earlierDate(start, longEvidenceStartForTemplate(template));
    addStep("Deep locked-family retest over longer history");
    addTarget(targetRegime ? "Increase target-regime OOS trades" : "Increase OOS trades");
    if (targetRegime) {
      runTargetRegime = targetRegime;
      includeRegimeScans = false;
      addStep(`Score only ${regimeLabel(targetRegime)} and force flat outside it`);
    } else if (dominantRegime || regimeDependent) {
      includeRegimeScans = true;
      regimeScanBudget = "";
      addStep(dominantRegime ? `Compare more trades with ${regimeLabel(dominantRegime)} specialists` : "Compare more trades with regime specialists");
    }
  }
  if (regimeDependent && !includeRegimeScans && !shouldFreezeValidate) {
    if (targetRegime) {
      runTargetRegime = targetRegime;
      addStep(`Keep scoring locked to ${regimeLabel(targetRegime)} only`);
    } else {
      includeRegimeScans = true;
      regimeScanBudget = "";
      addStep(dominantRegime ? `Run regime-gated specialists around ${regimeLabel(dominantRegime)}` : "Run regime-gated specialist checks");
    }
  }
  if (monthDependent && dominantMonth && !shouldFreezeValidate) {
    excludedMonths.push(dominantMonth);
    addStep(`Exclude ${dominantMonth} and require the edge to survive`);
    addTarget("Reduce best-month dependence");
  }
  if (fragileFolds && !shouldFreezeValidate) {
    budget = Math.max(budget, 120);
    start = earlierDate(start, longEvidenceStartForTemplate(template));
    addStep("Use longer-history fold and OOS evidence");
    addTarget("Improve fold consistency");
  }
  if (scanBias && !shouldFreezeValidate) {
    stress = Math.max(stress, 2.5);
    addStep("Use evidence-first ranking to reduce scan bias");
    addTarget("Reduce scan bias");
  }
  if (crossMarketDiscovery && !shouldFreezeValidate) {
    stress = Math.max(stress, 2.5);
    addStep("Keep this refinement on the source market only");
    addStep("Use Find similar elsewhere as a separate discovery run");
  }
  if (costStress && !shouldFreezeValidate) {
    stress = Math.max(stress, 3.0);
    addStep("Stress costs before trusting the net edge");
    addTarget("Survive higher costs");
  }
  if (shouldFreezeValidate || shouldConfirmFrozenValidation) {
    budget = 1;
    stress = Math.max(stress, 2.5);
    start = earlierDate(start, longEvidenceStartForTemplate(template));
    repairMode = "frozen_validation";
    includeRegimeScans = false;
    regimeScanBudget = "";
    addStep("Freeze exact template parameters");
    addStep(shouldFreezeValidate ? "Clear multiple-testing with no parameter hunting" : "Retest without parameter hunting");
    addTarget("Pass frozen validation");
  }
  if (steps.length === 0) {
    addStep("Focused locked-family confirmation");
    addTarget("Confirm the template without changing the market");
  }

  const runPatch = {
    market_id: marketIds[0] || researchRun.market_id,
    interval: marketIds.length > 1 ? "market_default" : template.interval,
    start,
    end: researchRun.end,
    trading_style: template.style,
    objective,
    risk_profile: riskProfile,
    search_preset: budget >= 120 ? "deep" : "balanced",
    search_budget: String(budget),
    strategy_families: family,
    cost_stress_multiplier: stress,
    include_regime_scans: includeRegimeScans && !runTargetRegime,
    regime_scan_budget_per_regime: regimeScanBudget,
    target_regime: runTargetRegime,
    excluded_months: uniqueMonths(excludedMonths),
    repair_mode: repairMode,
    account_size: researchRun.account_size || String(WORKING_ACCOUNT_SIZE),
    source_template: sourceTemplate,
    day_trading_mode: isDayTrading,
    force_flat_before_close: isDayTrading,
    paper_queue_limit: researchRun.paper_queue_limit || "3",
    review_queue_limit: researchRun.review_queue_limit || "10",
  };
  const summary = repairMode === "frozen_validation"
    ? "Frozen validation retests the exact template with no parameter search."
    : runTargetRegime
    ? `Regime-specific refine: results are scored inside ${regimeLabel(runTargetRegime)} with full-history gated evidence kept alongside.`
    : tooFewTrades && includeRegimeScans
    ? "Tests both the broader trade-count repair and the winning-regime specialist path."
    : crossMarketDiscovery
    ? "Keeps this template on its source market; other markets are separate discovery leads."
    : "Chooses the next repair run from the active promotion blockers.";
  const stagePrefix = mode === "repair_remaining" ? "Repair remaining" : "Make tradeable";
  return {
    marketIds,
    runPatch,
    steps: steps.slice(0, 6),
    syncCosts,
    summary,
    targetRegime: runTargetRegime,
    crossMarketDiscovery,
    targets: targets.slice(0, 5),
    stageMessage: `${stagePrefix} staged: ${steps.slice(0, 3).join("; ")}.`,
    repairAttempt,
    priorRepairAttempt,
  };
}

function frozenValidationPlanForTemplate(template, researchRun, activeMarketIds = []) {
  const sourceTemplate = frozenTemplatePayload(template);
  const marketId = String(template.market_id || activeMarketIds[0] || researchRun.market_id || "").trim();
  const marketIds = marketId ? [marketId] : [];
  const targetRegime = templateSpecificRegime(template);
  const attempt = repairAttemptCountForTemplate(template);
  const isDayTrading = Boolean(template.day_trading_mode || template.parameters?.day_trading_mode || researchRun.day_trading_mode);
  if (!sourceTemplate.parameters || Object.keys(sourceTemplate.parameters).length === 0) {
    return {
      marketIds,
      runPatch: {},
      steps: [],
      syncCosts: false,
      summary: "Frozen validation needs exact saved parameters first.",
      targetRegime,
      crossMarketDiscovery: false,
      targets: [],
      stageMessage: "Frozen validation unavailable.",
      repairAttempt: attempt,
      priorRepairAttempt: attempt,
      stopReason: "Frozen validation needs exact saved parameters. Use a trial/candidate with parameters or save the template first.",
    };
  }
  const family = template.family ? [template.family] : [];
  const stress = Math.max(2.5, Number(researchRun.cost_stress_multiplier || 2.0));
  return {
    marketIds,
    runPatch: {
      market_id: marketId || researchRun.market_id,
      interval: template.interval || researchRun.interval,
      start: longEvidenceStartForTemplate(template),
      end: researchRun.end,
      trading_style: template.style || researchRun.trading_style,
      objective: "profit_first",
      risk_profile: template.risk_profile || researchRun.risk_profile,
      search_preset: "balanced",
      search_budget: "1",
      strategy_families: family,
      cost_stress_multiplier: stress,
      include_regime_scans: false,
      regime_scan_budget_per_regime: "",
      target_regime: targetRegime,
      excluded_months: [],
      repair_mode: "frozen_validation",
      account_size: researchRun.account_size || String(WORKING_ACCOUNT_SIZE),
      source_template: sourceTemplate,
      day_trading_mode: isDayTrading,
      force_flat_before_close: isDayTrading,
      paper_queue_limit: researchRun.paper_queue_limit || "3",
      review_queue_limit: researchRun.review_queue_limit || "10",
    },
    steps: [
      "Freeze exact template parameters",
      targetRegime ? `Score only ${regimeLabel(targetRegime)}` : "Retest full-period evidence",
      "Run one no-search validation trial",
    ],
    syncCosts: false,
    summary: "Frozen validation retests the exact template with no parameter search.",
    targetRegime,
    crossMarketDiscovery: false,
    targets: ["Pass frozen validation"],
    stageMessage: "Frozen validation staged.",
    repairAttempt: attempt,
    priorRepairAttempt: attempt,
  };
}

function earlierDate(current, candidate) {
  if (!current) {
    return candidate;
  }
  return String(current) < String(candidate) ? current : candidate;
}

function longEvidenceStartForTemplate(template = {}) {
  const family = String(template.family || template.parameters?.family || "");
  const interval = intervalValue(template.interval || template.parameters?.timeframe || template.parameters?.interval);
  if (interval === "1day" || ["calendar_turnaround_tuesday", "month_end_seasonality", "everyday_long"].includes(family)) {
    return "2020-01-01";
  }
  return "2024-01-01";
}

function readinessIssueCodes(readiness) {
  return [...arrayValue(readiness.blockers), ...arrayValue(readiness.validation_warnings)];
}

function nextActionLabel(action) {
  return {
    rerun_with_fresh_diagnostics: "Next: rerun with fresh diagnostics.",
    sync_ig_costs_and_validate_prices: "Next: sync IG costs and validate prices.",
    reject_or_rework_cost_edge: "Next: reject or rework the cost edge.",
    retest_or_reject_fragile_edge: "Next: retest or reject fragile evidence.",
    paper_track: "Next: 30-day paper tracking.",
  }[action] ?? "Next: research review.";
}

function repairModeLabel(value) {
  return {
    standard: "Standard search",
    focused_retest: "Focused retest",
    evidence_first: "Evidence first",
    capital_fit: "Capital fit",
    more_trades: "More trades",
    cost_stress: "Cost stress",
    cross_market: "Cross-market",
    cross_market_discovery: "Similar-edge discovery",
    regime_repair: "Regime repair",
    month_exclusion: "Month exclusion",
    longer_history: "Longer history",
    auto_refine: "Auto-refine",
    frozen_validation: "Frozen validation",
  }[value] ?? value ?? "Standard search";
}

function runPurpose(run = {}) {
  const purpose = run.run_purpose ?? {};
  const repairMode = purpose.repair_mode ?? "standard";
  const kind = purpose.kind
    ?? (repairMode === "frozen_validation" ? "frozen_validation" : repairMode === "capital_fit" ? "capital_fit" : "backtest");
  return { ...purpose, kind, repair_mode: repairMode };
}

function runPurposeLabel(run = {}) {
  const purpose = runPurpose(run);
  return {
    day_trading_factory: "Intraday discovery",
    backtest: "Backtest",
    regime_scan: "Regime scan",
    repair: "Make tradeable",
    capital_fit: "Size / capital fit",
    frozen_validation: "Frozen validation",
    cross_market: "Discovery",
  }[purpose.kind] ?? repairModeLabel(purpose.repair_mode);
}

function runPurposeClass(run = {}) {
  const purpose = runPurpose(run);
  return `run-${purpose.kind || "backtest"}`;
}

function runPurposeSubLabel(run = {}) {
  const purpose = runPurpose(run);
  const bits = [
    purpose.day_trading_mode ? "intraday only" : "",
    repairModeLabel(purpose.repair_mode),
    purpose.trading_style ? strategyFamilyLabel(purpose.trading_style) : "",
    purpose.target_regime ? `${regimeLabel(purpose.target_regime)} only` : "",
    purpose.source_template_name ? `from ${purpose.source_template_name}` : "",
  ].filter(Boolean);
  return [...new Set(bits)].slice(0, 3).join(" · ");
}

function runPurposeDetail(run = {}) {
  const label = runPurposeLabel(run);
  const detail = runPurposeSubLabel(run);
  return detail ? `${label}: ${detail}` : label;
}

function calendarRiskLabel(value) {
  return {
    clear: "Calendar risk clear",
    watch: "Calendar risk on watch",
    elevated: "Elevated calendar risk",
    high: "High calendar risk",
    unavailable: "Calendar unavailable",
  }[value] ?? "Calendar context";
}

function calendarRiskClass(value) {
  if (value === "clear") {
    return "good";
  }
  if (value === "watch" || value === "elevated" || value === "high") {
    return "warn";
  }
  return "base";
}

function calendarPolicyLabel(value) {
  return {
    avoid_event_window: "Avoid event window",
    avoid_major_event_days: "Avoid event days",
    reduce_or_avoid_event_window: "Reduce/avoid events",
    none: "No filter",
    unavailable: "Unavailable",
  }[value] ?? value ?? "No filter";
}

function accountFeasibility(scenarios = [], accountSize = WORKING_ACCOUNT_SIZE) {
  const scenario = accountScenarioFor(scenarios, accountSize);
  if (!scenario) {
    return "Unknown";
  }
  if (scenario.feasible) {
    return "OK";
  }
  const reasons = capitalBlockReasons(scenario);
  if (reasons.length === 0) {
    return "Blocked";
  }
  const shown = reasons.slice(0, 2).join(" + ");
  const overflow = reasons.length > 2 ? ` +${reasons.length - 2}` : "";
  return `Blocked: ${shown}${overflow}`;
}

function accountScenarioFor(scenarios = [], accountSize = WORKING_ACCOUNT_SIZE) {
  return (scenarios ?? []).find((item) => Number(item.account_size) === Number(accountSize));
}

function testingAccountSizeForSource(source = {}) {
  const parameters = source.parameters ?? source.audit?.candidate?.parameters ?? {};
  const searchAudit = parameters.search_audit ?? {};
  return optionalNumber(parameters.testing_account_size) || optionalNumber(searchAudit.testing_account_size) || WORKING_ACCOUNT_SIZE;
}

function accountSizeLabel(value) {
  const number = Number(value ?? WORKING_ACCOUNT_SIZE);
  if (!Number.isFinite(number) || number <= 0) {
    return "£3k";
  }
  if (number >= 1000 && number % 1000 === 0) {
    return `£${number / 1000}k`;
  }
  return formatMoney(number);
}

function capitalBlockReasons(scenario = {}) {
  return arrayValue(scenario.violations).map((violation) => capitalBlockReason(violation, scenario)).filter(Boolean);
}

function capitalBlockReason(violation, scenario = {}) {
  const accountSize = Number(scenario.account_size ?? WORKING_ACCOUNT_SIZE);
  const maxMarginFraction = Number(scenario.max_margin_fraction ?? MAX_MARGIN_FRACTION);
  const marginCap = accountSize * maxMarginFraction;
  const values = {
    risk: formatMoney(scenario.estimated_stop_loss),
    riskBudget: formatMoney(scenario.risk_budget),
    margin: formatMoney(scenario.estimated_margin),
    marginLimit: formatMoney(marginCap),
    account: formatMoney(accountSize),
    drawdown: formatMoney(scenario.historical_max_drawdown),
    drawdownLimit: formatMoney(accountSize * 0.25),
    dailyLoss: formatMoney(scenario.worst_daily_loss),
    dailyLimit: formatMoney(scenario.daily_loss_limit),
    minStake: round(scenario.min_deal_size),
    stake: round(scenario.requested_stake),
  };
  return {
    missing_reference_price: "missing price",
    below_ig_min_deal_size: `IG min ${values.minStake} > stake ${values.stake}`,
    risk_budget_exceeded: `risk ${values.risk} > ${values.riskBudget}`,
    margin_too_large: `margin ${values.margin} > ${values.marginLimit}`,
    insufficient_account_for_margin: `margin ${values.margin} > ${values.account}`,
    ig_minimum_risk_too_large_for_account: `IG min risk ${values.risk} > ${values.riskBudget}`,
    ig_minimum_margin_too_large_for_account: `IG min margin ${values.margin} > ${values.marginLimit}`,
    historical_drawdown_too_large: `drawdown ${values.drawdown} > ${values.drawdownLimit}`,
    historical_daily_loss_stop_breached: `daily loss ${values.dailyLoss} > ${values.dailyLimit}`,
  }[violation] ?? humanWarnings([violation])[0];
}

function optionalNumber(value) {
  if (value === "" || value === null || value === undefined) {
    return null;
  }
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? number : null;
}

function humanWarnings(warnings = []) {
  const labels = {
    too_few_trades: "Too few trades",
    negative_after_costs: "Negative after costs",
    costs_overwhelm_edge: "Costs overwhelm edge",
    weak_net_cost_efficiency: "Weak net/cost efficiency",
    high_turnover_cost_drag: "High-turnover cost drag",
    negative_expectancy_after_costs: "Negative expectancy after costs",
    weak_sharpe: "Weak Sharpe",
    short_sharpe_sample: "Short Sharpe sample",
    limited_sharpe_sample: "Limited Sharpe sample",
    legacy_sharpe_diagnostics: "Needs fresh Sharpe run",
    missing_cost_profile: "Missing cost profile",
    missing_spread_slippage: "Missing spread/slippage",
    drawdown_too_high: "Drawdown too high",
    fails_higher_slippage: "Fails higher slippage",
    profits_not_consistent_across_folds: "Fragile folds",
    unstable_folds: "Unstable folds",
    low_oos_trades: "Low OOS trades",
    no_walk_forward_folds: "No walk-forward folds",
    weak_oos_economics: "Weak OOS economics",
    weak_oos_evidence: "Weak OOS evidence",
    one_fold_dependency: "One fold dominates",
    funding_eats_swing_edge: "Funding eats swing edge",
    needs_ig_price_validation: "Needs IG price validation",
    not_paper_ready_research_lead: "Research lead only",
    calendar_effect_needs_longer_history: "Needs longer calendar history",
    calendar_dependent_edge: "Calendar-dependent edge",
    calendar_filtered_oos_negative: "Calendar-filtered OOS negative",
    calendar_sample_too_thin: "Thin calendar-event sample",
    calendar_blackout_improves_result: "Calendar blackout helps",
    calendar_history_partial: "Partial calendar history",
    event_strategy_requires_label: "Needs event-strategy label",
    major_event_window_dependency: "Event-window dependency",
    known_edge_needs_cross_market_validation: "Needs independent market check",
    high_sharpe_low_trade_count: "High Sharpe, low trades",
    high_sharpe_short_sample: "High Sharpe, short sample",
    high_sharpe_weak_folds: "High Sharpe, weak folds",
    isolated_parameter_peak: "Isolated parameter peak",
    costs_small_vs_turnover: "Costs small vs turnover",
    multiple_testing_haircut: "Multiple-testing haircut",
    best_trades_dominate: "Best trades dominate",
    fails_normal_volatility_regime: "Fails normal-vol regime",
    high_volatility_only_edge: "High-vol only edge",
    headline_sharpe_not_regime_robust: "Headline Sharpe not robust",
    insufficient_regime_sample: "Thin regime sample",
    profit_concentrated_single_month: "Single-month profit",
    profit_concentrated_single_regime: "Single-regime profit",
    regime_gated_backtest_negative: "Gated backtest negative",
    regime_gated_oos_negative: "Gated OOS negative",
    shock_regime_dependency: "Shock-regime dependency",
    target_regime_low_oos_trades: "Target-regime low OOS trades",
    below_ig_min_deal_size: "Below IG min stake",
    day_trade_forbidden_overnight_family: "Overnight family blocked",
    day_trade_held_overnight: "Held overnight",
    day_trade_missing_flat_policy: "Missing flat policy",
    day_trade_requires_intraday_bars: "Needs intraday bars",
    diagnostics_deferred_fast_scan: "Fast-scan diagnostics deferred",
    ig_minimum_risk_too_large_for_account: "IG min risk too large",
    ig_minimum_margin_too_large_for_account: "IG min margin too large",
    historical_daily_loss_stop_breached: "Daily loss stop breached",
    historical_drawdown_too_large: "Historical drawdown too large",
    risk_budget_exceeded: "Risk budget exceeded",
    margin_too_large: "Margin too large",
    insufficient_account_for_margin: "Insufficient margin",
    missing_reference_price: "Missing reference price",
    stop_required_for_risk_preview: "Stop required",
    stop_not_below_entry: "Stop must be below entry",
    stop_not_above_entry: "Stop must be above entry",
    stop_distance_below_ig_minimum: "Stop below IG minimum",
    limit_not_above_entry: "Limit must be above entry",
    limit_not_below_entry: "Limit must be below entry",
    limit_distance_below_ig_minimum: "Limit below IG minimum",
    stake_must_be_positive: "Stake must be positive",
    missing_price_or_stake: "Missing price or stake",
    invalid_side: "Invalid side",
  };
  return (warnings ?? []).map((warning) => labels[warning] ?? warning);
}

function warningChipClass(warning) {
  return `warning-chip ${warningSeverity(warning)}`;
}

function warningChipTitle(warning) {
  return {
    blocker: "Paper promotion blocker: fix this before paper trading.",
    repair: "Needs repair or stronger evidence before promotion.",
    specialist: "Specialist identity: acceptable only if the template is explicitly gated to that regime.",
    info: "Diagnostic warning.",
  }[warningSeverity(warning)];
}

function warningSeverity(warning) {
  const code = String(warning);
  if (PAPER_BLOCKER_WARNINGS.has(code)) {
    return "blocker";
  }
  if (SPECIALIST_WARNINGS.has(code)) {
    return "specialist";
  }
  if (REPAIR_WARNINGS.has(code)) {
    return "repair";
  }
  return "info";
}

const PAPER_BLOCKER_WARNINGS = new Set([
  "below_ig_min_deal_size",
  "calendar_dependent_edge",
  "calendar_filtered_oos_negative",
  "calendar_sample_too_thin",
  "costs_overwhelm_edge",
  "day_trade_forbidden_overnight_family",
  "day_trade_held_overnight",
  "day_trade_missing_flat_policy",
  "day_trade_requires_intraday_bars",
  "diagnostics_deferred_fast_scan",
  "drawdown_too_high",
  "event_strategy_requires_label",
  "historical_daily_loss_stop_breached",
  "historical_drawdown_too_large",
  "ig_minimum_margin_too_large_for_account",
  "ig_minimum_risk_too_large_for_account",
  "insufficient_account_for_margin",
  "legacy_sharpe_diagnostics",
  "limited_sharpe_sample",
  "low_oos_trades",
  "margin_too_large",
  "missing_cost_profile",
  "missing_reference_price",
  "missing_spread_slippage",
  "needs_ig_price_validation",
  "negative_after_costs",
  "negative_expectancy_after_costs",
  "no_walk_forward_folds",
  "regime_gated_backtest_negative",
  "regime_gated_oos_negative",
  "major_event_window_dependency",
  "risk_budget_exceeded",
  "short_sharpe_sample",
  "target_regime_low_oos_trades",
  "too_few_trades",
  "weak_oos_economics",
  "weak_oos_evidence",
]);

const REPAIR_WARNINGS = new Set([
  "best_trades_dominate",
  "calendar_blackout_improves_result",
  "calendar_effect_needs_longer_history",
  "calendar_history_partial",
  "fails_higher_slippage",
  "funding_eats_swing_edge",
  "headline_sharpe_not_regime_robust",
  "high_sharpe_low_trade_count",
  "high_sharpe_short_sample",
  "high_sharpe_weak_folds",
  "insufficient_regime_sample",
  "isolated_parameter_peak",
  "known_edge_needs_cross_market_validation",
  "multiple_testing_haircut",
  "one_fold_dependency",
  "profit_concentrated_single_month",
  "profits_not_consistent_across_folds",
  "unstable_folds",
  "weak_net_cost_efficiency",
]);

const SPECIALIST_WARNINGS = new Set([
  "fails_normal_volatility_regime",
  "high_volatility_only_edge",
  "profit_concentrated_single_regime",
  "shock_regime_dependency",
]);

function regimeLabel(value) {
  return {
    shock_event: "Shock",
    rebound_after_selloff: "Rebound",
    high_volatility: "High vol",
    trend_up: "Trend up",
    trend_down: "Trend down",
    range_chop: "Range/chop",
    low_volatility: "Low vol",
    normal: "Normal",
    unknown: "Unknown",
  }[value] ?? value ?? "n/a";
}

function regimeVerdictLabel(value) {
  return {
    tradeable_across_regimes: "Tradeable",
    regime_tradeable: "Regime tradeable",
    regime_specific: "Regime-specific",
    headline_only: "Headline only",
    thin_regime_sample: "Thin sample",
    research_only: "Research only",
    unavailable: "Unavailable",
  }[value] ?? value ?? "n/a";
}

function gatedMetricLabel(pattern = {}, fallback, targeted) {
  return pattern?.target_regime ? targeted : fallback;
}

function effectiveSearchBudget(presetId, budget, marketCount, manualBudget) {
  if (manualBudget || marketCount <= 1) {
    return budget;
  }
  const totalCap = MULTI_MARKET_TOTAL_TRIAL_CAPS[presetId] ?? MULTI_MARKET_TOTAL_TRIAL_CAPS.balanced;
  const minimum = MULTI_MARKET_MIN_TRIALS_PER_MARKET[presetId] ?? MULTI_MARKET_MIN_TRIALS_PER_MARKET.balanced;
  return Math.min(budget, Math.max(minimum, Math.floor(totalCap / Math.max(1, marketCount))));
}

function regimeCountsLabel(counts = {}) {
  const entries = Object.entries(counts ?? {}).sort((left, right) => Number(right[1]) - Number(left[1])).slice(0, 4);
  if (entries.length === 0) {
    return "No regime labels yet";
  }
  return entries.map(([regime, count]) => `${regimeLabel(regime)} ${count}d`).join(" · ");
}

function scoreBasisLabel(audit = {}) {
  const parts = [];
  if (audit?.grade_mode === "target_regime" && audit.grade_regime) {
    parts.push(`graded on ${regimeLabel(audit.grade_regime)}`);
  }
  if (audit?.testing_account_size) {
    parts.push(`${accountSizeLabel(audit.testing_account_size)} paper score`);
  }
  return parts.join(" · ");
}

function discoveryBadgeLabel(audit = {}) {
  if (audit?.day_trading_mode) {
    return "Day trade";
  }
  if (audit?.repair_mode === "cross_market_discovery") {
    return "Discovery lead";
  }
  return "";
}

function regimePresetBudget(preset) {
  return { quick: 6, balanced: 12, deep: 18 }[preset] ?? 12;
}

function strategyFamilyLabel(value) {
  return {
    calendar_turnaround_tuesday: "Turnaround Tuesday",
    month_end_seasonality: "Turn of month",
    intraday_trend: "Intraday trend",
    swing_trend: "Swing trend",
    mean_reversion: "Mean reversion",
    volatility_expansion: "Volatility expansion",
    liquidity_sweep_reversal: "Liquidity sweep",
    everyday_long: "Everyday long",
    scalping: "Scalping",
    breakout: "Breakout",
    research_ideas: "Known research ideas",
    find_anything_robust: "Find anything robust",
  }[value] ?? value;
}

function readableSnake(value) {
  return String(value || "").replace(/_/g, " ");
}

function researchRecipeLabel(value) {
  return {
    turnaround_tuesday_after_down_previous_session: "Tests Tuesday rebounds after a down prior session",
    turn_of_month_long_bias: "Tests turn-of-month long bias",
    everyday_long_bias: "Tests always-long exposure as a benchmark, or only inside the selected regime when regime-gated",
  }[value] ?? "";
}

function labelForKind(kind) {
  return {
    best_balanced: "Best balanced",
    highest_sharpe: "Highest Sharpe",
    highest_profit: "Highest profit",
  }[kind] ?? kind;
}

function formatMoney(value) {
  const number = Number(value ?? 0);
  const prefix = number < 0 ? "-£" : "£";
  const absolute = Math.abs(number);
  const decimals = absolute > 0 && absolute < 10 ? 2 : 0;
  return `${prefix}${absolute.toFixed(decimals)}`;
}

function formatLargeMoney(value) {
  const number = Number(value ?? 0);
  if (!Number.isFinite(number) || number <= 0) {
    return "n/a";
  }
  if (number >= 1_000_000_000) {
    return `£${round(number / 1_000_000_000)}bn`;
  }
  if (number >= 1_000_000) {
    return `£${round(number / 1_000_000)}m`;
  }
  return formatMoney(number);
}

function formatLargeNumber(value) {
  const number = Number(value ?? 0);
  if (!Number.isFinite(number) || number <= 0) {
    return "n/a";
  }
  if (number >= 1_000_000) {
    return `${round(number / 1_000_000)}m`;
  }
  if (number >= 1_000) {
    return `${round(number / 1_000)}k`;
  }
  return String(Math.round(number));
}

function formatRatio(value) {
  const number = Number(value ?? 0);
  return `${number.toFixed(2)}x`;
}

function shortPlaybookLabel(playbook = {}) {
  const label = String(playbook?.label ?? "");
  return {
    "Opening range breakout": "OR breakout",
    "VWAP trend pullback": "VWAP pullback",
    "Failed breakout reversal": "Failed break",
    "High relative-volume trend": "High RVol",
    "Frozen signal confirmation": "Signal confirm",
  }[label] ?? label;
}

function percent(value) {
  if (value === undefined || value === null || Number.isNaN(Number(value))) {
    return "0%";
  }
  return `${Math.round(Number(value) * 100)}%`;
}

function round(value) {
  const number = Number(value ?? 0);
  return Number.isInteger(number) ? number : number.toFixed(2);
}

createRoot(document.getElementById("root")).render(<App />);
