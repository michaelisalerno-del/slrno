import React from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  Archive,
  BarChart3,
  BookOpen,
  Database,
  Download,
  Home,
  KeyRound,
  LineChart,
  LockKeyhole,
  Plug,
  RefreshCw,
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
  getBacktestsSummary,
  getBrokerSummary,
  getCockpitSummary,
  getIgCostProfile,
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
  installMarketPlugin,
  previewBrokerOrder,
  pruneMarketDataCache,
  researchRunExportUrl,
  saveEodhd,
  saveIg,
  saveMarket,
  saveResearchSchedule,
  syncIgCosts,
} from "./api";
import "./styles.css";

const WORKING_ACCOUNT_SIZE = 3000;

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

const STYLE_OPTIONS = [
  { id: "find_anything_robust", label: "Find anything robust" },
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

const MODULES = [
  ["cockpit", "Cockpit", Home],
  ["guide", "Guide", BookOpen],
  ["backtests", "Backtests", BarChart3],
  ["research", "Research", Sparkles],
  ["paper", "Paper Trading", LineChart],
  ["broker", "Broker", Wallet],
  ["risk", "Risk", LockKeyhole],
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
  const [candidates, setCandidates] = React.useState([]);
  const [critique, setCritique] = React.useState(null);
  const [runDetail, setRunDetail] = React.useState(null);
  const [costProfiles, setCostProfiles] = React.useState({});
  const [refinementTemplate, setRefinementTemplate] = React.useState(null);
  const [message, setMessage] = React.useState("");
  const [eodhdKey, setEodhdKey] = React.useState("");
  const [ig, setIg] = React.useState({ apiKey: "", username: "", password: "", accountId: "" });
  const [activeModule, setActiveModule] = React.useState("cockpit");
  const [loadingModule, setLoadingModule] = React.useState("");
  const [cockpit, setCockpit] = React.useState(null);
  const [paper, setPaper] = React.useState(null);
  const [broker, setBroker] = React.useState(null);
  const [risk, setRisk] = React.useState(null);
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
  const [activeTab, setActiveTab] = React.useState("builder");
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
    cost_stress_multiplier: 2.0,
    include_regime_scans: false,
    regime_scan_budget_per_regime: "",
    target_regime: "",
    excluded_months: [],
    repair_mode: "standard",
    account_size: String(WORKING_ACCOUNT_SIZE),
  });
  const [researchState, setResearchState] = React.useState({ status: "idle", detail: "Ready.", progress: 0 });
  const activePollRunIdRef = React.useRef(null);

  const eodhdStatus = providerStatus(status, "eodhd");
  const igStatus = providerStatus(status, "ig");
  const enabledMarkets = markets.filter((item) => item.enabled);
  const selectedMarkets = enabledMarkets.filter((item) => activeMarketIds.includes(item.market_id));
  const selectedEngine = engines.find((engine) => engine.id === researchRun.engine) ?? engines[0] ?? FALLBACK_ENGINES[0];
  const selectedPreset = SEARCH_PRESETS.find((preset) => preset.id === researchRun.search_preset) ?? SEARCH_PRESETS[1];
  const refinementRepairActions = refinementTemplate ? repairActionsForTemplate(refinementTemplate) : [];
  const autoRefinementPlan = refinementTemplate
    ? autoRefinementPlanForTemplate(refinementTemplate, researchRun, enabledMarkets, activeMarketIds)
    : null;

  const loadModule = React.useCallback(async (moduleId = activeModule) => {
    setLoadingModule(moduleId);
    try {
      if (moduleId === "cockpit") {
        const summary = await getCockpitSummary();
        setCockpit(summary);
        setStatus(summary.providers ?? []);
      } else if (moduleId === "research") {
        const summary = await getResearchSummary();
        setCandidates(summary.candidates ?? []);
        setCritique(summary.critique ?? null);
        getResearchCritique().then(setCritique).catch(() => undefined);
      } else if (moduleId === "backtests") {
        const [nextStatus, nextMarkets, nextPlugins, nextCacheStatus, summary] = await Promise.all([
          getStatus(),
          getMarkets(),
          getMarketPlugins(),
          getMarketDataCacheStatus().catch(() => null),
          getBacktestsSummary(includeArchivedRuns),
        ]);
        setStatus(nextStatus);
        setMarkets(nextMarkets);
        setPlugins(nextPlugins);
        setCacheStatus(nextCacheStatus);
        setEngines((summary.engines ?? []).length ? summary.engines : FALLBACK_ENGINES);
        setSpreadBetEngines(summary.spread_bet_engines ?? []);
        setResearchRuns(summary.runs ?? []);
        resumeLatestActiveRun(summary.runs ?? []);
      } else if (moduleId === "paper") {
        setPaper(await getPaperSummary());
      } else if (moduleId === "broker") {
        const [summary, nextMarkets] = await Promise.all([getBrokerSummary(), getMarkets()]);
        setBroker(summary);
        setStatus(summary.providers ?? []);
        setMarkets(nextMarkets);
      } else if (moduleId === "risk") {
        setRisk(await getRiskSummary());
      } else if (moduleId === "settings") {
        const summary = await getSettingsSummary();
        setStatus(summary.providers ?? []);
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
    if (activeModule === "backtests" && !["builder", "results"].includes(activeTab)) {
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

  async function syncCosts() {
    const market_ids = activeMarketIds.length ? activeMarketIds : enabledMarkets.map((item) => item.market_id);
    setMessage("Syncing IG cost profiles...");
    const result = await syncIgCosts({ market_ids });
    const next = { ...costProfiles };
    for (const profile of result.profiles) {
      next[profile.market_id] = profile;
    }
    setCostProfiles(next);
    setMessage(`Synced ${result.profile_count} IG cost profiles.`);
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
    const regimeScanNote = runConfig.include_regime_scans ? " plus capped regime-specialist scans" : "";
    const targetRegimeNote = runConfig.target_regime ? `, ${regimeLabel(runConfig.target_regime)} only` : "";
    const speedNote = effectiveBudget < budget ? " (auto-capped for multi-market speed)" : "";
    setMessage(launchMessage);
    setResearchState({
      status: "running",
      detail: `${engine.label}: ${effectiveBudget} strategy trials per market, ${plannedTrials} base total${speedNote}${regimeScanNote}${targetRegimeNote}, graded on ${accountSizeLabel(testingCapital)}.`,
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
        product_mode: "spread_bet",
      });
      setActiveTab("results");
      setMessage(`Run ${result.run_id} started: ${budget} base trials per market${regimeScanNote}.`);
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
    applyAutoRefinementPlan(autoRefinementPlan);
    setMessage(autoRefinementPlan.stageMessage);
  }

  async function runAutoRefinement() {
    if (!autoRefinementPlan) {
      return;
    }
    const runConfig = applyAutoRefinementPlan(autoRefinementPlan);
    try {
      if (autoRefinementPlan.syncCosts) {
        setMessage("Auto-refine: syncing IG cost profiles...");
        const result = await syncIgCosts({ market_ids: autoRefinementPlan.marketIds });
        setCostProfiles((current) => {
          const next = { ...current };
          for (const profile of result.profiles ?? []) {
            next[profile.market_id] = profile;
          }
          return next;
        });
      }
      await launchResearchRun(runConfig, autoRefinementPlan.marketIds, "Launching auto-refine run...");
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
    }));
    setActiveTab("builder");
    setMessage(`Refining ${source.strategy_name} on ${marketId || "selected market"}.`);
  }

  async function refineFurther(source) {
    const template = refinementTemplateFromSource(source, researchRun);
    const plan = autoRefinementPlanForTemplate(template, researchRun, enabledMarkets, template.market_id ? [template.market_id] : activeMarketIds);
    const runConfig = { ...researchRun, ...plan.runPatch };
    setRefinementTemplate(template);
    setActiveMarketIds(plan.marketIds);
    for (const marketId of plan.marketIds) {
      loadCostProfile(marketId).catch(() => undefined);
    }
    setResearchRun(runConfig);
    setActiveTab("builder");
    if (!plan.syncCosts) {
      setMessage(`Refine further staged: ${plan.steps.slice(0, 3).join("; ")}.`);
      return;
    }
    try {
      setMessage("Refine further: syncing IG cost profiles...");
      const result = await syncIgCosts({ market_ids: plan.marketIds });
      setCostProfiles((current) => {
        const next = { ...current };
        for (const profile of result.profiles ?? []) {
          next[profile.market_id] = profile;
        }
        return next;
      });
      setMessage(`Refine further staged and synced ${result.profile_count ?? 0} IG cost profile${result.profile_count === 1 ? "" : "s"}.`);
    } catch (error) {
      setMessage(`Refine further staged, but IG validation still needs attention: ${error.message}`);
    }
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
      recipe: researchRecipeLabel(parameters.research_recipe),
      warnings: warningCodesForSource(source),
      readiness: source.audit?.promotion_readiness ?? null,
      pattern,
      evidence: evidenceProfileForSource(source),
      backtest: source.audit?.backtest ?? source.backtest ?? {},
      parameters,
    };
  }

  function clearRefinementTemplate() {
    setRefinementTemplate(null);
    setResearchRun((current) => ({ ...current, strategy_families: [], excluded_months: [], target_regime: "" }));
  }

  function applyRobustnessPreset(preset) {
    if (!refinementTemplate) {
      return;
    }
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
          <p>Trading cockpit, research evidence, and paper-only validation.</p>
        </div>
        <div className="mode"><ShieldCheck size={18} /> Live orders locked</div>
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

      {activeModule === "cockpit" && <CockpitView summary={cockpit} setActiveModule={setActiveModule} />}
      {activeModule === "guide" && <GuideView setActiveModule={setActiveModule} />}

      {activeModule === "research" && (
        <section className="lab-shell">
          <div className="lab-header">
            <div>
              <h2><Sparkles size={20} /> Research Pipeline</h2>
              <p>Candidate readiness, capital feasibility, validation blockers, and next actions.</p>
            </div>
            <button type="button" className="secondary" onClick={() => loadModule("research")}><RefreshCw size={16} /> Refresh</button>
          </div>
          <CandidateView candidates={candidates} critique={critique} onRefineTemplate={refineTemplate} onRefineFurther={refineFurther} />
        </section>
      )}

      {activeModule === "backtests" && (
        <>
          <section className="lab-shell">
            <div className="lab-header">
              <div>
                <h2><BarChart3 size={20} /> Backtests</h2>
                <p>Build runs, inspect evidence, archive noise, and export bundles for analysis.</p>
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
            <div className="tabs">
              {[
                ["builder", "New Test"],
                ["results", "Runs"],
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
                            <strong><Sparkles size={16} /> Auto-refine plan</strong>
                            <span>{autoRefinementPlan.summary}</span>
                          </div>
                          <div className="badge-group">
                            {autoRefinementPlan.targetRegime && <span className="badge market-badge">{regimeLabel(autoRefinementPlan.targetRegime)} only</span>}
                            <span className="badge muted-badge">{autoRefinementPlan.marketIds.length} market{autoRefinementPlan.marketIds.length === 1 ? "" : "s"}</span>
                          </div>
                        </div>
                        {autoRefinementPlan.targetRegime && (
                          <div className="auto-refine-target">
                            <strong>Auto-refine target: {regimeLabel(autoRefinementPlan.targetRegime)} only</strong>
                            <span>Trades outside {regimeLabel(autoRefinementPlan.targetRegime)} are forced flat. The search is scored on the selected regime, with full-history gated evidence kept for context.</span>
                          </div>
                        )}
                        {autoRefinementPlan.crossMarketDiscovery && (
                          <div className="auto-refine-target">
                            <strong>Cross-market discovery is separate</strong>
                            <span>Auto-refine stays on this template's source market. If a winning regime is known, it stays gated to that regime. Use Find similar elsewhere to create independent leads; those scores are not blended into this template.</span>
                          </div>
                        )}
                        <div className="auto-refine-steps">
                          {autoRefinementPlan.steps.map((step) => (
                            <span key={step}>{step}</span>
                          ))}
                        </div>
                        <div className="button-row">
                          <button type="button" className="secondary" onClick={runAutoRefinement}><Sparkles size={16} /> Run auto-refine</button>
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
                    Thorough regime scan
                  </label>
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
                  <input value={researchRun.search_budget} onChange={(event) => setResearchRun({ ...researchRun, search_budget: event.target.value })} placeholder={`${selectedPreset.budget}`} type="number" min="6" max="500" />
                  <label>Repair mode</label>
                  <span className="badge muted-badge repair-mode-badge">{repairModeLabel(researchRun.repair_mode)}</span>
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

      {activeModule === "paper" && <PaperView summary={paper} />}
      {activeModule === "broker" && <BrokerView summary={broker} markets={markets} />}
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
            ig={ig}
            setIg={setIg}
            submitEodhd={submitEodhd}
            submitIg={submitIg}
            eodhdStatus={eodhdStatus}
            igStatus={igStatus}
            cacheStatus={cacheStatus}
            pruneCache={pruneCache}
          />
        </section>
      )}
    </main>
  );
}

function CockpitView({ summary, setActiveModule }) {
  const runs = summary?.runs ?? {};
  const latest = runs.latest;
  const risk = summary?.risk ?? {};
  return (
    <section className="lab-shell cockpit">
      <div className="lab-header">
        <div>
          <h2><Home size={20} /> Trading Cockpit</h2>
          <p>Account mode, system health, research state, and risk locks.</p>
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
              <button className="status action-status" type="button" key={item.kind} onClick={() => setActiveModule(item.kind === "no_runs" ? "backtests" : "research")}>
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
              <button className="secondary" type="button" onClick={() => setActiveModule("backtests")}>Open Backtests</button>
            </div>
          ) : <span className="muted">No research runs yet.</span>}
        </Panel>
      </div>
    </section>
  );
}

function GuideView({ setActiveModule }) {
  const workflow = [
    ["1", "Connect providers", "Use Settings for EODHD bars and IG demo credentials. IG validation matters because costs, margin, minimum stake, and stop rules change the result."],
    ["2", "Check markets", "Use Backtests to confirm each market has the right symbol, timeframe, spread, slippage, minimum bars, and IG mapping."],
    ["3", "Run a normal search", "Start with one market, Balanced preset, realistic dates, cost stress 2.0, and Thorough regime scan off."],
    ["4", "Read evidence first", "Focus on net profit after costs, compounded end balance, out-of-sample net, trade count, fold win rate, fold concentration, drawdown, capital fit, and warnings."],
    ["5", "Use Refine further", "Use Best by market first, then click Refine further to stage the locked repair plan. If a best regime exists, refinement stays on that market and grades that regime only."],
    ["6", "Export evidence", "Download the evidence ZIP when something is worth offline review. Include bars when you want Codex-assisted analysis later."],
    ["7", "Paper only", "Only move forward after freshness, IG validation, capital, OOS, fold, cost, and regime gates are clear."],
  ];
  const modules = [
    ["Cockpit", "The home view for system status, provider health, current mode, and next actions."],
    ["Backtests", "Run builder, run history, trial cards, regime evidence, repair workflow, archives, and exports."],
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
    ["Paper score", "A stricter score that weights capital fit, OOS trades, OOS profit, fold stability, cost stress, and Sharpe sample size before headline profit."],
    ["End balance", "Projected account balance after compounding from the selected account size."],
    ["Return", "Projected percentage return for that account scenario."],
    ["Edge active days", "Estimated days the best/target regime actually has capital at work. Sparse specialists can be useful portfolio slots if OOS and folds improve."],
    ["Capital use", "Active days divided by the available history. Low use means the capital can be scheduled elsewhere when this template is flat."],
    ["Net/active day", "Net profit divided by active days, useful for comparing small-window specialists without pretending they trade all year."],
    ["OOS net", "Walk-forward out-of-sample net profit after costs."],
    ["Fold win", "Share of walk-forward folds that made money."],
    ["Fold share", "How much positive fold profit came from the best fold. High values mean fragility."],
    ["Gated net", "Profit after forcing the strategy flat outside allowed regimes. In target-regime refinements this is labelled Target net because the whole run is already gated."],
    ["Gated OOS", "Out-of-sample profit after the regime gate is applied. In target-regime refinements this can match the run OOS by design."],
    ["Cost/gross", "How much gross edge is consumed by trading friction."],
    ["Net/cost", "How much net profit remains for each pound of cost."],
    ["Warning colours", "Red blocks paper promotion, orange needs repair, blue is a specialist/regime identity, and grey is diagnostic."],
  ];
  const repairs = [
    ["Too few trades / low OOS", "Auto-refine runs a deeper longer-history retest. If a best regime exists, the retest grades that regime only and forces the rest flat."],
    ["Fragile folds", "Use Longer history and Evidence first. The result needs to work across several walk-forward folds."],
    ["Single-month profit", "Use Exclude month. The run removes the dominant month from saved bars and retests."],
    ["Single-regime profit", "Use Regime repair. It retests full-history evidence and capped regime specialists."],
    ["Weak OOS evidence", "Use Evidence first or Longer history. Headline net is not enough if OOS is weak."],
    ["Missing IG validation", "Use Refine further or Sync costs, then rerun. Do not promote stale or generic cost evidence."],
    ["Capital fit blocked", "Use Capital fit. It reruns the same market and family with conservative sizing, smaller stops, and ranking weighted toward the selected account size."],
    ["Multiple-testing haircut", "Use Evidence first for this template. Use Find similar elsewhere only to create independent leads, not to upgrade the original score."],
    ["Costs overwhelm edge", "Use Higher costs. If the edge disappears, reject or redesign it."],
  ];
  const gates = [
    "Fresh Sharpe days and no stale-data warnings.",
    "Realistic IG/EODHD costs, spread, slippage, and minimum stake assumptions.",
    "Positive net profit after costs and positive out-of-sample net.",
    "Enough trades and enough walk-forward evidence.",
    "No one fold, month, or rare regime carries the whole result.",
    "Regime-gated retest remains positive.",
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
          <button type="button" className="secondary" onClick={() => setActiveModule("backtests")}><BarChart3 size={16} /> Backtests</button>
          <button type="button" className="ghost" onClick={() => setActiveModule("research")}><Sparkles size={16} /> Research</button>
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
              <span className="badge success-badge">Paper queue</span>
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

function BrokerView({ summary, markets }) {
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
          <h2><Wallet size={20} /> Broker</h2>
          <p>IG demo connectivity, market rules, and read-only broker state.</p>
        </div>
        <span className="mode"><LockKeyhole size={16} /> {summary?.order_placement ?? "disabled"}</span>
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
              <td>{normalizeInterval(item.default_timeframe)} · est {round(item.estimated_spread_bps ?? item.spread_bps)} / {round(item.estimated_slippage_bps ?? item.slippage_bps)} bps</td>
              <td>{item.enabled ? "Yes" : "No"}</td>
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

function ResultsView({ runDetail, researchRuns, loadRun, deleteRun, archiveRun, archiveRuns, deleteRuns, onRefineTemplate, onRefineFurther, exportIncludeBars }) {
  const pareto = runDetail?.pareto ?? [];
  const trials = runDetail?.trials ?? [];
  const marketStatuses = runDetail?.config?.market_statuses ?? [];
  const marketFailures = runDetail?.config?.market_failures ?? [];
  const [trialTierFilter, setTrialTierFilter] = React.useState("active");
  const [trialScanFilter, setTrialScanFilter] = React.useState("all");
  const [trialMarketView, setTrialMarketView] = React.useState("overall");
  const [showAllRuns, setShowAllRuns] = React.useState(false);
  const [selectedRunIds, setSelectedRunIds] = React.useState([]);
  const qualitySummary = runQualitySummary(trials);
  const filteredTrials = trials
    .filter((trial) => tierMatchesFilter(trial.promotion_tier, trialTierFilter))
    .filter((trial) => trialScanMatchesFilter(trial, trialScanFilter));
  const displayRankedTrials = sortTrialsForDisplay(filteredTrials);
  const rankedTrials = trialMarketView === "market_best" ? bestTrialsByMarket(displayRankedTrials, 3) : displayRankedTrials;
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
              <button className="run-pill" type="button" onClick={() => loadRun(run.id)}>
                <strong>Run {run.id}</strong>
                <span>{run.market_id} · {run.status} · {run.trial_count} trials · best {round(run.best_score)}</span>
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
      {runDetail && <RegimeEvidence runDetail={runDetail} trials={trials} />}
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
            <Metric label="Rejected" value={qualitySummary.rejected} />
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
        </div>
        <div className="warning-legend">
          <span className="warning-chip blocker">Paper blocker</span>
          <span className="warning-chip repair">Needs repair</span>
          <span className="warning-chip specialist">Specialist identity</span>
          <span className="warning-chip info">Diagnostic</span>
        </div>
        <div className="trial-list">
          {visibleTrials.map((trial) => <TrialCard key={trial.id} trial={trial} onRefineTemplate={onRefineTemplate} onRefineFurther={onRefineFurther} />)}
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
        <Metric label="Regime scans" value={runDetail?.config?.include_regime_scans ? "On" : "Off"} />
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

function TrialCard({ trial, onRefineTemplate, onRefineFurther }) {
  const backtest = trial.backtest ?? {};
  const accountSize = testingAccountSizeForSource(trial);
  const accountLabel = accountSizeLabel(accountSize);
  const capital = accountFeasibility(trial.capital_scenarios, accountSize);
  const accountScenario = accountScenarioFor(trial.capital_scenarios, accountSize);
  const pattern = trial.parameters?.bar_pattern_analysis ?? {};
  const gated = pattern.regime_gated_backtest ?? {};
  const evidence = evidenceProfileForSource(trial);
  const marketId = trialMarketId(trial);
  const interval = trialIntervalLabel(trial);
  const searchAudit = trial.parameters?.search_audit ?? {};
  const scoreBasis = scoreBasisLabel(searchAudit);
  const discoveryLabel = discoveryBadgeLabel(trial.parameters?.search_audit);
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
          <button type="button" className="secondary" onClick={() => onRefineFurther(trial)}>
            <Sparkles size={16} /> Refine further
          </button>
          <button type="button" className="ghost" onClick={() => onRefineTemplate(trial)}>
            <RefreshCw size={16} /> Refine
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
        <Metric label="Fold win" value={percent(evidence.positive_fold_rate)} />
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

function CandidateView({ candidates, critique, onRefineTemplate, onRefineFurther }) {
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
          {visibleCandidates.map((candidate) => <CandidateCard candidate={candidate} key={candidate.id} onRefineTemplate={onRefineTemplate} onRefineFurther={onRefineFurther} />)}
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

function CandidateCard({ candidate, onRefineTemplate, onRefineFurther }) {
  const readiness = candidateReadiness(candidate);
  const issues = readinessIssueCodes(readiness);
  const accountSize = testingAccountSizeForSource(candidate);
  const accountLabel = accountSizeLabel(accountSize);
  const capital = accountFeasibility(candidate.capital_scenarios, accountSize);
  const accountScenario = accountScenarioFor(candidate.capital_scenarios, accountSize);
  const pattern = candidate.audit?.candidate?.parameters?.bar_pattern_analysis ?? {};
  const gated = pattern.regime_gated_backtest ?? {};
  const evidence = evidenceProfileForSource(candidate);
  const searchAudit = candidate.audit?.candidate?.parameters?.search_audit ?? {};
  const scoreBasis = scoreBasisLabel(searchAudit);
  const discoveryLabel = discoveryBadgeLabel(candidate.audit?.candidate?.parameters?.search_audit);
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
        <button type="button" className="secondary" onClick={() => onRefineFurther(candidate)}>
          <Sparkles size={16} /> Refine further
        </button>
        <button type="button" className="ghost" onClick={() => onRefineTemplate(candidate)}>
          <RefreshCw size={16} /> Refine
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
        <Metric label="Fold win" value={percent(evidence.positive_fold_rate)} />
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

function SettingsView({ eodhdKey, setEodhdKey, ig, setIg, submitEodhd, submitIg, eodhdStatus, igStatus, cacheStatus, pruneCache }) {
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
          <label>IG account code</label>
          <input value={ig.accountId} onChange={(event) => setIg({ ...ig, accountId: event.target.value })} />
          <button>{igStatus?.configured ? "Replace IG demo" : "Validate IG demo"}</button>
        </form>
      </Panel>
      <Panel icon={<Activity />} title="Connection Status">
        <div className="status-list">
          {[eodhdStatus, igStatus].filter(Boolean).map((item) => (
            <div className="status" key={item.provider}>
              <strong>{item.provider.toUpperCase()}</strong>
              <span>{item.configured ? "saved on server" : "not saved"} · {item.last_status}</span>
              {item.last_error && <small>{item.last_error}</small>}
            </div>
          ))}
        </div>
      </Panel>
      <Panel icon={<Database />} title="EODHD Cache">
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
          {(cacheStatus?.namespaces ?? []).length === 0 && <span className="muted">No cached EODHD payloads yet.</span>}
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
        <Metric label="Funding" value={`${round((profile?.overnight_admin_fee_annual ?? 0.03) * 100)}%`} />
        <Metric label="FX" value={`${round(profile?.fx_conversion_bps ?? 80)} bps`} />
      </div>
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
  if (tier === "research_candidate") {
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
    return tier === "research_candidate" || tier === "watchlist";
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
  const tierScore = { validated_candidate: 400, paper_candidate: 350, research_candidate: 250, watchlist: 150, reject: 0 }[trial.promotion_tier] ?? 0;
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

function runQualitySummary(trials = []) {
  const warningCounts = new Map();
  let paperReady = 0;
  let researchWatch = 0;
  let rejected = 0;
  let costFragile = 0;
  for (const trial of trials) {
    const tier = trial.promotion_tier;
    if (tier === "validated_candidate" || tier === "paper_candidate") {
      paperReady += 1;
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
  return { paperReady, researchWatch, rejected, costFragile, topWarnings };
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
      positive_fold_rate: numberOrZero(stored.positive_fold_rate),
      single_fold_profit_share: numberOrZero(stored.single_fold_profit_share),
      oos_net_profit: numberOrZero(stored.oos_net_profit),
      oos_trade_count: numberOrZero(stored.oos_trade_count),
      worst_fold_net_profit: numberOrZero(stored.worst_fold_net_profit),
    };
  }
  const folds = arrayValue(source.audit?.fold_results).length ? source.audit.fold_results : arrayValue(source.folds);
  if (folds.length === 0) {
    return { fold_count: 0, positive_fold_rate: 0, single_fold_profit_share: 0, oos_net_profit: 0, oos_trade_count: 0, worst_fold_net_profit: 0 };
  }
  const foldNet = folds.map((fold) => numberOrZero(fold.net_profit));
  const positive = foldNet.filter((value) => value > 0);
  const positiveTotal = positive.reduce((total, value) => total + value, 0);
  return {
    fold_count: folds.length,
    positive_fold_rate: positive.length / folds.length,
    single_fold_profit_share: positiveTotal > 0 ? Math.max(...positive) / positiveTotal : 0,
    oos_net_profit: foldNet.reduce((total, value) => total + value, 0),
    oos_trade_count: folds.reduce((total, fold) => total + numberOrZero(fold.trade_count), 0),
    worst_fold_net_profit: Math.min(...foldNet),
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

function templateSpecificRegime(template) {
  return String(template?.target_regime || template?.parameters?.target_regime || template?.pattern?.target_regime || "").trim();
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
  const actions = [];
  const add = (action) => {
    if (!actions.some((item) => item.id === action.id)) {
      actions.push(action);
    }
  };
  const hasAny = (...codes) => codes.some((code) => warnings.has(code));

  if (hasAny("needs_ig_price_validation", "missing_cost_profile", "missing_spread_slippage")) {
    add({
      id: "sync-costs",
      kind: "sync_costs",
      title: "Fix IG validation",
      detail: "Refresh the spread, slippage, margin, and minimum stake rules before trusting the result.",
      button: "Sync costs",
      primary: true,
    });
  }
  if (hasAny("risk_budget_exceeded", "historical_drawdown_too_large", "historical_daily_loss_stop_breached", "margin_too_large", "insufficient_account_for_margin", "below_ig_min_deal_size", "missing_reference_price", "drawdown_too_high")) {
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
  if (
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
    ["headline_only", "regime_specific", "thin_regime_sample"].includes(verdict)
  ) {
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
  if (hasAny("multiple_testing_haircut", "isolated_parameter_peak")) {
    add({
      id: "evidence-first",
      preset: "evidence_first",
      title: "Reduce scan bias",
      detail: "Rerank a smaller locked search around fold strength, OOS profit, and concentration before trusting the headline.",
      button: "Evidence first",
      primary: actions.length === 0,
    });
  }
  if (hasAny("known_edge_needs_cross_market_validation", "multiple_testing_haircut")) {
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

function autoRefinementPlanForTemplate(template, researchRun, enabledMarkets = [], activeMarketIds = []) {
  const warnings = new Set(template.warnings ?? []);
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
  const hasAny = (...codes) => codes.some((code) => warnings.has(code));
  const tooFewTrades = hasAny("too_few_trades", "high_sharpe_low_trade_count", "low_oos_trades", "target_regime_low_oos_trades", "calendar_effect_needs_longer_history");
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
  const crossMarket = hasAny("known_edge_needs_cross_market_validation", "multiple_testing_haircut");
  const crossMarketDiscovery = crossMarket && discoveryMarketCount > 0;
  const costStress = hasAny("negative_after_costs", "costs_overwhelm_edge", "negative_expectancy_after_costs", "high_turnover_cost_drag");
  const syncCosts = hasAny("needs_ig_price_validation", "missing_cost_profile", "missing_spread_slippage");
  const capitalBlocked = hasAny("risk_budget_exceeded", "historical_drawdown_too_large", "historical_daily_loss_stop_breached", "margin_too_large", "insufficient_account_for_margin", "below_ig_min_deal_size", "missing_reference_price", "drawdown_too_high");

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

  if (runTargetRegime) {
    addStep(`Auto-refine target: ${regimeLabel(runTargetRegime)} only`);
    addStep(`Force flat outside ${regimeLabel(runTargetRegime)}`);
  }
  if (syncCosts) {
    addStep("Refresh IG costs, spread, slippage, margin, and minimum stake");
  }
  if (capitalBlocked) {
    budget = 120;
    stress = Math.max(stress, 2.5);
    start = earlierDate(start, longEvidenceStartForTemplate(template));
    objective = "balanced";
    riskProfile = "conservative";
    repairMode = "capital_fit";
    addStep(`Rank by ${accountSizeLabel(optionalNumber(researchRun.account_size) ?? WORKING_ACCOUNT_SIZE)} capital fit first`);
    addStep("Search smaller stakes and tighter stops before profit");
  }
  if (tooFewTrades) {
    budget = 120;
    start = earlierDate(start, longEvidenceStartForTemplate(template));
    addStep("Deep locked-family retest over longer history");
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
  if (regimeDependent && !includeRegimeScans) {
    if (targetRegime) {
      runTargetRegime = targetRegime;
      addStep(`Keep scoring locked to ${regimeLabel(targetRegime)} only`);
    } else {
      includeRegimeScans = true;
      regimeScanBudget = "";
      addStep(dominantRegime ? `Run regime-gated specialists around ${regimeLabel(dominantRegime)}` : "Run regime-gated specialist checks");
    }
  }
  if (monthDependent && dominantMonth) {
    excludedMonths.push(dominantMonth);
    addStep(`Exclude ${dominantMonth} and require the edge to survive`);
  }
  if (fragileFolds) {
    budget = Math.max(budget, 120);
    start = earlierDate(start, longEvidenceStartForTemplate(template));
    addStep("Use longer-history fold and OOS evidence");
  }
  if (scanBias) {
    stress = Math.max(stress, 2.5);
    addStep("Use evidence-first ranking to reduce scan bias");
  }
  if (crossMarketDiscovery) {
    stress = Math.max(stress, 2.5);
    addStep("Keep this refinement on the source market only");
    addStep("Use Find similar elsewhere as a separate discovery run");
  }
  if (costStress) {
    stress = Math.max(stress, 3.0);
    addStep("Stress costs before trusting the net edge");
  }
  if (steps.length === 0) {
    addStep("Focused locked-family confirmation");
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
  };
  const summary = runTargetRegime
    ? `Regime-specific refine: results are scored inside ${regimeLabel(runTargetRegime)} with full-history gated evidence kept alongside.`
    : tooFewTrades && includeRegimeScans
    ? "Tests both the broader trade-count repair and the winning-regime specialist path."
    : crossMarketDiscovery
    ? "Keeps this template on its source market; other markets are separate discovery leads."
    : "Combines the active blockers into one locked-family repair run.";
  return {
    marketIds,
    runPatch,
    steps: steps.slice(0, 6),
    syncCosts,
    summary,
    targetRegime: runTargetRegime,
    crossMarketDiscovery,
    stageMessage: `Auto-refine staged: ${steps.slice(0, 3).join("; ")}.`,
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
  if (interval === "1day" || ["calendar_turnaround_tuesday", "month_end_seasonality"].includes(family)) {
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
  }[value] ?? value ?? "Standard search";
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
  const halfAccount = accountSize * 0.5;
  const values = {
    risk: formatMoney(scenario.estimated_stop_loss),
    riskBudget: formatMoney(scenario.risk_budget),
    margin: formatMoney(scenario.estimated_margin),
    marginLimit: formatMoney(halfAccount),
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
  "costs_overwhelm_edge",
  "drawdown_too_high",
  "historical_daily_loss_stop_breached",
  "historical_drawdown_too_large",
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
  "risk_budget_exceeded",
  "short_sharpe_sample",
  "target_regime_low_oos_trades",
  "too_few_trades",
  "weak_oos_economics",
  "weak_oos_evidence",
]);

const REPAIR_WARNINGS = new Set([
  "best_trades_dominate",
  "calendar_effect_needs_longer_history",
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
    scalping: "Scalping",
    breakout: "Breakout",
    research_ideas: "Known research ideas",
    find_anything_robust: "Find anything robust",
  }[value] ?? value;
}

function researchRecipeLabel(value) {
  return {
    turnaround_tuesday_after_down_previous_session: "Tests Tuesday rebounds after a down prior session",
    turn_of_month_long_bias: "Tests turn-of-month long bias",
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

function formatRatio(value) {
  const number = Number(value ?? 0);
  return `${number.toFixed(2)}x`;
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
