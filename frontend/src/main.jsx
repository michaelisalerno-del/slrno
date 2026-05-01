import React from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  BarChart3,
  Database,
  KeyRound,
  Plug,
  RefreshCw,
  ShieldCheck,
  SlidersHorizontal,
  Sparkles,
  Trash2,
} from "lucide-react";
import {
  createResearchRun,
  deleteResearchRun,
  getIgCostProfile,
  getIgSpreadBetEngines,
  getMarketDataCacheStatus,
  getMarketPlugins,
  getMarkets,
  getResearchCandidates,
  getResearchCritique,
  getResearchEngines,
  getResearchRun,
  getResearchRuns,
  getStatus,
  installMarketPlugin,
  pruneMarketDataCache,
  saveEodhd,
  saveIg,
  saveMarket,
  saveResearchSchedule,
  syncIgCosts,
} from "./api";
import "./styles.css";

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
  const [message, setMessage] = React.useState("");
  const [eodhdKey, setEodhdKey] = React.useState("");
  const [ig, setIg] = React.useState({ apiKey: "", username: "", password: "", accountId: "" });
  const [market, setMarket] = React.useState({
    market_id: "GBPUSD",
    name: "GBP/USD",
    asset_class: "forex",
    eodhd_symbol: "GBPUSD.FOREX",
    ig_epic: "",
    ig_name: "GBP/USD",
    ig_search_terms: "GBP/USD,GBPUSD",
    default_timeframe: "5min",
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
    interval: "5min",
    search_preset: "balanced",
    trading_style: "find_anything_robust",
    objective: "balanced",
    search_budget: "",
    risk_profile: "balanced",
  });
  const [researchState, setResearchState] = React.useState({ status: "idle", detail: "Ready." });

  const eodhdStatus = providerStatus(status, "eodhd");
  const igStatus = providerStatus(status, "ig");
  const enabledMarkets = markets.filter((item) => item.enabled);
  const selectedMarkets = enabledMarkets.filter((item) => activeMarketIds.includes(item.market_id));
  const selectedEngine = engines.find((engine) => engine.id === researchRun.engine) ?? engines[0] ?? FALLBACK_ENGINES[0];
  const selectedPreset = SEARCH_PRESETS.find((preset) => preset.id === researchRun.search_preset) ?? SEARCH_PRESETS[1];

  const refresh = React.useCallback(async () => {
    const [nextStatus, nextMarkets, nextPlugins, nextCacheStatus, nextEngines, nextSpreadBetEngines, nextRuns, nextCandidates, nextCritique] = await Promise.all([
      getStatus(),
      getMarkets(),
      getMarketPlugins(),
      getMarketDataCacheStatus().catch(() => null),
      getResearchEngines().catch(() => FALLBACK_ENGINES),
      getIgSpreadBetEngines().catch(() => []),
      getResearchRuns(),
      getResearchCandidates(),
      getResearchCritique(),
    ]);
    setStatus(nextStatus);
    setMarkets(nextMarkets);
    setPlugins(nextPlugins);
    setCacheStatus(nextCacheStatus);
    setEngines(nextEngines.length ? nextEngines : FALLBACK_ENGINES);
    setSpreadBetEngines(nextSpreadBetEngines);
    setResearchRuns(nextRuns);
    setCandidates(nextCandidates);
    setCritique(nextCritique);
  }, []);

  React.useEffect(() => {
    refresh().catch((error) => setMessage(error.message));
  }, [refresh]);

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
      await refresh();
    } catch (error) {
      setMessage(error.message);
      await refresh().catch(() => undefined);
    }
  }

  async function submitIg(event) {
    event.preventDefault();
    setMessage("Validating IG demo...");
    try {
      const result = await saveIg(ig);
      setIg({ apiKey: "", username: "", password: "", accountId: "" });
      setMessage(`IG demo connected${result.account_id ? `: ${result.account_id}` : "."}`);
      await refresh();
    } catch (error) {
      setMessage(error.message);
      await refresh().catch(() => undefined);
    }
  }

  async function submitMarket(event) {
    event.preventDefault();
    await saveMarket(market);
    setMessage("Market mapping saved.");
    await refresh();
  }

  async function installPlugin(pluginId) {
    await installMarketPlugin(pluginId);
    setMessage("Market plugin installed.");
    await refresh();
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
    const market_ids = activeMarketIds.length ? activeMarketIds : [researchRun.market_id];
    const budget = researchRun.search_budget === "" ? selectedPreset.budget : Number(researchRun.search_budget);
    const plannedTrials = budget * market_ids.length;
    setMessage("Launching adaptive IG-aware search...");
    setResearchState({
      status: "running",
      detail: `${selectedEngine.label}: ${budget} strategy trials per market, ${plannedTrials} total.`,
    });
    try {
      const result = await createResearchRun({
        ...researchRun,
        market_id: market_ids[0],
        market_ids,
        search_budget: budget,
        product_mode: "spread_bet",
      });
      setActiveTab("results");
      setMessage(`Run ${result.run_id} started: ${budget} strategy trials per market, ${plannedTrials} total.`);
      const detail = await pollResearchRun(result.run_id, plannedTrials);
      setResearchState({
        status: detail.status,
        detail: runStateDetail(detail, plannedTrials),
      });
      setMessage(runCompletionMessage(result.run_id, detail));
      await refresh();
    } catch (error) {
      setResearchState({ status: "error", detail: error.message });
      setMessage(error.message);
      await refresh().catch(() => undefined);
    }
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

  async function pollResearchRun(runId, plannedTrials) {
    let detail = await getResearchRun(runId);
    setRunDetail(detail);
    getMarketDataCacheStatus().then(setCacheStatus).catch(() => undefined);
    for (let attempt = 0; attempt < 720 && ["created", "running"].includes(detail.status); attempt += 1) {
      setResearchState({ status: "running", detail: runStateDetail(detail, plannedTrials) });
      await sleep(2000);
      const [nextDetail, nextCacheStatus] = await Promise.all([
        getResearchRun(runId),
        getMarketDataCacheStatus().catch(() => null),
      ]);
      detail = nextDetail;
      setRunDetail(detail);
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
    await refresh().catch(() => undefined);
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
    await refresh();
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
      <header className="topbar">
        <div>
          <h1>slrno</h1>
          <p>Adaptive research, IG-aware costs, and paper-only validation.</p>
        </div>
        <div className="mode"><ShieldCheck size={18} /> Demo / paper only</div>
      </header>

      {message && <div className="notice">{message}</div>}

      <section className="lab-shell">
        <div className="lab-header">
          <div>
            <h2><Sparkles size={20} /> Backtesting Lab</h2>
            <p>Optimizes strategy and risk settings, then ranks results after spread, slippage, funding, and FX assumptions.</p>
          </div>
          <div className={`run-state ${researchState.status}`}>
            <strong>{researchState.status.toUpperCase()}</strong>
            <span>{researchState.detail}</span>
          </div>
        </div>
        <div className="tabs">
          {[
            ["builder", "New Test"],
            ["results", "Results"],
            ["candidates", "Candidates"],
            ["settings", "Settings"],
          ].map(([id, label]) => (
            <button className={activeTab === id ? "tab active" : "tab"} key={id} type="button" onClick={() => setActiveTab(id)}>
              {label}
            </button>
          ))}
        </div>

        {activeTab === "builder" && (
          <form onSubmit={submitResearchRun} className="lab-grid">
            <section className="lab-section span-2">
              <h3>Search Mode</h3>
              <div className="segmented">
                {SEARCH_PRESETS.map((preset) => (
                  <button
                    type="button"
                    className={researchRun.search_preset === preset.id ? "segment active" : "segment"}
                    key={preset.id}
                    onClick={() => setResearchRun({ ...researchRun, search_preset: preset.id, search_budget: "" })}
                  >
                    {preset.label}
                  </button>
                ))}
              </div>
              <div className="segmented wrap">
                {STYLE_OPTIONS.map((style) => (
                  <button
                    type="button"
                    className={researchRun.trading_style === style.id ? "segment active" : "segment"}
                    key={style.id}
                    onClick={() => setResearchRun({ ...researchRun, trading_style: style.id })}
                  >
                    {style.label}
                  </button>
                ))}
              </div>
            </section>

            <section className="lab-section span-2">
              <h3>Markets</h3>
              <div className="market-picker">
                {enabledMarkets.map((item) => (
                  <button
                    type="button"
                    className={activeMarketIds.includes(item.market_id) ? "market-chip active" : "market-chip"}
                    key={item.market_id}
                    onClick={() => toggleMarket(item.market_id)}
                  >
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
                placeholder={`${selectedPreset.budget}`}
                type="number"
                min="6"
                max="500"
              />
              <div className="button-row">
                <button type="button" className="secondary" onClick={syncCosts}><RefreshCw size={16} /> Sync costs</button>
                <button disabled={researchState.status === "running" || activeMarketIds.length === 0}>{researchState.status === "running" ? "Running..." : "Run search"}</button>
              </div>
              <button type="button" className="ghost" onClick={scheduleResearch}>Save nightly schedule</button>
            </section>

            <section className="lab-section span-2">
              <h3>Cost Profiles</h3>
              <div className="cost-grid">
                {selectedMarkets.map((item) => (
                  <CostProfile key={item.market_id} market={item} profile={costProfiles[item.market_id]} onLoad={() => loadCostProfile(item.market_id)} />
                ))}
                {selectedMarkets.length === 0 && <span className="muted">Choose at least one market.</span>}
              </div>
            </section>
          </form>
        )}

        {activeTab === "results" && (
          <ResultsView
            runDetail={runDetail}
            researchRuns={researchRuns}
            loadRun={async (id) => setRunDetail(await getResearchRun(id))}
            deleteRun={deleteRun}
          />
        )}

        {activeTab === "candidates" && <CandidateView candidates={candidates} critique={critique} />}

        {activeTab === "settings" && (
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
            <label className="check">
              <input type="checkbox" checked={market.enabled} onChange={(event) => setMarket({ ...market, enabled: event.target.checked })} />
              Enabled
            </label>
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
                <span className={`badge ${engine.eligible_for_adaptive_backtest ? "good" : "base"}`}>
                  {engine.eligible_for_adaptive_backtest ? "Backtest-ready" : "Needs product model"}
                </span>
              </div>
            ))}
            {spreadBetEngines.length === 0 && <span className="muted">Engine registry unavailable.</span>}
          </div>
        </Panel>
      </section>

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
    </main>
  );
}

function ResultsView({ runDetail, researchRuns, loadRun, deleteRun }) {
  const pareto = runDetail?.pareto ?? [];
  const trials = runDetail?.trials ?? [];
  const marketStatuses = runDetail?.config?.market_statuses ?? [];
  const marketFailures = runDetail?.config?.market_failures ?? [];
  const [trialTierFilter, setTrialTierFilter] = React.useState("active");
  const qualitySummary = runQualitySummary(trials);
  const filteredTrials = trials.filter((trial) => tierMatchesFilter(trial.promotion_tier, trialTierFilter));
  return (
    <div className="lab-grid">
      <section className="lab-section span-2">
        <h3>Recent Runs</h3>
        <div className="run-list">
          {researchRuns.slice(0, 6).map((run) => (
            <div className="run-item" key={run.id}>
              <button className="run-pill" type="button" onClick={() => loadRun(run.id)}>
                <strong>Run {run.id}</strong>
                <span>{run.market_id} · {run.trial_count} trials · best {round(run.best_score)}</span>
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
            </div>
          ))}
          {researchRuns.length === 0 && <span className="muted">No runs yet.</span>}
        </div>
      </section>
      {(marketStatuses.length > 0 || marketFailures.length > 0 || runDetail?.error) && (
        <section className="lab-section span-2">
          <h3>Market Data Status</h3>
          <div className="status-list">
            {marketStatuses.map((item) => (
              <div className="status" key={`${item.market_id}-${item.status}`}>
                <strong>{item.market_id} · <span className={`badge ${statusBadgeClass(item.status)}`}>{item.status}</span></strong>
                <span>{item.eodhd_symbol} · {normalizeInterval(item.interval)} · {item.data_source_status || "EODHD primary symbol"}</span>
                {item.bar_count !== undefined && <small>{item.bar_count} bars · {item.trial_count ?? 0} trials saved</small>}
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
          {pareto.map((item) => <ParetoCard key={`${item.kind}-${item.strategy_name}`} item={item} />)}
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
        </div>
        <div className="table-scroll">
          <table>
            <thead>
              <tr><th>Strategy</th><th>Tier</th><th>Style</th><th>Score</th><th>Daily Sharpe (ann.)</th><th>Sharpe days</th><th>Bar Sharpe</th><th>DSR</th><th>Net</th><th>Expectancy</th><th>Net/cost</th><th>Cost/gross</th><th>Est spread/slip</th><th>Trades</th><th>Warnings</th></tr>
            </thead>
            <tbody>
              {filteredTrials.slice(0, 12).map((trial) => (
                <tr key={trial.id}>
                  <td>{trial.strategy_name}</td>
                  <td><span className={`badge ${tierBadgeClass(trial.promotion_tier)}`}>{tierLabel(trial.promotion_tier)}</span></td>
                  <td>{strategyFamilyLabel(trial.strategy_family || trial.style)}</td>
                  <td>{round(trial.robustness_score)}</td>
                  <td>{round(trial.backtest?.daily_pnl_sharpe)}</td>
                  <td>{trial.backtest?.sharpe_observations ?? 0}</td>
                  <td>{round(trial.backtest?.sharpe)}</td>
                  <td>{percent(trial.parameters?.sharpe_diagnostics?.deflated_sharpe_probability)}</td>
                  <td>{formatMoney(trial.backtest?.net_profit)}</td>
                  <td>{formatMoney(trial.backtest?.expectancy_per_trade)}</td>
                  <td>{formatRatio(trial.backtest?.net_cost_ratio)}</td>
                  <td>{percent(trial.backtest?.cost_to_gross_ratio)}</td>
                  <td>{round(trial.backtest?.estimated_spread_bps)} / {round(trial.backtest?.estimated_slippage_bps)} bps</td>
                  <td>{trial.backtest?.trade_count ?? 0}</td>
                  <td>{humanWarnings(trial.warnings).join(", ") || "Clear"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {filteredTrials.length === 0 && trials.length > 0 && (
          <span className="muted">No trials in this filter. Rejected and fragile trials are still available under Rejected or All.</span>
        )}
      </section>
    </div>
  );
}

function ParetoCard({ item }) {
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
    </div>
  );
}

function CandidateView({ candidates, critique }) {
  return (
    <div className="lab-grid">
      <section className="lab-section span-2">
        <h3>Research Candidates</h3>
        <div className="candidate-grid">
          {candidates.slice(0, 8).map((candidate) => (
            <div className="candidate-card" key={candidate.id}>
              <div className="label-row">
                <span className="badge muted-badge">Research only</span>
                <span className={`badge ${tierBadgeClass(candidate.promotion_tier || candidate.audit?.promotion_tier)}`}>
                  {tierLabel(candidate.promotion_tier || candidate.audit?.promotion_tier)}
                </span>
              </div>
              <strong>{candidate.strategy_name}</strong>
              <span>{candidate.market_id} · score {round(candidate.robustness_score)}</span>
              {researchRecipeLabel(candidate.audit?.candidate?.parameters?.research_recipe) && (
                <small>{researchRecipeLabel(candidate.audit?.candidate?.parameters?.research_recipe)}</small>
              )}
              <small>{humanWarnings(candidate.audit?.warnings).join(" · ") || "Passed current research gates"}</small>
              <div className="mini-metrics">
                <Metric label="Daily Sharpe (ann.)" value={round(candidate.audit?.backtest?.daily_pnl_sharpe)} />
                <Metric label="Sharpe days" value={candidate.audit?.backtest?.sharpe_observations ?? 0} />
                <Metric label="Bar Sharpe" value={round(candidate.audit?.backtest?.sharpe)} />
                <Metric label="DSR" value={percent(candidate.audit?.candidate?.parameters?.sharpe_diagnostics?.deflated_sharpe_probability)} />
                <Metric label="Stability" value={percent(candidate.audit?.candidate?.parameters?.parameter_stability_score)} />
                <Metric label="Net" value={formatMoney(candidate.audit?.backtest?.net_profit)} />
                <Metric label="Costs" value={formatMoney(candidate.audit?.backtest?.total_cost)} />
                <Metric label="Expectancy" value={formatMoney(candidate.audit?.backtest?.expectancy_per_trade)} />
                <Metric label="Net/cost" value={formatRatio(candidate.audit?.backtest?.net_cost_ratio)} />
                <Metric label="Spread/slip" value={`${round(candidate.audit?.backtest?.estimated_spread_bps)} / ${round(candidate.audit?.backtest?.estimated_slippage_bps)} bps`} />
                <Metric label="Trades" value={candidate.audit?.backtest?.trade_count ?? 0} />
              </div>
            </div>
          ))}
          {candidates.length === 0 && <span className="muted">No saved research leads yet. Strong but flawed trials appear here with warnings.</span>}
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

function SecretBadge({ status }) {
  if (!status?.configured) {
    return <span className="secret-badge empty">Not saved</span>;
  }
  return <span className={`secret-badge ${status.last_status === "connected" ? "connected" : "saved"}`}>Saved on server</span>;
}

function providerStatus(statuses, provider) {
  return statuses.find((item) => item.provider === provider);
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
  if (value === "1h") {
    return "1hour";
  }
  return value || "5min";
}

function costBadge(profile, market) {
  const confidence = profile?.confidence ?? "ig_public_spread_baseline";
  if (confidence === "ig_live_epic_cost_profile") {
    return { label: "IG live EPIC cost profile", className: "good" };
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

function tierLabel(tier) {
  return {
    validated_candidate: "IG validated",
    paper_candidate: "Paper candidate",
    research_candidate: "Research",
    watchlist: "Watchlist",
    reject: "Reject",
  }[tier] ?? "Research";
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
    drawdown_too_high: "Drawdown too high",
    fails_higher_slippage: "Fails higher slippage",
    profits_not_consistent_across_folds: "Fragile folds",
    funding_eats_swing_edge: "Funding eats swing edge",
    needs_ig_price_validation: "Needs IG price validation",
    not_paper_ready_research_lead: "Research lead only",
    calendar_effect_needs_longer_history: "Needs longer calendar history",
    known_edge_needs_cross_market_validation: "Needs cross-market validation",
    high_sharpe_low_trade_count: "High Sharpe, low trades",
    high_sharpe_short_sample: "High Sharpe, short sample",
    high_sharpe_weak_folds: "High Sharpe, weak folds",
    isolated_parameter_peak: "Isolated parameter peak",
    costs_small_vs_turnover: "Costs small vs turnover",
    multiple_testing_haircut: "Multiple-testing haircut",
  };
  return (warnings ?? []).map((warning) => labels[warning] ?? warning);
}

function strategyFamilyLabel(value) {
  return {
    calendar_turnaround_tuesday: "Turnaround Tuesday",
    month_end_seasonality: "Turn of month",
    intraday_trend: "Intraday trend",
    swing_trend: "Swing trend",
    mean_reversion: "Mean reversion",
    volatility_expansion: "Volatility expansion",
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
  return `${prefix}${Math.abs(number).toFixed(0)}`;
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
