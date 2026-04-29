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
} from "lucide-react";
import {
  createResearchRun,
  getIgCostProfile,
  getMarketPlugins,
  getMarkets,
  getResearchCandidates,
  getResearchCritique,
  getResearchEngines,
  getResearchRun,
  getResearchRuns,
  getStatus,
  installMarketPlugin,
  saveFmp,
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
];

const SEARCH_PRESETS = [
  { id: "quick", label: "Quick", budget: 18 },
  { id: "balanced", label: "Balanced", budget: 54 },
  { id: "deep", label: "Deep", budget: 120 },
];

const STYLE_OPTIONS = [
  { id: "find_anything_robust", label: "Find anything robust" },
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
  const [engines, setEngines] = React.useState(FALLBACK_ENGINES);
  const [researchRuns, setResearchRuns] = React.useState([]);
  const [candidates, setCandidates] = React.useState([]);
  const [critique, setCritique] = React.useState(null);
  const [runDetail, setRunDetail] = React.useState(null);
  const [costProfiles, setCostProfiles] = React.useState({});
  const [message, setMessage] = React.useState("");
  const [fmpKey, setFmpKey] = React.useState("");
  const [ig, setIg] = React.useState({ apiKey: "", username: "", password: "", accountId: "" });
  const [market, setMarket] = React.useState({
    market_id: "GBPUSD",
    name: "GBP/USD",
    asset_class: "forex",
    fmp_symbol: "GBPUSD",
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

  const fmpStatus = providerStatus(status, "fmp");
  const igStatus = providerStatus(status, "ig");
  const enabledMarkets = markets.filter((item) => item.enabled);
  const selectedMarkets = enabledMarkets.filter((item) => activeMarketIds.includes(item.market_id));
  const selectedEngine = engines.find((engine) => engine.id === researchRun.engine) ?? engines[0] ?? FALLBACK_ENGINES[0];
  const selectedPreset = SEARCH_PRESETS.find((preset) => preset.id === researchRun.search_preset) ?? SEARCH_PRESETS[1];

  const refresh = React.useCallback(async () => {
    const [nextStatus, nextMarkets, nextPlugins, nextEngines, nextRuns, nextCandidates, nextCritique] = await Promise.all([
      getStatus(),
      getMarkets(),
      getMarketPlugins(),
      getResearchEngines().catch(() => FALLBACK_ENGINES),
      getResearchRuns(),
      getResearchCandidates(),
      getResearchCritique(),
    ]);
    setStatus(nextStatus);
    setMarkets(nextMarkets);
    setPlugins(nextPlugins);
    setEngines(nextEngines.length ? nextEngines : FALLBACK_ENGINES);
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

  async function submitFmp(event) {
    event.preventDefault();
    setMessage("Validating FMP...");
    try {
      await saveFmp(fmpKey);
      setFmpKey("");
      setMessage("FMP connected.");
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
    setMessage("Launching adaptive IG-aware search...");
    setResearchState({
      status: "running",
      detail: `${selectedEngine.label} across ${market_ids.length} market${market_ids.length === 1 ? "" : "s"}.`,
    });
    try {
      const result = await createResearchRun({
        ...researchRun,
        market_id: market_ids[0],
        market_ids,
        search_budget: budget,
        product_mode: "spread_bet",
      });
      const detail = await getResearchRun(result.run_id);
      setRunDetail(detail);
      setActiveTab("results");
      setResearchState({
        status: "finished",
        detail: `Run ${result.run_id}: ${result.trial_count} trials, ${result.candidate_count} research candidates, best score ${round(result.best_score)}.`,
      });
      setMessage(`Run ${result.run_id} finished after IG-style costs.`);
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
              <label>Trial budget</label>
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
          <ResultsView runDetail={runDetail} researchRuns={researchRuns} loadRun={async (id) => setRunDetail(await getResearchRun(id))} />
        )}

        {activeTab === "candidates" && <CandidateView candidates={candidates} critique={critique} />}

        {activeTab === "settings" && (
          <SettingsView
            status={status}
            fmpKey={fmpKey}
            setFmpKey={setFmpKey}
            ig={ig}
            setIg={setIg}
            submitFmp={submitFmp}
            submitIg={submitIg}
            fmpStatus={fmpStatus}
            igStatus={igStatus}
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
                  <small>{plugin.ig_search_terms.join(", ")}</small>
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
            <input value={market.fmp_symbol} onChange={(event) => setMarket({ ...market, fmp_symbol: event.target.value })} placeholder="FMP symbol" required />
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
      </section>

      <section className="market-table">
        <h2><SlidersHorizontal size={20} /> Markets</h2>
        <table>
          <thead>
            <tr><th>ID</th><th>Name</th><th>Class</th><th>FMP</th><th>IG market</th><th>EPIC</th><th>Costs</th><th>Enabled</th></tr>
          </thead>
          <tbody>
            {markets.map((item) => (
              <tr key={item.market_id}>
                <td>{item.market_id}</td>
                <td>{item.name}</td>
                <td>{item.asset_class}</td>
                <td>{item.fmp_symbol}</td>
                <td>{item.ig_name || "search required"}</td>
                <td>{item.ig_epic || "manual"}</td>
                <td>{normalizeInterval(item.default_timeframe)} · {item.spread_bps}/{item.slippage_bps} bps</td>
                <td>{item.enabled ? "Yes" : "No"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </main>
  );
}

function ResultsView({ runDetail, researchRuns, loadRun }) {
  const pareto = runDetail?.pareto ?? [];
  const trials = runDetail?.trials ?? [];
  return (
    <div className="lab-grid">
      <section className="lab-section span-2">
        <h3>Recent Runs</h3>
        <div className="run-list">
          {researchRuns.slice(0, 6).map((run) => (
            <button className="run-pill" type="button" key={run.id} onClick={() => loadRun(run.id)}>
              <strong>Run {run.id}</strong>
              <span>{run.market_id} · {run.trial_count} trials · best {round(run.best_score)}</span>
            </button>
          ))}
          {researchRuns.length === 0 && <span className="muted">No runs yet.</span>}
        </div>
      </section>
      <section className="lab-section span-2">
        <h3>Pareto Picks</h3>
        <div className="pareto-grid">
          {pareto.map((item) => <ParetoCard key={`${item.kind}-${item.strategy_name}`} item={item} />)}
          {pareto.length === 0 && <span className="muted">Run an adaptive search to see balanced, Sharpe, and profit alternatives.</span>}
        </div>
      </section>
      <section className="lab-section span-2">
        <h3>Top Trials</h3>
        <div className="table-scroll">
          <table>
            <thead>
              <tr><th>Strategy</th><th>Style</th><th>Score</th><th>Sharpe</th><th>Net</th><th>Cost</th><th>Trades</th><th>Warnings</th></tr>
            </thead>
            <tbody>
              {trials.slice(0, 12).map((trial) => (
                <tr key={trial.id}>
                  <td>{trial.strategy_name}</td>
                  <td>{trial.strategy_family || trial.style}</td>
                  <td>{round(trial.robustness_score)}</td>
                  <td>{round(trial.backtest?.sharpe)}</td>
                  <td>{formatMoney(trial.backtest?.net_profit)}</td>
                  <td>{formatMoney(trial.backtest?.total_cost)}</td>
                  <td>{trial.backtest?.trade_count ?? 0}</td>
                  <td>{humanWarnings(trial.warnings).join(", ") || "Clear"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

function ParetoCard({ item }) {
  return (
    <div className="pareto-card">
      <span className="eyebrow">{labelForKind(item.kind)}</span>
      <strong>{item.strategy_name}</strong>
      <div className="mini-metrics">
        <Metric label="Score" value={round(item.robustness_score)} />
        <Metric label="Sharpe" value={round(item.sharpe)} />
        <Metric label="Net" value={formatMoney(item.net_profit)} />
        <Metric label="Cost" value={formatMoney(item.total_cost)} />
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
              <span className="badge muted-badge">Research only</span>
              <strong>{candidate.strategy_name}</strong>
              <span>{candidate.market_id} · score {round(candidate.robustness_score)}</span>
              <small>{humanWarnings(candidate.audit?.warnings).join(" · ") || "Passed current research gates"}</small>
              <div className="mini-metrics">
                <Metric label="Sharpe" value={round(candidate.audit?.backtest?.sharpe)} />
                <Metric label="Net" value={formatMoney(candidate.audit?.backtest?.net_profit)} />
                <Metric label="Costs" value={formatMoney(candidate.audit?.backtest?.total_cost)} />
                <Metric label="Trades" value={candidate.audit?.backtest?.trade_count ?? 0} />
              </div>
            </div>
          ))}
          {candidates.length === 0 && <span className="muted">No research-only candidates promoted yet.</span>}
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

function SettingsView({ fmpKey, setFmpKey, ig, setIg, submitFmp, submitIg, fmpStatus, igStatus }) {
  return (
    <div className="grid two">
      <Panel icon={<KeyRound />} title="Provider Settings">
        <form onSubmit={submitFmp}>
          <div className="label-row">
            <label>FMP API key</label>
            <SecretBadge status={fmpStatus} />
          </div>
          <div className="row">
            <input value={fmpKey} onChange={(event) => setFmpKey(event.target.value)} type="password" required />
            <button>{fmpStatus?.configured ? "Replace" : "Validate"}</button>
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
          {[fmpStatus, igStatus].filter(Boolean).map((item) => (
            <div className="status" key={item.provider}>
              <strong>{item.provider.toUpperCase()}</strong>
              <span>{item.configured ? "saved on server" : "not saved"} · {item.last_status}</span>
              {item.last_error && <small>{item.last_error}</small>}
            </div>
          ))}
        </div>
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

function normalizeInterval(value) {
  if (value === "1h") {
    return "1hour";
  }
  return value || "5min";
}

function costBadge(profile, market) {
  const confidence = profile?.confidence ?? (market.plugin_id?.startsWith("fmp-") ? "fmp_proxy_ig_cost_envelope" : "ig_public_spread_baseline");
  if (confidence === "ig_live_epic_cost_profile") {
    return { label: "IG live EPIC cost profile", className: "good" };
  }
  if (confidence === "fmp_proxy_ig_cost_envelope") {
    return { label: "FMP proxy with IG cost envelope", className: "warn" };
  }
  if (confidence === "ig_public_spread_baseline") {
    return { label: "IG public spread baseline", className: "base" };
  }
  return { label: "Needs IG price validation", className: "warn" };
}

function humanWarnings(warnings = []) {
  const labels = {
    too_few_trades: "Too few trades",
    negative_after_costs: "Negative after costs",
    weak_sharpe: "Weak Sharpe",
    drawdown_too_high: "Drawdown too high",
    fails_higher_slippage: "Fails higher slippage",
    profits_not_consistent_across_folds: "Fragile folds",
    funding_eats_swing_edge: "Funding eats swing edge",
    needs_ig_price_validation: "Needs IG price validation",
  };
  return (warnings ?? []).map((warning) => labels[warning] ?? warning);
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
  return `£${number.toFixed(0)}`;
}

function round(value) {
  const number = Number(value ?? 0);
  return Number.isInteger(number) ? number : number.toFixed(2);
}

createRoot(document.getElementById("root")).render(<App />);
