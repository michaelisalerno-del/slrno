import React from "react";
import { createRoot } from "react-dom/client";
import { Activity, BarChart3, Database, KeyRound, Plug, ShieldCheck, SlidersHorizontal } from "lucide-react";
import {
  createResearchRun,
  getMarketPlugins,
  getMarkets,
  getResearchCandidates,
  getResearchCritique,
  getResearchRuns,
  getStatus,
  installMarketPlugin,
  saveFmp,
  saveIg,
  saveMarket,
  saveResearchSchedule,
} from "./api";
import "./styles.css";

function App() {
  const [status, setStatus] = React.useState([]);
  const [markets, setMarkets] = React.useState([]);
  const [plugins, setPlugins] = React.useState([]);
  const [researchRuns, setResearchRuns] = React.useState([]);
  const [candidates, setCandidates] = React.useState([]);
  const [critique, setCritique] = React.useState(null);
  const [message, setMessage] = React.useState("");
  const [fmpKey, setFmpKey] = React.useState("");
  const [ig, setIg] = React.useState({ apiKey: "", username: "", password: "", accountId: "" });
  const [market, setMarket] = React.useState({
    market_id: "GBPUSD",
    name: "GBP/USD",
    asset_class: "forex",
    fmp_symbol: "GBPUSD",
    ig_epic: "",
    ig_name: "US Tech 100",
    ig_search_terms: "US Tech 100,Nasdaq,NASDAQ 100",
    default_timeframe: "1h",
    spread_bps: 2,
    slippage_bps: 1,
    min_backtest_bars: 750,
    enabled: true,
  });
  const [researchRun, setResearchRun] = React.useState({
    market_id: "NAS100",
    start: "2024-01-01",
    end: "2026-01-01",
    interval: "1h",
  });

  const refresh = React.useCallback(async () => {
    const [nextStatus, nextMarkets, nextPlugins, nextRuns, nextCandidates, nextCritique] = await Promise.all([
      getStatus(),
      getMarkets(),
      getMarketPlugins(),
      getResearchRuns(),
      getResearchCandidates(),
      getResearchCritique(),
    ]);
    setStatus(nextStatus);
    setMarkets(nextMarkets);
    setPlugins(nextPlugins);
    setResearchRuns(nextRuns);
    setCandidates(nextCandidates);
    setCritique(nextCritique);
  }, []);

  React.useEffect(() => {
    refresh().catch((error) => setMessage(error.message));
  }, [refresh]);

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

  async function submitResearchRun(event) {
    event.preventDefault();
    setMessage("Launching FMP-first research run...");
    const result = await createResearchRun(researchRun);
    setMessage(`Research run ${result.run_id} finished: ${result.trial_count} trials, ${result.candidate_count} candidates.`);
    await refresh();
  }

  async function scheduleResearch() {
    const enabledMarkets = markets.filter((item) => item.enabled).map((item) => item.market_id);
    const result = await saveResearchSchedule({
      name: "Nightly FMP research",
      cadence: "nightly",
      enabled: true,
      market_ids: enabledMarkets,
      interval: "1h",
    });
    setMessage(`Research schedule ${result.schedule_id} saved.`);
  }

  return (
    <main>
      <header className="topbar">
        <div>
          <h1>slrno</h1>
          <p>FMP data, IG demo connectivity, paper execution, and backtesting research.</p>
        </div>
        <div className="mode"><ShieldCheck size={18} /> Demo / paper only</div>
      </header>

      {message && <div className="notice">{message}</div>}

      <section className="grid two">
        <Panel icon={<KeyRound />} title="Provider Settings">
          <form onSubmit={submitFmp}>
            <label>FMP API key</label>
            <div className="row">
              <input value={fmpKey} onChange={(event) => setFmpKey(event.target.value)} type="password" required />
              <button>Validate</button>
            </div>
          </form>

          <form onSubmit={submitIg}>
            <label>IG demo API key</label>
            <input value={ig.apiKey} onChange={(event) => setIg({ ...ig, apiKey: event.target.value })} type="password" required />
            <label>IG username</label>
            <input value={ig.username} onChange={(event) => setIg({ ...ig, username: event.target.value })} required />
            <label>IG password</label>
            <input value={ig.password} onChange={(event) => setIg({ ...ig, password: event.target.value })} type="password" required />
            <label>IG account code</label>
            <input value={ig.accountId} onChange={(event) => setIg({ ...ig, accountId: event.target.value })} placeholder="Optional, e.g. ABC12" />
            <button>Validate IG demo</button>
          </form>
        </Panel>

        <Panel icon={<Activity />} title="Connection Status">
          <div className="status-list">
            {status.length === 0 && <span className="muted">No providers configured yet.</span>}
            {status.map((item) => (
              <div className="status" key={item.provider}>
                <strong>{item.provider.toUpperCase()}</strong>
                <span>{item.last_status}</span>
                {item.last_error && <small>{item.last_error}</small>}
              </div>
            ))}
          </div>
        </Panel>
      </section>

      <section className="grid two">
        <Panel icon={<Plug />} title="Market Plugins">
          <div className="plugin-list">
            {plugins.map((plugin) => (
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

        <Panel icon={<SlidersHorizontal />} title="Backtest Readiness">
          <div className="metrics">
            <Metric label="Timeframes" value="15m-1h" />
            <Metric label="Execution" value="Paper" />
            <Metric label="Risk" value="Strict caps" />
            <Metric label="Markets" value={markets.filter((item) => item.enabled).length} />
          </div>
        </Panel>
      </section>

      <section className="grid two">
        <Panel icon={<BarChart3 />} title="Research Lab">
          <form onSubmit={submitResearchRun} className="compact">
            <input value={researchRun.market_id} onChange={(event) => setResearchRun({ ...researchRun, market_id: event.target.value })} placeholder="Market ID" required />
            <input value={researchRun.interval} onChange={(event) => setResearchRun({ ...researchRun, interval: event.target.value })} placeholder="Interval" required />
            <input value={researchRun.start} onChange={(event) => setResearchRun({ ...researchRun, start: event.target.value })} placeholder="Start YYYY-MM-DD" required />
            <input value={researchRun.end} onChange={(event) => setResearchRun({ ...researchRun, end: event.target.value })} placeholder="End YYYY-MM-DD" required />
            <button>Run FMP research</button>
            <button type="button" className="secondary" onClick={scheduleResearch}>Save nightly schedule</button>
          </form>
          <div className="status-list">
            {researchRuns.slice(0, 4).map((run) => (
              <div className="status" key={run.id}>
                <strong>Run {run.id} · {run.market_id}</strong>
                <span>{run.status} · {run.trial_count} trials · {run.passed_count} passed</span>
              </div>
            ))}
          </div>
        </Panel>

        <Panel icon={<ShieldCheck />} title="Candidate Watchlist">
          <div className="status-list">
            {candidates.length === 0 && <span className="muted">No research-only candidates promoted yet.</span>}
            {candidates.slice(0, 5).map((candidate) => (
              <div className="status" key={candidate.id}>
                <strong>{candidate.strategy_name} · {candidate.market_id}</strong>
                <span>Score {candidate.robustness_score} · research only</span>
              </div>
            ))}
          </div>
        </Panel>
      </section>

      <section className="grid two">
        <Panel icon={<ShieldCheck />} title="Research Critic">
          {critique ? (
            <>
              <div className="metrics">
                <Metric label="Decision" value={critique.decision} />
                <Metric label="Confidence" value={critique.confidence_score} />
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
          ) : (
            <span className="muted">No critique available yet.</span>
          )}
        </Panel>
      </section>

      <section className="market-table">
        <h2>Markets</h2>
        <table>
          <thead>
            <tr><th>ID</th><th>Name</th><th>Class</th><th>FMP</th><th>IG market</th><th>EPIC</th><th>Backtest</th><th>Enabled</th></tr>
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
                <td>{item.default_timeframe} · {item.spread_bps}/{item.slippage_bps} bps</td>
                <td>{item.enabled ? "Yes" : "No"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </main>
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

createRoot(document.getElementById("root")).render(<App />);
