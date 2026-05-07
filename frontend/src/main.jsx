import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  BarChart3,
  CheckCircle,
  Database,
  KeyRound,
  LineChart,
  LockKeyhole,
  RefreshCw,
  ShieldCheck,
  SlidersHorizontal,
  Sparkles,
  Wallet,
} from "lucide-react";
import {
  getBrokerSummary,
  getScenarioSummary,
  getSettingsSummary,
  getStatus,
  recordScenarioAfterClose,
  resetScenarioResearch,
  saveEodhd,
  saveFmp,
  saveIg,
  saveIgAccountRoles,
  startScenarioRecipeBuild,
  startScenarioScanner,
  syncIgCosts,
} from "./api";
import "./styles.css";

const WORKING_ACCOUNT_SIZE = 3000;

const SECTIONS = [
  ["today", "Today", Activity],
  ["build", "Build Recipe", Sparkles],
  ["review", "Paper Review", LineChart],
  ["settings", "Accounts/Settings", SlidersHorizontal],
];

const DEFAULT_SUMMARY = {
  recipes: [],
  recipe_cards: [],
  daily_paper_queue: [],
  review_signals: [],
  counts: {},
  latest_scan: null,
};

function App() {
  const [activeSection, setActiveSection] = useState("today");
  const [summary, setSummary] = useState(DEFAULT_SUMMARY);
  const [broker, setBroker] = useState(null);
  const [settings, setSettings] = useState(null);
  const [providers, setProviders] = useState([]);
  const [message, setMessage] = useState("");
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState("");
  const [reviewNotes, setReviewNotes] = useState("");
  const [resetText, setResetText] = useState("");
  const [eodhdToken, setEodhdToken] = useState("");
  const [fmpKey, setFmpKey] = useState("");
  const [ig, setIg] = useState({ apiKey: "", username: "", password: "", accountId: "" });
  const [roles, setRoles] = useState({ spreadBetAccountId: "", cfdAccountId: "", defaultProductMode: "spread_bet" });

  async function loadAll({ quiet = false } = {}) {
    if (!quiet) setLoading(true);
    try {
      const [scenarioPayload, brokerPayload, settingsPayload, statusPayload] = await Promise.all([
        getScenarioSummary({ accountSize: WORKING_ACCOUNT_SIZE }),
        getBrokerSummary(),
        getSettingsSummary(),
        getStatus(),
      ]);
      setSummary(scenarioPayload);
      setBroker(brokerPayload);
      setSettings(settingsPayload);
      setProviders(statusPayload);
      const accountRoles = settingsPayload?.ig_account_roles ?? brokerPayload?.ig_account_roles ?? {};
      setRoles({
        spreadBetAccountId: accountRoles?.spread_bet?.account_id ?? "",
        cfdAccountId: accountRoles?.cfd?.account_id ?? "",
        defaultProductMode: accountRoles?.default_product_mode ?? "spread_bet",
      });
    } catch (error) {
      setMessage(error.message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadAll();
  }, []);

  const productMode = roles.defaultProductMode || "spread_bet";
  const recipes = summary.recipes ?? [];
  const buildRuns = summary.build_runs ?? [];
  const buildRunning = buildRuns.some((run) => run.status === "running" || Number(run.progress_percent ?? 100) < 100);
  const recipeCards = summary.recipe_cards?.length ? summary.recipe_cards : recipes.map((recipe) => ({
    recipe_id: recipe.id,
    label: recipe.label,
    market_id: recipe.market_id,
    status: "not_scanned",
    scenario_label: "Not scanned today",
    no_trade_reason: "Press Start to scan today.",
    cost_profile: recipe.cost_profile,
  }));

  useEffect(() => {
    if (!buildRunning) return undefined;
    const timer = window.setInterval(async () => {
      try {
        const scenarioPayload = await getScenarioSummary({ accountSize: WORKING_ACCOUNT_SIZE });
        setSummary(scenarioPayload);
      } catch (error) {
        setMessage(error.message);
      }
    }, 3500);
    return () => window.clearInterval(timer);
  }, [buildRunning]);

  async function runAction(label, action, success) {
    setBusy(label);
    setMessage("");
    try {
      const result = await action();
      setMessage(success(result));
      await loadAll({ quiet: true });
    } catch (error) {
      setMessage(error.message);
    } finally {
      setBusy("");
    }
  }

  async function startScan() {
    await runAction(
      "scan",
      () =>
        startScenarioScanner({
          account_size: WORKING_ACCOUNT_SIZE,
          product_mode: productMode,
          paper_limit: 1,
          review_limit: 5,
          lookback_days: 10,
        }),
      (result) => `Scenario scan ${result.scan_id} finished: ${result.status}.`
    );
  }

  async function buildRecipe(recipeId) {
    await runAction(
      recipeId,
      () =>
        startScenarioRecipeBuild(recipeId, {
          account_size: WORKING_ACCOUNT_SIZE,
          product_mode: productMode,
          auto_sync_costs: true,
        }),
      (result) => `${result.recipe?.label ?? recipeId} build started as research run ${result.run_id}.`
    );
  }

  async function saveAfterClose() {
    const scanId = summary.latest_scan?.id;
    if (!scanId) {
      setMessage("Run a scenario scan before saving an after-close review.");
      return;
    }
    await runAction(
      "review",
      () => recordScenarioAfterClose(scanId, { status: "reviewed", results: { notes: reviewNotes, reviewed_at: new Date().toISOString() } }),
      () => "After-close review saved. Frozen rules were not changed."
    );
  }

  async function syncScenarioCosts() {
    await runAction(
      "costs",
      () => syncIgCosts({ market_ids: ["NAS100", "XAUUSD"], product_mode: productMode, skip_account_status: true }),
      (result) => `Cost sync ${result.status}: ${result.price_validated_count ?? 0}/${result.profile_count ?? 0} price validated.`
    );
  }

  async function resetResearch() {
    await runAction(
      "reset",
      () => resetScenarioResearch(resetText),
      (result) => `Research reset complete. Backup: ${result.backup_path}. Cost sync: ${result.cost_sync?.status ?? "unknown"}.`
    );
    setResetText("");
  }

  async function saveProvider(kind) {
    if (kind === "eodhd") {
      await runAction("eodhd", () => saveEodhd(eodhdToken), () => "EODHD saved and validated.");
      setEodhdToken("");
    } else if (kind === "fmp") {
      await runAction("fmp", () => saveFmp(fmpKey), () => "FMP saved and validated.");
      setFmpKey("");
    } else if (kind === "ig") {
      await runAction("ig", () => saveIg(ig), () => "IG demo login saved.");
      setIg({ apiKey: "", username: "", password: "", accountId: "" });
    } else if (kind === "roles") {
      await runAction("roles", () => saveIgAccountRoles(roles), () => "IG account roles saved.");
    }
  }

  return (
    <main>
      <header className="topbar app-topbar">
        <div>
          <h1>Scenario Trainer</h1>
          <p>NAS100 VWAP Pullback and Gold VWAP Rejection. Paper previews only; no live orders.</p>
        </div>
        <div className="topbar-actions">
          <span className="mode"><ShieldCheck size={16} /> Paper locked</span>
          <button className="ghost" onClick={() => loadAll()} disabled={loading || Boolean(busy)}>
            <RefreshCw size={16} /> Refresh
          </button>
        </div>
      </header>

      <nav className="module-nav" aria-label="Main sections">
        {SECTIONS.map(([id, label, Icon]) => (
          <button key={id} className={`module-tab ${activeSection === id ? "active" : ""}`} onClick={() => setActiveSection(id)}>
            <Icon size={16} /> {label}
          </button>
        ))}
      </nav>

      {message ? <div className="notice">{message}</div> : null}
      {loading ? <div className="notice loading">Loading the scenario workspace...</div> : null}

      {activeSection === "today" ? (
        <TodayView
          summary={summary}
          recipeCards={recipeCards}
          onStart={startScan}
          busy={busy === "scan"}
        />
      ) : null}

      {activeSection === "build" ? (
        <BuildRecipeView recipes={recipes} onBuild={buildRecipe} busy={busy} />
      ) : null}

      {activeSection === "review" ? (
        <PaperReviewView
          summary={summary}
          reviewNotes={reviewNotes}
          setReviewNotes={setReviewNotes}
          onSave={saveAfterClose}
          busy={busy === "review"}
        />
      ) : null}

      {activeSection === "settings" ? (
        <SettingsView
          broker={broker}
          settings={settings}
          providers={providers}
          eodhdToken={eodhdToken}
          setEodhdToken={setEodhdToken}
          fmpKey={fmpKey}
          setFmpKey={setFmpKey}
          ig={ig}
          setIg={setIg}
          roles={roles}
          setRoles={setRoles}
          resetText={resetText}
          setResetText={setResetText}
          resetResearch={resetResearch}
          saveProvider={saveProvider}
          syncCosts={syncScenarioCosts}
          busy={busy}
        />
      ) : null}
    </main>
  );
}

function TodayView({ summary, recipeCards, onStart, busy }) {
  const paperQueue = summary.daily_paper_queue ?? [];
  const reviewSignals = summary.review_signals ?? [];
  return (
    <div className="grid">
      <section className="panel factory-hero">
        <div>
          <h2><Activity size={18} /> Today</h2>
          <p>Scan only NAS100 and XAUUSD against exact frozen templates. If both pass, only the best one enters paper.</p>
        </div>
        <button onClick={onStart} disabled={busy}>
          <BarChart3 size={16} /> Start Today&apos;s Scan
        </button>
      </section>

      <section className="grid two">
        {recipeCards.map((card) => (
          <ScenarioCard key={card.recipe_id} card={card} />
        ))}
      </section>

      <section className="grid two">
        <Panel title="Paper Queue" icon={CheckCircle}>
          {paperQueue.length ? (
            <SignalList items={paperQueue} />
          ) : (
            <EmptyState text="No broker-safe paper preview yet. No-trade days are valid." />
          )}
        </Panel>
        <Panel title="Review Signals" icon={LineChart}>
          {reviewSignals.length ? (
            <SignalList items={reviewSignals.slice(0, 5)} />
          ) : (
            <EmptyState text="The review queue will show up to five eligible signals." />
          )}
        </Panel>
      </section>
    </div>
  );
}

function ScenarioCard({ card }) {
  const tape = card.today_tape ?? {};
  const cost = card.cost_profile ?? {};
  return (
    <article className="panel scenario-card">
      <div className="label-row">
        <div>
          <h2>{card.label}</h2>
          <p>{card.market_id} · {card.session ?? "5-minute scenario"}</p>
        </div>
        <span className={`badge ${card.paper_ready ? "good" : card.status === "needs_template" ? "warn" : "base"}`}>
          {card.paper_ready ? "Paper ready" : readable(card.status)}
        </span>
      </div>
      <div className="metrics four">
        <Metric label="Scenario" value={card.scenario_label ?? readable(card.scenario_state)} />
        <Metric label="Quality" value={formatNumber(card.scenario_quality_score, 0)} />
        <Metric label="RVOL" value={`${formatNumber(tape.relative_volume, 2)}x`} />
        <Metric label="Spread" value={`${formatNumber(cost.spread_bps, 2)} bps`} />
      </div>
      <p>{card.no_trade_reason || card.setup || "Waiting for a clean same-day setup."}</p>
      <BadgeList items={card.blockers ?? []} />
    </article>
  );
}

function BuildRecipeView({ recipes, onBuild, busy }) {
  return (
    <div className="grid">
      <section className="panel factory-hero">
        <div>
          <h2><Sparkles size={18} /> Build Recipe</h2>
          <p>Each button builds, repairs, freezes, and freeze-validates only that specialist recipe.</p>
        </div>
      </section>
      <section className="grid two">
        {recipes.map((recipe) => (
          <article className="panel" key={recipe.id}>
            <div className="label-row">
              <div>
                <h2>{recipe.label}</h2>
                <p>{recipe.market_id} · {recipe.session}</p>
              </div>
              <span className="badge base">{recipe.template_count ?? 0} templates</span>
            </div>
            <div className="metrics">
              <Metric label="Max spread" value={`${formatNumber(recipe.strict_gates?.max_spread_bps, 2)} bps`} />
              <Metric label="Min RVOL" value={`${formatNumber(recipe.strict_gates?.min_relative_volume, 2)}x`} />
              <Metric label="Market status" value={readable(recipe.cost_profile?.market_status || "unknown")} />
            </div>
            <p>{recipe.setup}</p>
            <BadgeList items={recipe.families ?? []} />
            {recipe.latest_build ? (
              <BuildRunProgress run={recipe.latest_build} />
            ) : (
              <EmptyState text="No build run yet for this recipe." />
            )}
            <button onClick={() => onBuild(recipe.id)} disabled={Boolean(busy)}>
              <Sparkles size={16} /> Build, Repair & Freeze
            </button>
          </article>
        ))}
      </section>
    </div>
  );
}

function BuildRunProgress({ run }) {
  const progress = Math.max(0, Math.min(100, Number(run.progress_percent ?? 0)));
  const autoFreeze = run.auto_freeze ?? {};
  const blockers = run.blocker_summary ?? [];
  return (
    <div className={`run-state ${run.status}`}>
      <div className="label-row">
        <div>
          <strong>Latest run #{run.run_id}</strong>
          <span>{run.step_label ?? readable(run.status)}</span>
        </div>
        <span className={`badge ${run.status === "running" ? "base" : blockers.length ? "warn" : "good"}`}>
          {readable(autoFreeze.status || run.status)}
        </span>
      </div>
      <div className="progress-row">
        <div className="progress-track" aria-label={`${progress}% complete`}>
          <div className={`progress-fill ${run.status}`} style={{ width: `${progress}%` }} />
        </div>
        <small>{formatNumber(progress, 0)}%</small>
      </div>
      <div className="metrics four">
        <Metric label="Trials" value={`${run.trial_count ?? 0}/${run.effective_search_budget ?? "?"}`} />
        <Metric label="Best score" value={formatNumber(run.best_score, 1)} />
        <Metric label="Passed" value={run.passed_count ?? 0} />
        <Metric label="Paper" value={readable(autoFreeze.readiness_status || "not ready")} />
      </div>
      {autoFreeze.detail ? <p>{autoFreeze.detail}</p> : null}
      {run.error ? <p>{run.error}</p> : null}
      {blockers.length ? (
        <div className="badge-group left">
          {blockers.map((item) => (
            <span className="badge warn" key={item.reason}>{item.label} · {item.count}</span>
          ))}
        </div>
      ) : null}
    </div>
  );
}

function PaperReviewView({ summary, reviewNotes, setReviewNotes, onSave, busy }) {
  const latest = summary.latest_scan;
  const reviewed = latest?.after_close_results ?? latest?.results ?? {};
  return (
    <div className="grid two">
      <Panel title="20-Sample Scorecard" icon={LineChart}>
        <div className="metrics">
          <Metric label="Latest scan" value={latest?.id ? `#${latest.id}` : "None"} />
          <Metric label="Paper queue" value={summary.counts?.daily_paper_queue ?? 0} />
          <Metric label="Review queue" value={summary.counts?.review_signals ?? 0} />
        </div>
        <p>Review is evidence-only. It does not change the template unless you start a new build/validation cycle.</p>
        {Object.keys(reviewed).length ? <pre className="json-preview">{JSON.stringify(reviewed, null, 2)}</pre> : null}
      </Panel>
      <Panel title="After Close" icon={Database}>
        <label htmlFor="review-notes">Review notes</label>
        <textarea
          id="review-notes"
          rows={8}
          value={reviewNotes}
          onChange={(event) => setReviewNotes(event.target.value)}
          placeholder="Expected vs actual, whether the scenario really matched, and what invalidated it."
        />
        <button onClick={onSave} disabled={busy || !latest?.id}>
          <CheckCircle size={16} /> Save Review
        </button>
      </Panel>
    </div>
  );
}

function SettingsView({
  broker,
  providers,
  eodhdToken,
  setEodhdToken,
  fmpKey,
  setFmpKey,
  ig,
  setIg,
  roles,
  setRoles,
  resetText,
  setResetText,
  resetResearch,
  saveProvider,
  syncCosts,
  busy,
}) {
  const statusByProvider = useMemo(() => Object.fromEntries((providers ?? []).map((item) => [item.provider, item])), [providers]);
  return (
    <div className="grid two">
      <Panel title="Provider Keys" icon={KeyRound}>
        <ProviderStatus statusByProvider={statusByProvider} />
        <form onSubmit={(event) => { event.preventDefault(); saveProvider("eodhd"); }}>
          <label htmlFor="eodhd">EODHD API token</label>
          <div className="row">
            <input id="eodhd" type="password" value={eodhdToken} onChange={(event) => setEodhdToken(event.target.value)} />
            <button disabled={busy === "eodhd" || !eodhdToken}>Save</button>
          </div>
        </form>
        <form onSubmit={(event) => { event.preventDefault(); saveProvider("fmp"); }}>
          <label htmlFor="fmp">FMP API key</label>
          <div className="row">
            <input id="fmp" type="password" value={fmpKey} onChange={(event) => setFmpKey(event.target.value)} />
            <button disabled={busy === "fmp" || !fmpKey}>Save</button>
          </div>
        </form>
      </Panel>

      <Panel title="IG Demo Accounts" icon={Wallet}>
        <form onSubmit={(event) => { event.preventDefault(); saveProvider("ig"); }}>
          <label htmlFor="ig-key">IG API key</label>
          <input id="ig-key" type="password" value={ig.apiKey} onChange={(event) => setIg({ ...ig, apiKey: event.target.value })} />
          <label htmlFor="ig-user">IG username</label>
          <input id="ig-user" value={ig.username} onChange={(event) => setIg({ ...ig, username: event.target.value })} />
          <label htmlFor="ig-pass">IG password</label>
          <input id="ig-pass" type="password" value={ig.password} onChange={(event) => setIg({ ...ig, password: event.target.value })} />
          <label htmlFor="ig-account">Optional default account ID</label>
          <input id="ig-account" value={ig.accountId} onChange={(event) => setIg({ ...ig, accountId: event.target.value })} />
          <button disabled={busy === "ig" || !ig.apiKey || !ig.username || !ig.password}>Save IG Login</button>
        </form>
        <form onSubmit={(event) => { event.preventDefault(); saveProvider("roles"); }}>
          <label htmlFor="spread-role">Spread Bet Demo account</label>
          <input id="spread-role" value={roles.spreadBetAccountId} onChange={(event) => setRoles({ ...roles, spreadBetAccountId: event.target.value })} />
          <label htmlFor="cfd-role">CFD Demo account</label>
          <input id="cfd-role" value={roles.cfdAccountId} onChange={(event) => setRoles({ ...roles, cfdAccountId: event.target.value })} />
          <label htmlFor="default-mode">Default product mode</label>
          <select id="default-mode" value={roles.defaultProductMode} onChange={(event) => setRoles({ ...roles, defaultProductMode: event.target.value })}>
            <option value="spread_bet">Spread bet</option>
            <option value="cfd">CFD</option>
          </select>
          <button disabled={busy === "roles"}>Save Account Roles</button>
        </form>
      </Panel>

      <Panel title="Broker Preview Safety" icon={LockKeyhole}>
        <div className="metrics">
          <Metric label="Mode" value={broker?.mode ?? "demo_read_only"} />
          <Metric label="Live orders" value={broker?.live_ordering_enabled ? "enabled" : "disabled"} />
          <Metric label="Account" value={`GBP ${WORKING_ACCOUNT_SIZE}`} />
        </div>
        <button onClick={syncCosts} disabled={busy === "costs"}>
          <RefreshCw size={16} /> Sync NAS100 & Gold Costs
        </button>
      </Panel>

      <Panel title="Fresh Start Reset" icon={Database}>
        <p>Backs up the research database, clears runs/templates/scans/cost profiles, preserves credentials and account roles, then syncs NAS100 and Gold costs.</p>
        <label htmlFor="reset">Confirmation</label>
        <input id="reset" value={resetText} onChange={(event) => setResetText(event.target.value)} placeholder="RESET_SCENARIO_APP" />
        <button className="secondary" onClick={resetResearch} disabled={busy === "reset" || resetText !== "RESET_SCENARIO_APP"}>
          Back Up & Reset Research
        </button>
      </Panel>
    </div>
  );
}

function ProviderStatus({ statusByProvider }) {
  return (
    <div className="badge-group provider-status">
      {["eodhd", "fmp", "ig", "ig_accounts"].map((provider) => {
        const status = statusByProvider[provider] ?? {};
        const connected = status.configured || status.last_status === "connected" || status.last_status === "saved";
        return (
          <span key={provider} className={`badge ${connected ? "good" : "base"}`}>
            {provider.replace("_", " ")}: {readable(status.last_status || "not configured")}
          </span>
        );
      })}
    </div>
  );
}

function SignalList({ items }) {
  return (
    <div className="run-list signal-list">
      {items.map((item, index) => (
        <article className="status" key={`${item.recipe_id}-${item.template_id}-${index}`}>
          <div className="label-row">
            <strong>{item.recipe_label ?? item.strategy_name}</strong>
            <span className={`badge ${item.paper_ready ? "good" : "base"}`}>{item.paper_ready ? "Paper" : "Review"}</span>
          </div>
          <span>{item.market_id} · {item.side ?? "FLAT"} · {readable(item.scenario_state)}</span>
          <small>{item.no_setup_reason || item.signal_explainer?.headline || "Broker-safe preview only."}</small>
        </article>
      ))}
    </div>
  );
}

function Panel({ title, icon: Icon, children }) {
  return (
    <section className="panel">
      <h2>{Icon ? <Icon size={18} /> : null}{title}</h2>
      {children}
    </section>
  );
}

function Metric({ label, value }) {
  return (
    <div className="metric">
      <span>{label}</span>
      <strong>{value ?? "—"}</strong>
    </div>
  );
}

function BadgeList({ items }) {
  if (!items?.length) return null;
  return (
    <div className="badge-group">
      {items.slice(0, 6).map((item) => (
        <span className="badge base" key={item}>{readable(item)}</span>
      ))}
    </div>
  );
}

function EmptyState({ text }) {
  return <p className="empty-state">{text}</p>;
}

function readable(value) {
  return String(value ?? "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase()) || "Unknown";
}

function formatNumber(value, digits = 2) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "0";
  return number.toFixed(digits);
}

createRoot(document.getElementById("root")).render(<App />);
