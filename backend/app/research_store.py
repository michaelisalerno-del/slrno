from __future__ import annotations

import json
import sqlite3
import gzip
import hashlib
import shutil
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from .config import app_home
from .ig_costs import IGCostProfile
from .promotion_readiness import MOVE_FORWARD_TIERS, gate_promotion_tier, promotion_readiness, readiness_warnings
from .providers.base import OHLCBar
from .research_lab import CandidateEvaluation


INCUBATOR_RESEARCH_BLOCKERS = {
    "best_trades_dominate",
    "calendar_dependent_edge",
    "calendar_filtered_oos_negative",
    "calendar_sample_too_thin",
    "event_strategy_requires_label",
    "insufficient_regime_sample",
    "low_oos_trades",
    "major_event_window_dependency",
    "one_fold_dependency",
    "profit_concentrated_single_month",
    "profit_concentrated_single_regime",
    "profits_not_consistent_across_folds",
    "target_regime_low_oos_trades",
    "too_few_trades",
    "weak_oos_evidence",
}


def research_db_path() -> Path:
    return app_home() / "research.sqlite3"


class ResearchStore:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or research_db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS research_runs (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  created_at TEXT NOT NULL,
                  status TEXT NOT NULL,
                  market_id TEXT NOT NULL,
                  data_source TEXT NOT NULL,
                  config_json TEXT NOT NULL
                )
                """
            )
            self._add_column(conn, "research_runs", "error", "TEXT NOT NULL DEFAULT ''")
            self._add_column(conn, "research_runs", "archived", "INTEGER NOT NULL DEFAULT 0")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS strategy_trials (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id INTEGER NOT NULL,
                  strategy_name TEXT NOT NULL,
                  passed INTEGER NOT NULL,
                  robustness_score REAL NOT NULL,
                  metrics_json TEXT NOT NULL,
                  warnings_json TEXT NOT NULL,
                  FOREIGN KEY(run_id) REFERENCES research_runs(id)
                )
                """
            )
            self._add_column(conn, "strategy_trials", "strategy_family", "TEXT NOT NULL DEFAULT ''")
            self._add_column(conn, "strategy_trials", "style", "TEXT NOT NULL DEFAULT ''")
            self._add_column(conn, "strategy_trials", "parameters_json", "TEXT NOT NULL DEFAULT '{}'")
            self._add_column(conn, "strategy_trials", "backtest_json", "TEXT NOT NULL DEFAULT '{}'")
            self._add_column(conn, "strategy_trials", "folds_json", "TEXT NOT NULL DEFAULT '[]'")
            self._add_column(conn, "strategy_trials", "costs_json", "TEXT NOT NULL DEFAULT '{}'")
            self._add_column(conn, "strategy_trials", "tags_json", "TEXT NOT NULL DEFAULT '[]'")
            self._add_column(conn, "strategy_trials", "promotion_tier", "TEXT NOT NULL DEFAULT 'reject'")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS candidates (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id INTEGER NOT NULL,
                  strategy_name TEXT NOT NULL,
                  market_id TEXT NOT NULL,
                  robustness_score REAL NOT NULL,
                  research_only INTEGER NOT NULL,
                  audit_json TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  FOREIGN KEY(run_id) REFERENCES research_runs(id)
                )
                """
            )
            self._add_column(conn, "candidates", "promotion_tier", "TEXT NOT NULL DEFAULT 'paper_candidate'")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ig_cost_profiles (
                  market_id TEXT PRIMARY KEY,
                  updated_at TEXT NOT NULL,
                  profile_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS research_run_bars (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  run_id INTEGER NOT NULL,
                  market_id TEXT NOT NULL,
                  interval TEXT NOT NULL,
                  source TEXT NOT NULL,
                  start TEXT NOT NULL,
                  end TEXT NOT NULL,
                  bar_count INTEGER NOT NULL,
                  sha256 TEXT NOT NULL,
                  payload_blob BLOB NOT NULL,
                  created_at TEXT NOT NULL,
                  FOREIGN KEY(run_id) REFERENCES research_runs(id)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_research_run_bars_run_id ON research_run_bars(run_id)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS research_schedules (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  name TEXT NOT NULL,
                  cadence TEXT NOT NULL,
                  enabled INTEGER NOT NULL,
                  config_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS strategy_templates (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  status TEXT NOT NULL,
                  name TEXT NOT NULL,
                  market_id TEXT NOT NULL,
                  interval TEXT NOT NULL,
                  strategy_family TEXT NOT NULL,
                  style TEXT NOT NULL,
                  target_regime TEXT NOT NULL DEFAULT '',
                  source_run_id INTEGER,
                  source_trial_id INTEGER,
                  source_candidate_id INTEGER,
                  source_kind TEXT NOT NULL DEFAULT '',
                  promotion_tier TEXT NOT NULL DEFAULT 'research_candidate',
                  readiness_status TEXT NOT NULL DEFAULT 'blocked',
                  robustness_score REAL NOT NULL DEFAULT 0,
                  testing_account_size REAL NOT NULL DEFAULT 0,
                  source_fingerprint TEXT NOT NULL DEFAULT '',
                  payload_json TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
            self._add_column(conn, "strategy_templates", "target_regime", "TEXT NOT NULL DEFAULT ''")
            self._add_column(conn, "strategy_templates", "source_run_id", "INTEGER")
            self._add_column(conn, "strategy_templates", "source_trial_id", "INTEGER")
            self._add_column(conn, "strategy_templates", "source_candidate_id", "INTEGER")
            self._add_column(conn, "strategy_templates", "source_kind", "TEXT NOT NULL DEFAULT ''")
            self._add_column(conn, "strategy_templates", "promotion_tier", "TEXT NOT NULL DEFAULT 'research_candidate'")
            self._add_column(conn, "strategy_templates", "readiness_status", "TEXT NOT NULL DEFAULT 'blocked'")
            self._add_column(conn, "strategy_templates", "robustness_score", "REAL NOT NULL DEFAULT 0")
            self._add_column(conn, "strategy_templates", "testing_account_size", "REAL NOT NULL DEFAULT 0")
            self._add_column(conn, "strategy_templates", "source_fingerprint", "TEXT NOT NULL DEFAULT ''")
            self._add_column(conn, "strategy_templates", "payload_json", "TEXT NOT NULL DEFAULT '{}'")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_strategy_templates_status ON strategy_templates(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_strategy_templates_market ON strategy_templates(market_id, interval)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_strategy_templates_fingerprint ON strategy_templates(source_fingerprint)")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS day_trading_scans (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  created_at TEXT NOT NULL,
                  trading_date TEXT NOT NULL,
                  status TEXT NOT NULL,
                  account_size REAL NOT NULL,
                  product_mode TEXT NOT NULL,
                  config_json TEXT NOT NULL,
                  queue_json TEXT NOT NULL,
                  review_json TEXT NOT NULL,
                  unsuitable_json TEXT NOT NULL,
                  results_json TEXT NOT NULL DEFAULT '{}',
                  error TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_day_trading_scans_created ON day_trading_scans(created_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_day_trading_scans_date ON day_trading_scans(trading_date)")

    def _add_column(self, conn: sqlite3.Connection, table: str, name: str, definition: str) -> None:
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
        if name not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")

    def create_run(self, market_id: str, config: dict[str, object], data_source: str = "eodhd", status: str = "created") -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO research_runs(created_at, status, market_id, data_source, config_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (_now(), status, market_id, data_source, json.dumps(config, sort_keys=True)),
            )
            return int(cursor.lastrowid)

    def update_run_status(self, run_id: int, status: str, error: str = "") -> None:
        with self._connect() as conn:
            conn.execute("UPDATE research_runs SET status = ?, error = ? WHERE id = ?", (status, error, run_id))

    def update_run_config(self, run_id: int, config: dict[str, object]) -> None:
        with self._connect() as conn:
            conn.execute("UPDATE research_runs SET config_json = ? WHERE id = ?", (json.dumps(config, sort_keys=True), run_id))

    def delete_run(self, run_id: int) -> dict[str, int] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT id FROM research_runs WHERE id = ?", (run_id,)).fetchone()
            if row is None:
                return None
            trial_count = int(conn.execute("SELECT COUNT(*) FROM strategy_trials WHERE run_id = ?", (run_id,)).fetchone()[0] or 0)
            candidate_count = int(conn.execute("SELECT COUNT(*) FROM candidates WHERE run_id = ?", (run_id,)).fetchone()[0] or 0)
            conn.execute("DELETE FROM research_run_bars WHERE run_id = ?", (run_id,))
            conn.execute("DELETE FROM candidates WHERE run_id = ?", (run_id,))
            conn.execute("DELETE FROM strategy_trials WHERE run_id = ?", (run_id,))
            conn.execute("DELETE FROM research_runs WHERE id = ?", (run_id,))
        return {"run_id": run_id, "deleted_trials": trial_count, "deleted_candidates": candidate_count}

    def run_has_move_forward_candidate(self, run_id: int) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM candidates
                WHERE run_id = ?
                  AND promotion_tier IN ('paper_candidate', 'validated_candidate')
                LIMIT 1
                """,
                (run_id,),
            ).fetchone()
        return row is not None

    def archive_run(self, run_id: int) -> dict[str, int] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT id FROM research_runs WHERE id = ?", (run_id,)).fetchone()
            if row is None:
                return None
            conn.execute("UPDATE research_runs SET archived = 1 WHERE id = ?", (run_id,))
        return {"run_id": run_id, "archived": 1}

    def save_trial(self, run_id: int, evaluation: CandidateEvaluation) -> None:
        parameters = dict(evaluation.candidate.parameters)
        backtest = _compact_backtest(asdict(evaluation.backtest))
        folds = [_compact_backtest(asdict(fold)) for fold in evaluation.fold_results]
        promotion_tier = _evaluation_tier(evaluation)
        costs = {
            "cost_confidence": evaluation.backtest.cost_confidence,
            "estimated_spread_bps": evaluation.backtest.estimated_spread_bps,
            "estimated_slippage_bps": evaluation.backtest.estimated_slippage_bps,
            "gross_profit": evaluation.backtest.gross_profit,
            "spread_cost": evaluation.backtest.spread_cost,
            "slippage_cost": evaluation.backtest.slippage_cost,
            "funding_cost": evaluation.backtest.funding_cost,
            "fx_cost": evaluation.backtest.fx_cost,
            "guaranteed_stop_cost": evaluation.backtest.guaranteed_stop_cost,
            "total_cost": evaluation.backtest.total_cost,
            "stress_net_profit": parameters.get("stress_net_profit"),
            "stress_sharpe": parameters.get("stress_sharpe"),
            "turnover_efficiency": evaluation.backtest.turnover_efficiency,
            "expectancy_per_trade": evaluation.backtest.expectancy_per_trade,
            "average_cost_per_trade": evaluation.backtest.average_cost_per_trade,
            "net_cost_ratio": evaluation.backtest.net_cost_ratio,
            "cost_to_gross_ratio": evaluation.backtest.cost_to_gross_ratio,
            "daily_pnl_sharpe": evaluation.backtest.daily_pnl_sharpe,
            "deflated_sharpe_probability": (parameters.get("sharpe_diagnostics") or {}).get("deflated_sharpe_probability")
            if isinstance(parameters.get("sharpe_diagnostics"), dict)
            else None,
        }
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO strategy_trials(
                  run_id, strategy_name, passed, robustness_score, metrics_json, warnings_json,
                  strategy_family, style, parameters_json, backtest_json, folds_json, costs_json, tags_json,
                  promotion_tier
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    evaluation.candidate.name,
                    int(promotion_tier in MOVE_FORWARD_TIERS),
                    evaluation.robustness_score,
                    json.dumps(asdict(evaluation.metrics), sort_keys=True),
                    json.dumps(list(evaluation.warnings), sort_keys=True),
                    str(parameters.get("family", "")),
                    str(parameters.get("style", "")),
                    json.dumps(parameters, sort_keys=True),
                    json.dumps(backtest, sort_keys=True),
                    json.dumps(folds, sort_keys=True),
                    json.dumps(costs, sort_keys=True),
                    json.dumps(list(evaluation.candidate.module_stack), sort_keys=True),
                    promotion_tier,
                ),
            )

    def save_candidate(self, run_id: int, market_id: str, evaluation: CandidateEvaluation) -> None:
        promotion_tier = _evaluation_tier(evaluation)
        if promotion_tier not in {"watchlist", "incubator", "research_candidate", "paper_candidate", "validated_candidate"}:
            return
        if promotion_tier == "watchlist" and not _is_material_watchlist_lead(evaluation):
            return
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO candidates(run_id, strategy_name, market_id, robustness_score, research_only, audit_json, created_at, promotion_tier)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    evaluation.candidate.name,
                    market_id,
                    evaluation.robustness_score,
                    int(evaluation.research_only),
                    json.dumps(_evaluation_audit(evaluation), sort_keys=True),
                    _now(),
                    promotion_tier,
                ),
            )

    def save_cost_profile(self, profile: IGCostProfile | dict[str, object]) -> None:
        payload = profile.as_dict() if isinstance(profile, IGCostProfile) else dict(profile)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO ig_cost_profiles(market_id, updated_at, profile_json)
                VALUES (?, ?, ?)
                ON CONFLICT(market_id) DO UPDATE SET
                  updated_at = excluded.updated_at,
                  profile_json = excluded.profile_json
                """,
                (str(payload["market_id"]), _now(), json.dumps(payload, sort_keys=True)),
            )

    def save_bar_snapshot(
        self,
        run_id: int,
        market_id: str,
        interval: str,
        source: str,
        start: str,
        end: str,
        bars: list[OHLCBar],
    ) -> dict[str, object]:
        payload = [_bar_snapshot_row(bar) for bar in bars]
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        digest = hashlib.sha256(raw).hexdigest()
        compressed = gzip.compress(raw)
        with self._connect() as conn:
            conn.execute(
                """
                DELETE FROM research_run_bars
                WHERE run_id = ? AND market_id = ? AND interval = ? AND source = ?
                """,
                (run_id, market_id, interval, source),
            )
            conn.execute(
                """
                INSERT INTO research_run_bars(
                  run_id, market_id, interval, source, start, end, bar_count, sha256, payload_blob, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (run_id, market_id, interval, source, start, end, len(payload), digest, compressed, _now()),
            )
        return {"market_id": market_id, "interval": interval, "source": source, "bar_count": len(payload), "sha256": digest, "exact": True}

    def list_bar_snapshots(self, run_id: int, include_payload: bool = True) -> list[dict[str, object]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT market_id, interval, source, start, end, bar_count, sha256, payload_blob, created_at
                FROM research_run_bars
                WHERE run_id = ?
                ORDER BY market_id, interval, id
                """,
                (run_id,),
            ).fetchall()
        output: list[dict[str, object]] = []
        for row in rows:
            item: dict[str, object] = {
                "market_id": row[0],
                "interval": row[1],
                "source": row[2],
                "start": row[3],
                "end": row[4],
                "bar_count": int(row[5] or 0),
                "sha256": row[6],
                "created_at": row[8],
                "exact": True,
            }
            if include_payload:
                item["bars"] = json.loads(gzip.decompress(row[7]).decode("utf-8"))
            output.append(item)
        return output

    def get_cost_profile(self, market_id: str) -> dict[str, object] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT profile_json, updated_at FROM ig_cost_profiles WHERE market_id = ?",
                (market_id,),
            ).fetchone()
        if row is None:
            return None
        payload = json.loads(row[0])
        payload["updated_at"] = row[1]
        return payload

    def list_runs(self, include_archived: bool = False) -> list[dict[str, object]]:
        where = "" if include_archived else "WHERE run.archived = 0"
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT run.id, run.created_at, run.status, run.market_id, run.data_source, run.error,
                       run.config_json,
                       run.archived,
                       COUNT(trial.id) AS trial_count,
                       COALESCE(SUM(trial.passed), 0) AS passed_count,
                       MAX(trial.robustness_score) AS best_score
                FROM research_runs run
                LEFT JOIN strategy_trials trial ON trial.run_id = run.id
                {where}
                GROUP BY run.id
                ORDER BY run.id DESC
                """
            ).fetchall()
        return [
            {
                "id": row[0],
                "created_at": row[1],
                "status": row[2],
                "market_id": row[3],
                "data_source": row[4],
                "error": row[5],
                "run_purpose": _run_purpose(row[6]),
                "archived": bool(row[7]),
                "trial_count": row[8],
                "passed_count": row[9],
                "best_score": row[10] or 0,
            }
            for row in rows
        ]

    def get_run(self, run_id: int) -> dict[str, object] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT run.id, run.created_at, run.status, run.market_id, run.data_source,
                       run.config_json, run.error, run.archived,
                       COUNT(trial.id) AS trial_count,
                       COALESCE(SUM(trial.passed), 0) AS passed_count,
                       MAX(trial.robustness_score) AS best_score
                FROM research_runs run
                LEFT JOIN strategy_trials trial ON trial.run_id = run.id
                WHERE run.id = ?
                GROUP BY run.id
                """,
                (run_id,),
            ).fetchone()
        if row is None:
            return None
        return {
            "id": row[0],
            "created_at": row[1],
            "status": row[2],
            "market_id": row[3],
            "data_source": row[4],
            "config": json.loads(row[5]),
            "error": row[6],
            "archived": bool(row[7]),
            "trial_count": row[8],
            "passed_count": row[9],
            "best_score": row[10] or 0,
        }

    def list_trials(self, run_id: int | None = None, limit: int | None = None) -> list[dict[str, object]]:
        query = """
            SELECT id, run_id, strategy_name, passed, robustness_score, metrics_json, warnings_json,
                   strategy_family, style, parameters_json, backtest_json, folds_json, costs_json, tags_json,
                   promotion_tier
            FROM strategy_trials
        """
        params: list[object] = []
        if run_id is not None:
            query += " WHERE run_id = ?"
            params.append(run_id)
        query += " ORDER BY robustness_score DESC, id"
        if limit is not None:
            query += " LIMIT ?"
            params.append(max(0, int(limit)))
        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return [_trial_from_row(row) for row in rows]

    def list_pareto(self, run_id: int) -> list[dict[str, object]]:
        trials = self._pareto_source_trials(run_id)
        if not trials:
            return []
        choices = [
            ("best_balanced", max(trials, key=lambda item: float(item["robustness_score"]))),
            ("highest_sharpe", max(trials, key=lambda item: _risk_adjusted_sharpe_from_payload(item["backtest"]))),
            ("highest_profit", max(trials, key=lambda item: float(item["backtest"].get("net_profit") or 0))),
        ]
        seen: set[int] = set()
        output: list[dict[str, object]] = []
        for kind, trial in choices:
            if int(trial["id"]) in seen:
                continue
            seen.add(int(trial["id"]))
            backtest = trial["backtest"]
            output.append(
                {
                    "kind": kind,
                    "trial_id": trial["id"],
                    "strategy_name": trial["strategy_name"],
                    "strategy_family": trial["strategy_family"],
                    "style": trial["style"],
                    "robustness_score": trial["robustness_score"],
                    "sharpe": backtest.get("sharpe", 0),
                    "net_profit": backtest.get("net_profit", 0),
                    "gross_profit": backtest.get("gross_profit", 0),
                    "total_cost": backtest.get("total_cost", 0),
                    "net_cost_ratio": backtest.get("net_cost_ratio", 0),
                    "expectancy_per_trade": backtest.get("expectancy_per_trade", 0),
                    "cost_to_gross_ratio": backtest.get("cost_to_gross_ratio", 0),
                    "estimated_spread_bps": backtest.get("estimated_spread_bps", 0),
                    "estimated_slippage_bps": backtest.get("estimated_slippage_bps", 0),
                    "max_drawdown": backtest.get("max_drawdown", 0),
                    "trade_count": backtest.get("trade_count", 0),
                    "warnings": trial["warnings"],
                    "settings": trial["parameters"],
                    "promotion_tier": trial["promotion_tier"],
                    "daily_pnl_sharpe": backtest.get("daily_pnl_sharpe", 0),
                    "daily_pnl_sample_sharpe": backtest.get("daily_pnl_sample_sharpe", 0),
                    "sharpe_observations": backtest.get("sharpe_observations", 0),
                    "sample_calendar_days": backtest.get("sample_calendar_days", 0),
                    "sample_trading_days": backtest.get("sample_trading_days", 0),
                    "sharpe_annualization_note": backtest.get("sharpe_annualization_note", ""),
                    "deflated_sharpe_probability": (trial["parameters"].get("sharpe_diagnostics") or {}).get("deflated_sharpe_probability")
                    if isinstance(trial["parameters"].get("sharpe_diagnostics"), dict)
                    else 0,
                }
            )
        return output

    def list_regime_picks(self, run_id: int, per_regime: int = 3, limit: int = 750) -> list[dict[str, object]]:
        trials = sorted(
            self.list_trials(run_id, limit=limit),
            key=lambda item: _trial_display_rank(item),
            reverse=True,
        )
        grouped: dict[str, list[dict[str, object]]] = {}
        for trial in trials:
            regime = _trial_regime_key(trial)
            if not regime:
                continue
            picks = grouped.setdefault(regime, [])
            if len(picks) < per_regime:
                picks.append(_regime_pick_trial(trial))
        return [
            {
                "regime": regime,
                "label": regime.replace("_", " ").title(),
                "trial_count": len(picks),
                "trials": picks,
            }
            for regime, picks in sorted(grouped.items())
            if picks
        ]

    def _pareto_source_trials(self, run_id: int) -> list[dict[str, object]]:
        choices: list[dict[str, object]] = []
        for order_key in (
            "robustness_score",
            "daily_pnl_sharpe",
            "net_profit",
        ):
            trial = self._top_trial(run_id, order_key)
            if trial is not None and all(int(item["id"]) != int(trial["id"]) for item in choices):
                choices.append(trial)
        return choices

    def _top_trial(self, run_id: int, order_key: str) -> dict[str, object] | None:
        order_sql = {
            "robustness_score": "robustness_score",
            "daily_pnl_sharpe": "CAST(json_extract(backtest_json, '$.daily_pnl_sharpe') AS REAL)",
            "net_profit": "CAST(json_extract(backtest_json, '$.net_profit') AS REAL)",
        }[order_key]
        with self._connect() as conn:
            try:
                row = conn.execute(
                    f"""
                    SELECT id, run_id, strategy_name, passed, robustness_score, metrics_json, warnings_json,
                           strategy_family, style, parameters_json, backtest_json, folds_json, costs_json, tags_json,
                           promotion_tier
                    FROM strategy_trials
                    WHERE run_id = ?
                    ORDER BY {order_sql} DESC, robustness_score DESC, id
                    LIMIT 1
                    """,
                    (run_id,),
                ).fetchone()
            except sqlite3.OperationalError:
                row = None
        if row is not None:
            return _trial_from_row(row)
        fallback = self.list_trials(run_id, limit=250)
        if not fallback:
            return None
        if order_key == "daily_pnl_sharpe":
            return max(fallback, key=lambda item: _risk_adjusted_sharpe_from_payload(item["backtest"]))
        if order_key == "net_profit":
            return max(fallback, key=lambda item: float(item["backtest"].get("net_profit") or 0))
        return fallback[0]

    def list_candidates(self, run_id: int | None = None, limit: int | None = None) -> list[dict[str, object]]:
        params: list[object] = []
        where = ""
        if run_id is not None:
            where = "WHERE run_id = ?"
            params.append(run_id)
        limit_clause = ""
        if limit is not None:
            limit_clause = " LIMIT ?"
            params.append(max(0, int(limit)))
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT id, run_id, strategy_name, market_id, robustness_score, research_only, audit_json, created_at, promotion_tier
                FROM candidates {where}
                ORDER BY
                  CASE promotion_tier
                    WHEN 'validated_candidate' THEN 5
                    WHEN 'paper_candidate' THEN 4
                    WHEN 'research_candidate' THEN 3
                    WHEN 'incubator' THEN 2
                    WHEN 'watchlist' THEN 1
                    ELSE 0
                  END DESC,
                  robustness_score DESC,
                  id DESC
                {limit_clause}
                """,
                tuple(params),
            ).fetchall()
        candidates = []
        for row in rows:
            audit = _compact_audit(json.loads(row[6]))
            candidates.append(
                {
                    "id": row[0],
                    "run_id": row[1],
                    "strategy_name": row[2],
                    "market_id": row[3],
                    "robustness_score": row[4],
                    "research_only": bool(row[5]),
                    "audit": audit,
                    "created_at": row[7],
                    "promotion_tier": str(audit.get("promotion_tier") or row[8]),
                }
            )
        lead_limit = None if limit is None else max(0, int(limit) - len(candidates))
        return self._include_trial_research_leads(candidates, run_id, lead_limit)

    def count_candidates(self, run_id: int | None = None) -> int:
        query = "SELECT COUNT(*) FROM candidates"
        params: tuple[object, ...] = ()
        if run_id is not None:
            query += " WHERE run_id = ?"
            params = (run_id,)
        with self._connect() as conn:
            return int(conn.execute(query, params).fetchone()[0])

    def get_candidate(self, candidate_id: int) -> dict[str, object] | None:
        if candidate_id < 0:
            trial = self._get_trial(abs(candidate_id))
            if trial is None or not _trial_should_surface_as_research_lead(trial):
                return None
            return _candidate_from_trial_lead(trial)
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, run_id, strategy_name, market_id, robustness_score, research_only, audit_json, created_at, promotion_tier
                FROM candidates WHERE id = ?
                """,
                (candidate_id,),
            ).fetchone()
        if row is None:
            return None
        audit = _compact_audit(json.loads(row[6]))
        return {
            "id": row[0],
            "run_id": row[1],
            "strategy_name": row[2],
            "market_id": row[3],
            "robustness_score": row[4],
            "research_only": bool(row[5]),
            "audit": audit,
            "created_at": row[7],
            "promotion_tier": str(audit.get("promotion_tier") or row[8]),
        }

    def _include_trial_research_leads(
        self,
        candidates: list[dict[str, object]],
        run_id: int | None,
        lead_limit: int | None = None,
    ) -> list[dict[str, object]]:
        existing = {(int(candidate["run_id"]), str(candidate["strategy_name"])) for candidate in candidates}
        limit = 12 if run_id is not None else 24
        if lead_limit is not None:
            limit = min(limit, max(0, int(lead_limit)))
        if limit <= 0:
            return sorted(candidates, key=_candidate_display_rank, reverse=True)
        added = 0
        for trial in self._list_candidate_lead_trials(run_id, limit * 4):
            key = (int(trial["run_id"]), str(trial["strategy_name"]))
            if key in existing or not _trial_should_surface_as_research_lead(trial):
                continue
            candidates.append(_candidate_from_trial_lead(trial))
            existing.add(key)
            added += 1
            if added >= limit:
                break
        return sorted(candidates, key=_candidate_display_rank, reverse=True)

    def _get_trial(self, trial_id: int) -> dict[str, object] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, run_id, strategy_name, passed, robustness_score, metrics_json, warnings_json,
                       strategy_family, style, parameters_json, backtest_json, folds_json, costs_json, tags_json,
                       promotion_tier
                FROM strategy_trials WHERE id = ?
                """,
                (trial_id,),
            ).fetchone()
        return _trial_from_row(row) if row is not None else None

    def _list_candidate_lead_trials(self, run_id: int | None, limit: int) -> list[dict[str, object]]:
        where = """
            WHERE (
              promotion_tier IN ('watchlist', 'incubator', 'research_candidate', 'paper_candidate', 'validated_candidate')
              OR robustness_score >= 25
            )
        """
        params: list[object] = []
        if run_id is not None:
            where += " AND run_id = ?"
            params.append(run_id)
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT id, run_id, strategy_name, passed, robustness_score, metrics_json, warnings_json,
                       strategy_family, style, parameters_json, backtest_json, folds_json, costs_json, tags_json,
                       promotion_tier
                FROM strategy_trials
                {where}
                ORDER BY robustness_score DESC, id DESC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
        return [_trial_from_row(row) for row in rows]

    def save_schedule(self, name: str, cadence: str, enabled: bool, config: dict[str, object]) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO research_schedules(name, cadence, enabled, config_json)
                VALUES (?, ?, ?, ?)
                """,
                (name, cadence, int(enabled), json.dumps(config, sort_keys=True)),
            )
            return int(cursor.lastrowid)

    def save_template(self, payload: dict[str, object]) -> dict[str, object]:
        normalized = _normalized_template_payload(payload)
        now = _now()
        with self._connect() as conn:
            existing = conn.execute(
                """
                SELECT id
                FROM strategy_templates
                WHERE source_fingerprint = ? AND source_fingerprint != ''
                ORDER BY id DESC
                LIMIT 1
                """,
                (normalized["source_fingerprint"],),
            ).fetchone()
            if existing is not None:
                template_id = int(existing[0])
                conn.execute(
                    """
                    UPDATE strategy_templates SET
                      updated_at = ?,
                      status = ?,
                      name = ?,
                      market_id = ?,
                      interval = ?,
                      strategy_family = ?,
                      style = ?,
                      target_regime = ?,
                      source_run_id = ?,
                      source_trial_id = ?,
                      source_candidate_id = ?,
                      source_kind = ?,
                      promotion_tier = ?,
                      readiness_status = ?,
                      robustness_score = ?,
                      testing_account_size = ?,
                      payload_json = ?
                    WHERE id = ?
                    """,
                    (
                        now,
                        normalized["status"],
                        normalized["name"],
                        normalized["market_id"],
                        normalized["interval"],
                        normalized["strategy_family"],
                        normalized["style"],
                        normalized["target_regime"],
                        normalized["source_run_id"],
                        normalized["source_trial_id"],
                        normalized["source_candidate_id"],
                        normalized["source_kind"],
                        normalized["promotion_tier"],
                        normalized["readiness_status"],
                        normalized["robustness_score"],
                        normalized["testing_account_size"],
                        json.dumps(normalized["payload"], sort_keys=True),
                        template_id,
                    ),
                )
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO strategy_templates(
                      created_at, updated_at, status, name, market_id, interval, strategy_family, style,
                      target_regime, source_run_id, source_trial_id, source_candidate_id, source_kind,
                      promotion_tier, readiness_status, robustness_score, testing_account_size,
                      source_fingerprint, payload_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        now,
                        now,
                        normalized["status"],
                        normalized["name"],
                        normalized["market_id"],
                        normalized["interval"],
                        normalized["strategy_family"],
                        normalized["style"],
                        normalized["target_regime"],
                        normalized["source_run_id"],
                        normalized["source_trial_id"],
                        normalized["source_candidate_id"],
                        normalized["source_kind"],
                        normalized["promotion_tier"],
                        normalized["readiness_status"],
                        normalized["robustness_score"],
                        normalized["testing_account_size"],
                        normalized["source_fingerprint"],
                        json.dumps(normalized["payload"], sort_keys=True),
                    ),
                )
                template_id = int(cursor.lastrowid)
        template = self.get_template(template_id)
        if template is None:
            raise RuntimeError("Saved template could not be reloaded")
        return template

    def get_template(self, template_id: int) -> dict[str, object] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, created_at, updated_at, status, name, market_id, interval,
                       strategy_family, style, target_regime, source_run_id, source_trial_id,
                       source_candidate_id, source_kind, promotion_tier, readiness_status,
                       robustness_score, testing_account_size, source_fingerprint, payload_json
                FROM strategy_templates
                WHERE id = ?
                """,
                (template_id,),
            ).fetchone()
        return _template_from_row(row) if row is not None else None

    def list_templates(self, include_inactive: bool = False, limit: int | None = None) -> list[dict[str, object]]:
        where = "" if include_inactive else "WHERE status != 'archived'"
        params: list[object] = []
        query = f"""
            SELECT id, created_at, updated_at, status, name, market_id, interval,
                   strategy_family, style, target_regime, source_run_id, source_trial_id,
                   source_candidate_id, source_kind, promotion_tier, readiness_status,
                   robustness_score, testing_account_size, source_fingerprint, payload_json
            FROM strategy_templates
            {where}
            ORDER BY
              CASE status WHEN 'active' THEN 0 WHEN 'paused' THEN 1 ELSE 2 END,
              robustness_score DESC,
              updated_at DESC,
              id DESC
        """
        if limit is not None:
            query += " LIMIT ?"
            params.append(max(0, int(limit)))
        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return [_template_from_row(row) for row in rows]

    def update_template_status(self, template_id: int, status: str) -> dict[str, object] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT id FROM strategy_templates WHERE id = ?", (template_id,)).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE strategy_templates SET status = ?, updated_at = ? WHERE id = ?",
                (status, _now(), template_id),
            )
        return self.get_template(template_id)

    def save_day_trading_scan(
        self,
        *,
        trading_date: str,
        status: str,
        account_size: float,
        product_mode: str,
        config: dict[str, object],
        daily_paper_queue: list[dict[str, object]],
        review_signals: list[dict[str, object]],
        unsuitable: list[dict[str, object]],
        results: dict[str, object] | None = None,
        error: str = "",
    ) -> dict[str, object]:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO day_trading_scans(
                  created_at, trading_date, status, account_size, product_mode, config_json,
                  queue_json, review_json, unsuitable_json, results_json, error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _now(),
                    trading_date,
                    status,
                    float(account_size),
                    product_mode,
                    json.dumps(config, sort_keys=True, default=str),
                    json.dumps(daily_paper_queue, sort_keys=True, default=str),
                    json.dumps(review_signals, sort_keys=True, default=str),
                    json.dumps(unsuitable, sort_keys=True, default=str),
                    json.dumps(results or {}, sort_keys=True, default=str),
                    error,
                ),
            )
            scan_id = int(cursor.lastrowid)
        scan = self.get_day_trading_scan(scan_id)
        if scan is None:
            raise RuntimeError("Saved day trading scan could not be reloaded")
        return scan

    def get_day_trading_scan(self, scan_id: int) -> dict[str, object] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, created_at, trading_date, status, account_size, product_mode, config_json,
                       queue_json, review_json, unsuitable_json, results_json, error
                FROM day_trading_scans
                WHERE id = ?
                """,
                (scan_id,),
            ).fetchone()
        return _day_trading_scan_from_row(row) if row is not None else None

    def latest_day_trading_scan(self) -> dict[str, object] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, created_at, trading_date, status, account_size, product_mode, config_json,
                       queue_json, review_json, unsuitable_json, results_json, error
                FROM day_trading_scans
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """
            ).fetchone()
        return _day_trading_scan_from_row(row) if row is not None else None

    def list_day_trading_scans(self, limit: int = 20) -> list[dict[str, object]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, created_at, trading_date, status, account_size, product_mode, config_json,
                       queue_json, review_json, unsuitable_json, results_json, error
                FROM day_trading_scans
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        return [_day_trading_scan_from_row(row) for row in rows]

    def update_day_trading_scan_results(
        self,
        scan_id: int,
        results: dict[str, object],
        status: str = "reviewed",
    ) -> dict[str, object] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT id FROM day_trading_scans WHERE id = ?", (scan_id,)).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE day_trading_scans SET status = ?, results_json = ? WHERE id = ?",
                (status, json.dumps(results, sort_keys=True, default=str), scan_id),
            )
        return self.get_day_trading_scan(scan_id)

    def backup_and_reset_research(self) -> dict[str, object]:
        """Back up and clear research-only state while leaving provider settings untouched."""
        backup_dir = self.db_path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        backup_path = backup_dir / f"research-before-scenario-reset-{timestamp}.sqlite3"
        if self.db_path.exists():
            shutil.copy2(self.db_path, backup_path)
        else:
            backup_path.touch()

        tables = [
            "day_trading_scans",
            "strategy_templates",
            "research_schedules",
            "research_run_bars",
            "candidates",
            "strategy_trials",
            "research_runs",
            "ig_cost_profiles",
        ]
        counts: dict[str, int] = {}
        with self._connect() as conn:
            for table in tables:
                row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                counts[table] = int(row[0] if row else 0)
            for table in tables:
                conn.execute(f"DELETE FROM {table}")
            for table in tables:
                conn.execute("DELETE FROM sqlite_sequence WHERE name = ?", (table,))
        return {
            "status": "reset",
            "backup_path": str(backup_path),
            "cleared_counts": counts,
            "preserved": ["settings", "provider credentials", "IG account roles", "market registry"],
        }


def _evaluation_audit(evaluation: CandidateEvaluation) -> dict[str, object]:
    parameters = evaluation.candidate.parameters
    backtest = _normalized_backtest_payload(asdict(evaluation.backtest), evaluation.candidate.parameters)
    warnings = _readiness_augmented_warnings(backtest, evaluation.warnings, parameters)
    readiness = promotion_readiness(backtest, warnings, parameters)
    promotion_tier = _display_promotion_tier(_raw_evaluation_tier(evaluation), readiness, backtest, parameters)
    return {
        "candidate": _compact_candidate(asdict(evaluation.candidate)),
        "metrics": asdict(evaluation.metrics),
        "backtest": _compact_backtest(backtest),
        "fold_results": [_compact_backtest(asdict(fold)) for fold in evaluation.fold_results],
        "warnings": warnings,
        "research_only": evaluation.research_only,
        "promotion_tier": promotion_tier,
        "promotion_readiness": readiness,
    }


def _display_promotion_tier(
    tier: str,
    readiness: dict[str, object],
    backtest: dict[str, object],
    parameters: dict[str, object],
) -> str:
    gated = gate_promotion_tier(tier, readiness)
    if gated != "research_candidate" or readiness.get("status") == "ready_for_paper":
        return gated
    blockers = set(readiness.get("blockers") or []) | set(readiness.get("validation_warnings") or [])
    if not blockers.intersection(INCUBATOR_RESEARCH_BLOCKERS):
        return gated
    stress_raw = parameters.get("stress_net_profit")
    stress_ok = stress_raw is None or _safe_float(stress_raw) > 0
    cost_ok = _safe_float(backtest.get("cost_to_gross_ratio")) <= 0.85 and _safe_float(backtest.get("net_cost_ratio")) >= 0.2
    if (
        _safe_float(backtest.get("net_profit")) > 0
        and _safe_float(backtest.get("test_profit")) > 0
        and int(_safe_float(backtest.get("trade_count"))) >= 10
        and stress_ok
        and cost_ok
    ):
        return "incubator"
    return gated


def _raw_evaluation_tier(evaluation: CandidateEvaluation) -> str:
    if evaluation.promotion_tier != "reject" or not evaluation.passed:
        return evaluation.promotion_tier
    return "paper_candidate"


def _evaluation_tier(evaluation: CandidateEvaluation) -> str:
    parameters = evaluation.candidate.parameters
    backtest = _normalized_backtest_payload(asdict(evaluation.backtest), parameters)
    warnings = _readiness_augmented_warnings(backtest, evaluation.warnings, parameters)
    readiness = promotion_readiness(backtest, warnings, parameters)
    return _display_promotion_tier(_raw_evaluation_tier(evaluation), readiness, backtest, parameters)


def _is_material_watchlist_lead(evaluation: CandidateEvaluation) -> bool:
    backtest = evaluation.backtest
    return (
        evaluation.robustness_score >= 25
        and backtest.trade_count >= 5
        and backtest.sharpe_observations > 0
        and (backtest.net_profit > 0 or backtest.test_profit > 0 or backtest.daily_pnl_sharpe >= 1.0)
    )


def _trial_regime_key(trial: dict[str, object]) -> str:
    parameters = trial.get("parameters") if isinstance(trial.get("parameters"), dict) else {}
    pattern = parameters.get("bar_pattern_analysis") if isinstance(parameters.get("bar_pattern_analysis"), dict) else {}
    dominant = pattern.get("dominant_profit_regime") if isinstance(pattern.get("dominant_profit_regime"), dict) else {}
    return str(parameters.get("target_regime") or pattern.get("target_regime") or dominant.get("key") or "").strip()


def _regime_pick_trial(trial: dict[str, object]) -> dict[str, object]:
    backtest = trial.get("backtest") if isinstance(trial.get("backtest"), dict) else {}
    parameters = trial.get("parameters") if isinstance(trial.get("parameters"), dict) else {}
    pattern = parameters.get("bar_pattern_analysis") if isinstance(parameters.get("bar_pattern_analysis"), dict) else {}
    evidence = parameters.get("evidence_profile") if isinstance(parameters.get("evidence_profile"), dict) else {}
    regime_evidence = pattern.get("regime_trade_evidence") if isinstance(pattern.get("regime_trade_evidence"), dict) else {}
    in_regime = regime_evidence.get("in_regime") if isinstance(regime_evidence.get("in_regime"), dict) else {}
    return {
        "id": trial.get("id"),
        "run_id": trial.get("run_id"),
        "strategy_name": trial.get("strategy_name"),
        "market_id": trial.get("market_id"),
        "promotion_tier": trial.get("promotion_tier"),
        "robustness_score": trial.get("robustness_score"),
        "strategy_family": trial.get("strategy_family"),
        "style": trial.get("style"),
        "target_regime": _trial_regime_key(trial),
        "is_specialist": bool(parameters.get("regime_scan") or parameters.get("target_regime")),
        "net_profit": backtest.get("net_profit"),
        "test_profit": backtest.get("test_profit"),
        "daily_pnl_sharpe": backtest.get("daily_pnl_sharpe"),
        "trade_count": backtest.get("trade_count"),
        "cost_to_gross_ratio": backtest.get("cost_to_gross_ratio"),
        "net_cost_ratio": backtest.get("net_cost_ratio"),
        "oos_net_profit": evidence.get("oos_net_profit"),
        "oos_trade_count": evidence.get("oos_trade_count"),
        "in_regime_net_profit": in_regime.get("net_profit"),
        "in_regime_test_profit": in_regime.get("test_profit"),
        "in_regime_test_trade_count": in_regime.get("test_trade_count"),
        "regime_trading_days": regime_evidence.get("regime_trading_days"),
        "regime_history_share": regime_evidence.get("regime_history_share"),
        "regime_episodes": regime_evidence.get("regime_episodes"),
        "regime_verdict": pattern.get("regime_verdict"),
        "warnings": trial.get("warnings") or [],
        "parameters": parameters,
        "backtest": backtest,
    }


def _trial_display_rank(trial: dict[str, object]) -> tuple[float, ...]:
    backtest = trial.get("backtest") if isinstance(trial.get("backtest"), dict) else {}
    parameters = trial.get("parameters") if isinstance(trial.get("parameters"), dict) else {}
    evidence = parameters.get("evidence_profile") if isinstance(parameters.get("evidence_profile"), dict) else {}
    tier_rank = {
        "validated_candidate": 5,
        "paper_candidate": 4,
        "research_candidate": 3,
        "incubator": 2,
        "watchlist": 1,
    }.get(str(trial.get("promotion_tier") or ""), 0)
    oos_trades = min(18.0, _safe_float(evidence.get("oos_trade_count")))
    return (
        float(tier_rank),
        _safe_float(evidence.get("oos_net_profit")),
        oos_trades,
        _safe_float(trial.get("robustness_score")),
        _safe_float(backtest.get("test_profit")),
        _safe_float(backtest.get("net_profit")),
        -_safe_float(backtest.get("cost_to_gross_ratio")),
    )


def _candidate_display_rank(candidate: dict[str, object]) -> tuple[float, ...]:
    audit = candidate.get("audit") if isinstance(candidate.get("audit"), dict) else {}
    backtest = audit.get("backtest") if isinstance(audit.get("backtest"), dict) else {}
    candidate_payload = audit.get("candidate") if isinstance(audit.get("candidate"), dict) else {}
    parameters = candidate_payload.get("parameters") if isinstance(candidate_payload.get("parameters"), dict) else {}
    evidence = parameters.get("evidence_profile") if isinstance(parameters.get("evidence_profile"), dict) else {}
    tier = str(candidate.get("promotion_tier") or audit.get("promotion_tier") or "")
    tier_rank = {
        "validated_candidate": 5,
        "paper_candidate": 4,
        "research_candidate": 3,
        "incubator": 2,
        "watchlist": 1,
    }.get(tier, 0)
    oos_trades = min(18.0, _safe_float(evidence.get("oos_trade_count")))
    return (
        float(tier_rank),
        _safe_float(evidence.get("oos_net_profit")),
        oos_trades,
        _safe_float(candidate.get("robustness_score")),
        _safe_float(backtest.get("test_profit")),
        _safe_float(backtest.get("net_profit")),
        -_safe_float(backtest.get("cost_to_gross_ratio")),
        float(candidate.get("id") or 0),
    )


def _trial_should_surface_as_research_lead(trial: dict[str, object]) -> bool:
    tier = str(trial.get("promotion_tier") or "reject")
    backtest = trial.get("backtest") if isinstance(trial.get("backtest"), dict) else {}
    if tier in {"watchlist", "incubator", "research_candidate", "paper_candidate", "validated_candidate"}:
        return int(backtest.get("sharpe_observations") or 0) > 0 or float(backtest.get("daily_pnl_sharpe") or 0.0) != 0.0
    return (
        float(trial.get("robustness_score") or 0.0) >= 25
        and float(backtest.get("net_profit") or 0.0) > 0
        and int(backtest.get("sharpe_observations") or 0) > 0
        and (float(backtest.get("test_profit") or 0.0) > 0 or float(backtest.get("daily_pnl_sharpe") or 0.0) >= 1.0)
        and int(backtest.get("trade_count") or 0) >= 5
    )


def _risk_adjusted_sharpe_from_payload(backtest: dict[str, object]) -> float:
    return float(backtest.get("daily_pnl_sharpe") or backtest.get("sharpe") or 0.0)


def _normalized_warnings(warnings: object, backtest: dict[str, object]) -> list[str]:
    output = list(warnings or [])
    if "weak_sharpe" in output and _risk_adjusted_sharpe_from_payload(backtest) >= 0.55:
        output = [warning for warning in output if warning != "weak_sharpe"]
    if int(backtest.get("sharpe_observations") or 0) <= 0 and (
        float(backtest.get("sharpe") or 0.0) != 0.0 or float(backtest.get("net_profit") or 0.0) != 0.0
    ):
        output.append("legacy_sharpe_diagnostics")
    if (
        float(backtest.get("estimated_spread_bps") or 0.0) <= 0.0
        and float(backtest.get("estimated_slippage_bps") or 0.0) <= 0.0
        and float(backtest.get("total_cost") or 0.0) > 0.0
    ):
        output.append("missing_cost_profile")
    return list(dict.fromkeys(output))


def _readiness_augmented_warnings(
    backtest: dict[str, object],
    warnings: object,
    parameters: dict[str, object] | None = None,
) -> list[str]:
    normalized = _normalized_warnings(warnings, backtest)
    readiness = promotion_readiness(backtest, normalized, parameters or {})
    return list(dict.fromkeys(normalized + readiness_warnings(readiness)))


def _candidate_from_trial_lead(trial: dict[str, object]) -> dict[str, object]:
    tier = str(trial.get("promotion_tier") or "reject")
    if tier == "reject":
        tier = "watchlist"
    warnings = _normalized_warnings(trial.get("warnings") or [], trial.get("backtest") if isinstance(trial.get("backtest"), dict) else {})
    if str(trial.get("promotion_tier") or "reject") == "reject" and "not_paper_ready_research_lead" not in warnings:
        warnings.insert(0, "not_paper_ready_research_lead")
    audit = {
        "candidate": {
            "name": trial.get("strategy_name"),
            "module_stack": trial.get("tags") or (),
            "parameters": trial.get("parameters") or {},
            "probability_count": 0,
        },
        "metrics": trial.get("metrics") or {},
        "backtest": trial.get("backtest") or {},
        "fold_results": trial.get("folds") or [],
        "warnings": warnings,
        "research_only": True,
        "promotion_tier": tier,
        "derived_from_trial_id": trial.get("id"),
    }
    return {
        "id": -int(trial["id"]),
        "run_id": trial["run_id"],
        "strategy_name": trial["strategy_name"],
        "market_id": (trial.get("parameters") or {}).get("market_id") or "",
        "robustness_score": trial["robustness_score"],
        "research_only": True,
        "audit": _compact_audit(audit),
        "created_at": "",
        "promotion_tier": tier,
        "source": "trial_research_lead",
    }


def _bar_snapshot_row(bar: OHLCBar) -> dict[str, object]:
    return {
        "symbol": bar.symbol,
        "timestamp": bar.timestamp.isoformat(),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
    }


def _trial_from_row(row: sqlite3.Row | tuple[object, ...]) -> dict[str, object]:
    parameters = json.loads(row[9])
    costs = json.loads(row[12])
    backtest = _normalized_backtest_payload(json.loads(row[10]), parameters, costs)
    warnings = _readiness_augmented_warnings(backtest, json.loads(row[6]), parameters)
    readiness = promotion_readiness(backtest, warnings, parameters)
    return {
        "id": row[0],
        "run_id": row[1],
        "strategy_name": row[2],
        "market_id": str(parameters.get("market_id") or ""),
        "passed": bool(row[3]),
        "robustness_score": row[4],
        "metrics": json.loads(row[5]),
        "warnings": warnings,
        "strategy_family": row[7],
        "style": row[8],
        "parameters": parameters,
        "backtest": backtest,
        "folds": json.loads(row[11]),
        "costs": costs,
        "tags": json.loads(row[13]),
        "promotion_tier": _display_promotion_tier(str(row[14]), readiness, backtest, parameters),
        "promotion_readiness": readiness,
    }


def _normalized_template_payload(payload: dict[str, object]) -> dict[str, object]:
    raw = dict(payload or {})
    details = _dict_value(raw.get("payload"))
    source_template = _dict_value(details.get("source_template") or raw.get("source_template"))
    parameters = _dict_value(details.get("parameters"))
    backtest = _dict_value(details.get("backtest"))
    pattern = _dict_value(details.get("pattern") or parameters.get("bar_pattern_analysis"))
    evidence = _dict_value(details.get("evidence"))
    readiness = _dict_value(details.get("readiness"))
    search_audit = _dict_value(details.get("search_audit") or parameters.get("search_audit"))
    capital_scenarios = _list_value(details.get("capital_scenarios"))
    warnings = _dedupe_strings(
        _list_value(raw.get("warnings"))
        + _list_value(details.get("warnings"))
        + _list_value(pattern.get("warnings"))
        + _list_value(readiness.get("blockers"))
        + _list_value(readiness.get("validation_warnings"))
    )
    name = _clean_string(raw.get("name") or source_template.get("name") or "Saved template")
    market_id = _clean_string(raw.get("market_id") or source_template.get("market_id") or parameters.get("market_id"))
    interval = _clean_string(raw.get("interval") or source_template.get("interval") or parameters.get("timeframe") or parameters.get("interval") or "5min")
    strategy_family = _clean_string(raw.get("strategy_family") or source_template.get("family") or parameters.get("family"))
    style = _clean_string(raw.get("style") or source_template.get("style") or parameters.get("style") or search_audit.get("trading_style") or "find_anything_robust")
    target_regime = _clean_string(raw.get("target_regime") or source_template.get("target_regime") or parameters.get("target_regime") or pattern.get("target_regime"))
    promotion_tier = _clean_string(raw.get("promotion_tier") or details.get("promotion_tier") or "research_candidate")
    readiness_status = _clean_string(raw.get("readiness_status") or readiness.get("status") or "blocked")
    testing_account_size = _safe_float(raw.get("testing_account_size") or parameters.get("testing_account_size") or search_audit.get("testing_account_size"))
    detail_payload = dict(details)
    detail_payload.update(
        {
            "source_template": source_template,
            "parameters": parameters,
            "backtest": backtest,
            "pattern": pattern,
            "evidence": evidence,
            "readiness": readiness,
            "warnings": warnings,
            "search_audit": search_audit,
            "capital_scenarios": capital_scenarios,
        }
    )
    fingerprint_payload = {
        "name": name,
        "market_id": market_id,
        "interval": interval,
        "family": strategy_family,
        "style": style,
        "target_regime": target_regime,
        "source_run_id": _safe_int_or_none(raw.get("source_run_id")),
        "source_trial_id": _safe_int_or_none(raw.get("source_trial_id")),
        "source_candidate_id": _safe_int_or_none(raw.get("source_candidate_id")),
        "template_parameters": source_template.get("parameters") or parameters,
    }
    fingerprint = hashlib.sha256(json.dumps(fingerprint_payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    return {
        "status": _template_status(raw.get("status")),
        "name": name,
        "market_id": market_id,
        "interval": interval,
        "strategy_family": strategy_family,
        "style": style,
        "target_regime": target_regime,
        "source_run_id": _safe_int_or_none(raw.get("source_run_id")),
        "source_trial_id": _safe_int_or_none(raw.get("source_trial_id")),
        "source_candidate_id": _safe_int_or_none(raw.get("source_candidate_id")),
        "source_kind": _clean_string(raw.get("source_kind") or details.get("source_kind")),
        "promotion_tier": promotion_tier,
        "readiness_status": readiness_status,
        "robustness_score": _safe_float(raw.get("robustness_score")),
        "testing_account_size": testing_account_size,
        "source_fingerprint": fingerprint,
        "payload": detail_payload,
    }


def _template_from_row(row: sqlite3.Row | tuple[object, ...]) -> dict[str, object]:
    payload = _json_dict(row[19])
    source_template = _dict_value(payload.get("source_template"))
    parameters = _dict_value(payload.get("parameters"))
    backtest = _dict_value(payload.get("backtest"))
    pattern = _dict_value(payload.get("pattern") or parameters.get("bar_pattern_analysis"))
    readiness = _dict_value(payload.get("readiness"))
    warnings = _dedupe_strings(
        _list_value(payload.get("warnings"))
        + _list_value(pattern.get("warnings"))
        + _list_value(readiness.get("blockers"))
        + _list_value(readiness.get("validation_warnings"))
    )
    return {
        "id": row[0],
        "created_at": row[1],
        "updated_at": row[2],
        "status": row[3],
        "name": row[4],
        "market_id": row[5],
        "interval": row[6],
        "strategy_family": row[7],
        "style": row[8],
        "target_regime": row[9],
        "source_run_id": row[10],
        "source_trial_id": row[11],
        "source_candidate_id": row[12],
        "source_kind": row[13],
        "promotion_tier": row[14],
        "readiness_status": row[15],
        "robustness_score": row[16],
        "testing_account_size": row[17],
        "source_fingerprint": row[18],
        "payload": payload,
        "source_template": source_template,
        "parameters": parameters,
        "backtest": backtest,
        "pattern": pattern,
        "readiness": readiness,
        "warnings": warnings,
        "capital_scenarios": _list_value(payload.get("capital_scenarios")),
    }


def _day_trading_scan_from_row(row: sqlite3.Row | tuple[object, ...]) -> dict[str, object]:
    config = _json_dict(row[6])
    daily_paper_queue = _json_list(row[7])
    review_signals = _json_list(row[8])
    unsuitable = _json_list(row[9])
    results = _json_dict(row[10])
    return {
        "id": row[0],
        "created_at": row[1],
        "trading_date": row[2],
        "status": row[3],
        "account_size": row[4],
        "product_mode": row[5],
        "config": config,
        "daily_paper_queue": daily_paper_queue,
        "review_signals": review_signals,
        "unsuitable": unsuitable,
        "after_close_results": results,
        "error": row[11],
        "counts": {
            "daily_paper_queue": len(daily_paper_queue),
            "review_signals": len(review_signals),
            "unsuitable": len(unsuitable),
        },
    }


def _template_status(value: object) -> str:
    status = _clean_string(value or "active")
    return status if status in {"active", "paused", "archived"} else "active"


def _dict_value(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, dict) else {}


def _list_value(value: object) -> list[object]:
    return list(value) if isinstance(value, list) else []


def _json_dict(value: object) -> dict[str, object]:
    try:
        decoded = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _json_list(value: object) -> list[dict[str, object]]:
    try:
        decoded = json.loads(str(value or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return [item for item in decoded if isinstance(item, dict)] if isinstance(decoded, list) else []


def _clean_string(value: object) -> str:
    return str(value or "").strip()


def _safe_int_or_none(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _dedupe_strings(values: list[object]) -> list[str]:
    return list(dict.fromkeys(str(value) for value in values if value))


def _compact_audit(audit: dict[str, object]) -> dict[str, object]:
    candidate = audit.get("candidate")
    parameters: dict[str, object] = {}
    if isinstance(candidate, dict):
        maybe_parameters = candidate.get("parameters")
        if isinstance(maybe_parameters, dict):
            parameters = maybe_parameters
        audit["candidate"] = _compact_candidate(candidate)
    backtest = audit.get("backtest")
    if isinstance(backtest, dict):
        backtest = _normalized_backtest_payload(backtest, parameters)
        backtest = _compact_backtest(backtest)
        audit["backtest"] = backtest
        warnings = _readiness_augmented_warnings(backtest, audit.get("warnings") or [], parameters)
        readiness = promotion_readiness(backtest, warnings, parameters)
        audit["warnings"] = warnings
        audit["promotion_readiness"] = readiness
        audit["promotion_tier"] = _display_promotion_tier(str(audit.get("promotion_tier") or "reject"), readiness, backtest, parameters)
    return audit


def _normalized_backtest_payload(
    backtest: dict[str, object],
    parameters: dict[str, object] | None = None,
    costs: dict[str, object] | None = None,
) -> dict[str, object]:
    payload = dict(backtest)
    parameters = parameters or {}
    costs = costs or {}

    def number(key: str) -> float:
        try:
            return float(payload.get(key) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    for key in ("estimated_spread_bps", "estimated_slippage_bps"):
        if number(key) <= 0.0:
            fallback = _first_positive_number(parameters.get(key), costs.get(key))
            if fallback > 0.0:
                payload[key] = fallback
    if not payload.get("cost_confidence"):
        fallback_confidence = costs.get("cost_confidence") or parameters.get("cost_confidence")
        if fallback_confidence:
            payload["cost_confidence"] = str(fallback_confidence)

    net_profit = number("net_profit")
    gross_profit = number("gross_profit")
    total_cost = number("total_cost")
    trade_count = int(number("trade_count"))
    if trade_count > 0 and number("expectancy_per_trade") == 0.0 and net_profit != 0.0:
        payload["expectancy_per_trade"] = net_profit / trade_count
    if total_cost > 0.0 and number("net_cost_ratio") == 0.0 and net_profit != 0.0:
        payload["net_cost_ratio"] = net_profit / total_cost
    if total_cost > 0.0 and number("cost_to_gross_ratio") == 0.0 and gross_profit != 0.0:
        payload["cost_to_gross_ratio"] = total_cost / abs(gross_profit)
    if int(number("sharpe_observations")) <= 0:
        daily_pnl_curve = payload.get("daily_pnl_curve")
        if isinstance(daily_pnl_curve, list) and daily_pnl_curve:
            payload["sharpe_observations"] = len(daily_pnl_curve)
    return payload


def _first_positive_number(*values: object) -> float:
    for value in values:
        try:
            number = float(value or 0.0)
        except (TypeError, ValueError):
            continue
        if number > 0.0:
            return number
    return 0.0


def _compact_candidate(candidate: dict[str, object]) -> dict[str, object]:
    probabilities = list(candidate.pop("probabilities", []) or [])
    if probabilities:
        candidate["probability_count"] = len(probabilities)
        candidate["probability_sample"] = _sample_values(probabilities, 120)
    return candidate


def _compact_backtest(backtest: dict[str, object]) -> dict[str, object]:
    for key in ("equity_curve", "drawdown_curve", "daily_pnl_curve", "compounded_projection_daily_pnl_curve"):
        values = list(backtest.get(key) or [])
        if len(values) > 120:
            backtest[key] = _sample_values(values, 120)
    return backtest


def _sample_values(values: list[object], limit: int) -> list[object]:
    if len(values) <= limit:
        return values
    step = max(1, len(values) // limit)
    return values[::step][:limit]


def _run_purpose(config_json: str | bytes | None) -> dict[str, object]:
    try:
        config = json.loads(config_json or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        config = {}
    source_template = config.get("source_template") if isinstance(config.get("source_template"), dict) else {}
    repair_mode = str(config.get("repair_mode") or "standard")
    include_regime_scans = bool(config.get("include_regime_scans"))
    target_regime = str(config.get("target_regime") or source_template.get("target_regime") or "")
    source_template_name = str(source_template.get("name") or "")
    day_trading_mode = bool(config.get("day_trading_mode"))
    if day_trading_mode and repair_mode == "standard":
        kind = "day_trading_factory"
    elif repair_mode == "frozen_validation":
        kind = "frozen_validation"
    elif repair_mode in {"capital_fit"}:
        kind = "capital_fit"
    elif repair_mode in {
        "auto_refine",
        "more_trades",
        "longer_history",
        "cost_stress",
        "regime_repair",
        "evidence_first",
        "focused_retest",
        "month_exclusion",
    }:
        kind = "repair"
    elif repair_mode == "cross_market_discovery":
        kind = "cross_market"
    elif include_regime_scans:
        kind = "regime_scan"
    else:
        kind = "backtest"
    return {
        "kind": kind,
        "repair_mode": repair_mode,
        "trading_style": config.get("trading_style"),
        "target_regime": target_regime,
        "source_template_name": source_template_name,
        "include_regime_scans": include_regime_scans,
        "day_trading_mode": day_trading_mode,
        "account_size": config.get("account_size"),
    }


def _now() -> str:
    return datetime.now(UTC).isoformat()
