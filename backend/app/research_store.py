from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from .config import app_home
from .ig_costs import IGCostProfile
from .research_lab import CandidateEvaluation


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
                CREATE TABLE IF NOT EXISTS research_schedules (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  name TEXT NOT NULL,
                  cadence TEXT NOT NULL,
                  enabled INTEGER NOT NULL,
                  config_json TEXT NOT NULL
                )
                """
            )

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
            conn.execute("DELETE FROM candidates WHERE run_id = ?", (run_id,))
            conn.execute("DELETE FROM strategy_trials WHERE run_id = ?", (run_id,))
            conn.execute("DELETE FROM research_runs WHERE id = ?", (run_id,))
        return {"run_id": run_id, "deleted_trials": trial_count, "deleted_candidates": candidate_count}

    def save_trial(self, run_id: int, evaluation: CandidateEvaluation) -> None:
        parameters = dict(evaluation.candidate.parameters)
        backtest = _compact_backtest(asdict(evaluation.backtest))
        folds = [_compact_backtest(asdict(fold)) for fold in evaluation.fold_results]
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
                    int(evaluation.passed),
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
                    _evaluation_tier(evaluation),
                ),
            )

    def save_candidate(self, run_id: int, market_id: str, evaluation: CandidateEvaluation) -> None:
        promotion_tier = _evaluation_tier(evaluation)
        if promotion_tier not in {"research_candidate", "paper_candidate", "validated_candidate"}:
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

    def list_runs(self) -> list[dict[str, object]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT run.id, run.created_at, run.status, run.market_id, run.data_source, run.error,
                       COUNT(trial.id) AS trial_count,
                       COALESCE(SUM(trial.passed), 0) AS passed_count,
                       MAX(trial.robustness_score) AS best_score
                FROM research_runs run
                LEFT JOIN strategy_trials trial ON trial.run_id = run.id
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
                "trial_count": row[6],
                "passed_count": row[7],
                "best_score": row[8] or 0,
            }
            for row in rows
        ]

    def get_run(self, run_id: int) -> dict[str, object] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT run.id, run.created_at, run.status, run.market_id, run.data_source,
                       run.config_json, run.error,
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
            "trial_count": row[7],
            "passed_count": row[8],
            "best_score": row[9] or 0,
        }

    def list_trials(self, run_id: int | None = None) -> list[dict[str, object]]:
        query = """
            SELECT id, run_id, strategy_name, passed, robustness_score, metrics_json, warnings_json,
                   strategy_family, style, parameters_json, backtest_json, folds_json, costs_json, tags_json,
                   promotion_tier
            FROM strategy_trials
        """
        params: tuple[object, ...] = ()
        if run_id is not None:
            query += " WHERE run_id = ?"
            params = (run_id,)
        query += " ORDER BY robustness_score DESC, id"
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            {
                "id": row[0],
                "run_id": row[1],
                "strategy_name": row[2],
                "passed": bool(row[3]),
                "robustness_score": row[4],
                "metrics": json.loads(row[5]),
                "warnings": json.loads(row[6]),
                "strategy_family": row[7],
                "style": row[8],
                "parameters": json.loads(row[9]),
                "backtest": json.loads(row[10]),
                "folds": json.loads(row[11]),
                "costs": json.loads(row[12]),
                "tags": json.loads(row[13]),
                "promotion_tier": row[14],
            }
            for row in rows
        ]

    def list_pareto(self, run_id: int) -> list[dict[str, object]]:
        trials = self.list_trials(run_id)
        if not trials:
            return []
        choices = [
            ("best_balanced", max(trials, key=lambda item: float(item["robustness_score"]))),
            ("highest_sharpe", max(trials, key=lambda item: float(item["backtest"].get("sharpe") or 0))),
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
                    "estimated_spread_bps": backtest.get("estimated_spread_bps", 0),
                    "estimated_slippage_bps": backtest.get("estimated_slippage_bps", 0),
                    "max_drawdown": backtest.get("max_drawdown", 0),
                    "trade_count": backtest.get("trade_count", 0),
                    "warnings": trial["warnings"],
                    "settings": trial["parameters"],
                    "promotion_tier": trial["promotion_tier"],
                    "daily_pnl_sharpe": backtest.get("daily_pnl_sharpe", 0),
                    "deflated_sharpe_probability": (trial["parameters"].get("sharpe_diagnostics") or {}).get("deflated_sharpe_probability")
                    if isinstance(trial["parameters"].get("sharpe_diagnostics"), dict)
                    else 0,
                }
            )
        return output

    def list_candidates(self, run_id: int | None = None) -> list[dict[str, object]]:
        params: tuple[object, ...] = ()
        where = ""
        if run_id is not None:
            where = "WHERE run_id = ?"
            params = (run_id,)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT id, run_id, strategy_name, market_id, robustness_score, research_only, audit_json, created_at, promotion_tier
                FROM candidates {where} ORDER BY robustness_score DESC, id DESC
                """,
                params,
            ).fetchall()
        return [
            {
                "id": row[0],
                "run_id": row[1],
                "strategy_name": row[2],
                "market_id": row[3],
                "robustness_score": row[4],
                "research_only": bool(row[5]),
                "audit": _compact_audit(json.loads(row[6])),
                "created_at": row[7],
                "promotion_tier": row[8],
            }
            for row in rows
        ]

    def get_candidate(self, candidate_id: int) -> dict[str, object] | None:
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
        return {
            "id": row[0],
            "run_id": row[1],
            "strategy_name": row[2],
            "market_id": row[3],
            "robustness_score": row[4],
            "research_only": bool(row[5]),
            "audit": _compact_audit(json.loads(row[6])),
            "created_at": row[7],
            "promotion_tier": row[8],
        }

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


def _evaluation_audit(evaluation: CandidateEvaluation) -> dict[str, object]:
    return {
        "candidate": _compact_candidate(asdict(evaluation.candidate)),
        "metrics": asdict(evaluation.metrics),
        "backtest": _compact_backtest(asdict(evaluation.backtest)),
        "fold_results": [_compact_backtest(asdict(fold)) for fold in evaluation.fold_results],
        "warnings": list(evaluation.warnings),
        "research_only": evaluation.research_only,
        "promotion_tier": _evaluation_tier(evaluation),
    }


def _evaluation_tier(evaluation: CandidateEvaluation) -> str:
    if evaluation.promotion_tier != "reject" or not evaluation.passed:
        return evaluation.promotion_tier
    return "paper_candidate"


def _compact_audit(audit: dict[str, object]) -> dict[str, object]:
    candidate = audit.get("candidate")
    if isinstance(candidate, dict):
        audit["candidate"] = _compact_candidate(candidate)
    return audit


def _compact_candidate(candidate: dict[str, object]) -> dict[str, object]:
    probabilities = list(candidate.pop("probabilities", []) or [])
    if probabilities:
        candidate["probability_count"] = len(probabilities)
        candidate["probability_sample"] = _sample_values(probabilities, 120)
    return candidate


def _compact_backtest(backtest: dict[str, object]) -> dict[str, object]:
    for key in ("equity_curve", "drawdown_curve", "daily_pnl_curve"):
        values = list(backtest.get(key) or [])
        if len(values) > 120:
            backtest[key] = _sample_values(values, 120)
    return backtest


def _sample_values(values: list[object], limit: int) -> list[object]:
    if len(values) <= limit:
        return values
    step = max(1, len(values) // limit)
    return values[::step][:limit]


def _now() -> str:
    return datetime.now(UTC).isoformat()
