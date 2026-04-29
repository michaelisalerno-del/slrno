from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from .config import app_home
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

    def create_run(self, market_id: str, config: dict[str, object], data_source: str = "fmp", status: str = "created") -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO research_runs(created_at, status, market_id, data_source, config_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (_now(), status, market_id, data_source, json.dumps(config, sort_keys=True)),
            )
            return int(cursor.lastrowid)

    def update_run_status(self, run_id: int, status: str) -> None:
        with self._connect() as conn:
            conn.execute("UPDATE research_runs SET status = ? WHERE id = ?", (status, run_id))

    def save_trial(self, run_id: int, evaluation: CandidateEvaluation) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO strategy_trials(run_id, strategy_name, passed, robustness_score, metrics_json, warnings_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    evaluation.candidate.name,
                    int(evaluation.passed),
                    evaluation.robustness_score,
                    json.dumps(asdict(evaluation.metrics), sort_keys=True),
                    json.dumps(list(evaluation.warnings), sort_keys=True),
                ),
            )

    def save_candidate(self, run_id: int, market_id: str, evaluation: CandidateEvaluation) -> None:
        if not evaluation.passed:
            return
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO candidates(run_id, strategy_name, market_id, robustness_score, research_only, audit_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    evaluation.candidate.name,
                    market_id,
                    evaluation.robustness_score,
                    int(evaluation.research_only),
                    json.dumps(_evaluation_audit(evaluation), sort_keys=True),
                    _now(),
                ),
            )

    def list_runs(self) -> list[dict[str, object]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT run.id, run.created_at, run.status, run.market_id, run.data_source,
                       COUNT(trial.id) AS trial_count,
                       COALESCE(SUM(trial.passed), 0) AS passed_count
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
                "trial_count": row[5],
                "passed_count": row[6],
            }
            for row in rows
        ]

    def get_run(self, run_id: int) -> dict[str, object] | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT run.id, run.created_at, run.status, run.market_id, run.data_source,
                       run.config_json,
                       COUNT(trial.id) AS trial_count,
                       COALESCE(SUM(trial.passed), 0) AS passed_count
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
            "trial_count": row[6],
            "passed_count": row[7],
        }

    def list_trials(self, run_id: int | None = None) -> list[dict[str, object]]:
        query = """
            SELECT id, run_id, strategy_name, passed, robustness_score, metrics_json, warnings_json
            FROM strategy_trials
        """
        params: tuple[object, ...] = ()
        if run_id is not None:
            query += " WHERE run_id = ?"
            params = (run_id,)
        query += " ORDER BY id"
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
            }
            for row in rows
        ]

    def list_candidates(self, run_id: int | None = None) -> list[dict[str, object]]:
        params: tuple[object, ...] = ()
        where = ""
        if run_id is not None:
            where = "WHERE run_id = ?"
            params = (run_id,)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT id, run_id, strategy_name, market_id, robustness_score, research_only, audit_json, created_at
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
                "audit": json.loads(row[6]),
                "created_at": row[7],
            }
            for row in rows
        ]

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
        "candidate": asdict(evaluation.candidate),
        "metrics": asdict(evaluation.metrics),
        "backtest": asdict(evaluation.backtest),
        "fold_results": [asdict(fold) for fold in evaluation.fold_results],
        "warnings": list(evaluation.warnings),
        "research_only": evaluation.research_only,
    }


def _now() -> str:
    return datetime.now(UTC).isoformat()
