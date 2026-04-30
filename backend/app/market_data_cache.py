from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .config import app_home


def market_data_cache_db_path() -> Path:
    return app_home() / "market_data_cache.sqlite3"


@dataclass(frozen=True)
class CacheStats:
    entry_count: int
    expired_count: int
    oldest_created_at: str | None
    newest_created_at: str | None
    payload_entry_count: int = 0
    provider_error_count: int = 0

    def as_dict(self) -> dict[str, object]:
        return self.__dict__


class MarketDataCache:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or market_data_cache_db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_data_cache (
                  cache_key TEXT PRIMARY KEY,
                  namespace TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  expires_at TEXT NOT NULL,
                  payload_json TEXT NOT NULL
                )
                """
            )
            self._add_column(conn, "base_url", "TEXT NOT NULL DEFAULT ''")
            self._add_column(conn, "params_json", "TEXT NOT NULL DEFAULT '{}'")
            self._add_column(conn, "metadata_json", "TEXT NOT NULL DEFAULT '{}'")
            self._add_column(conn, "last_accessed_at", "TEXT NOT NULL DEFAULT ''")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_market_data_cache_namespace ON market_data_cache(namespace)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_market_data_cache_expires_at ON market_data_cache(expires_at)")

    def _add_column(self, conn: sqlite3.Connection, name: str, definition: str) -> None:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(market_data_cache)").fetchall()}
        if name not in columns:
            conn.execute(f"ALTER TABLE market_data_cache ADD COLUMN {name} {definition}")

    def get_json(self, namespace: str, base_url: str, params: dict[str, object], allow_stale: bool = False) -> Any | None:
        key = _cache_key(namespace, base_url, params)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT expires_at, payload_json FROM market_data_cache WHERE cache_key = ?",
                (key,),
            ).fetchone()
            if row is not None:
                conn.execute(
                    "UPDATE market_data_cache SET last_accessed_at = ? WHERE cache_key = ?",
                    (_now().isoformat(), key),
                )
        if row is None:
            return None
        expires_at = _parse_timestamp(row[0])
        if not allow_stale and expires_at <= _now():
            return None
        return json.loads(row[1])

    def set_json(
        self,
        namespace: str,
        base_url: str,
        params: dict[str, object],
        payload: Any,
        ttl_seconds: int,
        metadata: dict[str, object] | None = None,
    ) -> None:
        key = _cache_key(namespace, base_url, params)
        created_at = _now()
        expires_at = created_at + timedelta(seconds=max(1, ttl_seconds))
        safe_params = _safe_params(params)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO market_data_cache(
                  cache_key, namespace, created_at, expires_at, payload_json,
                  base_url, params_json, metadata_json, last_accessed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                  namespace = excluded.namespace,
                  created_at = excluded.created_at,
                  expires_at = excluded.expires_at,
                  payload_json = excluded.payload_json,
                  base_url = excluded.base_url,
                  params_json = excluded.params_json,
                  metadata_json = excluded.metadata_json,
                  last_accessed_at = excluded.last_accessed_at
                """,
                (
                    key,
                    namespace,
                    created_at.isoformat(),
                    expires_at.isoformat(),
                    json.dumps(payload, sort_keys=True),
                    base_url.rstrip("/"),
                    json.dumps(safe_params, sort_keys=True),
                    json.dumps(metadata or {}, sort_keys=True),
                    created_at.isoformat(),
                ),
            )

    def stats(self) -> CacheStats:
        now = _now().isoformat()
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*),
                       SUM(CASE WHEN expires_at <= ? THEN 1 ELSE 0 END),
                       MIN(created_at),
                       MAX(created_at),
                       SUM(CASE WHEN namespace != 'provider_error' THEN 1 ELSE 0 END),
                       SUM(CASE WHEN namespace = 'provider_error' THEN 1 ELSE 0 END)
                FROM market_data_cache
                """,
                (now,),
            ).fetchone()
        return CacheStats(int(row[0] or 0), int(row[1] or 0), row[2], row[3], int(row[4] or 0), int(row[5] or 0))

    def namespace_stats(self) -> list[dict[str, object]]:
        now = _now().isoformat()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT namespace,
                       COUNT(*),
                       SUM(CASE WHEN expires_at <= ? THEN 1 ELSE 0 END),
                       MIN(created_at),
                       MAX(created_at),
                       MIN(expires_at),
                       MAX(expires_at)
                FROM market_data_cache
                GROUP BY namespace
                ORDER BY namespace
                """,
                (now,),
            ).fetchall()
        return [
            {
                "namespace": row[0],
                "entry_count": int(row[1] or 0),
                "expired_count": int(row[2] or 0),
                "oldest_created_at": row[3],
                "newest_created_at": row[4],
                "oldest_expires_at": row[5],
                "newest_expires_at": row[6],
            }
            for row in rows
        ]

    def recent_entries(self, namespace: str | None = None, limit: int = 20) -> list[dict[str, object]]:
        now = _now().isoformat()
        limit = min(max(1, int(limit)), 100)
        where = ""
        params: tuple[object, ...] = (now,)
        if namespace:
            where = "WHERE namespace = ?"
            params = (now, namespace)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT namespace, created_at, expires_at, last_accessed_at,
                       base_url, params_json, metadata_json, LENGTH(payload_json),
                       CASE WHEN expires_at <= ? THEN 1 ELSE 0 END
                FROM market_data_cache
                {where}
                ORDER BY created_at DESC
                LIMIT {limit}
                """,
                params,
            ).fetchall()
        return [
            {
                "namespace": row[0],
                "created_at": row[1],
                "expires_at": row[2],
                "last_accessed_at": row[3] or None,
                "request_url": row[4],
                "params": _loads_json_object(row[5]),
                "metadata": _loads_json_object(row[6]),
                "payload_bytes": int(row[7] or 0),
                "expired": bool(row[8]),
            }
            for row in rows
        ]

    def prune_expired(self) -> int:
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM market_data_cache WHERE expires_at <= ?", (_now().isoformat(),))
            return int(cursor.rowcount or 0)


def _cache_key(namespace: str, base_url: str, params: dict[str, object]) -> str:
    safe_params = _safe_params(params)
    payload = json.dumps(
        {"namespace": namespace, "base_url": base_url.rstrip("/"), "params": safe_params},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _safe_params(params: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in params.items() if key.lower() not in {"apikey", "api_token"}}


def _loads_json_object(value: str | None) -> dict[str, object]:
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _now() -> datetime:
    return datetime.now(UTC)


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed
