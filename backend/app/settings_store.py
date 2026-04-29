from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .crypto import Cipher, FernetCipher
from .config import key_path, settings_db_path


@dataclass(frozen=True)
class ProviderStatus:
    provider: str
    configured: bool
    last_status: str
    last_error: str | None = None


class SettingsStore:
    def __init__(self, db_path: Path | None = None, cipher: Cipher | None = None) -> None:
        self.db_path = db_path or settings_db_path()
        self.cipher = cipher or FernetCipher.from_key_file(key_path())
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS secrets (
                  provider TEXT NOT NULL,
                  name TEXT NOT NULL,
                  value BLOB NOT NULL,
                  PRIMARY KEY (provider, name)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS provider_status (
                  provider TEXT PRIMARY KEY,
                  configured INTEGER NOT NULL DEFAULT 0,
                  last_status TEXT NOT NULL DEFAULT 'unknown',
                  last_error TEXT
                )
                """
            )

    def set_secret(self, provider: str, name: str, value: str) -> None:
        encrypted = self.cipher.encrypt(value)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO secrets(provider, name, value)
                VALUES (?, ?, ?)
                ON CONFLICT(provider, name) DO UPDATE SET value = excluded.value
                """,
                (provider, name, encrypted),
            )
            conn.execute(
                """
                INSERT INTO provider_status(provider, configured, last_status)
                VALUES (?, 1, 'saved')
                ON CONFLICT(provider) DO UPDATE SET configured = 1
                """,
                (provider,),
            )

    def get_secret(self, provider: str, name: str) -> str | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT value FROM secrets WHERE provider = ? AND name = ?",
                (provider, name),
            ).fetchone()
        return self.cipher.decrypt(row[0]) if row else None

    def set_status(self, provider: str, status: str, error: str | None = None) -> None:
        configured = 1 if self.provider_has_secret(provider) else 0
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO provider_status(provider, configured, last_status, last_error)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(provider) DO UPDATE SET
                  configured = excluded.configured,
                  last_status = excluded.last_status,
                  last_error = excluded.last_error
                """,
                (provider, configured, status, error),
            )

    def provider_has_secret(self, provider: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM secrets WHERE provider = ? LIMIT 1",
                (provider,),
            ).fetchone()
        return row is not None

    def statuses(self) -> list[ProviderStatus]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT provider, configured, last_status, last_error FROM provider_status ORDER BY provider"
            ).fetchall()
        return [
            ProviderStatus(row[0], bool(row[1]), row[2], row[3])
            for row in rows
        ]
