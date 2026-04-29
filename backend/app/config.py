from __future__ import annotations

import os
from pathlib import Path


def app_home() -> Path:
    """Return the runtime directory used for secrets and SQLite files."""
    configured = os.environ.get("SLRNO_HOME")
    return Path(configured).expanduser() if configured else Path.home() / ".slrno"


def allowed_origins() -> list[str]:
    configured = os.environ.get("SLRNO_ALLOWED_ORIGINS", "http://127.0.0.1:5173,http://localhost:5173")
    return [origin.strip() for origin in configured.split(",") if origin.strip()]


def settings_db_path() -> Path:
    return app_home() / "settings.sqlite3"


def market_db_path() -> Path:
    return app_home() / "markets.sqlite3"


def key_path() -> Path:
    return app_home() / "secret.key"
