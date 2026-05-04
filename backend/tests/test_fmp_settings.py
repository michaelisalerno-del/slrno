from __future__ import annotations

import asyncio
import os
import tempfile

import pytest

pytest.importorskip("fastapi")

os.environ.setdefault("SLRNO_HOME", tempfile.mkdtemp(prefix="slrno-test-"))

import app.main as main
from app.settings_store import SettingsStore


class ReverseCipher:
    def encrypt(self, value: str) -> bytes:
        return value[::-1].encode("utf-8")

    def decrypt(self, value: bytes) -> str:
        return value.decode("utf-8")[::-1]


def test_save_fmp_validates_and_tracks_connected_status(tmp_path, monkeypatch):
    store = SettingsStore(tmp_path / "settings.sqlite3", ReverseCipher())
    validated_keys: list[str] = []

    class FakeFMPProvider:
        def __init__(self, api_key: str) -> None:
            self.api_key = api_key

        async def validate(self) -> bool:
            validated_keys.append(self.api_key)
            return True

    monkeypatch.setattr(main, "settings", store)
    monkeypatch.setattr(main, "FMPProvider", FakeFMPProvider)

    result = asyncio.run(main.save_fmp(main.FMPSettings(api_key="starter-key")))

    assert result == {"status": "connected"}
    assert validated_keys == ["starter-key"]
    assert store.get_secret("fmp", "api_key") == "starter-key"
    fmp_status = next(status for status in main.settings_status() if status["provider"] == "fmp")
    assert fmp_status["configured"] is True
    assert fmp_status["last_status"] == "connected"


def test_save_fmp_marks_error_when_validation_fails(tmp_path, monkeypatch):
    store = SettingsStore(tmp_path / "settings.sqlite3", ReverseCipher())

    class FailingFMPProvider:
        def __init__(self, _api_key: str) -> None:
            pass

        async def validate(self) -> bool:
            raise RuntimeError("bad key")

    monkeypatch.setattr(main, "settings", store)
    monkeypatch.setattr(main, "FMPProvider", FailingFMPProvider)

    with pytest.raises(main.HTTPException) as exc_info:
        asyncio.run(main.save_fmp(main.FMPSettings(api_key="bad-key")))

    assert exc_info.value.status_code == 400
    assert "FMP validation failed" in str(exc_info.value.detail)
    fmp_status = next(status for status in main.settings_status() if status["provider"] == "fmp")
    assert fmp_status["configured"] is True
    assert fmp_status["last_status"] == "error"
    assert fmp_status["last_error"] == "bad key"


def test_save_ig_account_roles_tracks_separate_spread_bet_and_cfd_accounts(tmp_path, monkeypatch):
    store = SettingsStore(tmp_path / "settings.sqlite3", ReverseCipher())
    monkeypatch.setattr(main, "settings", store)

    result = main.save_ig_account_roles(
        main.IGAccountRolesPayload(
            spread_bet_account_id="ABC12345",
            cfd_account_id="CFD98765",
            default_product_mode="cfd",
        )
    )

    roles = result["ig_account_roles"]
    assert result["status"] == "saved"
    assert roles["default_product_mode"] == "cfd"
    assert roles["spread_bet"]["configured"] is True
    assert roles["spread_bet"]["masked_account_id"].endswith("2345")
    assert roles["cfd"]["configured"] is True
    assert roles["cfd"]["masked_account_id"].endswith("8765")
    assert "ABC12345" not in str(roles)
    assert store.get_secret("ig_accounts", "spread_bet_account_id") == "ABC12345"
