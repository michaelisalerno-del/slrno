from __future__ import annotations

from app.settings_store import SettingsStore


class ReverseCipher:
    def encrypt(self, value: str) -> bytes:
        return value[::-1].encode("utf-8")

    def decrypt(self, value: bytes) -> str:
        return value.decode("utf-8")[::-1]


def test_settings_store_encrypts_and_recovers_secret(tmp_path):
    store = SettingsStore(tmp_path / "settings.sqlite3", ReverseCipher())

    store.set_secret("eodhd", "api_token", "secret-value")

    assert store.get_secret("eodhd", "api_token") == "secret-value"
    assert b"secret-value" not in (tmp_path / "settings.sqlite3").read_bytes()


def test_status_tracks_provider_state(tmp_path):
    store = SettingsStore(tmp_path / "settings.sqlite3", ReverseCipher())

    store.set_secret("ig", "api_key", "abc")
    store.set_status("ig", "connected")

    [status] = store.statuses()
    assert status.provider == "ig"
    assert status.configured is True
    assert status.last_status == "connected"
