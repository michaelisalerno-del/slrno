from __future__ import annotations

import asyncio
import os
import tempfile

import pytest

pytest.importorskip("fastapi")

os.environ.setdefault("SLRNO_HOME", tempfile.mkdtemp(prefix="slrno-test-"))

import app.main as main
from app.settings_store import SettingsStore
from app.market_registry import MarketMapping


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
    store.set_secret("ig", "api_key", "demo-api")
    store.set_secret("ig", "username", "demo-user")
    store.set_secret("ig", "password", "demo-pass")

    class FakeIGProvider:
        def __init__(self, api_key: str, username: str, password: str, account_id: str = "") -> None:
            self.api_key = api_key
            self.username = username
            self.password = password
            self.account_id = account_id

        async def accounts(self) -> list[dict[str, object]]:
            return [
                {"accountId": "ABC12345", "accountName": "Spread Bet Demo", "accountType": "SPREADBET"},
                {"accountId": "CFD98765", "accountName": "CFD Demo", "accountType": "CFD"},
            ]

    monkeypatch.setattr(main, "settings", store)
    monkeypatch.setattr(main, "IGDemoProvider", FakeIGProvider)

    result = asyncio.run(
        main.save_ig_account_roles(
            main.IGAccountRolesPayload(
                spread_bet_account_id="Spread Bet Demo",
                cfd_account_id="CFD98765",
                default_product_mode="cfd",
            )
        )
    )

    roles = result["ig_account_roles"]
    assert result["status"] == "saved"
    assert roles["default_product_mode"] == "cfd"
    assert roles["both_active"] is True
    assert roles["spread_bet"]["configured"] is True
    assert roles["spread_bet"]["active"] is True
    assert roles["spread_bet"]["display_name"] == "Spread Bet Demo"
    assert roles["spread_bet"]["validation_status"] == "validated"
    assert roles["spread_bet"]["masked_account_id"].endswith("2345")
    assert roles["cfd"]["configured"] is True
    assert roles["cfd"]["active"] is True
    assert roles["cfd"]["display_name"] == "CFD Demo"
    assert roles["cfd"]["masked_account_id"].endswith("8765")
    assert "ABC12345" not in str(roles)
    assert store.get_secret("ig_accounts", "spread_bet_account_id") == "ABC12345"
    assert store.get_secret("ig_accounts", "cfd_account_id") == "CFD98765"


def test_save_ig_account_roles_rejects_unknown_account_name(tmp_path, monkeypatch):
    store = SettingsStore(tmp_path / "settings.sqlite3", ReverseCipher())
    store.set_secret("ig", "api_key", "demo-api")
    store.set_secret("ig", "username", "demo-user")
    store.set_secret("ig", "password", "demo-pass")

    class FakeIGProvider:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def accounts(self) -> list[dict[str, object]]:
            return [{"accountId": "ABC12345", "accountName": "Spread Bet Demo"}]

    monkeypatch.setattr(main, "settings", store)
    monkeypatch.setattr(main, "IGDemoProvider", FakeIGProvider)

    with pytest.raises(main.HTTPException) as exc_info:
        asyncio.run(
            main.save_ig_account_roles(
                main.IGAccountRolesPayload(
                    spread_bet_account_id="Spread Bet Demo",
                    cfd_account_id="Missing CFD Demo",
                )
            )
        )

    assert exc_info.value.status_code == 400
    assert "CFD demo account 'Missing CFD Demo' was not found" in str(exc_info.value.detail)


def test_ig_provider_selection_uses_product_specific_demo_accounts_without_generic_fallback(tmp_path, monkeypatch):
    store = SettingsStore(tmp_path / "settings.sqlite3", ReverseCipher())
    store.set_secret("ig", "api_key", "demo-api")
    store.set_secret("ig", "username", "demo-user")
    store.set_secret("ig", "password", "demo-pass")
    store.set_secret("ig", "account_id", "GENERIC")
    store.set_secret("ig_accounts", "cfd_account_id", "CFD98765")
    monkeypatch.setattr(main, "settings", store)

    cfd_provider = main._ig_provider_from_settings("cfd")
    spread_provider = main._ig_provider_from_settings("spread_bet")

    assert cfd_provider is not None
    assert cfd_provider.account_id == "CFD98765"
    assert spread_provider is None


def test_recent_ig_price_snapshot_tries_daily_for_share_epics():
    calls: list[str] = []

    class FakeIGProvider:
        async def recent_price_snapshot(self, _epic: str, resolution: str = "MINUTE_5", max_points: int = 10) -> dict[str, object] | None:
            calls.append(resolution)
            if resolution == "MINUTE_5":
                raise RuntimeError("minute unavailable")
            if resolution == "DAY":
                return {"reference_price": 192.1, "resolution": resolution}
            return None

    market = MarketMapping("AAPL", "Apple", "share", "AAPL.US", "UC.D.AAPL.DAILY.IP")

    snapshot = asyncio.run(main._recent_ig_price_snapshot(FakeIGProvider(), market))

    assert snapshot == {"reference_price": 192.1, "resolution": "DAY"}
    assert calls == ["MINUTE_5", "DAY"]


def test_sync_ig_costs_still_uses_recent_price_when_account_status_fails(monkeypatch):
    market = MarketMapping(
        "PEGA",
        "Pegasystems Inc",
        "share",
        "PEGA.US",
        "UC.D.PEGAUS.CASH.IP",
        True,
        "discovered-pega",
        "Pegasystems Inc",
        "PEGA,Pegasystems Inc",
        "5min",
        15.0,
        7.5,
        750,
    )
    saved_profiles: list[dict[str, object]] = []

    class FakeSettings:
        def get_secret(self, provider: str, key: str) -> str | None:
            values = {
                ("ig", "api_key"): "demo-api",
                ("ig", "username"): "demo-user",
                ("ig", "password"): "demo-pass",
                ("ig_accounts", "cfd_account_id"): "CFD98765",
            }
            return values.get((provider, key))

    class FakeMarkets:
        def get(self, market_id: str):
            return market if market_id == "PEGA" else None

    class FakeStore:
        def save_cost_profile(self, profile) -> None:
            saved_profiles.append(profile.as_dict())

    class FakeIGProvider:
        def __init__(self, api_key: str, username: str, password: str, account_id: str = "") -> None:
            self.account_id = account_id

        async def account_status(self):
            raise RuntimeError("accounts endpoint unavailable")

        async def market_details(self, epic: str) -> dict[str, object]:
            return {
                "instrument": {"epic": epic, "name": "Pegasystems Inc", "type": "SHARES"},
                "snapshot": {"marketStatus": "CLOSED"},
                "dealingRules": {"minDealSize": {"value": 1}},
            }

        async def recent_price_snapshot(
            self,
            epic: str,
            resolution: str = "MINUTE_5",
            max_points: int = 10,
        ) -> dict[str, object] | None:
            return {"reference_price": 36.79, "snapshot_time": "2026-05-05T20:00:00", "resolution": resolution}

    monkeypatch.setattr(main, "settings", FakeSettings())
    monkeypatch.setattr(main, "markets", FakeMarkets())
    monkeypatch.setattr(main, "research_store", FakeStore())
    monkeypatch.setattr(main, "IGDemoProvider", FakeIGProvider)

    result = asyncio.run(main.sync_ig_market_costs(main.IGCostSyncPayload(market_ids=["PEGA"], product_mode="cfd")))

    assert result["status"] == "synced"
    assert result["price_validated_count"] == 1
    assert result["profiles"][0]["confidence"] == "ig_recent_epic_reference_profile"
    assert result["profiles"][0]["validation_status"] == "ig_price_validated"
    assert "account status check failed" in " ".join(result["profiles"][0]["notes"])
    assert saved_profiles[0]["confidence"] == "ig_recent_epic_reference_profile"


def test_midcap_endpoint_blocks_candidates_until_ig_catalogue_is_checked(tmp_path, monkeypatch):
    store = SettingsStore(tmp_path / "settings.sqlite3", ReverseCipher())
    monkeypatch.setattr(main, "settings", store)

    result = asyncio.run(
        main.discover_midcap_markets(
            country="UK",
            product_mode="spread_bet",
            limit=3,
            min_market_cap=250_000_000,
            max_market_cap=10_000_000_000,
            min_volume=100_000,
            max_spread_bps=60,
            account_size=3000,
            verify_ig=False,
            require_ig_catalogue=True,
        )
    )

    assert result["ig_catalogue_required"] is True
    assert result["eligible_count"] == 0
    assert result["ig_status"] == "ig_required_not_checked"
    assert "ig_catalogue_not_checked" in result["candidates"][0]["blockers"]


def test_midcap_endpoint_prefers_eodhd_screener_when_available(tmp_path, monkeypatch):
    store = SettingsStore(tmp_path / "settings.sqlite3", ReverseCipher())
    store.set_secret("eodhd", "api_token", "eodhd-token")
    monkeypatch.setattr(main, "settings", store)

    class FakeEODHDProvider:
        def __init__(self, api_token: str) -> None:
            self.api_token = api_token

        async def stock_screener(self, **kwargs):
            return [
                {
                    "code": "MKS",
                    "name": "Marks and Spencer Group",
                    "exchange": "LSE",
                    "currency_symbol": "GBp",
                    "market_capitalization": 6_500_000_000,
                    "adjusted_close": 323.95,
                    "avgvol_200d": 2_000_000,
                }
            ]

    monkeypatch.setattr(main, "EODHDProvider", FakeEODHDProvider)

    result = asyncio.run(
        main.discover_midcap_markets(
            country="UK",
            product_mode="spread_bet",
            limit=3,
            min_market_cap=250_000_000,
            max_market_cap=10_000_000_000,
            min_volume=100_000,
            max_spread_bps=60,
            account_size=3000,
            verify_ig=False,
            require_ig_catalogue=False,
        )
    )

    assert result["data_source"] == "eodhd_stock_screener"
    assert result["candidate_count"] == 1
    assert result["eligible_count"] == 1
    assert result["candidates"][0]["eodhd_symbol"] == "MKS.LSE"


def test_midcap_endpoint_checks_all_us_exchanges_before_ranking(tmp_path, monkeypatch):
    store = SettingsStore(tmp_path / "settings.sqlite3", ReverseCipher())
    store.set_secret("eodhd", "api_token", "eodhd-token")
    monkeypatch.setattr(main, "settings", store)
    calls: list[str] = []

    class FakeEODHDProvider:
        def __init__(self, api_token: str) -> None:
            self.api_token = api_token

        async def stock_screener(self, **kwargs):
            exchange = str(kwargs.get("exchange") or "")
            calls.append(exchange)
            return [
                {
                    "code": f"{exchange[:3]}A",
                    "name": f"{exchange} Test Software",
                    "exchange": exchange,
                    "currency_symbol": "$",
                    "market_capitalization": 5_000_000_000,
                    "adjusted_close": 25.0,
                    "avgvol_200d": 1_000_000,
                    "earnings_share": 1.0,
                }
            ]

    monkeypatch.setattr(main, "EODHDProvider", FakeEODHDProvider)

    result = asyncio.run(
        main.discover_midcap_markets(
            country="US",
            product_mode="cfd",
            limit=1,
            min_market_cap=250_000_000,
            max_market_cap=10_000_000_000,
            min_volume=100_000,
            max_spread_bps=60,
            account_size=3000,
            verify_ig=False,
            require_ig_catalogue=False,
        )
    )

    assert result["data_source"] == "eodhd_stock_screener"
    assert {"NASDAQ", "NYSE", "AMEX"}.issubset(set(calls))
    assert result["candidate_count"] == 1


def test_ig_rate_limit_text_is_reported_as_cost_sync_status():
    assert main._looks_like_ig_rate_limit("IG login failed: error.public-api.exceeded-API key hidden")
    assert not main._looks_like_ig_rate_limit("IG market not found")
