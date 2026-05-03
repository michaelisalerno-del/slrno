from __future__ import annotations

import asyncio
import os
import tempfile
from datetime import date

os.environ.setdefault("SLRNO_HOME", tempfile.mkdtemp(prefix="slrno-test-"))

import app.main as main
from app.market_context import summarize_economic_calendar
from app.settings_store import SettingsStore


class ReverseCipher:
    def encrypt(self, value: str) -> bytes:
        return value[::-1].encode("utf-8")

    def decrypt(self, value: bytes) -> str:
        return value.decode("utf-8")[::-1]


def test_calendar_context_marks_us_cpi_relevant_for_gold():
    summary = summarize_economic_calendar(
        [
            {"date": "2026-05-12 13:30:00", "country": "US", "currency": "USD", "event": "Consumer Price Index CPI", "impact": "High"},
            {"date": "2026-05-14 10:00:00", "country": "EU", "currency": "EUR", "event": "Industrial Production", "impact": "Low"},
        ],
        "2026-05-01",
        "2026-05-31",
        market_id="XAUUSD",
    )

    assert summary["available"] is True
    assert summary["calendar_risk"] == "elevated"
    assert summary["major_event_count"] == 1
    assert summary["blackout_dates"] == ["2026-05-12"]
    assert summary["events"][0]["category"] == "inflation"


def test_low_impact_global_macro_events_do_not_overstate_cockpit_risk():
    summary = summarize_economic_calendar(
        [
            {"date": "2026-05-12 13:30:00", "country": "BH", "currency": "BHD", "event": "CPI MoM", "impact": "Low"},
            {"date": "2026-05-13 19:00:00", "country": "US", "currency": "USD", "event": "FOMC Interest Rate Decision", "impact": "High"},
        ],
        "2026-05-01",
        "2026-05-31",
    )

    assert summary["major_event_count"] == 1
    assert summary["high_impact_count"] == 1
    assert summary["blackout_dates"] == ["2026-05-13"]


def test_market_context_summary_returns_unavailable_without_fmp_key(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "settings", SettingsStore(tmp_path / "settings.sqlite3", ReverseCipher()))

    result = asyncio.run(main.market_context_summary(start=date(2026, 5, 1), end=date(2026, 5, 31), market_id="NAS100"))

    assert result["available"] is False
    assert result["reason"] == "FMP API key is not configured"
    assert result["market_id"] == "NAS100"


def test_market_context_summary_fetches_fmp_calendar_without_exposing_key(tmp_path, monkeypatch):
    store = SettingsStore(tmp_path / "settings.sqlite3", ReverseCipher())
    store.set_secret("fmp", "api_key", "starter-secret")
    seen_keys: list[str] = []

    class FakeFMPProvider:
        def __init__(self, api_key: str) -> None:
            seen_keys.append(api_key)

        async def economic_calendar(self, start: str, end: str) -> list[dict[str, object]]:
            assert start == "2026-05-01"
            assert end == "2026-05-31"
            return [
                {
                    "date": "2026-05-06 19:00:00",
                    "country": "US",
                    "currency": "USD",
                    "event": "FOMC Interest Rate Decision",
                    "impact": "High",
                }
            ]

    monkeypatch.setattr(main, "settings", store)
    monkeypatch.setattr(main, "FMPProvider", FakeFMPProvider)

    result = asyncio.run(main.market_context_summary(start=date(2026, 5, 1), end=date(2026, 5, 31), market_id="SP500"))

    assert seen_keys == ["starter-secret"]
    assert result["available"] is True
    assert result["major_event_count"] == 1
    assert "starter-secret" not in str(result)


def test_market_context_stack_includes_calendar_vix_and_macro_context(tmp_path, monkeypatch):
    store = SettingsStore(tmp_path / "settings.sqlite3", ReverseCipher())
    store.set_secret("fmp", "api_key", "starter-secret")

    class FakeFMPProvider:
        def __init__(self, api_key: str) -> None:
            assert api_key == "starter-secret"

        async def economic_calendar(self, start: str, end: str) -> list[dict[str, object]]:
            return [
                {
                    "date": "2026-05-06 19:00:00",
                    "country": "US",
                    "currency": "USD",
                    "event": "FOMC Interest Rate Decision",
                    "impact": "High",
                }
            ]

    class FakeFREDProvider:
        async def series(self, series_id: str, start: str | date | None = None, end: str | date | None = None) -> list[dict[str, object]]:
            assert start == "2026-01-01"
            assert end == "2026-03-01"
            rows = {
                "VIXCLS": [15.0, 28.0],
                "BAMLH0A0HYM2": [4.3, 4.6],
                "T10Y2Y": [0.1, -0.2],
            }[series_id]
            return [
                {"date": "2026-02-01", "value": rows[0], "series_id": series_id},
                {"date": "2026-03-01", "value": rows[1], "series_id": series_id},
            ]

    monkeypatch.setattr(main, "settings", store)
    monkeypatch.setattr(main, "FMPProvider", FakeFMPProvider)
    monkeypatch.setattr(main, "FREDProvider", FakeFREDProvider)

    result = asyncio.run(main.market_context_stack(start=date(2026, 1, 1), end=date(2026, 3, 1), market_id="XAUUSD"))

    assert result["schema"] == "market_context_stack_v1"
    assert result["calendar"]["major_event_count"] == 1
    assert result["volatility"]["regime"] == "high_volatility"
    assert result["macro"]["high_yield_spread"]["risk"] == "credit_widening"
    assert result["macro"]["yield_curve_10y2y"]["regime"] == "inverted"
    assert result["positioning"]["status"] == "planned"
    assert result["tick_quote"]["status"] == "shortlist_only"
