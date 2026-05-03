from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

MAJOR_KEYWORDS: tuple[tuple[str, str], ...] = (
    ("central_bank", "fomc"),
    ("central_bank", "federal open market"),
    ("central_bank", "interest rate"),
    ("central_bank", "rate decision"),
    ("central_bank", "fed chair"),
    ("central_bank", "powell"),
    ("central_bank", "ecb"),
    ("central_bank", "boe"),
    ("inflation", "consumer price"),
    ("inflation", "inflation rate"),
    ("inflation", "cpi"),
    ("inflation", "pce"),
    ("jobs", "nonfarm"),
    ("jobs", "non-farm"),
    ("jobs", "nfp"),
    ("jobs", "payroll"),
    ("jobs", "unemployment"),
    ("jobs", "jobless"),
    ("growth", "gross domestic product"),
    ("growth", "gdp"),
    ("activity", "ism"),
    ("activity", "pmi"),
    ("consumer", "retail sales"),
)

COUNTRY_TO_CURRENCY = {
    "AU": "AUD",
    "AUSTRALIA": "AUD",
    "CA": "CAD",
    "CANADA": "CAD",
    "CH": "CHF",
    "SWITZERLAND": "CHF",
    "CN": "CNY",
    "CHINA": "CNY",
    "DE": "EUR",
    "EU": "EUR",
    "EURO AREA": "EUR",
    "EUROZONE": "EUR",
    "FR": "EUR",
    "GERMANY": "EUR",
    "GB": "GBP",
    "GREAT BRITAIN": "GBP",
    "UK": "GBP",
    "UNITED KINGDOM": "GBP",
    "JP": "JPY",
    "JAPAN": "JPY",
    "NZ": "NZD",
    "NEW ZEALAND": "NZD",
    "US": "USD",
    "USA": "USD",
    "UNITED STATES": "USD",
    "UNITED STATES OF AMERICA": "USD",
}

MARKET_USD_SENSITIVE_TOKENS = {
    "DOW",
    "GOLD",
    "NAS",
    "NAS100",
    "NDX",
    "OIL",
    "RUSSELL",
    "SP500",
    "US100",
    "US30",
    "US500",
    "WTI",
    "XAG",
    "XAU",
}

ISO_CURRENCIES = {
    "AUD",
    "CAD",
    "CHF",
    "CNY",
    "EUR",
    "GBP",
    "JPY",
    "NZD",
    "USD",
}


def summarize_economic_calendar(
    events: list[dict[str, Any]],
    start: str | date,
    end: str | date,
    market_id: str = "",
    limit: int = 12,
) -> dict[str, object]:
    start_date = _parse_date(start)
    end_date = _parse_date(end)
    normalized = [normalize_calendar_event(event, market_id=market_id) for event in events]
    normalized = [event for event in normalized if _event_in_range(event, start_date, end_date)]
    relevant = [event for event in normalized if event["relevant_to_market"]]
    high = [event for event in relevant if event["importance"] in {"high", "major"}]
    major = [event for event in relevant if event["importance"] == "major"]
    medium = [event for event in relevant if event["importance"] == "medium"]
    blackout_dates = sorted({str(event["day"]) for event in high if event.get("day")})
    return {
        "schema": "market_context_calendar_v1",
        "available": True,
        "source": "fmp_economic_calendar",
        "start": start_date.isoformat(),
        "end": end_date.isoformat(),
        "market_id": market_id,
        "event_count": len(normalized),
        "relevant_event_count": len(relevant),
        "high_impact_count": len(high),
        "major_event_count": len(major),
        "calendar_risk": _calendar_risk(major_count=len(major), high_count=len(high), medium_count=len(medium)),
        "blackout_dates": blackout_dates,
        "next_major_event": _next_major_event(major),
        "events": _compact_events(sorted(relevant, key=_event_sort_key), limit=limit),
        "data_completeness": {
            "events_available": bool(normalized),
            "market_relevance_applied": bool(market_id),
            "blackout_date_count": len(blackout_dates),
        },
    }


def unavailable_market_context(reason: str) -> dict[str, object]:
    return {
        "schema": "market_context_calendar_v1",
        "available": False,
        "source": "fmp_economic_calendar",
        "calendar_risk": "unavailable",
        "event_count": 0,
        "relevant_event_count": 0,
        "high_impact_count": 0,
        "major_event_count": 0,
        "blackout_dates": [],
        "events": [],
        "next_major_event": None,
        "reason": reason,
        "data_completeness": {
            "events_available": False,
            "market_relevance_applied": False,
            "blackout_date_count": 0,
        },
    }


def normalize_calendar_event(event: dict[str, Any], market_id: str = "") -> dict[str, object]:
    title = _clean_text(event.get("event") or event.get("title") or event.get("name"))
    day, event_time = _split_event_date(event)
    currency = _clean_text(event.get("currency")).upper()
    country = _clean_text(event.get("country") or event.get("region")).upper()
    impact = _clean_text(event.get("impact") or event.get("importance")).lower()
    category = _event_category(title)
    importance = _event_importance(title, impact, category)
    event_exposures = _event_exposures(currency, country)
    market_exposures = _market_exposures(market_id)
    relevant = _is_relevant(event_exposures, market_exposures, importance)
    return {
        "day": day.isoformat() if day else "",
        "time": event_time,
        "event": title or "Economic event",
        "country": country,
        "currency": currency,
        "impact": impact or "unknown",
        "category": category,
        "importance": importance,
        "relevant_to_market": relevant,
        "actual": event.get("actual"),
        "estimate": event.get("estimate") if "estimate" in event else event.get("forecast"),
        "previous": event.get("previous"),
    }


def _event_category(title: str) -> str:
    haystack = title.lower()
    for category, keyword in MAJOR_KEYWORDS:
        if keyword in haystack:
            return category
    return "calendar"


def _event_importance(title: str, impact: str, category: str) -> str:
    if "high" in impact or impact in {"3", "3-star", "red"}:
        return "major" if category != "calendar" else "high"
    if "medium" in impact or impact in {"2", "2-star", "orange"}:
        return "high" if category != "calendar" else "medium"
    if "low" in impact or impact in {"1", "1-star", "yellow"}:
        return "low"
    if category != "calendar":
        return "major"
    return "low"


def _event_exposures(currency: str, country: str) -> set[str]:
    exposures: set[str] = set()
    if currency in ISO_CURRENCIES:
        exposures.add(currency)
    mapped = COUNTRY_TO_CURRENCY.get(country)
    if mapped:
        exposures.add(mapped)
    return exposures


def _market_exposures(market_id: str) -> set[str]:
    value = re.sub(r"[^A-Z0-9]", "", str(market_id or "").upper())
    if not value:
        return set()
    exposures = {currency for currency in ISO_CURRENCIES if currency in value}
    if any(token in value for token in MARKET_USD_SENSITIVE_TOKENS):
        exposures.add("USD")
    return exposures


def _is_relevant(event_exposures: set[str], market_exposures: set[str], importance: str) -> bool:
    if not market_exposures:
        return True
    if event_exposures & market_exposures:
        return True
    return importance == "major" and "USD" in event_exposures and "USD" in market_exposures


def _calendar_risk(major_count: int, high_count: int, medium_count: int) -> str:
    risk_points = major_count * 3 + high_count * 2 + medium_count
    if major_count >= 3 or risk_points >= 10:
        return "high"
    if major_count >= 1 or risk_points >= 5:
        return "elevated"
    if risk_points >= 1:
        return "watch"
    return "clear"


def _next_major_event(events: list[dict[str, object]]) -> dict[str, object] | None:
    today = date.today().isoformat()
    for event in sorted(events, key=_event_sort_key):
        if str(event.get("day") or "") >= today:
            return _compact_event(event)
    return None


def _compact_events(events: list[dict[str, object]], limit: int) -> list[dict[str, object]]:
    major = [event for event in events if event["importance"] in {"major", "high"}]
    others = [event for event in events if event["importance"] not in {"major", "high"}]
    return [_compact_event(event) for event in (major + others)[: max(1, min(50, limit))]]


def _compact_event(event: dict[str, object]) -> dict[str, object]:
    return {
        "day": event.get("day"),
        "time": event.get("time"),
        "event": event.get("event"),
        "country": event.get("country"),
        "currency": event.get("currency"),
        "impact": event.get("impact"),
        "category": event.get("category"),
        "importance": event.get("importance"),
    }


def _event_in_range(event: dict[str, object], start: date, end: date) -> bool:
    day_text = str(event.get("day") or "")
    if not day_text:
        return True
    day = _parse_date(day_text)
    return start <= day <= end


def _event_sort_key(event: dict[str, object]) -> tuple[str, str, str]:
    return (str(event.get("day") or ""), str(event.get("time") or ""), str(event.get("event") or ""))


def _split_event_date(event: dict[str, Any]) -> tuple[date | None, str]:
    raw = _clean_text(event.get("date") or event.get("datetime"))
    time_text = _clean_text(event.get("time"))
    if not raw:
        return None, time_text
    normalized = raw.replace("T", " ")
    try:
        return date.fromisoformat(normalized[:10]), time_text or normalized[11:16].strip()
    except ValueError:
        try:
            parsed = datetime.fromisoformat(normalized)
            return parsed.date(), time_text or parsed.strftime("%H:%M")
        except ValueError:
            return None, time_text


def _parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _clean_text(value: object) -> str:
    return str(value or "").strip()
