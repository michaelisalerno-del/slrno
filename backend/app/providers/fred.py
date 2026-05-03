from __future__ import annotations

import csv
from datetime import date
from io import StringIO
from typing import Any

from ..market_data_cache import MarketDataCache


class FREDProviderError(RuntimeError):
    pass


class FREDProvider:
    CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
    SERIES_TTL_SECONDS = 12 * 60 * 60

    def __init__(
        self,
        base_url: str | None = None,
        cache: MarketDataCache | None = None,
        cache_enabled: bool = True,
    ) -> None:
        self.base_url = base_url or self.CSV_URL
        self.cache = cache if cache is not None else MarketDataCache()
        self.cache_enabled = cache_enabled

    async def series(self, series_id: str, start: str | date | None = None, end: str | date | None = None) -> list[dict[str, object]]:
        import httpx

        clean_id = str(series_id or "").strip().upper()
        if not clean_id:
            raise FREDProviderError("FRED series id is required")
        params: dict[str, object] = {"id": clean_id}
        if start:
            params["observation_start"] = _date_text(start)
        if end:
            params["observation_end"] = _date_text(end)
        if self.cache_enabled:
            cached = self.cache.get_json("fred_series", self.base_url, params)
            if cached is not None:
                return _rows_from_cached(cached)
        try:
            async with httpx.AsyncClient(timeout=12.0) as client:
                response = await client.get(self.base_url, params=params)
                response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise FREDProviderError(f"FRED series {clean_id} returned HTTP {exc.response.status_code}") from exc
        except httpx.TimeoutException as exc:
            raise FREDProviderError(f"FRED series {clean_id} timed out") from exc
        except httpx.HTTPError as exc:
            raise FREDProviderError(f"FRED series {clean_id} failed: {exc}") from exc
        rows = _parse_csv(clean_id, response.text, start, end)
        if self.cache_enabled:
            self.cache.set_json(
                "fred_series",
                self.base_url,
                params,
                rows,
                self.SERIES_TTL_SECONDS,
                metadata={"provider": "fred", "series_id": clean_id, "source_status": "fresh_fred"},
            )
        return rows


def _parse_csv(series_id: str, payload: str, start: str | date | None, end: str | date | None) -> list[dict[str, object]]:
    start_date = _parse_optional_date(start)
    end_date = _parse_optional_date(end)
    reader = csv.DictReader(StringIO(payload))
    rows: list[dict[str, object]] = []
    value_field = series_id
    for row in reader:
        row_date = _parse_optional_date(row.get("observation_date") or row.get("DATE") or row.get("date"))
        if row_date is None:
            continue
        if start_date is not None and row_date < start_date:
            continue
        if end_date is not None and row_date > end_date:
            continue
        raw_value = row.get(value_field) or row.get(value_field.lower()) or row.get(value_field.upper())
        value = _float_or_none(raw_value)
        if value is None:
            continue
        rows.append({"date": row_date.isoformat(), "value": value, "series_id": series_id})
    return rows


def _rows_from_cached(payload: Any) -> list[dict[str, object]]:
    return [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []


def _date_text(value: str | date) -> str:
    return value.isoformat() if isinstance(value, date) else str(value)[:10]


def _parse_optional_date(value: object) -> date | None:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _float_or_none(value: object) -> float | None:
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number
