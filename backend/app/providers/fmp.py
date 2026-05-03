from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from ..market_data_cache import MarketDataCache


class FMPProviderError(RuntimeError):
    pass


class FMPProvider:
    BASE_URL = "https://financialmodelingprep.com/stable"
    ECONOMIC_CALENDAR_TTL_SECONDS = 6 * 60 * 60
    ECONOMIC_CALENDAR_CHUNK_DAYS = 90

    def __init__(
        self,
        api_key: str,
        base_url: str | None = None,
        cache: MarketDataCache | None = None,
        cache_enabled: bool = True,
    ) -> None:
        self.api_key = api_key
        self.base_url = (base_url or self.BASE_URL).rstrip("/")
        self.cache = cache if cache is not None else MarketDataCache()
        self.cache_enabled = cache_enabled

    async def validate(self) -> bool:
        payload = await self._get_json(
            "/quote",
            {"symbol": "AAPL"},
            timeout_seconds=10.0,
            operation="quote lookup",
        )
        if _looks_like_quote_payload(payload):
            return True
        raise FMPProviderError("FMP returned an unexpected quote response")

    async def economic_calendar(self, start: str | date, end: str | date) -> list[dict[str, Any]]:
        start_date = _parse_date(start, "start")
        end_date = _parse_date(end, "end")
        if end_date < start_date:
            raise FMPProviderError("FMP economic calendar end date must be on or after start date")

        events: list[dict[str, Any]] = []
        cursor = start_date
        while cursor <= end_date:
            chunk_end = min(end_date, cursor + timedelta(days=self.ECONOMIC_CALENDAR_CHUNK_DAYS - 1))
            payload = await self._get_json(
                "/economic-calendar",
                {"from": cursor.isoformat(), "to": chunk_end.isoformat()},
                timeout_seconds=15.0,
                operation="economic calendar",
                use_cache=True,
                cache_namespace="fmp_economic_calendar",
                ttl_seconds=self.ECONOMIC_CALENDAR_TTL_SECONDS,
            )
            events.extend(_calendar_rows(payload))
            cursor = chunk_end + timedelta(days=1)
        return sorted(events, key=_calendar_sort_key)

    async def _get_json(
        self,
        endpoint: str,
        params: dict[str, object],
        timeout_seconds: float,
        operation: str,
        use_cache: bool = False,
        cache_namespace: str = "",
        ttl_seconds: int = 60,
    ) -> Any:
        import httpx

        cache_key_url = f"{self.base_url}{endpoint}"
        if self.cache_enabled and use_cache and cache_namespace:
            cached = self.cache.get_json(cache_namespace, cache_key_url, params)
            if cached is not None:
                return cached

        request_params = {**params, "apikey": self.api_key}
        try:
            async with httpx.AsyncClient(timeout=timeout_seconds) as client:
                response = await client.get(f"{self.base_url}{endpoint}", params=request_params)
                response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status in {401, 403}:
                raise FMPProviderError("FMP rejected the API key") from exc
            raise FMPProviderError(f"FMP {operation} failed with HTTP {status}") from exc
        except httpx.TimeoutException as exc:
            raise FMPProviderError(f"FMP {operation} timed out after {timeout_seconds:g} seconds") from exc
        except httpx.HTTPError as exc:
            raise FMPProviderError(f"FMP {operation} failed: {exc}") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise FMPProviderError("FMP returned non-JSON data") from exc
        if _looks_like_error_payload(payload):
            raise FMPProviderError("FMP returned an error response")
        if self.cache_enabled and use_cache and cache_namespace:
            self.cache.set_json(
                cache_namespace,
                cache_key_url,
                params,
                payload,
                ttl_seconds,
                metadata={"provider": "fmp", "operation": operation, "source_status": "fresh_fmp"},
            )
        return payload


def _looks_like_quote_payload(payload: Any) -> bool:
    if isinstance(payload, list):
        return any(_looks_like_quote_payload(item) for item in payload)
    if not isinstance(payload, dict):
        return False
    if payload.get("Error Message") or payload.get("error"):
        return False
    return bool(payload.get("symbol")) and any(key in payload for key in ("price", "previousClose", "volume"))


def _looks_like_error_payload(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    return bool(payload.get("Error Message") or payload.get("error") or payload.get("Information"))


def _parse_date(value: str | date, field: str) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError as exc:
        raise FMPProviderError(f"FMP economic calendar {field} date is invalid") from exc


def _calendar_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("calendar", "data", "results"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
    return []


def _calendar_sort_key(row: dict[str, Any]) -> str:
    return str(row.get("date") or row.get("datetime") or "")
