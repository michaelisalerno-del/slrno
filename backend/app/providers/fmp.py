from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Any

from ..fmp_cache import FMPCache
from .base import OHLCBar, Quote


class FMPProviderError(RuntimeError):
    pass


class FMPProvider:
    BASE_URL = "https://financialmodelingprep.com/stable"
    INTRADAY_CHUNK_DAYS = {
        "1min": 3,
        "5min": 7,
        "15min": 21,
        "30min": 45,
        "1hour": 90,
    }
    QUOTE_TTL_SECONDS = 60
    SEARCH_TTL_SECONDS = 7 * 24 * 60 * 60
    CLOSED_HISTORY_TTL_SECONDS = 180 * 24 * 60 * 60
    LIVE_HISTORY_TTL_SECONDS = 15 * 60

    def __init__(self, api_key: str, base_url: str | None = None, cache: FMPCache | None = None, cache_enabled: bool = True) -> None:
        self.api_key = api_key
        self.base_url = (base_url or self.BASE_URL).rstrip("/")
        self.cache = cache if cache is not None else FMPCache()
        self.cache_enabled = cache_enabled

    async def quote(self, symbol: str, use_cache: bool = True) -> Quote:
        payload = await self._get_json(
            "quote",
            "/quote",
            {"symbol": symbol},
            ttl_seconds=self.QUOTE_TTL_SECONDS,
            use_cache=use_cache,
            timeout_seconds=10.0,
            timeout_message="FMP validation timed out after 10 seconds",
            operation="quote lookup",
            symbol_for_error=symbol,
        )
        if not payload:
            raise FMPProviderError(f"FMP returned no quote rows for {symbol}")
        item = payload[0]
        price = float(item.get("price") or item.get("previousClose") or 0)
        return Quote(
            symbol=symbol,
            bid=_optional_float(item.get("bid")),
            ask=_optional_float(item.get("ask")),
            last=price,
        )

    async def historical_bars(self, symbol: str, interval: str, start: str, end: str) -> list[OHLCBar]:
        start_date = _parse_date(start, "start")
        end_date = _parse_date(end, "end")
        if end_date < start_date:
            raise FMPProviderError("FMP historical request end date must be on or after start date")

        chunk_days = self.INTRADAY_CHUNK_DAYS.get(interval)
        ranges = [(start_date, end_date)] if chunk_days is None else _date_chunks(start_date, end_date, chunk_days)
        bars_by_timestamp: dict[datetime, OHLCBar] = {}

        for chunk_start, chunk_end in ranges:
            payload = await self._get_json(
                "historical_bars",
                f"/historical-chart/{interval}",
                {"symbol": symbol, "from": chunk_start.isoformat(), "to": chunk_end.isoformat()},
                ttl_seconds=_historical_ttl_seconds(chunk_end),
                use_cache=True,
                timeout_seconds=30.0,
                timeout_message="FMP historical data request timed out after 30 seconds",
                operation="historical bars",
                symbol_for_error=symbol,
            )
            if not isinstance(payload, list):
                raise FMPProviderError(f"FMP returned an unexpected historical data shape for {symbol}")
            for row in payload:
                bar = _bar_from_row(symbol, row)
                bars_by_timestamp[bar.timestamp] = bar

        return sorted(bars_by_timestamp.values(), key=lambda bar: bar.timestamp)

    async def search(self, query: str) -> list[dict[str, str]]:
        payload = await self._get_json(
            "symbol_search",
            "/search-symbol",
            {"query": query},
            ttl_seconds=self.SEARCH_TTL_SECONDS,
            use_cache=True,
            timeout_seconds=10.0,
            timeout_message="FMP symbol search timed out after 10 seconds",
            operation="symbol search",
            symbol_for_error=query,
        )
        return [
            {
                "symbol": str(item.get("symbol", "")),
                "name": str(item.get("name", "")),
                "exchange": str(item.get("exchangeShortName", "")),
            }
            for item in payload
        ]

    async def validate(self) -> bool:
        await self.quote("AAPL", use_cache=False)
        return True

    async def _get_json(
        self,
        namespace: str,
        endpoint: str,
        params: dict[str, object],
        ttl_seconds: int,
        use_cache: bool,
        timeout_seconds: float,
        timeout_message: str,
        operation: str,
        symbol_for_error: str,
    ) -> Any:
        import httpx

        if self.cache_enabled and use_cache:
            cached = self.cache.get_json(namespace, self.base_url + endpoint, params)
            if cached is not None:
                return cached

        request_params = {**params, "apikey": self.api_key}
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_seconds, connect=5.0)) as client:
                response = await client.get(f"{self.base_url}{endpoint}", params=request_params)
                response.raise_for_status()
                payload = response.json()
        except httpx.TimeoutException as exc:
            if self.cache_enabled and use_cache:
                stale = self.cache.get_json(namespace, self.base_url + endpoint, params, allow_stale=True)
                if stale is not None:
                    return stale
            raise TimeoutError(timeout_message) from exc
        except httpx.HTTPStatusError as exc:
            if self.cache_enabled and use_cache:
                stale = self.cache.get_json(namespace, self.base_url + endpoint, params, allow_stale=True)
                if stale is not None:
                    return stale
            _raise_status_error(exc, operation, symbol_for_error)

        if self.cache_enabled and use_cache:
            self.cache.set_json(namespace, self.base_url + endpoint, params, payload, ttl_seconds)
        return payload


def _bar_from_row(symbol: str, row: dict[str, object]) -> OHLCBar:
    return OHLCBar(
        symbol=symbol,
        timestamp=datetime.fromisoformat(str(row["date"]).replace("Z", "+00:00")),
        open=float(row["open"]),
        high=float(row["high"]),
        low=float(row["low"]),
        close=float(row["close"]),
        volume=float(row.get("volume") or 0),
    )


def _date_chunks(start: date, end: date, chunk_days: int) -> list[tuple[date, date]]:
    ranges: list[tuple[date, date]] = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        ranges.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return ranges


def _parse_date(value: str, label: str) -> date:
    try:
        return date.fromisoformat(value[:10])
    except ValueError as exc:
        raise FMPProviderError(f"Invalid {label} date {value!r}; expected YYYY-MM-DD") from exc


def _historical_ttl_seconds(chunk_end: date) -> int:
    today = datetime.now(UTC).date()
    if chunk_end < today:
        return FMPProvider.CLOSED_HISTORY_TTL_SECONDS
    return FMPProvider.LIVE_HISTORY_TTL_SECONDS


def _raise_status_error(exc: object, operation: str, symbol: str) -> None:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", "unknown")
    if status_code == 404:
        raise FMPProviderError(
            f"FMP {operation} returned HTTP 404 for {symbol}. The symbol, interval, or dataset may not be available on this FMP plan."
        ) from exc
    raise FMPProviderError(f"FMP {operation} returned HTTP {status_code} for {symbol}") from exc


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return float(value)
