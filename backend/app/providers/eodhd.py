from __future__ import annotations

import asyncio
import json
import random
from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from ..market_data_cache import MarketDataCache
from .base import OHLCBar, Quote


class EODHDProviderError(RuntimeError):
    pass


class EODHDProvider:
    BASE_URL = "https://eodhd.com/api"
    QUOTE_TTL_SECONDS = 60
    SEARCH_TTL_SECONDS = 7 * 24 * 60 * 60
    STOCK_SCREENER_TTL_SECONDS = 6 * 60 * 60
    CLOSED_HISTORY_TTL_SECONDS = 180 * 24 * 60 * 60
    LIVE_HISTORY_TTL_SECONDS = 15 * 60
    NEGATIVE_ERROR_TTL_SECONDS = 2 * 60
    TRANSIENT_STATUS_CODES = {502, 503, 504}
    RETRY_DELAYS_SECONDS = (0.5, 1.5, 3.0)

    _INTERVALS = {
        "1min": ("1m", 1),
        "1m": ("1m", 1),
        "5min": ("5m", 1),
        "5m": ("5m", 1),
        "15min": ("5m", 3),
        "15m": ("5m", 3),
        "30min": ("5m", 6),
        "30m": ("5m", 6),
        "1hour": ("1h", 1),
        "1h": ("1h", 1),
    }
    _INTRADAY_CHUNK_DAYS = {"1m": 30, "5m": 90, "1h": 365}
    _DAILY_INTERVALS = {"1day", "1d", "day", "daily"}
    _COMMODITY_INTERVALS = {
        "1day": "daily",
        "1d": "daily",
        "day": "daily",
        "daily": "daily",
        "1week": "weekly",
        "1w": "weekly",
        "weekly": "weekly",
        "1month": "monthly",
        "1mo": "monthly",
        "monthly": "monthly",
    }
    _MONTHLY_COMMODITIES = {"ALUMINUM", "COPPER"}

    def __init__(
        self,
        api_token: str,
        base_url: str | None = None,
        cache: MarketDataCache | None = None,
        cache_enabled: bool = True,
        retry_delays_seconds: tuple[float, ...] | None = None,
        negative_error_ttl_seconds: int | None = None,
    ) -> None:
        self.api_token = api_token
        self.base_url = (base_url or self.BASE_URL).rstrip("/")
        self.cache = cache if cache is not None else MarketDataCache()
        self.cache_enabled = cache_enabled
        self.retry_delays_seconds = self.RETRY_DELAYS_SECONDS if retry_delays_seconds is None else retry_delays_seconds
        self.negative_error_ttl_seconds = self.NEGATIVE_ERROR_TTL_SECONDS if negative_error_ttl_seconds is None else negative_error_ttl_seconds

    async def quote(self, symbol: str, use_cache: bool = True) -> Quote:
        payload = await self._get_json(
            "quote",
            f"/real-time/{symbol}",
            {},
            ttl_seconds=self.QUOTE_TTL_SECONDS,
            use_cache=use_cache,
            timeout_seconds=10.0,
            timeout_message="EODHD validation timed out after 10 seconds",
            operation="quote lookup",
            symbol_for_error=symbol,
        )
        if not isinstance(payload, dict):
            raise EODHDProviderError(f"EODHD returned an unexpected quote shape for {symbol}")
        price = float(payload.get("close") or payload.get("previousClose") or payload.get("last") or 0)
        return Quote(
            symbol=symbol,
            bid=_optional_float(payload.get("bid")),
            ask=_optional_float(payload.get("ask")),
            last=price,
            timestamp=_optional_timestamp(payload.get("timestamp")),
        )

    async def historical_bars(self, symbol: str, interval: str, start: str, end: str) -> list[OHLCBar]:
        start_date = _parse_date(start, "start")
        end_date = _parse_date(end, "end")
        if end_date < start_date:
            raise EODHDProviderError("EODHD historical request end date must be on or after start date")
        if symbol.startswith("COMMODITY:"):
            return await self._commodity_bars(symbol, interval, start_date, end_date)
        if interval in self._DAILY_INTERVALS:
            return await self._eod_bars(symbol, start_date, end_date)

        provider_interval, aggregate_size = self._INTERVALS.get(interval, ("5m", 1))
        bars = await self._intraday_bars(symbol, provider_interval, start_date, end_date)
        return _aggregate_bars(bars, aggregate_size) if aggregate_size > 1 else bars

    async def search(self, query: str) -> list[dict[str, str]]:
        payload = await self._get_json(
            "symbol_search",
            f"/search/{query}",
            {"limit": 25},
            ttl_seconds=self.SEARCH_TTL_SECONDS,
            use_cache=True,
            timeout_seconds=10.0,
            timeout_message="EODHD symbol search timed out after 10 seconds",
            operation="symbol search",
            symbol_for_error=query,
        )
        if not isinstance(payload, list):
            return []
        return [
            {
                "symbol": str(item.get("Code", "")),
                "name": str(item.get("Name", "")),
                "exchange": str(item.get("Exchange", "")),
            }
            for item in payload
        ]

    async def stock_screener(
        self,
        *,
        exchange: str = "",
        market_cap_more_than: float | int | None = None,
        market_cap_lower_than: float | int | None = None,
        min_volume: float | int | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        filters: list[list[object]] = []
        if exchange:
            filters.append(["exchange", "=", exchange])
        if market_cap_more_than is not None:
            filters.append(["market_capitalization", ">", max(0.0, float(market_cap_more_than))])
        if market_cap_lower_than is not None:
            filters.append(["market_capitalization", "<", max(0.0, float(market_cap_lower_than))])
        if min_volume is not None:
            filters.append(["avgvol_200d", ">", max(0.0, float(min_volume))])
        params: dict[str, object] = {
            "sort": "market_capitalization.desc",
            "limit": max(1, min(100, int(limit))),
            "offset": max(0, min(999, int(offset))),
        }
        if filters:
            params["filters"] = json.dumps(filters, separators=(",", ":"))
        payload = await self._get_json(
            "eodhd_stock_screener",
            "/screener",
            params,
            ttl_seconds=self.STOCK_SCREENER_TTL_SECONDS,
            use_cache=True,
            timeout_seconds=20.0,
            timeout_message="EODHD stock screener timed out after 20 seconds",
            operation="stock screener",
            symbol_for_error=exchange or "stocks",
        )
        return _screening_rows(payload)

    async def validate(self) -> bool:
        await self.quote("AAPL.US", use_cache=False)
        return True

    async def _eod_bars(self, symbol: str, start_date: date, end_date: date) -> list[OHLCBar]:
        payload = await self._get_json(
            "daily_bars",
            f"/eod/{symbol}",
            {"from": start_date.isoformat(), "to": end_date.isoformat(), "period": "d"},
            ttl_seconds=_historical_ttl_seconds(end_date),
            use_cache=True,
            timeout_seconds=30.0,
            timeout_message="EODHD EOD data request timed out after 30 seconds",
            operation="daily bars",
            symbol_for_error=symbol,
        )
        if not isinstance(payload, list):
            raise EODHDProviderError(f"EODHD returned an unexpected EOD data shape for {symbol}")
        bars = [bar for row in payload if (bar := _bar_from_row(symbol, row)) is not None]
        return sorted(bars, key=lambda bar: bar.timestamp)

    async def _intraday_bars(self, symbol: str, provider_interval: str, start_date: date, end_date: date) -> list[OHLCBar]:
        rows: list[dict[str, object]] = []
        chunk_days = self._INTRADAY_CHUNK_DAYS.get(provider_interval, 90)
        for chunk_start, chunk_end in _date_chunks(start_date, end_date, chunk_days):
            payload = await self._get_json(
                "historical_bars",
                f"/intraday/{symbol}",
                {
                    "interval": provider_interval,
                    "from": int(datetime.combine(chunk_start, time.min, tzinfo=UTC).timestamp()),
                    "to": int(datetime.combine(chunk_end, time.max, tzinfo=UTC).timestamp()),
                },
                ttl_seconds=_historical_ttl_seconds(chunk_end),
                use_cache=True,
                timeout_seconds=30.0,
                timeout_message="EODHD historical data request timed out after 30 seconds",
                operation=f"historical bars {chunk_start.isoformat()} to {chunk_end.isoformat()}",
                symbol_for_error=symbol,
            )
            if not isinstance(payload, list):
                raise EODHDProviderError(f"EODHD returned an unexpected historical data shape for {symbol}")
            rows.extend(row for row in payload if isinstance(row, dict))
        bars = [bar for row in rows if (bar := _bar_from_row(symbol, row)) is not None]
        return sorted({bar.timestamp: bar for bar in bars}.values(), key=lambda bar: bar.timestamp)

    async def _commodity_bars(self, symbol: str, interval: str, start_date: date, end_date: date) -> list[OHLCBar]:
        provider_interval = self._COMMODITY_INTERVALS.get(interval)
        if provider_interval is None:
            raise EODHDProviderError(f"{symbol} is an EODHD commodity series and only supports daily, weekly, or monthly data")
        code = symbol.removeprefix("COMMODITY:")
        if code in self._MONTHLY_COMMODITIES and provider_interval == "daily":
            provider_interval = "monthly"
        payload = await self._get_json(
            "commodity_bars",
            f"/commodities/historical/{code}",
            {"interval": provider_interval},
            ttl_seconds=_historical_ttl_seconds(end_date),
            use_cache=True,
            timeout_seconds=30.0,
            timeout_message="EODHD commodity data request timed out after 30 seconds",
            operation="commodity bars",
            symbol_for_error=symbol,
        )
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
            raise EODHDProviderError(f"EODHD returned an unexpected commodity data shape for {symbol}")
        bars: list[OHLCBar] = []
        for row in payload["data"]:
            row_date = date.fromisoformat(str(row.get("date"))[:10])
            if row_date < start_date or row_date > end_date:
                continue
            value = _optional_float(row.get("value") or row.get("close"))
            if value is None:
                continue
            timestamp = datetime.combine(row_date, time.min, tzinfo=UTC)
            bars.append(OHLCBar(symbol=symbol, timestamp=timestamp, open=value, high=value, low=value, close=value, volume=0.0))
        return sorted(bars, key=lambda bar: bar.timestamp)

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
            cached_error = self.cache.get_json("provider_error", self.base_url + endpoint, params)
            if cached_error is not None:
                stale = self.cache.get_json(namespace, self.base_url + endpoint, params, allow_stale=True)
                if stale is not None:
                    return stale
                status = _cached_error_status(cached_error)
                raise EODHDProviderError(
                    f"Recent EODHD {operation} provider error cached for {symbol_for_error}: {status}; no stale cache available"
                )

        request_params = {**params, "api_token": self.api_token, "fmt": "json"}
        max_attempts = len(self.retry_delays_seconds) + 1
        for attempt in range(max_attempts):
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_seconds, connect=5.0)) as client:
                    response = await client.get(f"{self.base_url}{endpoint}", params=request_params)
                    response.raise_for_status()
                    payload = response.json()
                break
            except httpx.TimeoutException as exc:
                if attempt < len(self.retry_delays_seconds):
                    await _sleep_before_retry(self.retry_delays_seconds[attempt])
                    continue
                if self.cache_enabled and use_cache:
                    self._remember_provider_error(endpoint, params, operation, symbol_for_error, "timeout", timeout_message)
                    stale = self.cache.get_json(namespace, self.base_url + endpoint, params, allow_stale=True)
                    if stale is not None:
                        return stale
                raise TimeoutError(timeout_message) from exc
            except httpx.HTTPStatusError as exc:
                status_code = _status_code(exc)
                if status_code in self.TRANSIENT_STATUS_CODES and attempt < len(self.retry_delays_seconds):
                    await _sleep_before_retry(self.retry_delays_seconds[attempt])
                    continue
                if self.cache_enabled and use_cache:
                    if status_code in self.TRANSIENT_STATUS_CODES:
                        self._remember_provider_error(
                            endpoint,
                            params,
                            operation,
                            symbol_for_error,
                            status_code,
                            f"EODHD {operation} returned HTTP {status_code} for {symbol_for_error}",
                        )
                    stale = self.cache.get_json(namespace, self.base_url + endpoint, params, allow_stale=True)
                    if stale is not None:
                        return stale
                _raise_status_error(exc, operation, symbol_for_error)
        else:
            raise EODHDProviderError(f"EODHD {operation} did not return a response for {symbol_for_error}")

        if self.cache_enabled and use_cache:
            self.cache.set_json(
                namespace,
                self.base_url + endpoint,
                params,
                payload,
                ttl_seconds,
                metadata={
                    "provider": "eodhd",
                    "operation": operation,
                    "symbol": symbol_for_error,
                    "source_status": "fresh_eodhd",
                },
            )
        return payload

    def _remember_provider_error(
        self,
        endpoint: str,
        params: dict[str, object],
        operation: str,
        symbol: str,
        status_code: int | str,
        message: str,
    ) -> None:
        self.cache.set_json(
            "provider_error",
            self.base_url + endpoint,
            params,
            {
                "provider": "eodhd",
                "operation": operation,
                "symbol": symbol,
                "status_code": status_code,
                "message": message,
                "created_at": datetime.now(UTC).isoformat(),
            },
            self.negative_error_ttl_seconds,
            metadata={
                "provider": "eodhd",
                "operation": operation,
                "symbol": symbol,
                "source_status": "provider_error",
            },
        )


def _bar_from_row(symbol: str, row: dict[str, object]) -> OHLCBar | None:
    timestamp_value = row.get("datetime") or row.get("timestamp") or row.get("date")
    try:
        if isinstance(timestamp_value, (int, float)):
            timestamp = datetime.fromtimestamp(float(timestamp_value), UTC)
        else:
            timestamp = datetime.fromisoformat(str(timestamp_value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    open_price = _optional_float(row.get("open"))
    high_price = _optional_float(row.get("high"))
    low_price = _optional_float(row.get("low"))
    close_price = _optional_float(row.get("close"))
    if open_price is None or high_price is None or low_price is None or close_price is None:
        return None
    return OHLCBar(
        symbol=symbol,
        timestamp=timestamp,
        open=open_price,
        high=high_price,
        low=low_price,
        close=close_price,
        volume=_optional_float(row.get("volume")) or 0.0,
    )


def _aggregate_bars(bars: list[OHLCBar], size: int) -> list[OHLCBar]:
    output: list[OHLCBar] = []
    for index in range(0, len(bars), size):
        group = bars[index : index + size]
        if len(group) < size:
            continue
        output.append(
            OHLCBar(
                symbol=group[0].symbol,
                timestamp=group[0].timestamp,
                open=group[0].open,
                high=max(bar.high for bar in group),
                low=min(bar.low for bar in group),
                close=group[-1].close,
                volume=sum(bar.volume for bar in group),
            )
        )
    return output


def _parse_date(value: str, label: str) -> date:
    try:
        return date.fromisoformat(value[:10])
    except ValueError as exc:
        raise EODHDProviderError(f"Invalid {label} date {value!r}; expected YYYY-MM-DD") from exc


def _historical_ttl_seconds(chunk_end: date) -> int:
    today = datetime.now(UTC).date()
    if chunk_end < today - timedelta(days=3):
        return EODHDProvider.CLOSED_HISTORY_TTL_SECONDS
    return EODHDProvider.LIVE_HISTORY_TTL_SECONDS


def _date_chunks(start_date: date, end_date: date, chunk_days: int):
    current = start_date
    safe_chunk_days = max(1, chunk_days)
    while current <= end_date:
        chunk_end = min(end_date, current + timedelta(days=safe_chunk_days - 1))
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def _raise_status_error(exc: object, operation: str, symbol: str) -> None:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", "unknown")
    if status_code in {401, 403}:
        raise EODHDProviderError(f"EODHD {operation} was rejected for {symbol}. Check the API token and plan permissions.") from exc
    if status_code == 404:
        raise EODHDProviderError(f"EODHD {operation} returned HTTP 404 for {symbol}. Check the EODHD symbol and exchange suffix.") from exc
    if status_code in EODHDProvider.TRANSIENT_STATUS_CODES:
        raise EODHDProviderError(f"EODHD {operation} returned HTTP {status_code} for {symbol}; no stale cache available") from exc
    raise EODHDProviderError(f"EODHD {operation} returned HTTP {status_code} for {symbol}") from exc


def _status_code(exc: object) -> int | str:
    response = getattr(exc, "response", None)
    return getattr(response, "status_code", "unknown")


def _screening_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("data", "results", "stocks", "items"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
    return []


def _cached_error_status(payload: object) -> str:
    if isinstance(payload, dict):
        status = payload.get("status_code") or "unknown"
        return f"HTTP {status}" if isinstance(status, int) else str(status)
    return "provider error"


async def _sleep_before_retry(delay_seconds: float) -> None:
    jitter = random.uniform(0.0, min(0.1, max(0.0, delay_seconds * 0.1))) if delay_seconds > 0 else 0.0
    await asyncio.sleep(max(0.0, delay_seconds) + jitter)


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _optional_timestamp(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), UTC)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
