from __future__ import annotations

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
    CLOSED_HISTORY_TTL_SECONDS = 180 * 24 * 60 * 60
    LIVE_HISTORY_TTL_SECONDS = 15 * 60

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

    def __init__(self, api_token: str, base_url: str | None = None, cache: MarketDataCache | None = None, cache_enabled: bool = True) -> None:
        self.api_token = api_token
        self.base_url = (base_url or self.BASE_URL).rstrip("/")
        self.cache = cache if cache is not None else MarketDataCache()
        self.cache_enabled = cache_enabled

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
        payload = await self._get_json(
            "historical_bars",
            f"/intraday/{symbol}",
            {
                "interval": provider_interval,
                "from": int(datetime.combine(start_date, time.min, tzinfo=UTC).timestamp()),
                "to": int(datetime.combine(end_date, time.max, tzinfo=UTC).timestamp()),
            },
            ttl_seconds=_historical_ttl_seconds(end_date),
            use_cache=True,
            timeout_seconds=30.0,
            timeout_message="EODHD historical data request timed out after 30 seconds",
            operation="historical bars",
            symbol_for_error=symbol,
        )
        if not isinstance(payload, list):
            raise EODHDProviderError(f"EODHD returned an unexpected historical data shape for {symbol}")
        bars = [_bar_from_row(symbol, row) for row in payload]
        bars = sorted({bar.timestamp: bar for bar in bars}.values(), key=lambda bar: bar.timestamp)
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
        return sorted((_bar_from_row(symbol, row) for row in payload), key=lambda bar: bar.timestamp)

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
            value = float(row.get("value") or row.get("close") or 0)
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

        request_params = {**params, "api_token": self.api_token, "fmt": "json"}
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
    timestamp_value = row.get("datetime") or row.get("timestamp") or row.get("date")
    if isinstance(timestamp_value, (int, float)):
        timestamp = datetime.fromtimestamp(float(timestamp_value), UTC)
    else:
        timestamp = datetime.fromisoformat(str(timestamp_value).replace("Z", "+00:00"))
    return OHLCBar(
        symbol=symbol,
        timestamp=timestamp,
        open=float(row["open"]),
        high=float(row["high"]),
        low=float(row["low"]),
        close=float(row["close"]),
        volume=float(row.get("volume") or 0),
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


def _raise_status_error(exc: object, operation: str, symbol: str) -> None:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", "unknown")
    if status_code in {401, 403}:
        raise EODHDProviderError(f"EODHD {operation} was rejected for {symbol}. Check the API token and plan permissions.") from exc
    if status_code == 404:
        raise EODHDProviderError(f"EODHD {operation} returned HTTP 404 for {symbol}. Check the EODHD symbol and exchange suffix.") from exc
    raise EODHDProviderError(f"EODHD {operation} returned HTTP {status_code} for {symbol}") from exc


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
