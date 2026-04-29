from __future__ import annotations

from datetime import date, datetime, timedelta

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

    def __init__(self, api_key: str, base_url: str | None = None) -> None:
        self.api_key = api_key
        self.base_url = (base_url or self.BASE_URL).rstrip("/")

    async def quote(self, symbol: str) -> Quote:
        import httpx

        url = f"{self.base_url}/quote"
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
                response = await client.get(url, params={"symbol": symbol, "apikey": self.api_key})
                response.raise_for_status()
                payload = response.json()
        except httpx.TimeoutException as exc:
            raise TimeoutError("FMP validation timed out after 10 seconds") from exc
        except httpx.HTTPStatusError as exc:
            _raise_status_error(exc, "quote lookup", symbol)
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
        import httpx

        start_date = _parse_date(start, "start")
        end_date = _parse_date(end, "end")
        if end_date < start_date:
            raise FMPProviderError("FMP historical request end date must be on or after start date")

        chunk_days = self.INTRADAY_CHUNK_DAYS.get(interval)
        ranges = [(start_date, end_date)] if chunk_days is None else _date_chunks(start_date, end_date, chunk_days)
        bars_by_timestamp: dict[datetime, OHLCBar] = {}
        url = f"{self.base_url}/historical-chart/{interval}"

        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=5.0)) as client:
                for chunk_start, chunk_end in ranges:
                    response = await client.get(
                        url,
                        params={
                            "symbol": symbol,
                            "from": chunk_start.isoformat(),
                            "to": chunk_end.isoformat(),
                            "apikey": self.api_key,
                        },
                    )
                    response.raise_for_status()
                    payload = response.json()
                    if not isinstance(payload, list):
                        raise FMPProviderError(f"FMP returned an unexpected historical data shape for {symbol}")
                    for row in payload:
                        bar = _bar_from_row(symbol, row)
                        bars_by_timestamp[bar.timestamp] = bar
        except httpx.TimeoutException as exc:
            raise TimeoutError("FMP historical data request timed out after 30 seconds") from exc
        except httpx.HTTPStatusError as exc:
            _raise_status_error(exc, "historical bars", symbol)

        return sorted(bars_by_timestamp.values(), key=lambda bar: bar.timestamp)

    async def search(self, query: str) -> list[dict[str, str]]:
        import httpx

        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
                response = await client.get(
                    f"{self.base_url}/search-symbol",
                    params={"query": query, "apikey": self.api_key},
                )
                response.raise_for_status()
                payload = response.json()
        except httpx.TimeoutException as exc:
            raise TimeoutError("FMP symbol search timed out after 10 seconds") from exc
        except httpx.HTTPStatusError as exc:
            _raise_status_error(exc, "symbol search", query)
        return [
            {
                "symbol": str(item.get("symbol", "")),
                "name": str(item.get("name", "")),
                "exchange": str(item.get("exchangeShortName", "")),
            }
            for item in payload
        ]

    async def validate(self) -> bool:
        await self.quote("AAPL")
        return True


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
