from __future__ import annotations

from datetime import datetime

from .base import OHLCBar, Quote


class FMPProviderError(RuntimeError):
    pass


class FMPProvider:
    BASE_URL = "https://financialmodelingprep.com/stable"

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

        url = f"{self.base_url}/historical-chart/{interval}"
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=5.0)) as client:
                response = await client.get(
                    url,
                    params={"symbol": symbol, "from": start, "to": end, "apikey": self.api_key},
                )
                response.raise_for_status()
                payload = response.json()
        except httpx.TimeoutException as exc:
            raise TimeoutError("FMP historical data request timed out after 30 seconds") from exc
        except httpx.HTTPStatusError as exc:
            _raise_status_error(exc, "historical bars", symbol)
        if not isinstance(payload, list):
            raise FMPProviderError(f"FMP returned an unexpected historical data shape for {symbol}")
        bars = [
            OHLCBar(
                symbol=symbol,
                timestamp=datetime.fromisoformat(row["date"].replace("Z", "+00:00")),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row.get("volume") or 0),
            )
            for row in payload
        ]
        return sorted(bars, key=lambda bar: bar.timestamp)

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
