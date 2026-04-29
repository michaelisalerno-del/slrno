from __future__ import annotations

from datetime import datetime
from urllib.parse import quote_plus

from .base import OHLCBar, Quote


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
        if not payload:
            raise ValueError(f"No quote returned for {symbol}")
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

        encoded = quote_plus(symbol)
        url = f"{self.base_url}/historical-chart/{interval}/{encoded}"
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=5.0)) as client:
            response = await client.get(
                url,
                params={"from": start, "to": end, "apikey": self.api_key},
            )
            response.raise_for_status()
            payload = response.json()
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

        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
            response = await client.get(
                f"{self.base_url}/search-symbol",
                params={"query": query, "apikey": self.api_key},
            )
            response.raise_for_status()
            payload = response.json()
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


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return float(value)
