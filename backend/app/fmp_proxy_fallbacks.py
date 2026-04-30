from __future__ import annotations

from functools import wraps

from .providers.base import OHLCBar
from .providers.fmp import FMPProvider, FMPProviderError

INDEX_PROXY_SYMBOLS = {
    "^NDX": "QQQ",
    "^GSPC": "SPY",
    "^GDAXI": "EWG",
    "^FTSE": "EWU",
}


def install_fmp_proxy_fallbacks() -> None:
    if getattr(FMPProvider, "_slrno_proxy_fallbacks_installed", False):
        return

    original_historical_bars = FMPProvider.historical_bars

    @wraps(original_historical_bars)
    async def historical_bars(self: FMPProvider, symbol: str, interval: str, start: str, end: str) -> list[OHLCBar]:
        try:
            return await original_historical_bars(self, symbol, interval, start, end)
        except FMPProviderError as exc:
            proxy_symbol = INDEX_PROXY_SYMBOLS.get(symbol)
            if proxy_symbol is None or not _looks_like_plan_gap(exc):
                raise
            return await original_historical_bars(self, proxy_symbol, interval, start, end)

    FMPProvider.historical_bars = historical_bars
    FMPProvider._slrno_proxy_fallbacks_installed = True


def _looks_like_plan_gap(exc: FMPProviderError) -> bool:
    message = str(exc)
    return "HTTP 402" in message or "HTTP 404" in message
