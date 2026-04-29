from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


@dataclass(frozen=True)
class Quote:
    symbol: str
    bid: float | None
    ask: float | None
    last: float
    timestamp: datetime | None = None


@dataclass(frozen=True)
class OHLCBar:
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass(frozen=True)
class AccountStatus:
    provider: str
    account_id: str
    currency: str
    available: float | None
    mode: str


@dataclass(frozen=True)
class PaperOrder:
    market_id: str
    side: str
    size: float
    price: float
    stop: float | None = None
    limit: float | None = None


@dataclass(frozen=True)
class Position:
    market_id: str
    side: str
    size: float
    entry_price: float
    current_price: float
    unrealized_pnl: float


class MarketDataProvider(Protocol):
    async def quote(self, symbol: str) -> Quote:
        ...

    async def historical_bars(self, symbol: str, interval: str, start: str, end: str) -> list[OHLCBar]:
        ...

    async def search(self, query: str) -> list[dict[str, str]]:
        ...


class BrokerProvider(Protocol):
    async def account_status(self) -> AccountStatus:
        ...

    async def find_market(self, query: str) -> list[dict[str, str]]:
        ...

    async def positions(self) -> list[Position]:
        ...

    async def place_paper_order(self, order: PaperOrder) -> Position:
        ...
