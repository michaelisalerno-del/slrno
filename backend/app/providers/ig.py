from __future__ import annotations

from .base import AccountStatus, PaperOrder, Position


class IGDemoProvider:
    DEMO_URL = "https://demo-api.ig.com/gateway/deal"

    def __init__(self, api_key: str, username: str, password: str, base_url: str | None = None) -> None:
        self.api_key = api_key
        self.username = username
        self.password = password
        self.base_url = (base_url or self.DEMO_URL).rstrip("/")
        self._headers: dict[str, str] | None = None

    async def login(self) -> None:
        import httpx

        headers = {
            "X-IG-API-KEY": self.api_key,
            "Version": "2",
            "Content-Type": "application/json",
            "Accept": "application/json; charset=UTF-8",
        }
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(
                f"{self.base_url}/session",
                headers=headers,
                json={"identifier": self.username, "password": self.password},
            )
            response.raise_for_status()
        self._headers = {
            **headers,
            "CST": response.headers.get("CST", ""),
            "X-SECURITY-TOKEN": response.headers.get("X-SECURITY-TOKEN", ""),
        }

    async def account_status(self) -> AccountStatus:
        import httpx

        headers = await self._authenticated_headers()
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(f"{self.base_url}/accounts", headers=headers)
            response.raise_for_status()
            payload = response.json()
        account = (payload.get("accounts") or [{}])[0]
        balance = account.get("balance") or {}
        return AccountStatus(
            provider="ig",
            account_id=str(account.get("accountId", "")),
            currency=str(account.get("currency", "")),
            available=_optional_float(balance.get("available")),
            mode="demo",
        )

    async def find_market(self, query: str) -> list[dict[str, str]]:
        import httpx

        headers = await self._authenticated_headers()
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(f"{self.base_url}/markets", headers=headers, params={"searchTerm": query})
            response.raise_for_status()
            payload = response.json()
        return [
            {
                "epic": str(item.get("epic", "")),
                "name": str(item.get("instrumentName", "")),
                "type": str(item.get("instrumentType", "")),
            }
            for item in payload.get("markets", [])
        ]

    async def positions(self) -> list[Position]:
        import httpx

        headers = await self._authenticated_headers()
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(f"{self.base_url}/positions", headers=headers)
            response.raise_for_status()
            payload = response.json()
        positions: list[Position] = []
        for row in payload.get("positions", []):
            market = row.get("market") or {}
            position = row.get("position") or {}
            positions.append(
                Position(
                    market_id=str(market.get("epic", "")),
                    side=str(position.get("direction", "")),
                    size=float(position.get("size") or 0),
                    entry_price=float(position.get("level") or 0),
                    current_price=float(market.get("bid") or market.get("offer") or 0),
                    unrealized_pnl=float(position.get("profit") or 0),
                )
            )
        return positions

    async def place_paper_order(self, order: PaperOrder) -> Position:
        raise NotImplementedError("IGDemoProvider never places orders in v1. Use PaperBrokerProvider.")

    async def _authenticated_headers(self) -> dict[str, str]:
        if self._headers is None:
            await self.login()
        return dict(self._headers or {})


class PaperBrokerProvider:
    def __init__(self) -> None:
        self._positions: list[Position] = []

    async def account_status(self) -> AccountStatus:
        return AccountStatus("paper", "paper-demo", "GBP", None, "paper")

    async def find_market(self, query: str) -> list[dict[str, str]]:
        return [{"epic": query, "name": query, "type": "paper"}]

    async def positions(self) -> list[Position]:
        return list(self._positions)

    async def place_paper_order(self, order: PaperOrder) -> Position:
        position = Position(
            market_id=order.market_id,
            side=order.side,
            size=order.size,
            entry_price=order.price,
            current_price=order.price,
            unrealized_pnl=0.0,
        )
        self._positions.append(position)
        return position


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return float(value)
