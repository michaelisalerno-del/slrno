from __future__ import annotations

from datetime import datetime

from .base import AccountStatus, OHLCBar, PaperOrder, Position


class IGDemoProvider:
    DEMO_URL = "https://demo-api.ig.com/gateway/deal"

    def __init__(
        self,
        api_key: str,
        username: str,
        password: str,
        account_id: str | None = None,
        base_url: str | None = None,
    ) -> None:
        self.api_key = api_key
        self.username = username
        self.password = password
        self.account_id = (account_id or "").strip()
        self.base_url = (base_url or self.DEMO_URL).rstrip("/")
        self._headers: dict[str, str] | None = None

    async def login(self) -> None:
        import httpx

        headers = {
            "X-IG-API-KEY": self.api_key,
            "Version": "3",
            "Content-Type": "application/json",
            "Accept": "application/json; charset=UTF-8",
        }
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
                response = await client.post(
                    f"{self.base_url}/session",
                    headers=headers,
                    json={"identifier": self.username, "password": self.password},
                )
                response.raise_for_status()
        except httpx.TimeoutException as exc:
            raise TimeoutError("IG demo validation timed out after 10 seconds") from exc
        except httpx.HTTPStatusError as exc:
            raise ValueError(_ig_error_message(exc.response, "IG login failed")) from exc

        payload = response.json()
        oauth = payload.get("oauthToken") or {}
        access_token = str(oauth.get("access_token") or "")
        active_account_id = str(payload.get("accountId") or "")
        selected_account_id = self.account_id or active_account_id
        if not access_token:
            raise ValueError("IG login succeeded but no OAuth access token was returned")
        if not selected_account_id:
            raise ValueError("IG login succeeded but no account id was returned; enter your IG account code manually")

        self.account_id = selected_account_id
        self._headers = {
            "X-IG-API-KEY": self.api_key,
            "Authorization": f"Bearer {access_token}",
            "IG-ACCOUNT-ID": selected_account_id,
            "Content-Type": "application/json",
            "Accept": "application/json; charset=UTF-8",
        }

    async def account_status(self) -> AccountStatus:
        import httpx

        headers = await self._authenticated_headers()
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
                response = await client.get(f"{self.base_url}/accounts", headers={**headers, "Version": "1"})
                response.raise_for_status()
                payload = response.json()
        except httpx.TimeoutException as exc:
            raise TimeoutError("IG accounts validation timed out after 10 seconds") from exc
        except httpx.HTTPStatusError as exc:
            raise ValueError(_ig_error_message(exc.response, "IG account validation failed")) from exc

        accounts = payload.get("accounts") or []
        account = _select_account(accounts, self.account_id)
        if account is None:
            available = ", ".join(str(item.get("accountId", "")) for item in accounts if item.get("accountId"))
            raise ValueError(f"IG account code '{self.account_id}' was not found. Available accounts: {available or 'none returned'}")
        balance = account.get("balance") or {}
        return AccountStatus(
            provider="ig",
            account_id=str(account.get("accountId", self.account_id)),
            currency=str(account.get("currency") or account.get("currencyIsoCode") or ""),
            available=_optional_float(balance.get("available")),
            mode="demo",
        )

    async def find_market(self, query: str) -> list[dict[str, str]]:
        import httpx

        headers = await self._authenticated_headers()
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
            response = await client.get(f"{self.base_url}/markets", headers={**headers, "Version": "1"}, params={"searchTerm": query})
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

    async def market_details(self, epic: str) -> dict[str, object]:
        import httpx

        headers = await self._authenticated_headers()
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
                response = await client.get(f"{self.base_url}/markets/{epic}", headers={**headers, "Version": "4"})
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as exc:
            raise ValueError(_ig_error_message(exc.response, f"IG market detail lookup failed for {epic}")) from exc

    async def historical_prices(self, epic: str, resolution: str, start: str, end: str) -> list[OHLCBar]:
        import httpx

        headers = await self._authenticated_headers()
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=5.0)) as client:
                response = await client.get(
                    f"{self.base_url}/prices/{epic}/{resolution}/{start}/{end}",
                    headers={**headers, "Version": "3"},
                )
                response.raise_for_status()
                payload = response.json()
        except httpx.HTTPStatusError as exc:
            raise ValueError(_ig_error_message(exc.response, f"IG price lookup failed for {epic}")) from exc
        return [_ig_price_bar(epic, row) for row in payload.get("prices", [])]

    async def positions(self) -> list[Position]:
        import httpx

        headers = await self._authenticated_headers()
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
            response = await client.get(f"{self.base_url}/positions", headers={**headers, "Version": "2"})
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


def _select_account(accounts: list[dict[str, object]], account_id: str) -> dict[str, object] | None:
    if not accounts:
        return None
    if account_id:
        for account in accounts:
            if str(account.get("accountId", "")) == account_id:
                return account
        return None
    return accounts[0]


def _ig_error_message(response: object, fallback: str) -> str:
    try:
        payload = response.json()
    except Exception:
        payload = {}
    error = payload.get("errorCode") or payload.get("error") or payload.get("message")
    if error:
        return f"{fallback}: {error}"
    return f"{fallback}: HTTP {getattr(response, 'status_code', 'unknown')}"


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _ig_price_bar(epic: str, row: dict[str, object]) -> OHLCBar:
    close = row.get("closePrice") or {}
    open_price = row.get("openPrice") or close
    high = row.get("highPrice") or close
    low = row.get("lowPrice") or close
    timestamp = str(row.get("snapshotTimeUTC") or row.get("snapshotTime") or "").replace("/", "-")
    return OHLCBar(
        symbol=epic,
        timestamp=datetime.fromisoformat(timestamp.replace("Z", "+00:00")),
        open=_mid(open_price),
        high=_mid(high),
        low=_mid(low),
        close=_mid(close),
        volume=float(row.get("lastTradedVolume") or 0),
    )


def _mid(value: dict[str, object]) -> float:
    bid = _optional_float(value.get("bid"))
    ask = _optional_float(value.get("ask") or value.get("offer"))
    if bid is not None and ask is not None:
        return (bid + ask) / 2
    return float(value.get("lastTraded") or value.get("bid") or value.get("ask") or value.get("offer") or 0)
