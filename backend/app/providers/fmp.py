from __future__ import annotations

from typing import Any


class FMPProviderError(RuntimeError):
    pass


class FMPProvider:
    BASE_URL = "https://financialmodelingprep.com/stable"

    def __init__(self, api_key: str, base_url: str | None = None) -> None:
        self.api_key = api_key
        self.base_url = (base_url or self.BASE_URL).rstrip("/")

    async def validate(self) -> bool:
        payload = await self._get_json(
            "/quote",
            {"symbol": "AAPL"},
            timeout_seconds=10.0,
            operation="quote lookup",
        )
        if _looks_like_quote_payload(payload):
            return True
        raise FMPProviderError("FMP returned an unexpected quote response")

    async def _get_json(
        self,
        endpoint: str,
        params: dict[str, object],
        timeout_seconds: float,
        operation: str,
    ) -> Any:
        import httpx

        request_params = {**params, "apikey": self.api_key}
        try:
            async with httpx.AsyncClient(timeout=timeout_seconds) as client:
                response = await client.get(f"{self.base_url}{endpoint}", params=request_params)
                response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            if status in {401, 403}:
                raise FMPProviderError("FMP rejected the API key") from exc
            raise FMPProviderError(f"FMP {operation} failed with HTTP {status}") from exc
        except httpx.TimeoutException as exc:
            raise FMPProviderError(f"FMP {operation} timed out after {timeout_seconds:g} seconds") from exc
        except httpx.HTTPError as exc:
            raise FMPProviderError(f"FMP {operation} failed: {exc}") from exc

        try:
            return response.json()
        except ValueError as exc:
            raise FMPProviderError("FMP returned non-JSON data") from exc


def _looks_like_quote_payload(payload: Any) -> bool:
    if isinstance(payload, list):
        return any(_looks_like_quote_payload(item) for item in payload)
    if not isinstance(payload, dict):
        return False
    if payload.get("Error Message") or payload.get("error"):
        return False
    return bool(payload.get("symbol")) and any(key in payload for key in ("price", "previousClose", "volume"))
