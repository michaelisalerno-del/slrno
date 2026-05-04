from __future__ import annotations

import re
from dataclasses import asdict, dataclass, replace
from typing import Any

from .market_registry import MarketMapping
from .share_spread_betting import share_spread_bet_model

DEFAULT_MIDCAP_ACCOUNT_SIZE_GBP = 3_000.0
DEFAULT_MIN_MARKET_CAP = 250_000_000.0
DEFAULT_MAX_MARKET_CAP = 10_000_000_000.0
DEFAULT_MIN_VOLUME = 100_000.0
DEFAULT_MAX_SPREAD_BPS = 60.0
DEFAULT_STAKE_PROBE = 1.0
DISCOVERED_SHARE_DEFAULT_TIMEFRAME = "5min"
DISCOVERED_SHARE_MIN_BACKTEST_BARS = 750

COUNTRY_EXCHANGE_HINTS = {
    "uk": ("LSE", "GB"),
    "gb": ("LSE", "GB"),
    "united_kingdom": ("LSE", "GB"),
    "us": ("NASDAQ", "US"),
    "usa": ("NASDAQ", "US"),
    "united_states": ("NASDAQ", "US"),
}

UK_MIDCAP_STARTER_ROWS = [
    {"symbol": "JD.L", "companyName": "JD Sports Fashion", "exchangeShortName": "LSE", "country": "GB", "currency": "GBp", "marketCap": 6_000_000_000, "price": 120, "volume": 18_000_000},
    {"symbol": "MKS.L", "companyName": "Marks and Spencer Group", "exchangeShortName": "LSE", "country": "GB", "currency": "GBp", "marketCap": 7_000_000_000, "price": 340, "volume": 6_000_000},
    {"symbol": "IAG.L", "companyName": "International Consolidated Airlines Group", "exchangeShortName": "LSE", "country": "GB", "currency": "GBp", "marketCap": 8_000_000_000, "price": 170, "volume": 15_000_000},
    {"symbol": "SBRY.L", "companyName": "J Sainsbury", "exchangeShortName": "LSE", "country": "GB", "currency": "GBp", "marketCap": 6_000_000_000, "price": 270, "volume": 5_000_000},
    {"symbol": "ITV.L", "companyName": "ITV", "exchangeShortName": "LSE", "country": "GB", "currency": "GBp", "marketCap": 3_000_000_000, "price": 75, "volume": 8_000_000},
    {"symbol": "EZJ.L", "companyName": "easyJet", "exchangeShortName": "LSE", "country": "GB", "currency": "GBp", "marketCap": 4_000_000_000, "price": 520, "volume": 2_500_000},
    {"symbol": "TW.L", "companyName": "Taylor Wimpey", "exchangeShortName": "LSE", "country": "GB", "currency": "GBp", "marketCap": 5_000_000_000, "price": 145, "volume": 9_000_000},
    {"symbol": "WPP.L", "companyName": "WPP", "exchangeShortName": "LSE", "country": "GB", "currency": "GBp", "marketCap": 8_000_000_000, "price": 760, "volume": 2_000_000},
    {"symbol": "CNA.L", "companyName": "Centrica", "exchangeShortName": "LSE", "country": "GB", "currency": "GBp", "marketCap": 7_000_000_000, "price": 130, "volume": 20_000_000},
    {"symbol": "PSN.L", "companyName": "Persimmon", "exchangeShortName": "LSE", "country": "GB", "currency": "GBp", "marketCap": 4_000_000_000, "price": 1250, "volume": 800_000},
]


@dataclass(frozen=True)
class MidcapDiscoveryCriteria:
    country: str = "UK"
    product_mode: str = "spread_bet"
    min_market_cap: float = DEFAULT_MIN_MARKET_CAP
    max_market_cap: float = DEFAULT_MAX_MARKET_CAP
    min_volume: float = DEFAULT_MIN_VOLUME
    max_spread_bps: float = DEFAULT_MAX_SPREAD_BPS
    account_size: float = DEFAULT_MIDCAP_ACCOUNT_SIZE_GBP
    stake_probe: float = DEFAULT_STAKE_PROBE

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MidcapDiscoveryCandidate:
    market_id: str
    name: str
    symbol: str
    fmp_symbol: str
    eodhd_symbol: str
    exchange: str
    country: str
    currency: str
    market_cap: float
    price: float
    volume: float
    product_mode: str
    estimated_spread_bps: float
    estimated_slippage_bps: float
    margin_percent: float
    contract_point_size: float
    estimated_margin_for_probe_stake: float
    account_size: float
    eligible: bool
    feasible_for_account: bool
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    score: float
    source: str
    ig_status: str = "not_checked"
    ig_epic: str = ""
    ig_name: str = ""

    def market_mapping(self) -> MarketMapping:
        return MarketMapping(
            self.market_id,
            self.name,
            "share",
            self.eodhd_symbol,
            self.ig_epic,
            True,
            f"discovered-{self.market_id.lower()}",
            self.ig_name or self.name,
            ",".join(term for term in (self.market_id, self.name, self.symbol) if term),
            DISCOVERED_SHARE_DEFAULT_TIMEFRAME,
            self.estimated_spread_bps,
            self.estimated_slippage_bps,
            DISCOVERED_SHARE_MIN_BACKTEST_BARS,
        )

    def with_ig_match(self, epic: str, name: str) -> "MidcapDiscoveryCandidate":
        status = "ig_matched" if epic else "ig_not_found"
        blockers = self.blockers
        warnings = tuple(item for item in self.warnings if item != "ig_availability_not_checked")
        if not epic and "ig_availability_unconfirmed" not in warnings:
            warnings = (*warnings, "ig_availability_unconfirmed")
            if "ig_market_not_found" not in blockers:
                blockers = (*blockers, "ig_market_not_found")
        return replace(self, ig_status=status, ig_epic=epic, ig_name=name, blockers=blockers, eligible=not blockers)

    def with_ig_blocker(self, status: str, blocker: str, warning: str = "") -> "MidcapDiscoveryCandidate":
        blockers = self.blockers if blocker in self.blockers else (*self.blockers, blocker)
        warnings = tuple(item for item in self.warnings if item != "ig_availability_not_checked")
        if warning and warning not in warnings:
            warnings = (*warnings, warning)
        return replace(self, ig_status=status, blockers=blockers, warnings=warnings, eligible=False)

    def as_dict(self) -> dict[str, object]:
        mapping = self.market_mapping()
        return {
            **asdict(self),
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "market_mapping": mapping.__dict__,
        }


def country_exchange_hint(country: str) -> tuple[str, str]:
    key = _normal_key(country)
    return COUNTRY_EXCHANGE_HINTS.get(key, ("", country.upper()[:2] if country else ""))


def fallback_midcap_rows(country: str) -> list[dict[str, object]]:
    key = _normal_key(country)
    if key in {"uk", "gb", "united_kingdom"}:
        return [dict(row, source="built_in_uk_midcap_starter") for row in UK_MIDCAP_STARTER_ROWS]
    return []


def build_midcap_candidates(rows: list[dict[str, Any]], criteria: MidcapDiscoveryCriteria, source: str) -> list[MidcapDiscoveryCandidate]:
    candidates: list[MidcapDiscoveryCandidate] = []
    seen: set[str] = set()
    for row in rows:
        candidate = _candidate_from_row(row, criteria, source)
        if candidate is None or candidate.market_id in seen:
            continue
        seen.add(candidate.market_id)
        candidates.append(candidate)
    return sorted(candidates, key=lambda item: (item.eligible, item.score, item.volume), reverse=True)


def _candidate_from_row(row: dict[str, Any], criteria: MidcapDiscoveryCriteria, source: str) -> MidcapDiscoveryCandidate | None:
    symbol = str(row.get("symbol") or row.get("ticker") or "").strip()
    name = str(row.get("companyName") or row.get("company_name") or row.get("name") or symbol).strip()
    if not symbol or not name:
        return None
    market_id = _market_id_from_symbol(symbol)
    eodhd_symbol = _eodhd_symbol_from_fmp(symbol, row, criteria.country)
    if not market_id or not eodhd_symbol:
        return None
    market = MarketMapping(
        market_id,
        name,
        "share",
        eodhd_symbol,
        "",
        True,
        f"discovered-{market_id.lower()}",
        name,
        ",".join(term for term in (market_id, name, symbol) if term),
        "1day",
        10.0,
        5.0,
        250,
    )
    share_model = share_spread_bet_model(market)
    if share_model is None:
        return None
    market_cap = _float(row.get("marketCap"), row.get("market_cap"), row.get("mktCap"))
    price = _float(row.get("price"), row.get("lastSale"), row.get("last"))
    volume = _float(row.get("volume"), row.get("avgVolume"), row.get("averageVolume"))
    spread_bps = max(market.spread_bps, share_model.dealing_spread_bps)
    slippage_bps = max(market.slippage_bps, share_model.slippage_bps)
    margin_percent = share_model.margin_percent
    contract_point_size = share_model.contract_point_size
    estimated_margin = _estimated_margin(price, contract_point_size, criteria.stake_probe, margin_percent)
    blockers: list[str] = []
    warnings: list[str] = ["ig_availability_not_checked"]
    if market_cap <= 0:
        warnings.append("missing_market_cap")
    elif market_cap < criteria.min_market_cap:
        blockers.append("below_midcap_market_cap_floor")
    elif market_cap > criteria.max_market_cap:
        blockers.append("above_midcap_market_cap_ceiling")
    if price <= 0:
        blockers.append("missing_price")
    if volume < criteria.min_volume:
        blockers.append("low_liquidity")
    if spread_bps > criteria.max_spread_bps:
        blockers.append("spread_too_wide_for_scan")
    feasible = True
    if price > 0 and estimated_margin > criteria.account_size * 0.5:
        feasible = False
        blockers.append("probe_stake_margin_too_large")
    if criteria.product_mode == "cfd":
        warnings.append("cfd_cost_model_not_yet_enabled")
    if source.startswith("built_in"):
        warnings.append("starter_universe_not_live_constituents")
    score = _score_candidate(market_cap, price, volume, spread_bps, estimated_margin, criteria)
    return MidcapDiscoveryCandidate(
        market_id=market_id,
        name=name,
        symbol=symbol,
        fmp_symbol=symbol,
        eodhd_symbol=eodhd_symbol,
        exchange=str(row.get("exchangeShortName") or row.get("exchange") or ""),
        country=str(row.get("country") or country_exchange_hint(criteria.country)[1]),
        currency=str(row.get("currency") or ""),
        market_cap=round(market_cap, 2),
        price=round(price, 6),
        volume=round(volume, 2),
        product_mode=criteria.product_mode,
        estimated_spread_bps=round(spread_bps, 6),
        estimated_slippage_bps=round(slippage_bps, 6),
        margin_percent=round(margin_percent, 6),
        contract_point_size=round(contract_point_size, 8),
        estimated_margin_for_probe_stake=round(estimated_margin, 4),
        account_size=round(criteria.account_size, 2),
        eligible=not blockers,
        feasible_for_account=feasible,
        blockers=tuple(dict.fromkeys(blockers)),
        warnings=tuple(dict.fromkeys(warnings)),
        score=round(score, 4),
        source=source,
    )


def _market_id_from_symbol(symbol: str) -> str:
    root = symbol.split(".")[0].upper()
    return re.sub(r"[^A-Z0-9]+", "", root)[:24]


def _eodhd_symbol_from_fmp(symbol: str, row: dict[str, Any], country: str) -> str:
    clean = symbol.strip().upper()
    exchange = str(row.get("exchangeShortName") or row.get("exchange") or "").upper()
    country_code = str(row.get("country") or country_exchange_hint(country)[1]).upper()
    if clean.endswith(".L"):
        return f"{clean.removesuffix('.L')}.LSE"
    if clean.endswith(".US") or exchange in {"NASDAQ", "NYSE", "AMEX"} or country_code == "US":
        return clean if clean.endswith(".US") else f"{clean.split('.')[0]}.US"
    if exchange in {"LSE", "XLON", "LONDON"} or country_code in {"GB", "UK"}:
        return clean if clean.endswith(".LSE") else f"{clean.split('.')[0]}.LSE"
    return clean


def _estimated_margin(price: float, contract_point_size: float, stake: float, margin_percent: float) -> float:
    if price <= 0 or contract_point_size <= 0 or stake <= 0:
        return 0.0
    return abs((price / contract_point_size) * stake * margin_percent / 100)


def _score_candidate(
    market_cap: float,
    price: float,
    volume: float,
    spread_bps: float,
    estimated_margin: float,
    criteria: MidcapDiscoveryCriteria,
) -> float:
    liquidity_score = min(35.0, volume / max(criteria.min_volume, 1.0) * 7.0)
    cap_midpoint = (criteria.min_market_cap + criteria.max_market_cap) / 2
    cap_distance = abs(market_cap - cap_midpoint) / max(cap_midpoint, 1.0) if market_cap > 0 else 1.0
    cap_score = max(0.0, 25.0 * (1.0 - min(1.0, cap_distance)))
    cost_score = max(0.0, 20.0 * (1.0 - min(1.0, spread_bps / max(criteria.max_spread_bps, 1.0))))
    margin_score = max(0.0, 20.0 * (1.0 - min(1.0, estimated_margin / max(criteria.account_size * 0.5, 1.0))))
    price_score = 5.0 if price > 0 else 0.0
    return liquidity_score + cap_score + cost_score + margin_score + price_score


def _normal_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


def _float(*values: object) -> float:
    for value in values:
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            continue
    return 0.0
