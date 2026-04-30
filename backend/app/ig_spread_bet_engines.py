from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class IGSpreadBetEngine:
    engine_id: str
    label: str
    instrument_types: tuple[str, ...]
    default_asset_class: str
    eligible_for_adaptive_backtest: bool
    notes: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


IG_SPREAD_BET_ENGINES: tuple[IGSpreadBetEngine, ...] = (
    IGSpreadBetEngine(
        "indices",
        "Indices",
        ("INDICES",),
        "index",
        True,
        "Cash and forward index spread-bet markets such as US Tech 100, US 500, FTSE 100, and Germany 40.",
    ),
    IGSpreadBetEngine(
        "forex",
        "Forex",
        ("CURRENCIES",),
        "forex",
        True,
        "Spot and forward currency spread-bet markets.",
    ),
    IGSpreadBetEngine(
        "commodities",
        "Commodities",
        ("COMMODITIES",),
        "commodity",
        True,
        "Metals, energies, and agricultural commodity spread-bet markets.",
    ),
    IGSpreadBetEngine(
        "shares",
        "Shares",
        ("SHARES",),
        "share",
        True,
        "Share spread-bet markets. Availability, borrow, and overnight assumptions need market-specific review.",
    ),
    IGSpreadBetEngine(
        "sectors",
        "Sectors",
        ("SECTORS",),
        "sector",
        True,
        "Sector basket spread-bet markets.",
    ),
    IGSpreadBetEngine(
        "rates",
        "Rates",
        ("RATES",),
        "rates",
        True,
        "Government bond and interest-rate spread-bet markets.",
    ),
    IGSpreadBetEngine(
        "options",
        "Options",
        ("OPT_COMMODITIES", "OPT_CURRENCIES", "OPT_INDICES", "OPT_RATES", "OPT_SHARES"),
        "option",
        False,
        "IG option spread-bet instrument types. The current adaptive backtester treats them as discoverable but not yet production-scored.",
    ),
    IGSpreadBetEngine(
        "knockouts",
        "Knock-outs",
        ("KNOCKOUTS_COMMODITIES", "KNOCKOUTS_CURRENCIES", "KNOCKOUTS_INDICES", "KNOCKOUTS_SHARES"),
        "knockout",
        False,
        "Knock-out spread-bet instrument types. They need product-specific stop/barrier modeling before ranking.",
    ),
    IGSpreadBetEngine(
        "bungees",
        "Bungees",
        ("BUNGEE_CAPPED", "BUNGEE_COMMODITIES", "BUNGEE_CURRENCIES", "BUNGEE_INDICES"),
        "bungee",
        False,
        "Bungee instrument types. They need capped-risk payoff modeling before ranking.",
    ),
    IGSpreadBetEngine(
        "binaries",
        "Binaries and Sprints",
        ("BINARY", "SPRINT_MARKET"),
        "binary",
        False,
        "Binary and sprint instrument types. They are surfaced for IG coverage but excluded from the linear P/L backtester.",
    ),
)


def list_spread_bet_engines() -> list[dict[str, object]]:
    return [engine.as_dict() for engine in IG_SPREAD_BET_ENGINES]


def spread_bet_engine_for_instrument_type(instrument_type: str) -> IGSpreadBetEngine | None:
    normalized = instrument_type.upper()
    for engine in IG_SPREAD_BET_ENGINES:
        if normalized in engine.instrument_types:
            return engine
    return None
