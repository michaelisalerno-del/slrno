from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .config import market_db_path


@dataclass(frozen=True)
class MarketMapping:
    market_id: str
    name: str
    asset_class: str
    fmp_symbol: str
    ig_epic: str
    enabled: bool = True
    plugin_id: str = ""
    ig_name: str = ""
    ig_search_terms: str = ""
    default_timeframe: str = "5min"
    spread_bps: float = 2.0
    slippage_bps: float = 1.0
    min_backtest_bars: int = 750


DEFAULT_MARKETS = [
    MarketMapping("US500", "S&P 500", "index", "^GSPC", "", True, "", "US 500", "US 500,S&P 500,SPX", "5min", 2.0, 1.0),
    MarketMapping("NAS100", "Nasdaq 100", "index", "^NDX", "", True, "", "US Tech 100", "US Tech 100,Nasdaq,NASDAQ 100", "5min", 2.0, 1.0),
    MarketMapping("FTSE100", "FTSE 100", "index", "^FTSE", "", True, "ig-ftse-100", "FTSE 100", "FTSE 100,UK 100", "5min", 2.0, 1.0),
    MarketMapping("DE40", "Germany 40", "index", "^GDAXI", "", True, "ig-germany-40", "Germany 40", "Germany 40,DAX", "5min", 2.0, 1.0),
    MarketMapping("QQQ", "QQQ Nasdaq 100 ETF proxy", "etf", "QQQ", "", True, "fmp-qqq-nasdaq-proxy", "US Tech 100", "US Tech 100,Nasdaq,QQQ", "5min", 2.0, 1.0),
    MarketMapping("SPY", "SPY S&P 500 ETF proxy", "etf", "SPY", "", True, "fmp-spy-sp500-proxy", "US 500", "US 500,S&P 500,SPY", "5min", 2.0, 1.0),
    MarketMapping("EURUSD", "EUR/USD", "forex", "EURUSD", "", True, "ig-eur-usd", "EUR/USD", "EUR/USD,EURUSD,Euro Dollar", "5min", 1.2, 0.8),
    MarketMapping("GBPUSD", "GBP/USD", "forex", "GBPUSD", "", True, "ig-gbp-usd", "GBP/USD", "GBP/USD,GBPUSD,Cable", "5min", 1.4, 0.9),
    MarketMapping("EURGBP", "EUR/GBP", "forex", "EURGBP", "", True, "ig-eur-gbp", "EUR/GBP", "EUR/GBP,EURGBP", "5min", 1.5, 0.9),
    MarketMapping("USDJPY", "USD/JPY", "forex", "USDJPY", "", True, "ig-usd-jpy", "USD/JPY", "USD/JPY,USDJPY,Dollar Yen", "5min", 1.3, 0.9),
    MarketMapping("XAUUSD", "Spot Gold", "commodity", "XAUUSD", "", True, "", "Spot Gold", "Spot Gold,Gold,XAU/USD", "5min", 3.0, 1.5),
    MarketMapping("XAGUSD", "Spot Silver", "commodity", "XAGUSD", "", True, "ig-spot-silver", "Spot Silver", "Spot Silver,Silver,XAG/USD", "5min", 4.0, 2.0),
    MarketMapping("BRENT", "Brent Crude", "commodity", "BZ=F", "", True, "ig-brent-crude", "Brent Crude", "Brent Crude,Brent Oil", "5min", 3.5, 2.0),
    MarketMapping("NATGAS", "Natural Gas", "commodity", "NG=F", "", True, "ig-natural-gas", "Natural Gas", "Natural Gas,US Natural Gas", "5min", 5.0, 2.5),
]


class MarketRegistry:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or market_db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS markets (
                  market_id TEXT PRIMARY KEY,
                  name TEXT NOT NULL,
                  asset_class TEXT NOT NULL,
                  fmp_symbol TEXT NOT NULL,
                  ig_epic TEXT NOT NULL DEFAULT '',
                  enabled INTEGER NOT NULL DEFAULT 1,
                  plugin_id TEXT NOT NULL DEFAULT '',
                  ig_name TEXT NOT NULL DEFAULT '',
                  ig_search_terms TEXT NOT NULL DEFAULT '',
                  default_timeframe TEXT NOT NULL DEFAULT '5min',
                  spread_bps REAL NOT NULL DEFAULT 2.0,
                  slippage_bps REAL NOT NULL DEFAULT 1.0,
                  min_backtest_bars INTEGER NOT NULL DEFAULT 750
                )
                """
            )
            self._add_column(conn, "plugin_id", "TEXT NOT NULL DEFAULT ''")
            self._add_column(conn, "ig_name", "TEXT NOT NULL DEFAULT ''")
            self._add_column(conn, "ig_search_terms", "TEXT NOT NULL DEFAULT ''")
            self._add_column(conn, "default_timeframe", "TEXT NOT NULL DEFAULT '5min'")
            self._add_column(conn, "spread_bps", "REAL NOT NULL DEFAULT 2.0")
            self._add_column(conn, "slippage_bps", "REAL NOT NULL DEFAULT 1.0")
            self._add_column(conn, "min_backtest_bars", "INTEGER NOT NULL DEFAULT 750")

    def _add_column(self, conn: sqlite3.Connection, name: str, definition: str) -> None:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(markets)").fetchall()}
        if name not in columns:
            conn.execute(f"ALTER TABLE markets ADD COLUMN {name} {definition}")

    def seed_defaults(self) -> None:
        with self._connect() as conn:
            for market in DEFAULT_MARKETS:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO markets(
                      market_id, name, asset_class, fmp_symbol, ig_epic, enabled,
                      plugin_id, ig_name, ig_search_terms, default_timeframe,
                      spread_bps, slippage_bps, min_backtest_bars
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        market.market_id,
                        market.name,
                        market.asset_class,
                        market.fmp_symbol,
                        market.ig_epic,
                        int(market.enabled),
                        market.plugin_id,
                        market.ig_name,
                        market.ig_search_terms,
                        market.default_timeframe,
                        market.spread_bps,
                        market.slippage_bps,
                        market.min_backtest_bars,
                    ),
                )
            conn.execute(
                """
                UPDATE markets
                SET default_timeframe = '5min'
                WHERE market_id IN ('NAS100', 'US500', 'XAUUSD')
                  AND default_timeframe = '1h'
                """
            )

    def upsert(self, market: MarketMapping) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO markets(
                  market_id, name, asset_class, fmp_symbol, ig_epic, enabled,
                  plugin_id, ig_name, ig_search_terms, default_timeframe,
                  spread_bps, slippage_bps, min_backtest_bars
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market_id) DO UPDATE SET
                  name = excluded.name,
                  asset_class = excluded.asset_class,
                  fmp_symbol = excluded.fmp_symbol,
                  ig_epic = excluded.ig_epic,
                  enabled = excluded.enabled,
                  plugin_id = excluded.plugin_id,
                  ig_name = excluded.ig_name,
                  ig_search_terms = excluded.ig_search_terms,
                  default_timeframe = excluded.default_timeframe,
                  spread_bps = excluded.spread_bps,
                  slippage_bps = excluded.slippage_bps,
                  min_backtest_bars = excluded.min_backtest_bars
                """,
                (
                    market.market_id,
                    market.name,
                    market.asset_class,
                    market.fmp_symbol,
                    market.ig_epic,
                    int(market.enabled),
                    market.plugin_id,
                    market.ig_name,
                    market.ig_search_terms,
                    market.default_timeframe,
                    market.spread_bps,
                    market.slippage_bps,
                    market.min_backtest_bars,
                ),
            )

    def list(self, enabled_only: bool = False) -> list[MarketMapping]:
        query = """
            SELECT
              market_id, name, asset_class, fmp_symbol, ig_epic, enabled,
              plugin_id, ig_name, ig_search_terms, default_timeframe,
              spread_bps, slippage_bps, min_backtest_bars
            FROM markets
        """
        params: tuple[object, ...] = ()
        if enabled_only:
            query += " WHERE enabled = ?"
            params = (1,)
        query += " ORDER BY asset_class, market_id"
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            MarketMapping(
                row[0], row[1], row[2], row[3], row[4], bool(row[5]),
                row[6], row[7], row[8], row[9], row[10], row[11], row[12],
            )
            for row in rows
        ]

    def get(self, market_id: str) -> MarketMapping | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                  market_id, name, asset_class, fmp_symbol, ig_epic, enabled,
                  plugin_id, ig_name, ig_search_terms, default_timeframe,
                  spread_bps, slippage_bps, min_backtest_bars
                FROM markets WHERE market_id = ?
                """,
                (market_id,),
            ).fetchone()
        return (
            MarketMapping(
                row[0], row[1], row[2], row[3], row[4], bool(row[5]),
                row[6], row[7], row[8], row[9], row[10], row[11], row[12],
            )
            if row
            else None
        )
