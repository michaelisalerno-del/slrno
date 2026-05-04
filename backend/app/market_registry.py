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
    eodhd_symbol: str
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
    MarketMapping("US500", "S&P 500", "index", "GSPC.INDX", "", True, "ig-us-500", "US 500", "US 500,S&P 500,SPX", "5min", 2.0, 1.0),
    MarketMapping("NAS100", "Nasdaq 100", "index", "NDX.INDX", "", True, "ig-us-tech-100", "US Tech 100", "US Tech 100,Nasdaq,NASDAQ 100", "5min", 2.0, 1.0),
    MarketMapping("US30", "Wall Street", "index", "DJI.INDX", "", True, "ig-wall-street", "Wall Street", "Wall Street,US 30,Dow", "5min", 2.0, 1.0),
    MarketMapping("RUSSELL2000", "US Russell 2000", "index", "RUT.INDX", "", True, "ig-russell-2000", "US Russell 2000", "Russell 2000,US Small Cap", "5min", 2.5, 1.2),
    MarketMapping("FTSE100", "FTSE 100", "index", "FTSE.INDX", "", True, "ig-ftse-100", "FTSE 100", "FTSE 100,UK 100", "5min", 2.0, 1.0),
    MarketMapping("DE40", "Germany 40", "index", "GDAXI.INDX", "", True, "ig-germany-40", "Germany 40", "Germany 40,DAX", "5min", 2.0, 1.0),
    MarketMapping("FR40", "France 40", "index", "FCHI.INDX", "", True, "ig-france-40", "France 40", "France 40,CAC", "5min", 2.2, 1.1),
    MarketMapping("EU50", "EU Stocks 50", "index", "STOXX50E.INDX", "", True, "ig-eu-stocks-50", "EU Stocks 50", "EU Stocks 50,Euro Stoxx", "5min", 2.2, 1.1),
    MarketMapping("JP225", "Japan 225", "index", "N225.INDX", "", True, "ig-japan-225", "Japan 225", "Japan 225,Nikkei", "5min", 2.5, 1.2),
    MarketMapping("KOSPI200", "KOSPI 200", "index", "KOSPI200.INDX", "", True, "ig-kospi-200", "Korea 200", "Korea 200,KOSPI 200,KOSPI,Korea", "1day", 3.0, 1.5),
    MarketMapping("HK50", "Hong Kong HS50", "index", "HSI.INDX", "", True, "ig-hong-kong-hs50", "Hong Kong HS50", "Hong Kong HS50,Hang Seng", "5min", 3.0, 1.5),
    MarketMapping("AUS200", "Australia 200", "index", "AXJO.INDX", "", True, "ig-australia-200", "Australia 200", "Australia 200,ASX 200", "5min", 2.5, 1.2),
    MarketMapping("SA40", "South Africa 40", "index", "JTOPI.INDX", "IX.D.SAF.DAILY.IP", False, "ig-south-africa-40", "South Africa 40", "South Africa 40,SA40,South Africa,JSE Top 40", "1day", 8.0, 4.0),
    MarketMapping("VIX", "Volatility Index", "index", "VIX.INDX", "", True, "ig-volatility-index", "Volatility Index", "Volatility Index,VIX", "5min", 5.0, 2.5),
    MarketMapping("EURUSD", "EUR/USD", "forex", "EURUSD.FOREX", "", True, "ig-eur-usd", "EUR/USD", "EUR/USD,EURUSD,Euro Dollar", "5min", 1.2, 0.8),
    MarketMapping("GBPUSD", "GBP/USD", "forex", "GBPUSD.FOREX", "", True, "ig-gbp-usd", "GBP/USD", "GBP/USD,GBPUSD,Cable", "5min", 1.4, 0.9),
    MarketMapping("EURGBP", "EUR/GBP", "forex", "EURGBP.FOREX", "", True, "ig-eur-gbp", "EUR/GBP", "EUR/GBP,EURGBP", "5min", 1.5, 0.9),
    MarketMapping("USDJPY", "USD/JPY", "forex", "USDJPY.FOREX", "", True, "ig-usd-jpy", "USD/JPY", "USD/JPY,USDJPY,Dollar Yen", "5min", 1.3, 0.9),
    MarketMapping("AUDUSD", "AUD/USD", "forex", "AUDUSD.FOREX", "", True, "ig-aud-usd", "AUD/USD", "AUD/USD,AUDUSD", "5min", 1.5, 0.9),
    MarketMapping("USDCAD", "USD/CAD", "forex", "USDCAD.FOREX", "", True, "ig-usd-cad", "USD/CAD", "USD/CAD,USDCAD", "5min", 1.7, 1.0),
    MarketMapping("USDCHF", "USD/CHF", "forex", "USDCHF.FOREX", "", True, "ig-usd-chf", "USD/CHF", "USD/CHF,USDCHF", "5min", 1.7, 1.0),
    MarketMapping("NZDUSD", "NZD/USD", "forex", "NZDUSD.FOREX", "", True, "ig-nzd-usd", "NZD/USD", "NZD/USD,NZDUSD", "5min", 1.8, 1.0),
    MarketMapping("EURJPY", "EUR/JPY", "forex", "EURJPY.FOREX", "", True, "ig-eur-jpy", "EUR/JPY", "EUR/JPY,EURJPY", "5min", 1.8, 1.0),
    MarketMapping("GBPJPY", "GBP/JPY", "forex", "GBPJPY.FOREX", "", True, "ig-gbp-jpy", "GBP/JPY", "GBP/JPY,GBPJPY", "5min", 2.2, 1.1),
    MarketMapping("XAUUSD", "Spot Gold", "commodity", "XAUUSD.FOREX", "CS.D.USCGC.TODAY.IP", True, "ig-spot-gold", "Spot Gold", "Spot Gold,Gold,XAU/USD", "5min", 3.0, 1.5),
    MarketMapping("XAGUSD", "Spot Silver", "commodity", "XAGUSD.FOREX", "", True, "ig-spot-silver", "Spot Silver", "Spot Silver,Silver,XAG/USD", "5min", 4.0, 2.0),
    MarketMapping("BRENT", "Brent Crude", "commodity", "COMMODITY:BRENT", "", True, "ig-brent-crude", "Brent Crude", "Brent Crude,Brent Oil", "1day", 3.5, 2.0),
    MarketMapping("WTI", "US Crude", "commodity", "COMMODITY:WTI", "", True, "ig-wti-crude", "US Crude", "US Crude,WTI Oil", "1day", 3.5, 2.0),
    MarketMapping("NATGAS", "Natural Gas", "commodity", "COMMODITY:NATURAL_GAS", "", True, "ig-natural-gas", "Natural Gas", "Natural Gas,US Natural Gas", "1day", 5.0, 2.5),
    MarketMapping("COPPER", "Copper", "commodity", "COMMODITY:COPPER", "", True, "ig-copper", "Copper", "Copper", "1day", 4.0, 2.0),
    MarketMapping("US10Y", "US 10Y Treasury Yield", "rates", "US10Y.GBOND", "", True, "ig-us-10y", "US 10 Year T-Note", "US 10 Year,T-Note,Treasury", "1day", 2.0, 1.0),
    MarketMapping("UK10Y", "UK 10Y Gilt Yield", "rates", "UK10Y.GBOND", "", True, "ig-uk-10y", "UK Long Gilt", "UK Long Gilt,Gilt", "1day", 2.0, 1.0),
    MarketMapping("DE10Y", "Germany 10Y Bund Yield", "rates", "DE10Y.GBOND", "", True, "ig-de-10y", "Bund", "Bund,Germany 10 Year", "1day", 2.0, 1.0),
    MarketMapping("AAPL", "Apple", "share", "AAPL.US", "", True, "ig-aapl", "Apple", "Apple,AAPL", "1day", 10.0, 5.0, 250),
    MarketMapping("MSFT", "Microsoft", "share", "MSFT.US", "", True, "ig-msft", "Microsoft", "Microsoft,MSFT", "1day", 10.0, 5.0, 250),
    MarketMapping("NVDA", "NVIDIA", "share", "NVDA.US", "", True, "ig-nvda", "NVIDIA", "NVIDIA,NVDA", "1day", 10.0, 5.0, 250),
    MarketMapping("TSLA", "Tesla", "share", "TSLA.US", "", True, "ig-tsla", "Tesla", "TSLA,Tesla", "1day", 10.0, 5.0, 250),
    MarketMapping("SHEL", "Shell", "share", "SHEL.LSE", "", True, "ig-shell", "Shell", "SHEL,Shell", "1day", 10.0, 5.0, 250),
    MarketMapping("BP", "BP", "share", "BP.LSE", "", True, "ig-bp", "BP", "BP", "1day", 10.0, 5.0, 250),
    MarketMapping("HSBA", "HSBC", "share", "HSBA.LSE", "", True, "ig-hsbc", "HSBC", "HSBC,HSBA", "1day", 10.0, 5.0, 250),
    MarketMapping("LLOY", "Lloyds Banking Group", "share", "LLOY.LSE", "", True, "ig-lloy", "Lloyds Banking Group", "LLOY,Lloyds,Lloyds Banking Group", "1day", 10.0, 5.0, 250),
    MarketMapping("BARC", "Barclays", "share", "BARC.LSE", "", True, "ig-barc", "Barclays", "BARC,Barclays", "1day", 10.0, 5.0, 250),
    MarketMapping("RR", "Rolls-Royce Holdings", "share", "RR.LSE", "", True, "ig-rr", "Rolls-Royce Holdings", "RR,Rolls-Royce,Rolls Royce", "1day", 10.0, 5.0, 250),
    MarketMapping("VOD", "Vodafone", "share", "VOD.LSE", "", True, "ig-vod", "Vodafone", "VOD,Vodafone", "1day", 10.0, 5.0, 250),
    MarketMapping("GLEN", "Glencore", "share", "GLEN.LSE", "", True, "ig-glen", "Glencore", "GLEN,Glencore", "1day", 10.0, 5.0, 250),
    MarketMapping("DGE", "Diageo", "share", "DGE.LSE", "", True, "ig-dge", "Diageo", "DGE,Diageo", "1day", 10.0, 5.0, 250),
    MarketMapping("AZN", "AstraZeneca", "share", "AZN.LSE", "", True, "ig-azn", "AstraZeneca", "AZN,AstraZeneca", "1day", 10.0, 5.0, 250),
    MarketMapping("GSK", "GSK", "share", "GSK.LSE", "", True, "ig-gsk", "GSK", "GSK,GlaxoSmithKline", "1day", 10.0, 5.0, 250),
    MarketMapping("BAE", "BAE Systems", "share", "BA.LSE", "", True, "ig-bae", "BAE Systems", "BAE Systems,BA.", "1day", 10.0, 5.0, 250),
    MarketMapping("TSCO", "Tesco", "share", "TSCO.LSE", "", True, "ig-tsco", "Tesco", "TSCO,Tesco", "1day", 10.0, 5.0, 250),
    MarketMapping("BTCUSD", "Bitcoin/USD", "crypto", "BTC-USD.CC", "", False, "ig-bitcoin", "Bitcoin", "Bitcoin,BTC/USD", "5min", 8.0, 4.0),
    MarketMapping("ETHUSD", "Ethereum/USD", "crypto", "ETH-USD.CC", "", False, "ig-ethereum", "Ethereum", "Ethereum,ETH/USD", "5min", 10.0, 5.0),
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
                  eodhd_symbol TEXT NOT NULL DEFAULT '',
                  fmp_symbol TEXT NOT NULL DEFAULT '',
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
            self._add_column(conn, "eodhd_symbol", "TEXT NOT NULL DEFAULT ''")
            self._add_column(conn, "fmp_symbol", "TEXT NOT NULL DEFAULT ''")
            self._add_column(conn, "plugin_id", "TEXT NOT NULL DEFAULT ''")
            self._add_column(conn, "ig_name", "TEXT NOT NULL DEFAULT ''")
            self._add_column(conn, "ig_search_terms", "TEXT NOT NULL DEFAULT ''")
            self._add_column(conn, "default_timeframe", "TEXT NOT NULL DEFAULT '5min'")
            self._add_column(conn, "spread_bps", "REAL NOT NULL DEFAULT 2.0")
            self._add_column(conn, "slippage_bps", "REAL NOT NULL DEFAULT 1.0")
            self._add_column(conn, "min_backtest_bars", "INTEGER NOT NULL DEFAULT 750")
            columns = {row[1] for row in conn.execute("PRAGMA table_info(markets)").fetchall()}
            if "fmp_symbol" in columns:
                conn.execute(
                    """
                    UPDATE markets
                    SET eodhd_symbol = fmp_symbol
                    WHERE eodhd_symbol = ''
                      AND fmp_symbol != ''
                    """
                )

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
                      market_id, name, asset_class, eodhd_symbol, fmp_symbol, ig_epic, enabled,
                      plugin_id, ig_name, ig_search_terms, default_timeframe,
                      spread_bps, slippage_bps, min_backtest_bars
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        market.market_id,
                        market.name,
                        market.asset_class,
                        market.eodhd_symbol,
                        market.eodhd_symbol,
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
                    SET name = ?,
                        asset_class = ?,
                        eodhd_symbol = ?,
                        fmp_symbol = ?,
                        plugin_id = ?,
                        ig_name = ?,
                        ig_search_terms = ?
                    WHERE market_id = ?
                    """,
                    (
                        market.name,
                        market.asset_class,
                        market.eodhd_symbol,
                        market.eodhd_symbol,
                        market.plugin_id,
                        market.ig_name,
                        market.ig_search_terms,
                        market.market_id,
                    ),
                )
            conn.execute(
                """
                UPDATE markets
                SET enabled = 0,
                    plugin_id = 'retired-etf-proxy',
                    ig_search_terms = 'retired ETF proxy'
                WHERE market_id IN ('QQQ', 'SPY')
                """
            )
            for market in DEFAULT_MARKETS:
                if market.asset_class != "share":
                    continue
                conn.execute(
                    """
                    UPDATE markets
                    SET default_timeframe = CASE WHEN default_timeframe IN ('', '5min', '5m') THEN ? ELSE default_timeframe END,
                        spread_bps = CASE WHEN spread_bps <= 5.0 THEN ? ELSE spread_bps END,
                        slippage_bps = CASE WHEN slippage_bps <= 2.5 THEN ? ELSE slippage_bps END,
                        min_backtest_bars = CASE WHEN min_backtest_bars >= 750 THEN ? ELSE min_backtest_bars END
                    WHERE market_id = ?
                      AND asset_class = 'share'
                    """,
                    (market.default_timeframe, market.spread_bps, market.slippage_bps, market.min_backtest_bars, market.market_id),
                )
            conn.execute(
                """
                UPDATE markets
                SET default_timeframe = '5min',
                    min_backtest_bars = CASE WHEN min_backtest_bars < 750 THEN 750 ELSE min_backtest_bars END
                WHERE asset_class = 'share'
                  AND plugin_id LIKE 'discovered-%'
                  AND default_timeframe IN ('', '1day', '1d', 'day', 'daily')
                """
            )

    def upsert(self, market: MarketMapping) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO markets(
                  market_id, name, asset_class, eodhd_symbol, fmp_symbol, ig_epic, enabled,
                  plugin_id, ig_name, ig_search_terms, default_timeframe,
                  spread_bps, slippage_bps, min_backtest_bars
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market_id) DO UPDATE SET
                  name = excluded.name,
                  asset_class = excluded.asset_class,
                  eodhd_symbol = excluded.eodhd_symbol,
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
                    market.eodhd_symbol,
                    market.eodhd_symbol,
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
              market_id, name, asset_class, eodhd_symbol, ig_epic, enabled,
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
                  market_id, name, asset_class, eodhd_symbol, ig_epic, enabled,
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
