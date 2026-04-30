from __future__ import annotations

from app.ig_spread_bet_engines import list_spread_bet_engines, spread_bet_engine_for_instrument_type


def test_spread_bet_engines_cover_known_ig_instrument_types():
    engines = list_spread_bet_engines()
    instrument_types = {item for engine in engines for item in engine["instrument_types"]}

    assert "INDICES" in instrument_types
    assert "CURRENCIES" in instrument_types
    assert "COMMODITIES" in instrument_types
    assert "SHARES" in instrument_types
    assert "RATES" in instrument_types
    assert "OPT_INDICES" in instrument_types
    assert "KNOCKOUTS_INDICES" in instrument_types
    assert "BUNGEE_INDICES" in instrument_types
    assert "BINARY" in instrument_types
    assert spread_bet_engine_for_instrument_type("CURRENCIES").engine_id == "forex"
