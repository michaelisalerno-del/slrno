from __future__ import annotations

from app.market_plugins import get_market_plugin, list_market_plugins


def test_builtin_plugins_cover_initial_ig_markets():
    plugins = {plugin.market_id: plugin for plugin in list_market_plugins()}

    assert plugins["NAS100"].ig_name == "US Tech 100"
    assert plugins["NAS100"].as_dict()["estimated_spread_bps"] > 0
    assert plugins["US500"].ig_name == "US 500"
    assert plugins["XAUUSD"].ig_name == "Spot Gold"


def test_plugin_maps_to_backtest_ready_market_mapping():
    plugin = get_market_plugin("ig-spot-gold")

    mapping = plugin.to_mapping()

    assert mapping.market_id == "XAUUSD"
    assert mapping.plugin_id == "ig-spot-gold"
    assert mapping.ig_epic == "CS.D.USCGC.TODAY.IP"
    assert mapping.spread_bps > 0
    assert mapping.min_backtest_bars >= 500
