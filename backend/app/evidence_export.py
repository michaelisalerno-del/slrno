from __future__ import annotations

import csv
import hashlib
import io
import json
from datetime import UTC, date, datetime, time
from zipfile import ZIP_DEFLATED, ZipFile

from .capital import capital_scenarios, capital_summary, scenario_account_sizes
from .market_data_cache import MarketDataCache
from .providers.base import OHLCBar
from .providers.eodhd import EODHDProvider, _bar_from_row
from .research_store import ResearchStore

SENSITIVE_KEYS = {
    "account_id",
    "accountid",
    "api_key",
    "apikey",
    "api_token",
    "apitoken",
    "password",
    "secret",
    "token",
    "username",
}


def build_research_export_zip(
    store: ResearchStore,
    run_id: int,
    include_bars: bool = True,
    cache: MarketDataCache | None = None,
) -> bytes:
    run = store.get_run(run_id)
    if run is None:
        raise ValueError("Research run not found")

    trials = store.list_trials(run_id)
    candidates = store.list_candidates(run_id)
    cost_profiles = _cost_profiles_for_run(store, run, trials, candidates)
    bar_snapshots = store.list_bar_snapshots(run_id, include_payload=include_bars)
    cached_bar_exports = _cached_bar_exports(run, cache) if include_bars and not bar_snapshots else []
    exported_trials = [_trial_export(trial, cost_profiles) for trial in trials]
    exported_candidates = [_candidate_export(candidate, cost_profiles) for candidate in candidates]
    warning_rows = _warning_rows(exported_trials, exported_candidates)
    capital_rows = _capital_rows(exported_trials, exported_candidates)
    bar_analysis = _bar_analysis_payload(run, exported_trials, exported_candidates)
    manifest = {
        "app": "slrno",
        "schema": "research_evidence_bundle_v1",
        "exported_at": datetime.now(UTC).isoformat(),
        "run_id": run_id,
        "include_bars": include_bars,
        "bar_snapshots": [_bar_snapshot_metadata(snapshot) for snapshot in bar_snapshots],
        "best_available_bars": [_cached_bar_metadata(export) for export in cached_bar_exports],
        "data_completeness": {
            "exact_run_bars_available": bool(bar_snapshots),
            "bars_exact": bool(bar_snapshots),
            "cached_bars_exported": bool(cached_bar_exports),
            "bars_note": _bars_note(bool(bar_snapshots), bool(cached_bar_exports)),
            "trial_count": len(trials),
            "candidate_count": len(candidates),
        },
    }

    buffer = io.BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", _json_bytes(manifest))
        archive.writestr("run.json", _json_bytes(_redact_sensitive(run)))
        archive.writestr("trials.json", _json_bytes(_redact_sensitive(exported_trials)))
        archive.writestr("trials.csv", _csv_bytes(_trial_csv_rows(exported_trials)))
        archive.writestr("candidates.json", _json_bytes(_redact_sensitive(exported_candidates)))
        archive.writestr("candidates.csv", _csv_bytes(_candidate_csv_rows(exported_candidates)))
        archive.writestr("capital_scenarios.csv", _csv_bytes(capital_rows))
        archive.writestr("cost_profiles.json", _json_bytes(_redact_sensitive(cost_profiles)))
        archive.writestr("warnings.csv", _csv_bytes(warning_rows))
        archive.writestr("bar_analysis.json", _json_bytes(_redact_sensitive(bar_analysis)))
        archive.writestr("regime_segments.csv", _csv_bytes(_regime_segment_rows(run)))
        archive.writestr("regime_pnl.csv", _csv_bytes(_pattern_summary_rows(bar_analysis, "regime_summary")))
        archive.writestr("regime_gated_backtests.csv", _csv_bytes(_regime_gated_backtest_rows(bar_analysis)))
        archive.writestr("monthly_pnl.csv", _csv_bytes(_pattern_summary_rows(bar_analysis, "monthly_summary")))
        archive.writestr("session_pnl.csv", _csv_bytes(_pattern_summary_rows(bar_analysis, "session_summary")))
        archive.writestr("pattern_warnings.csv", _csv_bytes(_pattern_warning_rows(bar_analysis)))
        archive.writestr("README.md", _readme(run_id, include_bars, bool(bar_snapshots), bool(cached_bar_exports)))
        if include_bars:
            if bar_snapshots:
                for snapshot in bar_snapshots:
                    filename = f"bars/{snapshot['market_id']}_{snapshot['interval']}.csv"
                    archive.writestr(filename, _csv_bytes(snapshot.get("bars") if isinstance(snapshot.get("bars"), list) else []))
            elif cached_bar_exports:
                for export in cached_bar_exports:
                    filename = f"bars/{export['market_id']}_{export['interval']}_cached_not_exact.csv"
                    archive.writestr(filename, _csv_bytes(export.get("bars") if isinstance(export.get("bars"), list) else []))
                archive.writestr(
                    "bars/README.md",
                    "These bars came from the market-data cache, not an exact per-run snapshot, and are not guaranteed to match the original run exactly. Treat them as best-available evidence only.\n",
                )
            else:
                archive.writestr(
                    "bars/README.md",
                    "No exact per-run bars were saved for this historical run. Re-run the backtest to create a reproducible bar snapshot.\n",
                )
    return buffer.getvalue()


def _cached_bar_exports(run: dict[str, object], cache: MarketDataCache | None = None) -> list[dict[str, object]]:
    config = run.get("config") if isinstance(run.get("config"), dict) else {}
    start = str(config.get("start") or "")
    end = str(config.get("end") or "")
    if not start or not end:
        return []
    market_statuses = config.get("market_statuses") if isinstance(config.get("market_statuses"), list) else []
    if not market_statuses:
        return []
    cache = cache or MarketDataCache()
    output: list[dict[str, object]] = []
    used_cache_keys: set[str] = set()
    for status in market_statuses:
        if not isinstance(status, dict):
            continue
        market_id = str(status.get("market_id") or "")
        symbol = str(status.get("eodhd_symbol") or "")
        interval = str(status.get("interval") or config.get("interval") or "")
        expected_count = _positive_int(status.get("bar_count"))
        if not market_id or not symbol or not interval:
            continue
        match = _cached_payload_for_request(cache, symbol, interval, start, end)
        if match is None:
            match = _legacy_cached_payload_match(cache, symbol, expected_count, start, end, used_cache_keys)
        if match is None:
            continue
        cache_key = str(match.get("cache_key") or match.get("created_at") or f"{market_id}:{interval}")
        used_cache_keys.add(cache_key)
        payload = match.get("payload")
        bars = _payload_bars(symbol, payload)
        if expected_count and len(bars) != expected_count:
            continue
        rows = [_bar_row(bar) for bar in bars]
        output.append(
            {
                "market_id": market_id,
                "interval": interval,
                "source": match.get("source") or "market_data_cache",
                "start": start,
                "end": end,
                "bar_count": len(rows),
                "sha256": _bars_sha256(rows),
                "exact": False,
                "not_guaranteed_exact": True,
                "created_at": match.get("created_at"),
                "eodhd_symbol": symbol,
                "cache_match": match.get("cache_match"),
                "bars": rows,
            }
        )
    return output


def _cached_payload_for_request(
    cache: MarketDataCache,
    symbol: str,
    interval: str,
    start: str,
    end: str,
) -> dict[str, object] | None:
    request = _eodhd_cache_request(symbol, interval, start, end)
    if request is None:
        return None
    namespace, base_url, params = request
    payload = cache.get_json(namespace, base_url, params, allow_stale=True)
    if payload is None:
        return None
    return {"payload": payload, "source": "market_data_cache_request_match", "cache_match": "request_params"}


def _legacy_cached_payload_match(
    cache: MarketDataCache,
    symbol: str,
    expected_count: int,
    start: str,
    end: str,
    used_cache_keys: set[str],
) -> dict[str, object] | None:
    if not expected_count:
        return None
    matches: list[dict[str, object]] = []
    for entry in cache.payload_entries("historical_bars", limit=100):
        cache_key = str(entry.get("created_at") or entry.get("request_url") or "")
        if cache_key in used_cache_keys:
            continue
        metadata = entry.get("metadata") if isinstance(entry.get("metadata"), dict) else {}
        entry_symbol = str(metadata.get("symbol") or "")
        if entry_symbol and entry_symbol != symbol:
            continue
        bars = _payload_bars(symbol, entry.get("payload"))
        if len(bars) != expected_count:
            continue
        if not _bars_cover_range(bars, start, end):
            continue
        matches.append(
            {
                "payload": entry.get("payload"),
                "source": "market_data_cache_legacy_bar_count_match",
                "cache_match": "legacy_bar_count_and_date_range",
                "created_at": entry.get("created_at"),
                "cache_key": cache_key,
            }
        )
    return matches[0] if len(matches) == 1 else None


def _eodhd_cache_request(symbol: str, interval: str, start: str, end: str) -> tuple[str, str, dict[str, object]] | None:
    start_date = _parse_date(start)
    end_date = _parse_date(end)
    if start_date is None or end_date is None:
        return None
    if interval in {"1day", "1d", "day", "daily"}:
        return (
            "daily_bars",
            f"{EODHDProvider.BASE_URL}/eod/{symbol}",
            {"from": start_date.isoformat(), "to": end_date.isoformat(), "period": "d"},
        )
    provider_interval = {
        "1min": "1m",
        "1m": "1m",
        "5min": "5m",
        "5m": "5m",
        "15min": "5m",
        "15m": "5m",
        "30min": "5m",
        "30m": "5m",
        "1hour": "1h",
        "1h": "1h",
    }.get(interval, "5m")
    return (
        "historical_bars",
        f"{EODHDProvider.BASE_URL}/intraday/{symbol}",
        {
            "interval": provider_interval,
            "from": int(datetime.combine(start_date, time.min, tzinfo=UTC).timestamp()),
            "to": int(datetime.combine(end_date, time.max, tzinfo=UTC).timestamp()),
        },
    )


def _payload_bars(symbol: str, payload: object) -> list[OHLCBar]:
    if not isinstance(payload, list):
        return []
    bars = [bar for row in payload if isinstance(row, dict) and (bar := _bar_from_row(symbol, row)) is not None]
    return sorted({bar.timestamp: bar for bar in bars}.values(), key=lambda bar: bar.timestamp)


def _bars_cover_range(bars: list[OHLCBar], start: str, end: str) -> bool:
    start_date = _parse_date(start)
    end_date = _parse_date(end)
    if not bars or start_date is None or end_date is None:
        return False
    first_date = bars[0].timestamp.date()
    last_date = bars[-1].timestamp.date()
    return first_date >= start_date and last_date <= end_date


def _bar_row(bar: OHLCBar) -> dict[str, object]:
    return {
        "symbol": bar.symbol,
        "timestamp": bar.timestamp.isoformat(),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
    }


def _bars_sha256(rows: list[dict[str, object]]) -> str:
    raw = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _trial_export(trial: dict[str, object], cost_profiles: dict[str, dict[str, object]]) -> dict[str, object]:
    parameters = trial.get("parameters") if isinstance(trial.get("parameters"), dict) else {}
    backtest = trial.get("backtest") if isinstance(trial.get("backtest"), dict) else {}
    market_id = str(parameters.get("market_id") or "")
    scenarios = capital_scenarios(backtest, parameters, cost_profiles.get(market_id), account_sizes=_scenario_sizes_for_parameters(parameters))
    return {**trial, "capital_scenarios": scenarios, "capital_summary": capital_summary(scenarios)}


def _candidate_export(candidate: dict[str, object], cost_profiles: dict[str, dict[str, object]]) -> dict[str, object]:
    audit = candidate.get("audit") if isinstance(candidate.get("audit"), dict) else {}
    candidate_payload = audit.get("candidate") if isinstance(audit.get("candidate"), dict) else {}
    parameters = candidate_payload.get("parameters") if isinstance(candidate_payload.get("parameters"), dict) else {}
    backtest = audit.get("backtest") if isinstance(audit.get("backtest"), dict) else {}
    market_id = str(candidate.get("market_id") or parameters.get("market_id") or "")
    scenarios = capital_scenarios(backtest, parameters, cost_profiles.get(market_id), account_sizes=_scenario_sizes_for_parameters(parameters))
    return {**candidate, "capital_scenarios": scenarios, "capital_summary": capital_summary(scenarios)}


def _scenario_sizes_for_parameters(parameters: dict[str, object]) -> tuple[float, ...]:
    search_audit = parameters.get("search_audit") if isinstance(parameters.get("search_audit"), dict) else {}
    return scenario_account_sizes(parameters.get("testing_account_size") or search_audit.get("testing_account_size"))


def _cost_profiles_for_run(
    store: ResearchStore,
    run: dict[str, object],
    trials: list[dict[str, object]],
    candidates: list[dict[str, object]],
) -> dict[str, dict[str, object]]:
    market_ids: set[str] = set()
    config = run.get("config") if isinstance(run.get("config"), dict) else {}
    for market_id in config.get("market_ids") or []:
        market_ids.add(str(market_id))
    if run.get("market_id") and str(run.get("market_id")) != "MULTI":
        market_ids.add(str(run.get("market_id")))
    for trial in trials:
        parameters = trial.get("parameters") if isinstance(trial.get("parameters"), dict) else {}
        if parameters.get("market_id"):
            market_ids.add(str(parameters["market_id"]))
    for candidate in candidates:
        if candidate.get("market_id"):
            market_ids.add(str(candidate["market_id"]))
    profiles: dict[str, dict[str, object]] = {}
    for market_id in sorted(market_ids):
        profile = store.get_cost_profile(market_id)
        if profile is not None:
            profiles[market_id] = profile
    return profiles


def _trial_csv_rows(trials: list[dict[str, object]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for trial in trials:
        backtest = trial.get("backtest") if isinstance(trial.get("backtest"), dict) else {}
        parameters = trial.get("parameters") if isinstance(trial.get("parameters"), dict) else {}
        evidence = parameters.get("evidence_profile") if isinstance(parameters.get("evidence_profile"), dict) else {}
        pattern = parameters.get("bar_pattern_analysis") if isinstance(parameters.get("bar_pattern_analysis"), dict) else {}
        gated = pattern.get("regime_gated_backtest") if isinstance(pattern.get("regime_gated_backtest"), dict) else {}
        regime_evidence = pattern.get("regime_trade_evidence") if isinstance(pattern.get("regime_trade_evidence"), dict) else {}
        in_regime = regime_evidence.get("in_regime") if isinstance(regime_evidence.get("in_regime"), dict) else {}
        summary = trial.get("capital_summary") if isinstance(trial.get("capital_summary"), dict) else {}
        rows.append(
            {
                "id": trial.get("id"),
                "run_id": trial.get("run_id"),
                "strategy_name": trial.get("strategy_name"),
                "market_id": parameters.get("market_id", ""),
                "promotion_tier": trial.get("promotion_tier"),
                "robustness_score": trial.get("robustness_score"),
                "net_profit": backtest.get("net_profit"),
                "test_profit": backtest.get("test_profit"),
                "daily_pnl_sharpe": backtest.get("daily_pnl_sharpe"),
                "sharpe_observations": backtest.get("sharpe_observations"),
                "compounded_projection_final_equity": backtest.get("compounded_projection_final_equity"),
                "compounded_projection_return_pct": backtest.get("compounded_projection_return_pct"),
                "max_drawdown": backtest.get("max_drawdown"),
                "trade_count": backtest.get("trade_count"),
                "oos_net_profit": evidence.get("oos_net_profit"),
                "oos_trade_count": evidence.get("oos_trade_count"),
                "active_fold_count": evidence.get("active_fold_count"),
                "inactive_fold_count": evidence.get("inactive_fold_count"),
                "positive_fold_rate": evidence.get("positive_fold_rate"),
                "active_positive_fold_rate": evidence.get("active_positive_fold_rate"),
                "single_fold_profit_share": evidence.get("single_fold_profit_share"),
                "worst_fold_net_profit": evidence.get("worst_fold_net_profit"),
                "worst_active_fold_net_profit": evidence.get("worst_active_fold_net_profit"),
                "total_cost": backtest.get("total_cost"),
                "net_cost_ratio": backtest.get("net_cost_ratio"),
                "cost_to_gross_ratio": backtest.get("cost_to_gross_ratio"),
                "target_regime": pattern.get("target_regime"),
                "regime_verdict": pattern.get("regime_verdict"),
                "allowed_regimes": "|".join(str(item) for item in pattern.get("allowed_regimes", [])),
                "blocked_regimes": "|".join(str(item) for item in pattern.get("blocked_regimes", [])),
                "regime_gated_net_profit": gated.get("net_profit"),
                "regime_gated_test_profit": gated.get("test_profit"),
                "regime_gated_daily_pnl_sharpe": gated.get("daily_pnl_sharpe"),
                "in_regime_net_profit": in_regime.get("net_profit"),
                "in_regime_test_profit": in_regime.get("test_profit"),
                "in_regime_test_trade_count": in_regime.get("test_trade_count"),
                "in_regime_daily_pnl_sharpe": in_regime.get("daily_pnl_sharpe"),
                "in_regime_sharpe_days": in_regime.get("sharpe_days"),
                "target_regime_trading_days": regime_evidence.get("regime_trading_days"),
                "target_regime_history_share": regime_evidence.get("regime_history_share"),
                "target_regime_episodes": regime_evidence.get("regime_episodes"),
                "outside_regime_trade_count": regime_evidence.get("outside_trade_count"),
                "smallest_feasible_account": summary.get("smallest_feasible_account"),
                "warnings": "|".join(str(item) for item in trial.get("warnings", [])),
            }
        )
    return rows


def _candidate_csv_rows(candidates: list[dict[str, object]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for candidate in candidates:
        audit = candidate.get("audit") if isinstance(candidate.get("audit"), dict) else {}
        backtest = audit.get("backtest") if isinstance(audit.get("backtest"), dict) else {}
        candidate_payload = audit.get("candidate") if isinstance(audit.get("candidate"), dict) else {}
        parameters = candidate_payload.get("parameters") if isinstance(candidate_payload.get("parameters"), dict) else {}
        evidence = parameters.get("evidence_profile") if isinstance(parameters.get("evidence_profile"), dict) else {}
        pattern = parameters.get("bar_pattern_analysis") if isinstance(parameters.get("bar_pattern_analysis"), dict) else {}
        gated = pattern.get("regime_gated_backtest") if isinstance(pattern.get("regime_gated_backtest"), dict) else {}
        regime_evidence = pattern.get("regime_trade_evidence") if isinstance(pattern.get("regime_trade_evidence"), dict) else {}
        in_regime = regime_evidence.get("in_regime") if isinstance(regime_evidence.get("in_regime"), dict) else {}
        readiness = audit.get("promotion_readiness") if isinstance(audit.get("promotion_readiness"), dict) else {}
        summary = candidate.get("capital_summary") if isinstance(candidate.get("capital_summary"), dict) else {}
        rows.append(
            {
                "id": candidate.get("id"),
                "run_id": candidate.get("run_id"),
                "strategy_name": candidate.get("strategy_name"),
                "market_id": candidate.get("market_id"),
                "promotion_tier": candidate.get("promotion_tier"),
                "readiness_status": readiness.get("status"),
                "next_action": readiness.get("next_action"),
                "robustness_score": candidate.get("robustness_score"),
                "net_profit": backtest.get("net_profit"),
                "daily_pnl_sharpe": backtest.get("daily_pnl_sharpe"),
                "sharpe_observations": backtest.get("sharpe_observations"),
                "compounded_projection_final_equity": backtest.get("compounded_projection_final_equity"),
                "compounded_projection_return_pct": backtest.get("compounded_projection_return_pct"),
                "total_cost": backtest.get("total_cost"),
                "oos_net_profit": evidence.get("oos_net_profit"),
                "oos_trade_count": evidence.get("oos_trade_count"),
                "active_fold_count": evidence.get("active_fold_count"),
                "inactive_fold_count": evidence.get("inactive_fold_count"),
                "positive_fold_rate": evidence.get("positive_fold_rate"),
                "active_positive_fold_rate": evidence.get("active_positive_fold_rate"),
                "single_fold_profit_share": evidence.get("single_fold_profit_share"),
                "worst_fold_net_profit": evidence.get("worst_fold_net_profit"),
                "worst_active_fold_net_profit": evidence.get("worst_active_fold_net_profit"),
                "target_regime": pattern.get("target_regime"),
                "regime_verdict": pattern.get("regime_verdict"),
                "allowed_regimes": "|".join(str(item) for item in pattern.get("allowed_regimes", [])),
                "blocked_regimes": "|".join(str(item) for item in pattern.get("blocked_regimes", [])),
                "regime_gated_net_profit": gated.get("net_profit"),
                "regime_gated_test_profit": gated.get("test_profit"),
                "regime_gated_daily_pnl_sharpe": gated.get("daily_pnl_sharpe"),
                "in_regime_net_profit": in_regime.get("net_profit"),
                "in_regime_test_profit": in_regime.get("test_profit"),
                "in_regime_test_trade_count": in_regime.get("test_trade_count"),
                "in_regime_daily_pnl_sharpe": in_regime.get("daily_pnl_sharpe"),
                "in_regime_sharpe_days": in_regime.get("sharpe_days"),
                "target_regime_trading_days": regime_evidence.get("regime_trading_days"),
                "target_regime_history_share": regime_evidence.get("regime_history_share"),
                "target_regime_episodes": regime_evidence.get("regime_episodes"),
                "outside_regime_trade_count": regime_evidence.get("outside_trade_count"),
                "smallest_feasible_account": summary.get("smallest_feasible_account"),
                "blockers": "|".join(str(item) for item in readiness.get("blockers", [])),
                "validation_warnings": "|".join(str(item) for item in readiness.get("validation_warnings", [])),
            }
        )
    return rows


def _capital_rows(trials: list[dict[str, object]], candidates: list[dict[str, object]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for entity_type, items in (("trial", trials), ("candidate", candidates)):
        for item in items:
            for scenario in item.get("capital_scenarios", []):
                row = {
                    "entity_type": entity_type,
                    "id": item.get("id"),
                    "run_id": item.get("run_id"),
                    "strategy_name": item.get("strategy_name"),
                    "market_id": _entity_market_id(item),
                    **scenario,
                    "violations": "|".join(str(value) for value in scenario.get("violations", [])),
                }
                rows.append(row)
    return rows


def _warning_rows(trials: list[dict[str, object]], candidates: list[dict[str, object]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for trial in trials:
        for warning in trial.get("warnings", []):
            rows.append({"entity_type": "trial", "id": trial.get("id"), "strategy_name": trial.get("strategy_name"), "warning": warning})
    for candidate in candidates:
        audit = candidate.get("audit") if isinstance(candidate.get("audit"), dict) else {}
        for warning in audit.get("warnings", []):
            rows.append({"entity_type": "candidate", "id": candidate.get("id"), "strategy_name": candidate.get("strategy_name"), "warning": warning})
    return rows


def _bar_analysis_payload(
    run: dict[str, object],
    trials: list[dict[str, object]],
    candidates: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "schema": "research_bar_analysis_v1",
        "market_regimes": _market_regimes(run),
        "items": _pattern_items("trial", trials) + _pattern_items("candidate", candidates),
    }


def _market_regimes(run: dict[str, object]) -> list[dict[str, object]]:
    config = run.get("config") if isinstance(run.get("config"), dict) else {}
    statuses = config.get("market_statuses") if isinstance(config.get("market_statuses"), list) else []
    output: list[dict[str, object]] = []
    for status in statuses:
        if not isinstance(status, dict):
            continue
        regime = status.get("bar_regime") if isinstance(status.get("bar_regime"), dict) else None
        if regime is None:
            continue
        output.append(
            {
                "market_id": status.get("market_id"),
                "interval": status.get("interval"),
                "eodhd_symbol": status.get("eodhd_symbol"),
                **regime,
            }
        )
    return output


def _pattern_items(entity_type: str, items: list[dict[str, object]]) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for item in items:
        analysis = _item_pattern_analysis(item)
        if not analysis:
            continue
        output.append(
            {
                "entity_type": entity_type,
                "id": item.get("id"),
                "run_id": item.get("run_id"),
                "strategy_name": item.get("strategy_name"),
                "market_id": _entity_market_id(item),
                "promotion_tier": item.get("promotion_tier"),
                "analysis": analysis,
            }
        )
    return output


def _item_pattern_analysis(item: dict[str, object]) -> dict[str, object] | None:
    parameters = item.get("parameters") if isinstance(item.get("parameters"), dict) else None
    if parameters is None:
        audit = item.get("audit") if isinstance(item.get("audit"), dict) else {}
        candidate = audit.get("candidate") if isinstance(audit.get("candidate"), dict) else {}
        parameters = candidate.get("parameters") if isinstance(candidate.get("parameters"), dict) else None
    if not parameters:
        return None
    analysis = parameters.get("bar_pattern_analysis")
    return analysis if isinstance(analysis, dict) else None


def _regime_segment_rows(run: dict[str, object]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for market in _market_regimes(run):
        for segment in market.get("segments", []) if isinstance(market.get("segments"), list) else []:
            if not isinstance(segment, dict):
                continue
            rows.append(
                {
                    "market_id": market.get("market_id"),
                    "interval": market.get("interval"),
                    **segment,
                }
            )
    return rows


def _pattern_summary_rows(bar_analysis: dict[str, object], key: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in bar_analysis.get("items", []) if isinstance(bar_analysis.get("items"), list) else []:
        if not isinstance(item, dict):
            continue
        analysis = item.get("analysis") if isinstance(item.get("analysis"), dict) else {}
        for summary in analysis.get(key, []) if isinstance(analysis.get(key), list) else []:
            if not isinstance(summary, dict):
                continue
            rows.append(
                {
                    "entity_type": item.get("entity_type"),
                    "id": item.get("id"),
                    "run_id": item.get("run_id"),
                    "strategy_name": item.get("strategy_name"),
                    "market_id": item.get("market_id"),
                    "bucket_type": key.removesuffix("_summary"),
                    **summary,
                }
            )
    return rows


def _regime_gated_backtest_rows(bar_analysis: dict[str, object]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in bar_analysis.get("items", []) if isinstance(bar_analysis.get("items"), list) else []:
        if not isinstance(item, dict):
            continue
        analysis = item.get("analysis") if isinstance(item.get("analysis"), dict) else {}
        gated = analysis.get("regime_gated_backtest") if isinstance(analysis.get("regime_gated_backtest"), dict) else {}
        regime_evidence = analysis.get("regime_trade_evidence") if isinstance(analysis.get("regime_trade_evidence"), dict) else {}
        in_regime = regime_evidence.get("in_regime") if isinstance(regime_evidence.get("in_regime"), dict) else {}
        rows.append(
            {
                "entity_type": item.get("entity_type"),
                "id": item.get("id"),
                "run_id": item.get("run_id"),
                "strategy_name": item.get("strategy_name"),
                "market_id": item.get("market_id"),
                "target_regime": analysis.get("target_regime"),
                "regime_verdict": analysis.get("regime_verdict"),
                "allowed_regimes": "|".join(str(value) for value in analysis.get("allowed_regimes", [])),
                "blocked_regimes": "|".join(str(value) for value in analysis.get("blocked_regimes", [])),
                "in_regime_net_profit": in_regime.get("net_profit"),
                "in_regime_test_profit": in_regime.get("test_profit"),
                "in_regime_test_trade_count": in_regime.get("test_trade_count"),
                "in_regime_daily_pnl_sharpe": in_regime.get("daily_pnl_sharpe"),
                "in_regime_sharpe_days": in_regime.get("sharpe_days"),
                "target_regime_trading_days": regime_evidence.get("regime_trading_days"),
                "target_regime_history_share": regime_evidence.get("regime_history_share"),
                "target_regime_episodes": regime_evidence.get("regime_episodes"),
                "outside_regime_trade_count": regime_evidence.get("outside_trade_count"),
                **gated,
            }
        )
    return rows


def _pattern_warning_rows(bar_analysis: dict[str, object]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in bar_analysis.get("items", []) if isinstance(bar_analysis.get("items"), list) else []:
        if not isinstance(item, dict):
            continue
        analysis = item.get("analysis") if isinstance(item.get("analysis"), dict) else {}
        for warning in analysis.get("warnings", []) if isinstance(analysis.get("warnings"), list) else []:
            rows.append(
                {
                    "entity_type": item.get("entity_type"),
                    "id": item.get("id"),
                    "run_id": item.get("run_id"),
                    "strategy_name": item.get("strategy_name"),
                    "market_id": item.get("market_id"),
                    "warning": warning,
                }
            )
    return rows


def _entity_market_id(item: dict[str, object]) -> str:
    if item.get("market_id"):
        return str(item["market_id"])
    parameters = item.get("parameters") if isinstance(item.get("parameters"), dict) else {}
    return str(parameters.get("market_id") or "")


def _bar_snapshot_metadata(snapshot: dict[str, object]) -> dict[str, object]:
    return {
        "market_id": snapshot.get("market_id"),
        "interval": snapshot.get("interval"),
        "source": snapshot.get("source"),
        "start": snapshot.get("start"),
        "end": snapshot.get("end"),
        "bar_count": snapshot.get("bar_count"),
        "sha256": snapshot.get("sha256"),
        "exact": snapshot.get("exact", False),
        "created_at": snapshot.get("created_at"),
    }


def _cached_bar_metadata(export: dict[str, object]) -> dict[str, object]:
    return {
        "market_id": export.get("market_id"),
        "interval": export.get("interval"),
        "source": export.get("source"),
        "start": export.get("start"),
        "end": export.get("end"),
        "bar_count": export.get("bar_count"),
        "sha256": export.get("sha256"),
        "exact": False,
        "not_guaranteed_exact": True,
        "created_at": export.get("created_at"),
        "eodhd_symbol": export.get("eodhd_symbol"),
        "cache_match": export.get("cache_match"),
    }


def _bars_note(has_exact_bars: bool, has_cached_bars: bool) -> str:
    if has_exact_bars:
        return "Exact per-run bars are included."
    if has_cached_bars:
        return "Best-available cached bars are included but are not guaranteed to be the exact run snapshot."
    return "No exact run bar snapshot is available for this run."


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value[:10])
    except (TypeError, ValueError):
        return None


def _positive_int(value: object) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return 0
    return number if number > 0 else 0


def _json_bytes(payload: object) -> bytes:
    return json.dumps(payload, indent=2, sort_keys=True, default=str).encode("utf-8")


def _csv_bytes(rows: list[dict[str, object]]) -> bytes:
    buffer = io.StringIO()
    if not rows:
        return b""
    fields = sorted({key for row in rows for key in row.keys()})
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    for row in rows:
        writer.writerow({key: _csv_value(row.get(key)) for key in fields})
    return buffer.getvalue().encode("utf-8")


def _csv_value(value: object) -> object:
    if isinstance(value, (list, tuple)):
        return "|".join(str(item) for item in value)
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    return value


def _redact_sensitive(value: object) -> object:
    if isinstance(value, dict):
        output: dict[str, object] = {}
        for key, item in value.items():
            if key.lower() in SENSITIVE_KEYS:
                output[key] = "***"
            else:
                output[key] = _redact_sensitive(item)
        return output
    if isinstance(value, list):
        return [_redact_sensitive(item) for item in value]
    return value


def _readme(run_id: int, include_bars: bool, has_exact_bars: bool, has_cached_bars: bool = False) -> str:
    if has_exact_bars:
        bars_note = "Exact bars are included and can be matched against manifest hashes."
    elif has_cached_bars:
        bars_note = "Best-available cached bars are included, but they are not guaranteed to be the exact bars used during the run."
    else:
        bars_note = "No exact bars were saved for this run."
    if not include_bars:
        bars_note = "Bars were not requested for this export."
    return f"""# slrno Research Evidence Bundle

Run ID: {run_id}

This bundle is designed for offline review and Codex-assisted analysis. JSON files preserve nested audit evidence, while CSV files are intended for spreadsheet inspection.

Bars: {bars_note}

Pattern diagnostics: `bar_analysis.json`, `regime_segments.csv`, `regime_pnl.csv`, `regime_gated_backtests.csv`, `monthly_pnl.csv`, `session_pnl.csv`, and `pattern_warnings.csv` describe regime fit and concentration risks where available.

No API keys, passwords, or secret tokens are intentionally included in this export.
"""
