from __future__ import annotations

import csv
import io
import json
from datetime import UTC, datetime
from zipfile import ZIP_DEFLATED, ZipFile

from .capital import capital_scenarios, capital_summary
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


def build_research_export_zip(store: ResearchStore, run_id: int, include_bars: bool = True) -> bytes:
    run = store.get_run(run_id)
    if run is None:
        raise ValueError("Research run not found")

    trials = store.list_trials(run_id)
    candidates = store.list_candidates(run_id)
    cost_profiles = _cost_profiles_for_run(store, run, trials, candidates)
    bar_snapshots = store.list_bar_snapshots(run_id, include_payload=include_bars)
    exported_trials = [_trial_export(trial, cost_profiles) for trial in trials]
    exported_candidates = [_candidate_export(candidate, cost_profiles) for candidate in candidates]
    warning_rows = _warning_rows(exported_trials, exported_candidates)
    capital_rows = _capital_rows(exported_trials, exported_candidates)
    manifest = {
        "app": "slrno",
        "schema": "research_evidence_bundle_v1",
        "exported_at": datetime.now(UTC).isoformat(),
        "run_id": run_id,
        "include_bars": include_bars,
        "bar_snapshots": [_bar_snapshot_metadata(snapshot) for snapshot in bar_snapshots],
        "data_completeness": {
            "exact_run_bars_available": bool(bar_snapshots),
            "bars_exact": bool(bar_snapshots),
            "bars_note": "Exact per-run bars are included." if bar_snapshots else "No exact run bar snapshot is available for this run.",
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
        archive.writestr("README.md", _readme(run_id, include_bars, bool(bar_snapshots)))
        if include_bars:
            if bar_snapshots:
                for snapshot in bar_snapshots:
                    filename = f"bars/{snapshot['market_id']}_{snapshot['interval']}.csv"
                    archive.writestr(filename, _csv_bytes(snapshot.get("bars") if isinstance(snapshot.get("bars"), list) else []))
            else:
                archive.writestr(
                    "bars/README.md",
                    "No exact per-run bars were saved for this historical run. Re-run the backtest to create a reproducible bar snapshot.\n",
                )
    return buffer.getvalue()


def _trial_export(trial: dict[str, object], cost_profiles: dict[str, dict[str, object]]) -> dict[str, object]:
    parameters = trial.get("parameters") if isinstance(trial.get("parameters"), dict) else {}
    backtest = trial.get("backtest") if isinstance(trial.get("backtest"), dict) else {}
    market_id = str(parameters.get("market_id") or "")
    scenarios = capital_scenarios(backtest, parameters, cost_profiles.get(market_id))
    return {**trial, "capital_scenarios": scenarios, "capital_summary": capital_summary(scenarios)}


def _candidate_export(candidate: dict[str, object], cost_profiles: dict[str, dict[str, object]]) -> dict[str, object]:
    audit = candidate.get("audit") if isinstance(candidate.get("audit"), dict) else {}
    candidate_payload = audit.get("candidate") if isinstance(audit.get("candidate"), dict) else {}
    parameters = candidate_payload.get("parameters") if isinstance(candidate_payload.get("parameters"), dict) else {}
    backtest = audit.get("backtest") if isinstance(audit.get("backtest"), dict) else {}
    market_id = str(candidate.get("market_id") or parameters.get("market_id") or "")
    scenarios = capital_scenarios(backtest, parameters, cost_profiles.get(market_id))
    return {**candidate, "capital_scenarios": scenarios, "capital_summary": capital_summary(scenarios)}


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
                "max_drawdown": backtest.get("max_drawdown"),
                "trade_count": backtest.get("trade_count"),
                "total_cost": backtest.get("total_cost"),
                "net_cost_ratio": backtest.get("net_cost_ratio"),
                "cost_to_gross_ratio": backtest.get("cost_to_gross_ratio"),
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
                "total_cost": backtest.get("total_cost"),
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


def _readme(run_id: int, include_bars: bool, has_exact_bars: bool) -> str:
    bars_note = "Exact bars are included and can be matched against manifest hashes." if has_exact_bars else "No exact bars were saved for this run."
    if not include_bars:
        bars_note = "Bars were not requested for this export."
    return f"""# slrno Research Evidence Bundle

Run ID: {run_id}

This bundle is designed for offline review and Codex-assisted analysis. JSON files preserve nested audit evidence, while CSV files are intended for spreadsheet inspection.

Bars: {bars_note}

No API keys, passwords, or secret tokens are intentionally included in this export.
"""
