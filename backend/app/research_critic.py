from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Protocol


@dataclass(frozen=True)
class CriticFinding:
    severity: str
    code: str
    message: str
    evidence: dict[str, object]


@dataclass(frozen=True)
class CriticContext:
    run: dict[str, object]
    trials: list[dict[str, object]]
    candidates: list[dict[str, object]]


@dataclass(frozen=True)
class CriticReport:
    run_id: int | None
    market_id: str | None
    data_source: str | None
    decision: str
    confidence_score: float
    trial_count: int
    candidate_count: int
    findings: tuple[CriticFinding, ...]

    def as_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["findings"] = [asdict(finding) for finding in self.findings]
        return payload


class CriticRule(Protocol):
    def evaluate(self, context: CriticContext) -> list[CriticFinding]:
        ...


@dataclass(frozen=True)
class ResearchCritic:
    rules: tuple[CriticRule, ...]

    @classmethod
    def default(cls) -> "ResearchCritic":
        return cls(
            (
                AuditTrailRule(),
                ProviderValidationRule(),
                SearchBreadthRule(),
                CandidateQualityRule(),
            )
        )

    def critique(self, run: dict[str, object] | None, trials: list[dict[str, object]], candidates: list[dict[str, object]]) -> CriticReport:
        if run is None:
            return CriticReport(
                run_id=None,
                market_id=None,
                data_source=None,
                decision="no_research_run",
                confidence_score=0.0,
                trial_count=0,
                candidate_count=0,
                findings=(
                    CriticFinding(
                        "blocker",
                        "no_research_run",
                        "No research run exists yet, so there is nothing to promote or critique.",
                        {},
                    ),
                ),
            )

        context = CriticContext(run, trials, candidates)
        findings: list[CriticFinding] = []
        for rule in self.rules:
            findings.extend(rule.evaluate(context))

        return CriticReport(
            run_id=int(run["id"]),
            market_id=str(run["market_id"]),
            data_source=str(run["data_source"]),
            decision=_decision(findings, candidates),
            confidence_score=_confidence_score(findings),
            trial_count=len(trials),
            candidate_count=len(candidates),
            findings=tuple(findings),
        )


class AuditTrailRule:
    def evaluate(self, context: CriticContext) -> list[CriticFinding]:
        expected_trials = int(context.run.get("trial_count", 0))
        findings: list[CriticFinding] = []
        if not context.trials:
            findings.append(
                CriticFinding(
                    "blocker",
                    "no_trial_audit",
                    "No individual trials were stored. A run without rejected trials is not useful evidence.",
                    {"expected_trials": expected_trials},
                )
            )
        elif expected_trials != len(context.trials):
            findings.append(
                CriticFinding(
                    "warning",
                    "trial_count_mismatch",
                    "Stored trial count does not match the run summary, so the audit trail should be checked before trusting the results.",
                    {"summary_count": expected_trials, "stored_count": len(context.trials)},
                )
            )
        return findings


class ProviderValidationRule:
    def evaluate(self, context: CriticContext) -> list[CriticFinding]:
        data_source = str(context.run.get("data_source") or "")
        if data_source.startswith("eodhd"):
            return [
                CriticFinding(
                    "warning",
                    "eodhd_only_validation",
                    "This is still EODHD discovery evidence. It must be re-tested on IG bid/offer prices for the exact EPIC before it can leave the research watchlist.",
                    {"required_next_step": "ig_price_validation"},
                )
            ]
        return []


class SearchBreadthRule:
    def evaluate(self, context: CriticContext) -> list[CriticFinding]:
        trial_count = len(context.trials)
        passed = sum(1 for trial in context.trials if trial.get("passed"))
        pass_rate = passed / trial_count if trial_count else 0.0
        findings: list[CriticFinding] = []
        if trial_count < 10:
            findings.append(
                CriticFinding(
                    "note",
                    "narrow_search",
                    "The search space is intentionally small so far. Treat results as a smoke test, not a discovered edge.",
                    {"trial_count": trial_count},
                )
            )
        if context.trials and passed == 0:
            findings.append(
                CriticFinding(
                    "warning",
                    "no_promoted_candidates",
                    "No candidate passed the gates. The correct action is to inspect rejected trials, not loosen gates blindly.",
                    {"trial_count": trial_count},
                )
            )
        if pass_rate > 0.35:
            findings.append(
                CriticFinding(
                    "warning",
                    "gates_may_be_too_loose",
                    "A high pass rate can indicate loose gates or a narrow trial family. Robust searches should reject most ideas.",
                    {"pass_rate": round(pass_rate, 4), "passed": passed, "trial_count": trial_count},
                )
            )
        return findings


class CandidateQualityRule:
    def evaluate(self, context: CriticContext) -> list[CriticFinding]:
        findings: list[CriticFinding] = []
        for candidate in context.candidates:
            audit = candidate.get("audit", {})
            metrics = audit.get("metrics", {}) if isinstance(audit, dict) else {}
            backtest = audit.get("backtest", {}) if isinstance(audit, dict) else {}
            folds = audit.get("fold_results", []) if isinstance(audit, dict) else []
            warnings = audit.get("warnings", []) if isinstance(audit, dict) else []
            strategy_name = str(candidate.get("strategy_name", "unknown"))

            findings.extend(_metric_findings(strategy_name, metrics))
            findings.extend(_backtest_findings(strategy_name, backtest, folds))
            if warnings:
                findings.append(
                    CriticFinding(
                        "warning",
                        "candidate_saved_with_warnings",
                        "A promoted candidate still carries warning flags. Review the exact warning list before using it as evidence.",
                        {"strategy_name": strategy_name, "warnings": list(warnings)},
                    )
                )
        return findings


def _metric_findings(strategy_name: str, metrics: dict[str, object]) -> list[CriticFinding]:
    findings: list[CriticFinding] = []
    roc_auc = metrics.get("roc_auc")
    pr_auc = metrics.get("pr_auc")
    positive_rate = float(metrics.get("positive_rate") or 0.0)
    top_precision = float(metrics.get("precision_at_top_quantile") or 0.0)

    if roc_auc is None:
        findings.append(
            CriticFinding(
                "warning",
                "auc_unavailable",
                "ROC-AUC could not be computed, usually because labels are one-sided. That is weak evidence.",
                {"strategy_name": strategy_name},
            )
        )
    elif float(roc_auc) > 0.9:
        findings.append(
            CriticFinding(
                "warning",
                "possible_leakage_or_overfit",
                "Very high AUC is a smell in noisy markets. Check label timing, feature leakage, and whether one regime dominates.",
                {"strategy_name": strategy_name, "roc_auc": roc_auc},
            )
        )

    if pr_auc is not None and float(pr_auc) <= positive_rate:
        findings.append(
            CriticFinding(
                "warning",
                "no_pr_auc_lift",
                "PR-AUC does not beat the base positive rate, so the probability ranking may not isolate tradable opportunities.",
                {"strategy_name": strategy_name, "pr_auc": pr_auc, "positive_rate": positive_rate},
            )
        )
    if top_precision <= positive_rate:
        findings.append(
            CriticFinding(
                "warning",
                "no_top_bucket_lift",
                "The highest-probability bucket is not better than the base rate, so filtering trades may not add edge.",
                {"strategy_name": strategy_name, "top_precision": top_precision, "positive_rate": positive_rate},
            )
        )
    return findings


def _backtest_findings(strategy_name: str, backtest: dict[str, object], folds: list[dict[str, object]]) -> list[CriticFinding]:
    findings: list[CriticFinding] = []
    trade_count = int(backtest.get("trade_count") or 0)
    sharpe = float(backtest.get("daily_pnl_sharpe") or backtest.get("sharpe") or 0.0)
    net_profit = float(backtest.get("net_profit") or 0.0)
    turnover = float(backtest.get("turnover") or 0.0)
    net_cost_ratio = float(backtest.get("net_cost_ratio") or 0.0)
    cost_to_gross_ratio = float(backtest.get("cost_to_gross_ratio") or 0.0)
    expectancy_per_trade = float(backtest.get("expectancy_per_trade") or 0.0)

    if trade_count < 50:
        findings.append(
            CriticFinding(
                "warning",
                "low_trade_count",
                "Too few trades can make the risk-adjusted statistics unstable.",
                {"strategy_name": strategy_name, "trade_count": trade_count},
            )
        )
    if sharpe < 0.7 or net_profit <= 0:
        findings.append(
            CriticFinding(
                "warning",
                "weak_oos_economics",
                "The candidate does not clear the minimum out-of-sample economics after costs.",
                {"strategy_name": strategy_name, "risk_adjusted_sharpe": sharpe, "net_profit": net_profit},
            )
        )
    if turnover > max(100.0, trade_count * 5):
        findings.append(
            CriticFinding(
                "note",
                "high_turnover",
                "High turnover makes spread, slippage, and rejected fills more important than this first model assumes.",
                {"strategy_name": strategy_name, "turnover": turnover, "trade_count": trade_count},
            )
        )
    if net_profit > 0 and net_cost_ratio < 0.5:
        findings.append(
            CriticFinding(
                "warning",
                "weak_net_cost_efficiency",
                "The candidate makes less than 0.5 units of net profit per unit of modeled cost.",
                {"strategy_name": strategy_name, "net_cost_ratio": round(net_cost_ratio, 4), "net_profit": net_profit},
            )
        )
    if cost_to_gross_ratio > 0.65:
        findings.append(
            CriticFinding(
                "warning",
                "costs_overwhelm_edge",
                "Modeled costs consume most of the gross edge, so small slippage changes can flip the result.",
                {"strategy_name": strategy_name, "cost_to_gross_ratio": round(cost_to_gross_ratio, 4)},
            )
        )
    if trade_count > 0 and expectancy_per_trade <= 0:
        findings.append(
            CriticFinding(
                "warning",
                "negative_expectancy_after_costs",
                "Average net expectancy per trade is not positive after modeled costs.",
                {"strategy_name": strategy_name, "expectancy_per_trade": round(expectancy_per_trade, 4)},
            )
        )
    if folds:
        positive_folds = sum(1 for fold in folds if float(fold.get("net_profit") or 0.0) > 0)
        positive_rate = positive_folds / len(folds)
        if positive_rate < 0.6:
            findings.append(
                CriticFinding(
                    "warning",
                    "one_regime_dependency",
                    "Less than 60% of walk-forward folds are profitable, so the result may depend on one regime.",
                    {"strategy_name": strategy_name, "positive_fold_rate": round(positive_rate, 4)},
                )
            )
        fold_profits = [float(fold.get("net_profit") or 0.0) for fold in folds if float(fold.get("net_profit") or 0.0) > 0]
        total_positive = sum(fold_profits)
        if total_positive > 0 and max(fold_profits) / total_positive > 0.7:
            findings.append(
                CriticFinding(
                    "warning",
                    "one_fold_dependency",
                    "Most fold profit comes from a single fold. That is fragile evidence.",
                    {"strategy_name": strategy_name, "largest_fold_share": round(max(fold_profits) / total_positive, 4)},
                )
            )
    return findings


def _decision(findings: list[CriticFinding], candidates: list[dict[str, object]]) -> str:
    if any(finding.severity == "blocker" for finding in findings):
        return "reject"
    if not candidates:
        return "reject"
    if any(finding.code == "eodhd_only_validation" for finding in findings):
        return "watchlist_only"
    if any(finding.severity == "warning" for finding in findings):
        return "revise_before_validation"
    return "promote_to_ig_validation"


def _confidence_score(findings: list[CriticFinding]) -> float:
    score = 100.0
    for finding in findings:
        if finding.severity == "blocker":
            score -= 35.0
        elif finding.severity == "warning":
            score -= 12.0
        else:
            score -= 3.0
    return max(0.0, round(score, 2))
