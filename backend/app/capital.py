from __future__ import annotations

from dataclasses import dataclass

WORKING_ACCOUNT_SIZE_GBP = 2_000.0
CAPITAL_SCENARIOS_GBP = (250.0, 500.0, 1_000.0, WORKING_ACCOUNT_SIZE_GBP, 10_000.0)
RISK_PER_TRADE_FRACTION = 0.01
DAILY_LOSS_FRACTION = 0.05
MAX_MARGIN_FRACTION = 0.5
MAX_HISTORICAL_DRAWDOWN_FRACTION = 0.25


@dataclass(frozen=True)
class CapitalScenario:
    account_size: float
    risk_budget: float
    daily_loss_limit: float
    requested_stake: float
    effective_stake: float
    min_deal_size: float
    estimated_margin: float
    estimated_stop_loss: float
    historical_max_drawdown: float
    worst_daily_loss: float
    margin_percent: float
    feasible: bool
    violations: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "account_size": self.account_size,
            "risk_budget": self.risk_budget,
            "daily_loss_limit": self.daily_loss_limit,
            "requested_stake": self.requested_stake,
            "effective_stake": self.effective_stake,
            "min_deal_size": self.min_deal_size,
            "estimated_margin": self.estimated_margin,
            "estimated_stop_loss": self.estimated_stop_loss,
            "historical_max_drawdown": self.historical_max_drawdown,
            "worst_daily_loss": self.worst_daily_loss,
            "margin_percent": self.margin_percent,
            "feasible": self.feasible,
            "violations": list(self.violations),
        }


def capital_scenarios(
    backtest: dict[str, object],
    parameters: dict[str, object] | None = None,
    cost_profile: dict[str, object] | None = None,
) -> list[dict[str, object]]:
    parameters = parameters or {}
    cost_profile = cost_profile or {}
    requested_stake = _positive_float(parameters.get("position_size"), backtest.get("position_size"), 1.0)
    min_deal_size = _positive_float(cost_profile.get("min_deal_size"), 0.0)
    effective_stake = max(requested_stake, min_deal_size or requested_stake)
    price = _midpoint(cost_profile) or _positive_float(parameters.get("reference_price"), cost_profile.get("reference_price"))
    has_reference_price = price > 0
    stop_bps = _positive_float(parameters.get("stop_loss_bps"), parameters.get("stop_bps"), 100.0)
    margin_percent = _positive_float(cost_profile.get("margin_percent"), _fallback_margin_percent(cost_profile), 5.0)
    stop_points = price * stop_bps / 10_000
    estimated_stop_loss = abs(stop_points * effective_stake)
    estimated_margin = abs(price * effective_stake * margin_percent / 100)
    historical_max_drawdown = _positive_float(backtest.get("max_drawdown"), 0.0)
    worst_daily_loss = _worst_daily_loss(backtest.get("daily_pnl_curve"))

    output: list[dict[str, object]] = []
    for account_size in CAPITAL_SCENARIOS_GBP:
        risk_budget = account_size * RISK_PER_TRADE_FRACTION
        daily_loss_limit = account_size * DAILY_LOSS_FRACTION
        violations: list[str] = []
        if not has_reference_price:
            violations.append("missing_reference_price")
        if min_deal_size and requested_stake < min_deal_size:
            violations.append("below_ig_min_deal_size")
        if estimated_stop_loss > risk_budget:
            violations.append("risk_budget_exceeded")
        if estimated_margin > account_size * MAX_MARGIN_FRACTION:
            violations.append("margin_too_large")
        if estimated_margin > account_size:
            violations.append("insufficient_account_for_margin")
        if historical_max_drawdown > account_size * MAX_HISTORICAL_DRAWDOWN_FRACTION:
            violations.append("historical_drawdown_too_large")
        if worst_daily_loss > daily_loss_limit:
            violations.append("historical_daily_loss_stop_breached")
        output.append(
            CapitalScenario(
                account_size=account_size,
                risk_budget=round(risk_budget, 4),
                daily_loss_limit=round(daily_loss_limit, 4),
                requested_stake=round(requested_stake, 6),
                effective_stake=round(effective_stake, 6),
                min_deal_size=round(min_deal_size, 6),
                estimated_margin=round(estimated_margin, 4),
                estimated_stop_loss=round(estimated_stop_loss, 4),
                historical_max_drawdown=round(historical_max_drawdown, 4),
                worst_daily_loss=round(worst_daily_loss, 4),
                margin_percent=round(margin_percent, 6),
                feasible=not violations,
                violations=tuple(violations),
            ).as_dict()
        )
    return output


def capital_summary(scenarios: list[dict[str, object]]) -> dict[str, object]:
    if not scenarios:
        return {"smallest_feasible_account": None, "feasible_accounts": [], "blocked_accounts": []}
    feasible = [item for item in scenarios if item.get("feasible")]
    return {
        "smallest_feasible_account": feasible[0]["account_size"] if feasible else None,
        "feasible_accounts": [item["account_size"] for item in feasible],
        "blocked_accounts": [item["account_size"] for item in scenarios if not item.get("feasible")],
    }


def _positive_float(*values: object) -> float:
    for value in values:
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if number > 0:
            return number
    return 0.0


def _midpoint(cost_profile: dict[str, object]) -> float:
    bid = _positive_float(cost_profile.get("bid"))
    offer = _positive_float(cost_profile.get("offer"))
    if bid and offer:
        return (bid + offer) / 2
    return 0.0


def _fallback_margin_percent(cost_profile: dict[str, object]) -> float:
    instrument_type = str(cost_profile.get("instrument_type") or "").lower()
    if "currenc" in instrument_type or "forex" in instrument_type:
        return 3.33
    if "commod" in instrument_type:
        return 10.0
    if "share" in instrument_type:
        return 20.0
    return 5.0


def _worst_daily_loss(value: object) -> float:
    if not isinstance(value, (list, tuple)):
        return 0.0
    losses: list[float] = []
    for item in value:
        try:
            number = float(item)
        except (TypeError, ValueError):
            continue
        if number < 0:
            losses.append(abs(number))
    return max(losses) if losses else 0.0
