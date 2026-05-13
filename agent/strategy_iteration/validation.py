from __future__ import annotations

from typing import Dict, Iterable, List

from .schemas import AgentRole, RoleReport


ROLE_REQUIRED_FIELDS: Dict[str, tuple[str, ...]] = {
    "data_factor_analyst": ("thesis", "evidence", "proposals", "risks"),
    "model_analyst": ("thesis", "evidence", "proposals", "risks"),
    "backtest_analyst": ("thesis", "evidence", "proposals", "risks"),
    "execution_analyst": ("thesis", "evidence", "proposals", "risks"),
    "bull_researcher": ("thesis", "evidence", "proposals"),
    "bear_researcher": ("thesis", "evidence", "risks"),
    "research_manager": ("thesis", "evidence", "proposals", "verdict"),
    "experiment_designer": ("thesis", "proposals", "next_actions"),
    "aggressive_risk_reviewer": ("thesis", "evidence", "proposals"),
    "conservative_risk_reviewer": ("thesis", "risks", "next_actions"),
    "neutral_risk_reviewer": ("thesis", "proposals", "next_actions"),
    "research_portfolio_manager": ("thesis", "evidence", "proposals", "verdict"),
}


def validate_role_report(role: AgentRole, report: RoleReport) -> List[str]:
    """Return schema warnings without rejecting the run."""

    warnings: List[str] = []
    required = ROLE_REQUIRED_FIELDS.get(role.name, ("thesis",))
    for field_name in required:
        value = getattr(report, field_name, None)
        if value is None or value == "" or value == []:
            warnings.append(f"{role.name}: missing required output `{field_name}`")
    if not 0 <= report.confidence <= 1:
        warnings.append(f"{role.name}: confidence outside [0, 1]")
    if report.role != role.name:
        warnings.append(f"{role.name}: report role mismatch `{report.role}`")
    return warnings


def attach_role_metadata(
    role: AgentRole,
    report: RoleReport,
    prior_reports: Iterable[RoleReport],
) -> RoleReport:
    report.prompt_name = report.prompt_name or role.name
    report.required_outputs = list(role.required_outputs)
    report.upstream_roles = [item.role for item in prior_reports]
    report.schema_warnings = validate_role_report(role, report)
    return report
