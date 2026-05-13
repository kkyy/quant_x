from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from .schemas import MetricSnapshot, StrategyFeedback


METRIC_PRIORITY = (
    "robust_score",
    "information_ratio",
    "sharpe",
    "mean_sharpe",
    "annual_return",
)

COMPARISON_METRICS = (
    "robust_score",
    "information_ratio",
    "sharpe",
    "mean_sharpe",
    "min_sharpe",
    "annual_return",
    "max_drawdown",
    "worst_max_drawdown",
    "rank_ic",
    "mean_rank_ic",
)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in ("", None):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _read_rows(path: Path) -> list[Dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _choose_rank_metric(columns: Iterable[str], requested: Optional[str] = None) -> str:
    column_set = set(columns)
    if requested and requested in column_set:
        return requested
    for metric in METRIC_PRIORITY:
        if metric in column_set:
            return metric
    return next(iter(column_set), "")


def parse_metric_snapshot(
    csv_path: str | Path,
    *,
    result_kind: str = "auto",
    rank_metric: Optional[str] = None,
) -> MetricSnapshot:
    path = Path(csv_path)
    rows = _read_rows(path)
    columns = rows[0].keys() if rows else []
    chosen_metric = _choose_rank_metric(columns, rank_metric)
    best_row = max(rows, key=lambda row: _to_float(row.get(chosen_metric))) if rows and chosen_metric else {}
    return MetricSnapshot(
        source_path=str(path),
        result_kind=result_kind,
        rank_metric=chosen_metric,
        row_count=len(rows),
        best_row=dict(best_row),
    )


def _deltas(result: MetricSnapshot, control: Optional[MetricSnapshot]) -> Dict[str, float]:
    if control is None:
        return {}
    deltas: Dict[str, float] = {}
    for metric in COMPARISON_METRICS:
        if metric in result.best_row and metric in control.best_row:
            deltas[metric] = _to_float(result.best_row.get(metric)) - _to_float(control.best_row.get(metric))
    return deltas


def _metric(snapshot: MetricSnapshot, *names: str) -> Optional[float]:
    for name in names:
        if name in snapshot.best_row:
            return _to_float(snapshot.best_row.get(name))
    return None


def _decide(result: MetricSnapshot, control: Optional[MetricSnapshot], deltas: Dict[str, float]) -> tuple[str, str]:
    if result.row_count == 0:
        return "reject", "inconclusive"

    if control is None:
        if result.result_kind == "walk_forward":
            mean_sharpe = _metric(result, "mean_sharpe", "sharpe") or 0.0
            min_sharpe = _metric(result, "min_sharpe")
            pvalue = _metric(result, "sharpe_ttest_pvalue")
            if mean_sharpe >= 0.9 and (min_sharpe is None or min_sharpe >= 0) and (pvalue is None or pvalue <= 0.3):
                return "compare_next", "supported"
            if mean_sharpe < 0.5 or (min_sharpe is not None and min_sharpe < -0.5):
                return "reject", "refuted"
        return "hold", "inconclusive"

    rank_delta = deltas.get(result.rank_metric, 0.0)
    sharpe_delta = deltas.get("sharpe", deltas.get("mean_sharpe", 0.0))
    drawdown_delta = deltas.get("max_drawdown", deltas.get("worst_max_drawdown", 0.0))
    if rank_delta > 0 and sharpe_delta >= 0.1 and drawdown_delta >= -0.05:
        return "compare_next", "supported"
    if sharpe_delta < -0.1 or rank_delta < -0.1:
        return "reject", "refuted"
    return "hold", "mixed"


def generate_feedback(
    *,
    run_id: str,
    result_csv: str | Path,
    result_kind: str = "auto",
    control_csv: str | Path | None = None,
    rank_metric: Optional[str] = None,
) -> StrategyFeedback:
    result = parse_metric_snapshot(result_csv, result_kind=result_kind, rank_metric=rank_metric)
    control = (
        parse_metric_snapshot(control_csv, result_kind="control", rank_metric=rank_metric)
        if control_csv
        else None
    )
    deltas = _deltas(result, control)
    decision, evaluation = _decide(result, control, deltas)
    observations = _build_observations(result, control, deltas)
    reflection = _reflect(run_id, decision, evaluation, observations)
    return StrategyFeedback(
        run_id=run_id,
        generated_at=datetime.now().isoformat(timespec="seconds"),
        result=result,
        control=control,
        deltas=deltas,
        observations=observations,
        hypothesis_evaluation=evaluation,
        decision=decision,
        new_hypothesis=_new_hypothesis(decision, evaluation),
        next_ablation=_next_ablation(decision, evaluation),
        do_not_repeat=_do_not_repeat(decision, result, deltas),
        reflection=reflection,
    )


def _build_observations(
    result: MetricSnapshot,
    control: Optional[MetricSnapshot],
    deltas: Dict[str, float],
) -> list[str]:
    observations = [
        f"Parsed {result.row_count} rows from {result.source_path}; selected best row by {result.rank_metric}.",
    ]
    for metric in ("robust_score", "information_ratio", "sharpe", "mean_sharpe", "max_drawdown", "worst_max_drawdown"):
        if metric in result.best_row:
            observations.append(f"Result {metric}={_to_float(result.best_row.get(metric)):.4f}.")
    if control:
        observations.append(f"Compared against control {control.source_path}.")
        for metric, delta in deltas.items():
            observations.append(f"Delta {metric}={delta:+.4f}.")
    else:
        observations.append("No control CSV was supplied; decision remains conservative unless WFV evidence is strong.")
    return observations


def _reflect(run_id: str, decision: str, evaluation: str, observations: list[str]) -> str:
    core = observations[0] if observations else "No measurable observation was available."
    return (
        f"For {run_id}, the outcome is {evaluation} with a {decision} decision. "
        f"{core} The next run should keep the same comparability assumptions and only escalate after validated evidence, not narrative confidence."
    )


def _new_hypothesis(decision: str, evaluation: str) -> str:
    if decision == "compare_next":
        return "The treatment may contain useful signal, but it needs a stricter follow-up comparison before promotion."
    if decision == "reject":
        return "The tested direction likely fails under the current validation assumptions; search for a simpler or more orthogonal variant."
    if evaluation == "mixed":
        return "The result is ambiguous; isolate the changed variable and recheck the control assumptions before broadening the experiment."
    return "More evidence is needed before changing the current strategy candidate set."


def _next_ablation(decision: str, evaluation: str) -> str:
    if decision == "compare_next":
        return "Run the same arm through the next validation rung with unchanged benchmark/rank_metric/deal_price/cost settings."
    if decision == "reject":
        return "Do not rerun this exact configuration; design a smaller ablation or return to the baseline control."
    return "Collect a control-matched result or WFV summary before deciding."


def _do_not_repeat(decision: str, result: MetricSnapshot, deltas: Dict[str, float]) -> list[str]:
    notes: list[str] = []
    if decision == "reject":
        notes.append(f"Do not promote {result.source_path} without new control-matched evidence.")
    if deltas.get("max_drawdown", 0.0) < -0.05 or deltas.get("worst_max_drawdown", 0.0) < -0.05:
        notes.append("Do not accept a return improvement that materially worsens drawdown.")
    return notes
