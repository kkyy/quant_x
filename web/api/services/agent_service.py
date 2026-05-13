"""Read-mostly helpers for agent strategy iteration run artifacts."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from quant_ex.agent.strategy_iteration import (
    StrategyIterationOrchestrator,
    attach_feedback_candidates,
    build_command_plan,
    save_approval_template,
    save_command_plan,
)
from quant_ex.agent.strategy_iteration.schemas import (
    CommandExecutionPlan,
    CommandExecutionResult,
    CommandProposal,
    ExperimentArm,
    FeedbackCandidate,
    RoleReport,
    StrategyIterationPlan,
)
from web.api.deps import AGENT_RUNS_DIR, PROJECT_ROOT

TEXT_ARTIFACTS = (
    "plan.md",
    "commands.md",
    "execution_summary.md",
    "feedback.md",
    "approval_template.yaml",
)
JSON_ARTIFACTS = ("run.json", "commands.json", "feedback.json")


def list_agent_runs() -> list[dict[str, Any]]:
    runs_dir = AGENT_RUNS_DIR
    if not runs_dir.exists():
        return []
    run_dirs = [path for path in runs_dir.iterdir() if path.is_dir()]
    run_dirs.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return [_summarize_run(path) for path in run_dirs]


def get_agent_run(run_id: str) -> dict[str, Any]:
    run_dir = _safe_run_dir(run_id)
    summary = _summarize_run(run_dir)
    payload: dict[str, Any] = {**summary, "artifacts": {}}

    for name in JSON_ARTIFACTS:
        parsed = _read_json(run_dir / name)
        if parsed is not None:
            payload["artifacts"][name] = parsed

    for name in TEXT_ARTIFACTS:
        path = run_dir / name
        if path.exists():
            payload["artifacts"][name] = path.read_text(encoding="utf-8")

    return payload


def create_agent_run(
    *,
    objective: str,
    run_id: str | None = None,
    use_llm: bool = False,
    propose_actions: bool = True,
    write_approval_template: bool = True,
    append_memory: bool = False,
) -> dict[str, Any]:
    if not objective.strip():
        raise HTTPException(status_code=400, detail="objective is required")
    if run_id:
        _validate_run_id(run_id)
        if (AGENT_RUNS_DIR / run_id).exists():
            raise HTTPException(status_code=409, detail="Agent run already exists")

    orchestrator = StrategyIterationOrchestrator.from_config(PROJECT_ROOT / "config" / "agent_strategy_iteration.yaml")
    orchestrator.root = PROJECT_ROOT
    orchestrator.output_dir = AGENT_RUNS_DIR
    if not orchestrator.memory_log_path.is_absolute():
        orchestrator.memory_log_path = PROJECT_ROOT / orchestrator.memory_log_path
    run = orchestrator.build_run(objective.strip(), use_llm=use_llm, run_id=run_id)
    run_dir = orchestrator.save_run(run, append_memory=append_memory)

    if propose_actions:
        command_plan = build_command_plan(run.plan)
        command_plan = attach_feedback_candidates(command_plan, root=orchestrator.root)
        save_command_plan(command_plan, run_dir)
        if write_approval_template:
            save_approval_template(command_plan, run_dir)

    summary = _summarize_run(run_dir)
    return {"run_id": run.run_id, **_artifact_flags(summary)}


def regenerate_approval_template(run_id: str) -> dict[str, Any]:
    run_dir = _safe_run_dir(run_id)
    command_plan = _load_command_plan(run_dir)
    save_approval_template(command_plan, run_dir)
    summary = _summarize_run(run_dir)
    return {"run_id": summary["run_id"], **_artifact_flags(summary)}


def _safe_run_dir(run_id: str) -> Path:
    _validate_run_id(run_id)
    base = AGENT_RUNS_DIR.resolve()
    run_dir = (base / run_id).resolve()
    if not run_dir.is_relative_to(base) or not run_dir.is_dir():
        raise HTTPException(status_code=404, detail="Agent run not found")
    return run_dir


def _validate_run_id(run_id: str) -> None:
    if not run_id or "/" in run_id or "\\" in run_id or run_id in {".", ".."}:
        raise HTTPException(status_code=400, detail="Invalid run_id")
    candidate = (AGENT_RUNS_DIR.resolve() / run_id).resolve()
    if not candidate.is_relative_to(AGENT_RUNS_DIR.resolve()):
        raise HTTPException(status_code=400, detail="Invalid run_id")


def _summarize_run(run_dir: Path) -> dict[str, Any]:
    run_payload = _read_json(run_dir / "run.json") or {}
    commands_payload = _read_json(run_dir / "commands.json") or {}
    feedback_payload = _read_json(run_dir / "feedback.json")
    commands = commands_payload.get("commands") or []
    results = commands_payload.get("results") or []
    feedback_candidates = commands_payload.get("feedback_candidates") or []
    stat = run_dir.stat()

    return {
        "run_id": run_dir.name,
        "objective": run_payload.get("objective"),
        "generated_at": run_payload.get("generated_at"),
        "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
        "has_plan": (run_dir / "plan.md").exists(),
        "has_commands": (run_dir / "commands.json").exists(),
        "has_feedback": feedback_payload is not None or (run_dir / "feedback.md").exists(),
        "has_execution_summary": (run_dir / "execution_summary.md").exists(),
        "has_approval_template": (run_dir / "approval_template.yaml").exists(),
        "commands_count": len(commands),
        "results_count": len(results),
        "feedback_candidates_count": len(feedback_candidates),
    }


def _artifact_flags(summary: dict[str, Any]) -> dict[str, bool]:
    return {
        "has_plan": bool(summary["has_plan"]),
        "has_commands": bool(summary["has_commands"]),
        "has_feedback": bool(summary["has_feedback"]),
        "has_execution_summary": bool(summary["has_execution_summary"]),
        "has_approval_template": bool(summary["has_approval_template"]),
    }


def _read_json(path: Path) -> Any | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail=f"Invalid JSON artifact: {path.name}") from exc


def _load_command_plan(run_dir: Path) -> CommandExecutionPlan:
    commands_payload = _read_json(run_dir / "commands.json")
    if commands_payload:
        return _command_plan_from_dict(commands_payload)

    run_payload = _read_json(run_dir / "run.json")
    plan_payload = (run_payload or {}).get("plan")
    if not plan_payload:
        raise HTTPException(status_code=404, detail="No saved plan or command plan for this run")
    return build_command_plan(_strategy_plan_from_dict(plan_payload))


def _command_plan_from_dict(payload: dict[str, Any]) -> CommandExecutionPlan:
    return CommandExecutionPlan(
        run_id=str(payload.get("run_id") or ""),
        generated_at=str(payload.get("generated_at") or ""),
        policy=str(payload.get("policy") or ""),
        commands=[CommandProposal(**item) for item in payload.get("commands") or []],
        results=[CommandExecutionResult(**item) for item in payload.get("results") or []],
        feedback_candidates=[FeedbackCandidate(**item) for item in payload.get("feedback_candidates") or []],
    )


def _strategy_plan_from_dict(payload: dict[str, Any]) -> StrategyIterationPlan:
    return StrategyIterationPlan(
        run_id=str(payload.get("run_id") or ""),
        objective=str(payload.get("objective") or ""),
        generated_at=str(payload.get("generated_at") or ""),
        role_reports=[RoleReport(**item) for item in payload.get("role_reports") or []],
        experiment_arms=[ExperimentArm(**item) for item in payload.get("experiment_arms") or []],
        validation_ladder=[str(item) for item in payload.get("validation_ladder") or []],
        decision_gates=[str(item) for item in payload.get("decision_gates") or []],
        synthesis=str(payload.get("synthesis") or ""),
        next_actions=[str(item) for item in payload.get("next_actions") or []],
    )
