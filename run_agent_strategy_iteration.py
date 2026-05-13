from __future__ import annotations

import argparse
import json
from pathlib import Path

from agent.strategy_iteration import StrategyIterationOrchestrator
from agent.strategy_iteration.evaluator import generate_feedback
from agent.strategy_iteration.execution import (
    attach_feedback_candidates,
    build_command_plan,
    execute_approved_commands,
    execute_safe_commands,
    save_approval_template,
    save_command_plan,
)
from agent.strategy_iteration.memory import StrategyAgentMemoryLog


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a multi-role strategy iteration planning bundle.")
    parser.add_argument("--objective", help="Research objective for this planning run.")
    parser.add_argument("--config", default="config/agent_strategy_iteration.yaml", help="Planner config YAML path.")
    parser.add_argument("--run-id", default=None, help="Optional explicit run id.")
    parser.add_argument("--output-dir", default=None, help="Optional output directory override.")
    parser.add_argument("--use-llm", action="store_true", help="Enable optional OpenAI-compatible role execution.")
    parser.add_argument("--no-llm", action="store_true", help="Force offline role execution.")
    parser.add_argument("--no-memory", action="store_true", help="Do not append the memory log.")
    parser.add_argument("--propose-actions", action="store_true", help="Write commands.json/md with gated action proposals.")
    parser.add_argument(
        "--write-approval-template",
        action="store_true",
        help="Write approval_template.yaml with command ids and hashes.",
    )
    parser.add_argument(
        "--execute-safe",
        action="store_true",
        help="Execute only commands classified as safe_local and not requiring approval.",
    )
    parser.add_argument(
        "--execute-approved",
        action="store_true",
        help="Execute only commands explicitly approved by --approval-file.",
    )
    parser.add_argument("--approval-file", help="Approval YAML/JSON with matching command_id and command_sha256 entries.")
    parser.add_argument("--feedback-run-id", help="Generate feedback for an existing agent run id.")
    parser.add_argument("--result-csv", help="CSV result to parse for feedback.")
    parser.add_argument("--control-csv", help="Optional control CSV for deltas.")
    parser.add_argument("--result-kind", default="auto", help="Result kind label, e.g. backtest or walk_forward.")
    parser.add_argument("--rank-metric", default=None, help="Optional rank metric override.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    orchestrator = StrategyIterationOrchestrator.from_config(args.config)
    if args.output_dir:
        orchestrator.output_dir = Path(args.output_dir)
        if not orchestrator.output_dir.is_absolute():
            orchestrator.output_dir = orchestrator.root / orchestrator.output_dir

    if args.feedback_run_id:
        if not args.result_csv:
            raise SystemExit("--result-csv is required with --feedback-run-id")
        feedback = generate_feedback(
            run_id=args.feedback_run_id,
            result_csv=args.result_csv,
            result_kind=args.result_kind,
            control_csv=args.control_csv,
            rank_metric=args.rank_metric,
        )
        run_dir = orchestrator.output_dir / args.feedback_run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "feedback.json").write_text(
            json.dumps(feedback.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (run_dir / "feedback.md").write_text(feedback.to_markdown(), encoding="utf-8")
        if not args.no_memory:
            StrategyAgentMemoryLog(orchestrator.memory_log_path).append_feedback(feedback)
        print(run_dir / "feedback.json")
        return 0

    if not args.objective:
        raise SystemExit("--objective is required unless --feedback-run-id is used")

    use_llm = bool(args.use_llm and not args.no_llm)
    run = orchestrator.build_run(args.objective, use_llm=use_llm, run_id=args.run_id)
    run_dir = orchestrator.save_run(run, append_memory=not args.no_memory)
    if args.execute_approved and not args.approval_file:
        raise SystemExit("--approval-file is required with --execute-approved")

    if args.propose_actions or args.execute_safe or args.write_approval_template or args.execute_approved:
        command_plan = build_command_plan(run.plan)
        if args.execute_safe:
            command_plan = execute_safe_commands(command_plan, root=orchestrator.root)
        if args.execute_approved:
            command_plan = execute_approved_commands(
                command_plan,
                approval_file=args.approval_file,
                root=orchestrator.root,
                include_safe=args.execute_safe,
            )
        command_plan = attach_feedback_candidates(command_plan, root=orchestrator.root)
        save_command_plan(command_plan, run_dir)
        if args.write_approval_template:
            save_approval_template(command_plan, run_dir)
    print(run_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
