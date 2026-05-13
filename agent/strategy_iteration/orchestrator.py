from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import yaml

from .context import build_project_context
from .memory import StrategyAgentMemoryLog
from .prompt_loader import load_prompt_catalog
from .roles import DEFAULT_ROLES, RoleRunner
from .schemas import AgentRole, ExperimentArm, RoleReport, StrategyIterationPlan, StrategyIterationRun


class StrategyIterationOrchestrator:
    """Lightweight multi-role strategy iteration planner.

    This intentionally does not execute training, WFV, data fetches, notifications,
    or code generation. It creates a structured experiment plan that the existing
    quant_ex pipeline can validate.
    """

    def __init__(
        self,
        *,
        root: Path | str = ".",
        roles: Optional[Iterable[AgentRole]] = None,
        output_dir: Path | str = "docs/strategy_log/agent_runs",
        memory_log_path: Path | str = "docs/strategy_log/agent_memory.md",
        llm_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.root = Path(root)
        self.roles = list(roles or DEFAULT_ROLES)
        self.llm_config = llm_config or {}
        self.output_dir = Path(output_dir)
        if not self.output_dir.is_absolute():
            self.output_dir = self.root / self.output_dir
        self.memory_log_path = Path(memory_log_path)
        if not self.memory_log_path.is_absolute():
            self.memory_log_path = self.root / self.memory_log_path

    @classmethod
    def from_config(cls, config_path: Path | str = "config/agent_strategy_iteration.yaml") -> "StrategyIterationOrchestrator":
        path = Path(config_path)
        if not path.exists() and path.name == "agent_strategy_iteration.yaml":
            example_path = path.with_name("agent_strategy_iteration.example.yaml")
            if example_path.exists():
                path = example_path
        data = yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else {}
        data = data or {}
        roles = [
            AgentRole(
                name=item["name"],
                mission=item["mission"],
                perspective=item.get("perspective", ""),
                model_tier=item.get("model_tier", "quick"),
                required_outputs=item.get("required_outputs", []),
            )
            for item in data.get("roles", [])
        ] or None
        return cls(
            root=data.get("root", "."),
            roles=roles,
            output_dir=data.get("output_dir", "docs/strategy_log/agent_runs"),
            memory_log_path=data.get("memory_log_path", "docs/strategy_log/agent_memory.md"),
            llm_config=data.get("llm", {}),
        )

    def build_run(
        self,
        objective: str,
        *,
        use_llm: bool = False,
        run_id: Optional[str] = None,
    ) -> StrategyIterationRun:
        context = build_project_context(objective, root=self.root)
        runner = RoleRunner(self.roles, llm_config=self.llm_config)
        reports = runner.run_all(context, use_llm=use_llm)
        now = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        run_id = run_id or f"agent_strategy_{now}"
        arms = self._build_experiment_arms(reports)
        plan = StrategyIterationPlan(
            run_id=run_id,
            objective=objective,
            generated_at=datetime.now().isoformat(timespec="seconds"),
            role_reports=reports,
            experiment_arms=arms,
            validation_ladder=[
                "./.venv/bin/python -c \"from agent.strategy_iteration import StrategyIterationOrchestrator; print('OK')\"",
                "./.venv/bin/python run_train.py --list-registry",
                "./.venv/bin/python -m pytest test/test_backtest_metrics.py test/test_grid_search.py test/test_walk_forward_validation.py",
                "./.venv/bin/python -m pytest test/test_agent_strategy_iteration.py",
                "Same-model backtest for each surviving arm with identical benchmark/rank_metric/deal_price/cost/slippage.",
                "WFV only after Phase-1 evidence and explicit user approval.",
            ],
            decision_gates=[
                "Reject if import, registry, or focused tests fail.",
                "Reject if same-model backtest is clearly worse than the control without a risk-control benefit.",
                "Max decision is compare_next until WFV confirms robustness.",
                "Promotion requires better IR or Sharpe with no material drawdown, turnover, concentration, or cost regression.",
                "Any live notification, data update, full WFV, or trading-like action requires explicit user approval.",
            ],
            synthesis=self._synthesize(reports, arms),
            next_actions=[
                "Review the generated arms and choose one implementation target.",
                "Implement only the chosen arm with disabled-by-default config where possible.",
                "Run the validation ladder from cheapest to most expensive.",
            ],
        )
        return StrategyIterationRun(
            run_id=run_id,
            objective=objective,
            generated_at=plan.generated_at,
            context=context,
            plan=plan,
            prompts=load_prompt_catalog(),
            role_traces=runner.traces,
        )

    def create_plan(
        self,
        objective: str,
        *,
        use_llm: bool = False,
        run_id: Optional[str] = None,
    ) -> StrategyIterationPlan:
        return self.build_run(objective, use_llm=use_llm, run_id=run_id).plan

    def save_run(self, run: StrategyIterationRun, *, append_memory: bool = True) -> Path:
        run_dir = self.output_dir / run.run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "run.json").write_text(json.dumps(run.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
        (run_dir / "plan.md").write_text(run.plan.to_markdown(), encoding="utf-8")
        (run_dir / "context.json").write_text(
            json.dumps(run.context.to_prompt_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (run_dir / "prompts.json").write_text(json.dumps(run.prompts, ensure_ascii=False, indent=2), encoding="utf-8")
        (run_dir / "role_traces.json").write_text(
            json.dumps(run.role_traces, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (run_dir / "role_traces.md").write_text(self._role_traces_to_markdown(run), encoding="utf-8")
        if append_memory:
            approved = [arm.arm_id for arm in run.plan.experiment_arms if not arm.requires_approval]
            StrategyAgentMemoryLog(self.memory_log_path).append_plan(run.plan, approved)
        return run_dir

    @staticmethod
    def _role_traces_to_markdown(run: StrategyIterationRun) -> str:
        lines = [
            f"# Agent Role Traces: {run.run_id}",
            "",
            f"- Generated: {run.generated_at}",
            f"- Objective: {run.objective}",
            "",
            "Full prompts and parsed payloads are available in `role_traces.json`.",
            "",
        ]
        for trace in run.role_traces:
            report = trace.get("parsed_report") or {}
            lines.extend(
                [
                    f"## {trace.get('role')}",
                    "",
                    f"- Used LLM: {trace.get('used_llm')}",
                    f"- Model tier: {trace.get('model_tier')}",
                    f"- Model: {trace.get('model') or 'offline'}",
                    f"- Reasoning effort: {trace.get('reasoning_effort') or 'none'}",
                    f"- Upstream roles: {', '.join(trace.get('upstream_roles') or []) or 'none'}",
                    f"- System prompt chars: {len(trace.get('system_prompt') or '')}",
                    f"- User prompt chars: {len(trace.get('user_prompt') or '')}",
                    "",
                    "Thesis:",
                    str(report.get("thesis") or ""),
                    "",
                    "Evidence:",
                    *[f"- {item}" for item in report.get("evidence", [])],
                    "Proposals:",
                    *[f"- {item}" for item in report.get("proposals", [])],
                    "Risks:",
                    *[f"- {item}" for item in report.get("risks", [])],
                    f"Verdict: {report.get('verdict')} | Confidence: {report.get('confidence')}",
                    "",
                ]
            )
        return "\n".join(lines) + "\n"

    @staticmethod
    def _build_experiment_arms(reports: list[RoleReport]) -> list[ExperimentArm]:
        return [
            ExperimentArm(
                arm_id="phase1_control_bundle",
                hypothesis="Reproduce the current durable baseline before testing any treatment arm.",
                change_type="control",
                owner_role="backtest_analyst",
                config_path="config/daily_csi1000.yaml",
                validation_commands=[
                    "./.venv/bin/python run_agent_strategy_iteration.py --objective \"phase1 control\" --no-llm --no-memory",
                ],
                success_criteria=[
                    "CLI builds a run bundle without using network or live execution paths.",
                    "Saved run contains context, prompts, and plan artifacts.",
                ],
                risk_notes=["This control validates the planner itself, not a strategy alpha claim."],
            ),
            ExperimentArm(
                arm_id="phase1_prompt_context_layer",
                hypothesis="Prompt discipline and explicit context packs will make later multi-role LLM runs auditable and less likely to hallucinate.",
                change_type="prompt_context",
                owner_role="data_factor_analyst",
                target_files=["agent/strategy_iteration/prompts/", "agent/strategy_iteration/context.py"],
                validation_commands=[
                    "./.venv/bin/python -m pytest test/test_agent_strategy_iteration.py",
                ],
                success_criteria=[
                    "Prompt catalog loads cleanly and is saved with each run.",
                    "Context pack includes candidates, recent logs, and available artifact snapshots.",
                ],
                risk_notes=["Prompt quality still needs later iteration once LLM mode is exercised."],
            ),
            ExperimentArm(
                arm_id="phase1_memory_layer",
                hypothesis="An append-only research memory log will make future agent runs cumulative without polluting durable strategy logs.",
                change_type="research_memory",
                owner_role="execution_analyst",
                target_files=["agent/strategy_iteration/memory.py", "docs/strategy_log/agent_memory.md"],
                validation_commands=[
                    "./.venv/bin/python -m pytest test/test_agent_strategy_iteration.py",
                ],
                success_criteria=[
                    "Planner can append a structured memory entry without writing secrets or mutable metrics.",
                    "Memory entries stay separate from strategy_iteration_log.csv.",
                ],
                risk_notes=["Memory is advisory only until later phases add validated outcome reflections."],
            ),
            ExperimentArm(
                arm_id="phase1_optional_llm_gateway",
                hypothesis="Optional OpenAI-compatible role execution can be added safely if it remains explicit, offline-by-default, and secret-clean.",
                change_type="optional_llm",
                owner_role="research_portfolio_manager",
                target_files=["agent/strategy_iteration/llm.py", "run_agent_strategy_iteration.py", "config/agent_strategy_iteration.yaml"],
                validation_commands=[
                    "./.venv/bin/python -c \"from agent.strategy_iteration.llm import OpenAICompatibleChatClient; print(OpenAICompatibleChatClient.from_env(model_tier='quick').model)\"",
                ],
                success_criteria=[
                    "LLM mode stays opt-in and degrades cleanly to offline planning if env vars are absent.",
                    "No API key is written into run bundles or logs.",
                ],
                risk_notes=["Do not auto-enable LLM mode in scripts or tests."],
            ),
        ]

    @staticmethod
    def _synthesize(reports: list[RoleReport], arms: list[ExperimentArm]) -> str:
        supportive = [r for r in reports if _is_supportive_research_verdict(r.verdict)]
        rejecting = [r for r in reports if _is_rejecting_research_verdict(r.verdict)]
        return (
            "The recommended adaptation is a modular agentic planning layer, not a heavy autonomous trading system. "
            "It borrows RD-Agent's hypothesis-experiment-feedback trace and TradingAgents-ex's analyst/debate/risk/manager roles, "
            f"then emits {len(arms)} controlled experiment arms for quant_ex's existing validation stack. "
            f"{len(supportive)} roles support continued research and {len(rejecting)} roles recommend rejection, "
            "with explicit approval gates for expensive or externally impactful work."
        )


def _normalize_verdict(value: str) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def _is_supportive_research_verdict(value: str) -> bool:
    verdict = _normalize_verdict(value)
    supportive_tokens = (
        "approve",
        "continue",
        "support",
        "compare_next",
        "comparenext",
        "research_budget_worthy",
        "hold",
        "keep",
    )
    rejecting_tokens = ("reject", "do_not_promote", "downgrade", "stop")
    return any(token in verdict for token in supportive_tokens) and not any(
        token in verdict for token in rejecting_tokens
    )


def _is_rejecting_research_verdict(value: str) -> bool:
    verdict = _normalize_verdict(value)
    return any(token in verdict for token in ("reject", "do_not_promote", "downgrade", "stop"))
