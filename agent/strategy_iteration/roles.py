from __future__ import annotations

import json
from typing import Any, Dict, Iterable, List, Optional

from .llm import OpenAICompatibleChatClient
from .prompt_loader import load_prompt
from .schemas import AgentRole, RoleReport, StrategyProjectContext
from .validation import attach_role_metadata


DEFAULT_ROLES: List[AgentRole] = [
    AgentRole(
        name="data_factor_analyst",
        mission="Audit data coverage, factor redundancy, lag safety, and orthogonal evidence opportunities.",
        perspective="Data-centric quant analyst influenced by RD-Agent's factor trace discipline.",
        required_outputs=["findings", "hypotheses", "screening plan", "leakage risks"],
    ),
    AgentRole(
        name="model_analyst",
        mission="Judge whether model changes are justified versus data, factor, or hyperparameter causes.",
        perspective="Quant model reviewer focused on avoiding costly architecture churn without evidence.",
        required_outputs=["diagnosis", "model hypotheses", "training risks"],
    ),
    AgentRole(
        name="backtest_analyst",
        mission="Protect experimental comparability across benchmark, rank metric, deal price, cost, and time windows.",
        perspective="Validation specialist who assumes apparent gains may be measurement artifacts.",
        required_outputs=["comparability checks", "control arm", "validation ladder"],
    ),
    AgentRole(
        name="execution_analyst",
        mission="Review implementation risk around turnover, concentration, liquidity, rebalance, and notification flows.",
        perspective="Execution engineer who protects real-world operability.",
        required_outputs=["execution constraints", "risk controls", "approval gates"],
    ),
    AgentRole(
        name="bull_researcher",
        mission="Make the strongest case for the most promising strategy upgrade path.",
        perspective="Debate role inspired by TradingAgents-ex bull researcher.",
        required_outputs=["supporting case", "expected upside", "why now"],
    ),
    AgentRole(
        name="bear_researcher",
        mission="Attack the proposal for overfit, leakage, redundancy, regime fragility, and operational traps.",
        perspective="Debate role inspired by TradingAgents-ex bear researcher.",
        required_outputs=["failure modes", "kill tests", "do-not-promote conditions"],
    ),
    AgentRole(
        name="research_manager",
        mission="Turn analyst and debate output into a compact research rating and experiment brief.",
        perspective="Research judge using explicit promotion criteria instead of vibes.",
        model_tier="deep",
        required_outputs=["rating", "why", "preferred directions"],
    ),
    AgentRole(
        name="experiment_designer",
        mission="Translate the research brief into a control arm and narrowly-scoped treatment arms.",
        perspective="Experiment designer who enforces one-major-variable-per-arm.",
        required_outputs=["control arm", "treatment arms", "commands", "success criteria"],
    ),
    AgentRole(
        name="aggressive_risk_reviewer",
        mission="Argue why a bolder experiment is worth the research budget.",
        perspective="High-upside risk reviewer from the aggressive side of the risk triangle.",
        required_outputs=["upside justification", "acceptable risks", "expansion triggers"],
    ),
    AgentRole(
        name="conservative_risk_reviewer",
        mission="Argue why the plan should be scaled back, delayed, or rejected.",
        perspective="Capital-preserving risk reviewer from the conservative side of the risk triangle.",
        required_outputs=["blockers", "downside scenarios", "approval blockers"],
    ),
    AgentRole(
        name="neutral_risk_reviewer",
        mission="Find the smallest reliable validation path between ambition and caution.",
        perspective="Balanced risk reviewer looking for cheaper and cleaner evidence.",
        required_outputs=["compromise path", "phased validation", "decision conditions"],
    ),
    AgentRole(
        name="research_portfolio_manager",
        mission="Make the final research-capital allocation decision and choose approved experiment arms.",
        perspective="Portfolio manager for strategy research effort, not live trading capital.",
        model_tier="deep",
        required_outputs=["decision", "approved arms", "blocked arms", "validation ladder"],
    ),
]


class RoleRunner:
    """Runs strategy roles with optional LLM calls and deterministic fallback."""

    def __init__(
        self,
        roles: Optional[Iterable[AgentRole]] = None,
        *,
        llm_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.roles = list(roles or DEFAULT_ROLES)
        self.llm_config = llm_config or {}
        self.traces: List[Dict[str, Any]] = []

    def run_all(
        self,
        context: StrategyProjectContext,
        *,
        use_llm: bool = False,
    ) -> List[RoleReport]:
        self.traces = []
        reports: List[RoleReport] = []
        for role in self.roles:
            reports.append(self.run_role(role, context, reports, use_llm=use_llm))
        return reports

    def run_role(
        self,
        role: AgentRole,
        context: StrategyProjectContext,
        prior_reports: List[RoleReport],
        *,
        use_llm: bool = False,
    ) -> RoleReport:
        system, user = self._build_prompt(role, context, prior_reports)
        if use_llm:
            client = OpenAICompatibleChatClient.from_env(model_tier=role.model_tier, llm_config=self.llm_config)
            if client.is_configured:
                payload = client.complete_json(system=system, user=user)
                report = RoleReport.from_dict(
                    role.name,
                    payload,
                    raw_response=json.dumps(payload, ensure_ascii=False),
                )
                report = attach_role_metadata(role, report, prior_reports)
                self.traces.append(
                    self._build_trace(
                        role=role,
                        prior_reports=prior_reports,
                        system_prompt=system,
                        user_prompt=user,
                        report=report,
                        used_llm=True,
                        client=client,
                    )
                )
                return report
        report = attach_role_metadata(role, self._fallback_report(role, context, prior_reports), prior_reports)
        self.traces.append(
            self._build_trace(
                role=role,
                prior_reports=prior_reports,
                system_prompt=system,
                user_prompt=user,
                report=report,
                used_llm=False,
                client=None,
            )
        )
        return report

    @staticmethod
    def _build_trace(
        *,
        role: AgentRole,
        prior_reports: List[RoleReport],
        system_prompt: str,
        user_prompt: str,
        report: RoleReport,
        used_llm: bool,
        client: Optional[OpenAICompatibleChatClient],
    ) -> Dict[str, Any]:
        return {
            "role": role.name,
            "model_tier": role.model_tier,
            "used_llm": used_llm,
            "model": client.model if client else None,
            "reasoning_effort": client.reasoning_effort if client else None,
            "temperature": client.temperature if client else None,
            "max_tokens": client.max_tokens if client else None,
            "stream": client.stream if client else None,
            "upstream_roles": [item.role for item in prior_reports],
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
            "raw_response": report.raw_response,
            "parsed_report": report.to_dict(),
        }

    @staticmethod
    def _build_prompt(
        role: AgentRole,
        context: StrategyProjectContext,
        prior_reports: List[RoleReport],
    ) -> tuple[str, str]:
        system = (
            load_prompt("shared_system").strip()
            + "\n\n"
            + load_prompt(role.name).strip()
            + "\n\nOutput strictly valid JSON with keys: role, thesis, evidence, proposals, risks, verdict, confidence, next_actions, prompt_name. "
            "Keep proposals testable and compatible with the local framework."
        )
        user = json.dumps(
            {
                "role": role.to_dict(),
                "context": context.to_prompt_dict(),
                "prior_reports": [r.to_dict() for r in prior_reports],
                "upstream_role_order": [r.role for r in prior_reports],
            },
            ensure_ascii=False,
            default=str,
        )
        return system, user

    @staticmethod
    def _fallback_report(
        role: AgentRole,
        context: StrategyProjectContext,
        prior_reports: List[RoleReport],
    ) -> RoleReport:
        selected = context.candidate_summary.get("selected", {}) if context.candidate_summary else {}
        control = selected.get("conservative_candidate") or selected.get("stability_candidate") or "csi1000_balanced"
        active = selected.get("stability_candidate") or selected.get("active_candidate") or control
        memory_note = context.memory_context[-1].splitlines()[0] if context.memory_context else "no prior agent memory"
        backtest_count = len(context.artifact_summaries.get("recent_backtests", []))

        templates: Dict[str, RoleReport] = {
            "data_factor_analyst": RoleReport(
                role=role.name,
                prompt_name=role.name,
                thesis="The next durable alpha improvement is more likely to come from orthogonal evidence than from more Alpha158-like feature churn.",
                evidence=[
                    "Recent logs show repeated same-model uplifts that did not survive WFV.",
                    "The project already contains broad factor families and a factor screener.",
                    "Fundamental gating experiments demonstrated how easy it is to overread same-model improvements.",
                    f"Phase 2 context includes {backtest_count} recent backtest artifact summaries.",
                ],
                proposals=[
                    "Prioritize proposals that specify point-in-time lag, cache policy, and redundancy checks.",
                    "Do not promote any new factor family without an explicit IC/ICIR and correlation screening plan.",
                    "Keep news/sentiment or alternative data as future plugin roles instead of first-pass hard dependencies.",
                ],
                risks=[
                    "Agent-generated factor ideas can duplicate Alpha158 signals under new names.",
                    "Coverage gaps and lag mistakes are more dangerous than missing a fashionable factor family.",
                ],
                verdict="continue",
                confidence=0.77,
                next_actions=["Require each future factor proposal to include orthogonality and lag claims."],
            ),
            "model_analyst": RoleReport(
                role=role.name,
                prompt_name=role.name,
                thesis="Model changes should stay secondary until there is concrete evidence that the current LGBM line is the bottleneck.",
                evidence=[
                    "Past system iterations found LGBM plus Alpha158 to be the most validated line so far.",
                    "Alternative models and extra factors already degraded WFV in several recent cycles.",
                    f"Current durable controls include {control} and {active}.",
                ],
                proposals=[
                    "Keep model experiments narrow: hyperparameters, ranking objective, or stability checks before architecture changes.",
                    "Require a diagnosis explaining why a model change is needed instead of a data or execution change.",
                ],
                risks=[
                    "A larger model can increase training cost without improving generalization.",
                    "Model experiments easily mask comparability issues when dates or cost assumptions shift.",
                ],
                verdict="continue",
                confidence=0.72,
                next_actions=["Default the first agentic loop to planning and validation, not model rewrites."],
            ),
            "backtest_analyst": RoleReport(
                role=role.name,
                prompt_name=role.name,
                thesis="The fastest way to destroy signal quality is to compare non-equivalent backtests and mistake the delta for alpha.",
                evidence=[
                    "This repo already treats benchmark-aware IR ranking and deal_price consistency as first-class concerns.",
                    "Recent conclusions explicitly warn against mixing Sharpe-only and IR-ranked results.",
                    "The Phase 2 context pack carries CSV summaries instead of only file paths.",
                ],
                proposals=[
                    "Every plan must define control, metric priority, benchmark, rank_metric, deal_price, cost, and slippage.",
                    "Same-model backtest remains a filter only; promotion evidence comes from WFV after approval.",
                ],
                risks=["Without a hard control arm, multi-role output becomes prose with no experiment value."],
                verdict="continue",
                confidence=0.85,
                next_actions=["Encode comparability checks into every saved agent plan."],
            ),
            "execution_analyst": RoleReport(
                role=role.name,
                prompt_name=role.name,
                thesis="Execution and rebalance semantics deserve their own review before any experiment is considered promotable.",
                evidence=[
                    "Overlay branches have already shown concentration and regime sensitivity.",
                    "Scheduled rebalance, hold protection, and reminders can have external effects or misleading dry-run assumptions.",
                    f"Latest agent memory marker: {memory_note}.",
                ],
                proposals=[
                    "Tag any plan that touches scheduled rebalance, live-like reminders, or data updates as approval-gated.",
                    "Require concentration, turnover, cost, and liquidity notes alongside Sharpe and IR.",
                ],
                risks=["A planner must never imply it can safely trigger live notifications or trading semantics."],
                verdict="continue",
                confidence=0.81,
                next_actions=["Make approval flags explicit in every experiment arm."],
            ),
            "bull_researcher": RoleReport(
                role=role.name,
                prompt_name=role.name,
                thesis="A role-based research layer can raise experiment quality without forcing a wholesale framework rewrite.",
                evidence=[
                    "RD-Agent contributes a stronger hypothesis-feedback discipline than the current manual iteration loop.",
                    "TradingAgents-ex contributes structured adversarial review before the final decision.",
                    f"The bull role receives {len(prior_reports)} upstream reports.",
                ],
                proposals=[
                    "Use the agent layer to surface better experiments before spending WFV budget.",
                    "Keep the underlying quant_ex execution path unchanged for trust and reuse.",
                ],
                risks=["Process quality improves immediately, but alpha improves only if execution discipline stays strict."],
                verdict="support",
                confidence=0.78,
                next_actions=["Approve Phase 1 infrastructure so later experiments are cleaner."],
            ),
            "bear_researcher": RoleReport(
                role=role.name,
                prompt_name=role.name,
                thesis="The easiest failure mode is letting convincing multi-agent prose outrun actual validation evidence.",
                evidence=[
                    "This project already has evidence that same-model improvements often fail WFV.",
                    "Role systems can drift into non-comparable bundles when each role proposes different changes.",
                    f"The bear role can inspect {len(prior_reports)} upstream reports before objecting.",
                ],
                proposals=[
                    "Require one-major-variable-per-arm and an explicit kill test for each treatment.",
                    "Do not let Phase 1 execute expensive commands automatically.",
                ],
                risks=[
                    "Memory logs can create false confidence if they are confused with durable strategy evidence.",
                    "A planner that writes no prompts and no context pack will be hard to audit later.",
                ],
                verdict="continue_with_gates",
                confidence=0.86,
                next_actions=["Bake approval gates and context snapshots into the saved run bundle."],
            ),
            "research_manager": RoleReport(
                role=role.name,
                prompt_name=role.name,
                thesis="The proposal earns a `CompareNext` style research rating: valuable enough to integrate as infrastructure, but not yet evidence of strategy improvement.",
                evidence=[
                    "All analyst and debate roles point toward the same near-term need: a disciplined planning layer.",
                    "No role argues for immediate autonomous execution or model rewriting.",
                    f"Upstream role count at manager stage: {len(prior_reports)}.",
                ],
                proposals=[
                    "Implement Phase 1 as offline planner plus prompt system, memory log, CLI, and tests.",
                    "Delay expensive execution hooks until the planner proves stable.",
                ],
                risks=["Infrastructure progress should not be logged as alpha progress."],
                verdict="compare_next",
                confidence=0.84,
                next_actions=["Translate this brief into a concrete phased implementation plan."],
            ),
            "experiment_designer": RoleReport(
                role=role.name,
                prompt_name=role.name,
                thesis="The first phase should implement planning artifacts, not live experiment execution automation.",
                evidence=[
                    "The repo already has stable training/backtest/WFV commands to reference in generated plans.",
                    "A run bundle with prompts, context, plan, and memory is enough to test the research layer.",
                    "Schema warnings are retained in role reports instead of being discarded.",
                ],
                proposals=[
                    "Create one control arm and three infrastructure-oriented treatment arms for the agent layer itself.",
                    "Save JSON, Markdown, context pack, and prompt catalog per run.",
                ],
                risks=["If Phase 1 tries to run experiments, it will inherit too many approval and cost concerns."],
                verdict="continue",
                confidence=0.82,
                next_actions=["Implement CLI and save-run bundle before any execution adapters."],
            ),
            "aggressive_risk_reviewer": RoleReport(
                role=role.name,
                prompt_name=role.name,
                thesis="It is worth spending engineering effort now because the research layer can improve every future iteration cycle.",
                evidence=[
                    "The current strategy process already has enough history to benefit from structured memory.",
                    "Prompted adversarial review can prevent wasting future WFV cycles.",
                    f"Aggressive review receives {len(prior_reports)} upstream reports.",
                ],
                proposals=[
                    "Land prompt files and memory now, so later LLM use has a disciplined frame.",
                    "Keep optional LLM support available behind explicit flags.",
                ],
                risks=["There is moderate implementation cost, but little market or data risk in Phase 1."],
                verdict="support",
                confidence=0.74,
                next_actions=["Include the prompt catalog in the run bundle for auditability."],
            ),
            "conservative_risk_reviewer": RoleReport(
                role=role.name,
                prompt_name=role.name,
                thesis="The safe path is to keep Phase 1 offline, deterministic where possible, and locally testable.",
                evidence=[
                    "No new runtime should be introduced beyond the existing repo dependencies.",
                    "LLM calls are optional and must never write secrets to disk.",
                    "Role schema validation is warning-only in Phase 2, keeping offline planning resilient.",
                ],
                proposals=[
                    "Persist only local context and plan outputs.",
                    "Add tests before any future execution adapters are considered.",
                ],
                risks=["Even infrastructure changes can become noisy if run bundles are inconsistent."],
                verdict="support_with_limits",
                confidence=0.88,
                next_actions=["Add deterministic tests around context, save-run, and memory append."],
            ),
            "neutral_risk_reviewer": RoleReport(
                role=role.name,
                prompt_name=role.name,
                thesis="The balanced path is a phased rollout: planning first, optional LLM second, execution adapters later.",
                evidence=[
                    "This captures the best of both source projects while staying small.",
                    "The repo already has enough local context to make an offline planner useful immediately.",
                    "Phase 2 adds richer context without coupling to expensive command execution.",
                ],
                proposals=[
                    "Document the implementation phases inside the repo.",
                    "Use the CLI itself as the first integration boundary.",
                ],
                risks=["Skipping the phased rollout would couple planning, execution, and validation too early."],
                verdict="support",
                confidence=0.90,
                next_actions=["Ship an implementation roadmap alongside the code."],
            ),
            "research_portfolio_manager": RoleReport(
                role=role.name,
                prompt_name=role.name,
                thesis="Approve Phase 1 infrastructure and block anything resembling autonomous expensive execution.",
                evidence=[
                    "The plan is modular, testable, and consistent with the repo's current validation discipline.",
                    "All major risks can be contained with offline defaults and explicit approval gates.",
                    f"Final decision sees {len(prior_reports)} upstream role reports and preserved schema warnings.",
                ],
                proposals=[
                    "Land implementation plan, prompt system, context builder, memory log, CLI, and tests.",
                    "Record the system iteration as an infrastructure cycle with no expected Sharpe change.",
                ],
                risks=["Research memory must stay advisory until fed by validated outcomes."],
                verdict="approve_phase1",
                confidence=0.91,
                next_actions=["Save a sample run bundle and update the system iteration log."],
            ),
        }
        return templates[role.name]
