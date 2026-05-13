"""Multi-role agent loop for strategy research iteration."""

from .orchestrator import StrategyIterationOrchestrator
from .evaluator import generate_feedback, parse_metric_snapshot
from .execution import (
    attach_feedback_candidates,
    build_execution_summary,
    build_command_plan,
    execute_approved_commands,
    execute_safe_commands,
    save_approval_template,
    save_command_plan,
)
from .schemas import (
    AgentRole,
    CommandApproval,
    CommandExecutionPlan,
    CommandExecutionResult,
    CommandProposal,
    ExperimentArm,
    FeedbackCandidate,
    MetricSnapshot,
    RoleReport,
    StrategyFeedback,
    StrategyIterationPlan,
    StrategyProjectContext,
    StrategyIterationRun,
)
from .validation import validate_role_report

__all__ = [
    "AgentRole",
    "CommandApproval",
    "CommandExecutionPlan",
    "CommandExecutionResult",
    "CommandProposal",
    "ExperimentArm",
    "FeedbackCandidate",
    "MetricSnapshot",
    "RoleReport",
    "StrategyFeedback",
    "StrategyIterationOrchestrator",
    "StrategyIterationPlan",
    "StrategyProjectContext",
    "StrategyIterationRun",
    "attach_feedback_candidates",
    "build_execution_summary",
    "build_command_plan",
    "execute_approved_commands",
    "execute_safe_commands",
    "generate_feedback",
    "parse_metric_snapshot",
    "save_approval_template",
    "save_command_plan",
    "validate_role_report",
]
