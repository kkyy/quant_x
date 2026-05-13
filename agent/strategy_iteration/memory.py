from __future__ import annotations

from pathlib import Path
from typing import Iterable, List

from .schemas import StrategyFeedback, StrategyIterationPlan


class StrategyAgentMemoryLog:
    """Append-only markdown memory log for strategy planning decisions."""

    separator = "\n\n<!-- AGENT_MEMORY_END -->\n\n"

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append_plan(self, plan: StrategyIterationPlan, approved_arms: Iterable[str]) -> None:
        entry = self._format_entry(plan, list(approved_arms))
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(entry + self.separator)

    def append_feedback(self, feedback: StrategyFeedback) -> None:
        entry = self._format_feedback_entry(feedback)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(entry + self.separator)

    def load_entries(self) -> List[str]:
        if not self.path.exists():
            return []
        text = self.path.read_text(encoding="utf-8")
        return [chunk.strip() for chunk in text.split(self.separator) if chunk.strip()]

    @staticmethod
    def _format_entry(plan: StrategyIterationPlan, approved_arms: List[str]) -> str:
        header = f"[{plan.generated_at} | {plan.run_id} | {approved_arms or ['no-approved-arms']}]"
        return "\n".join(
            [
                header,
                "",
                "DECISION:",
                plan.synthesis,
                "",
                "APPROVED_ARMS:",
                ", ".join(approved_arms) if approved_arms else "none",
                "",
                "NEXT_ACTIONS:",
                *[f"- {item}" for item in plan.next_actions],
            ]
        )

    @staticmethod
    def _format_feedback_entry(feedback: StrategyFeedback) -> str:
        return "\n".join(
            [
                f"[{feedback.generated_at} | {feedback.run_id} | outcome | {feedback.decision}]",
                "",
                "OUTCOME:",
                feedback.reflection,
                "",
                "HYPOTHESIS_EVALUATION:",
                feedback.hypothesis_evaluation,
                "",
                "OBSERVATIONS:",
                *[f"- {item}" for item in feedback.observations],
                "",
                "NEXT_ABLATION:",
                feedback.next_ablation,
            ]
        )
