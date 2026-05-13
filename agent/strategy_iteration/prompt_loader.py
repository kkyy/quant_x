from __future__ import annotations

from pathlib import Path
from typing import Dict


PROMPT_FILES = {
    "shared_system": "shared_system.md",
    "data_factor_analyst": "data_factor_analyst.md",
    "model_analyst": "model_analyst.md",
    "backtest_analyst": "backtest_analyst.md",
    "execution_analyst": "execution_analyst.md",
    "bull_researcher": "bull_researcher.md",
    "bear_researcher": "bear_researcher.md",
    "research_manager": "research_manager.md",
    "experiment_designer": "experiment_designer.md",
    "aggressive_risk_reviewer": "aggressive_risk_reviewer.md",
    "conservative_risk_reviewer": "conservative_risk_reviewer.md",
    "neutral_risk_reviewer": "neutral_risk_reviewer.md",
    "research_portfolio_manager": "research_portfolio_manager.md",
}


def prompt_dir() -> Path:
    return Path(__file__).resolve().parent / "prompts"


def load_prompt(name: str) -> str:
    filename = PROMPT_FILES[name]
    return (prompt_dir() / filename).read_text(encoding="utf-8")


def load_prompt_catalog() -> Dict[str, str]:
    return {name: load_prompt(name) for name in PROMPT_FILES}
