from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, List

import yaml

from .schemas import StrategyProjectContext


DEFAULT_REPO_CAPABILITIES = [
    "qlib Alpha158 data pipeline",
    "custom and qlib-native model training",
    "benchmark-aware TopkDropout backtest with IR/alpha/tracking error",
    "walk-forward validation with statistical summaries",
    "factor screener and factor diagnostics",
    "signal postprocess: industry/size neutralization and stock-vs-sector filter",
    "regime strategy switch and drawdown-gated overlay",
    "scheduled rebalance dry-run and reminder cache",
]

DEFAULT_CONSTRAINTS = [
    "Use ./.venv/bin/python for local verification.",
    "Do not run full training, full WFV, live notifications, data updates, or real trading without user approval.",
    "Keep each experiment comparable: same benchmark, rank_metric, deal_price, cost and slippage assumptions unless explicitly varied.",
    "Treat same-model backtest uplift as a filter, not promotion evidence.",
    "Prefer disabled-by-default modular additions over invasive rewrites.",
]

SOURCE_PROJECT_SUMMARY = {
    "RD-Agent": {
        "essence": [
            "research loop: hypothesis -> experiment -> code/run -> feedback -> trace",
            "scenario abstraction keeps domain rules separate from generic loop control",
            "trace and knowledge base convert failed experiments into reusable context",
            "factor/model co-optimization can choose the next action by bandit, LLM, or random policy",
            "resume/checkpoint and step-level logging matter more than one-shot answers",
        ],
        "avoid_importing": [
            "heavy workspace injection and autonomous code generation by default",
            "Docker/session UI requirements for a local quant iteration module",
            "large qlib template system that duplicates quant_ex's existing pipeline",
        ],
    },
    "TradingAgents-ex": {
        "essence": [
            "role topology: analysts -> bull/bear debate -> trader -> risk debate -> portfolio manager",
            "state object carries reports between roles and makes handoffs explicit",
            "structured outputs on decision roles keep downstream parsing deterministic",
            "memory log stores decisions and later reflections for future prompts",
            "provider abstraction supports quick/deep model tiers and multiple endpoints",
        ],
        "avoid_importing": [
            "LangGraph dependency for a workflow that can be expressed as a small local DAG",
            "single-stock discretionary trading semantics",
            "live market/news tool calls inside strategy research planning",
        ],
    },
}


def _read_csv_tail(path: Path, limit: int) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    return rows[-limit:]


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return data if isinstance(data, dict) else {}


def _recent_paths(pattern: str, *, root: Path, limit: int) -> List[str]:
    files = sorted(root.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return [str(path.relative_to(root)) for path in files[:limit]]


def _summarize_csv(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    columns = list(rows[0].keys()) if rows else []
    summary: Dict[str, Any] = {
        "path": str(path),
        "row_count": len(rows),
        "columns": columns[:20],
    }
    metric = "information_ratio" if "information_ratio" in columns else "sharpe" if "sharpe" in columns else None
    if rows and metric:
        def metric_value(row: Dict[str, Any]) -> float:
            try:
                return float(row.get(metric) or 0)
            except (TypeError, ValueError):
                return 0.0

        best = max(rows, key=metric_value)
        keep = {
            "topk",
            "n_drop",
            "hold_thresh",
            "annual_return",
            "sharpe",
            "information_ratio",
            "max_drawdown",
            "rank_ic",
        }
        summary["rank_metric"] = metric
        summary["best_row"] = {key: best.get(key) for key in columns if key in keep}
    return summary


def _summarize_config(path: Path) -> Dict[str, Any]:
    data = _load_yaml(path)
    if not data:
        return {"path": str(path), "sections": []}
    summary = {
        "path": str(path),
        "sections": sorted(data.keys()),
        "strategy": data.get("strategy", {}),
        "backtest": data.get("backtest", {}),
        "signal": data.get("signal", {}),
        "portfolio": data.get("portfolio", {}),
    }
    return {key: value for key, value in summary.items() if value not in ({}, None)}


def _load_memory_tail(path: Path, limit: int = 3) -> List[str]:
    if not path.exists():
        return []
    text = path.read_text(encoding="utf-8")
    chunks = [chunk.strip() for chunk in text.split("<!-- AGENT_MEMORY_END -->") if chunk.strip()]
    return chunks[-limit:]


def build_project_context(
    objective: str,
    *,
    root: Path | str = ".",
    strategy_rows: int = 12,
    system_rows: int = 6,
) -> StrategyProjectContext:
    """Build a compact, local-only context for strategy role agents."""

    root = Path(root)
    context = StrategyProjectContext.new(objective)
    context.candidate_summary = _load_yaml(root / "config" / "strategy_candidates.yaml")
    context.recent_strategy_rows = _read_csv_tail(
        root / "docs" / "strategy_log" / "strategy_iteration_log.csv",
        strategy_rows,
    )
    context.recent_system_rows = _read_csv_tail(
        root / "docs" / "strategy_log" / "system_iteration_log.csv",
        system_rows,
    )
    context.available_artifacts = {
        "recent_models": _recent_paths("models/*.pkl", root=root, limit=5),
        "recent_backtests": _recent_paths("backtest_results/**/*.csv", root=root, limit=8),
        "config_candidates": _recent_paths("config/*.yaml", root=root, limit=12),
        "daily_commands": _recent_paths("command/daily/*.sh", root=root, limit=8),
    }
    context.artifact_summaries = {
        "recent_backtests": [
            _summarize_csv(root / item)
            for item in context.available_artifacts["recent_backtests"][:5]
        ],
    }
    context.config_summaries = {
        item: _summarize_config(root / item)
        for item in context.available_artifacts["config_candidates"][:8]
    }
    context.memory_context = _load_memory_tail(root / "docs" / "strategy_log" / "agent_memory.md")
    context.repo_capabilities = list(DEFAULT_REPO_CAPABILITIES)
    context.constraints = list(DEFAULT_CONSTRAINTS)
    context.source_projects = SOURCE_PROJECT_SUMMARY
    return context
