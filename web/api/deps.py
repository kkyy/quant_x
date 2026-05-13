"""Shared FastAPI dependencies."""
from pathlib import Path
from functools import lru_cache
from quant_ex.utils.config import load_config


@lru_cache(maxsize=1)
def get_config() -> dict:
    return load_config()


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
MODELS_DIR = PROJECT_ROOT / "models"
CACHE_DIR = PROJECT_ROOT / "cache"
SIGNALS_DIR = PROJECT_ROOT / "signals"
BACKTEST_RESULTS_DIR = PROJECT_ROOT / "backtest_results"
LOGS_DIR = PROJECT_ROOT / "logs"
CONFIG_DIR = PROJECT_ROOT / "config"
AGENT_RUNS_DIR = PROJECT_ROOT / "docs" / "strategy_log" / "agent_runs"
