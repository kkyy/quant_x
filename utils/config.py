"""Configuration loading with deep-merge support."""
from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, Optional
import yaml
import logging

logger = logging.getLogger(__name__)

_CONFIG_DIR = Path(__file__).parent.parent / "config"
_SUB_FILES = ["model.yaml", "strategy.yaml", "notify.yaml"]


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load and merge YAML configs.
    Load order: base.yaml → model.yaml → strategy.yaml → notify.yaml → user override.
    Later files win on conflicts.
    """
    config: Dict[str, Any] = {}

    base = _CONFIG_DIR / "base.yaml"
    if base.exists():
        config = yaml.safe_load(base.read_text(encoding="utf-8")) or {}

    for sub in _SUB_FILES:
        p = _CONFIG_DIR / sub
        if p.exists():
            _deep_merge(config, yaml.safe_load(p.read_text(encoding="utf-8")) or {})

    if config_path:
        user = Path(config_path)
        if user.exists():
            _deep_merge(config, yaml.safe_load(user.read_text(encoding="utf-8")) or {})
            logger.info(f"Loaded user config: {config_path}")
        else:
            logger.warning(f"Config file not found: {config_path}")

    return config


def deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge *override* into *base* (mutates *base*).

    Dict values are merged recursively; all other types are replaced.
    Returns *base* for convenience.
    """
    for k, v in override.items():
        if k in base and isinstance(base[k], dict) and isinstance(v, dict):
            deep_merge(base[k], v)
        else:
            base[k] = v
    return base


# Backward-compatible alias used internally by this module.
_deep_merge = deep_merge
