"""Regime-aware strategy parameter switching.

Maps the current market regime (produced by RegimeFeatureEngine) to
overrides for TopkDropout strategy parameters.

Usage
-----
    from quant_ex.strategy.regime_switch import RegimeStrategySwitch

    switch = RegimeStrategySwitch.from_config(config)
    if switch is not None:
        regime = switch.detect_regime(price_data)
        params = switch.adjust({"topk": 15, "n_drop": 3, "hold_thresh": 5}, regime)
        # params now contains regime-specific overrides

Configuration (config/base.yaml)
--------------------------------
    strategy:
      regime_switch:
        enabled: true
        rules:
          0:  # calm_bull
            topk: 15
            n_drop: 3
            hold_thresh: 5
          1:  # calm_bear
            topk: 10
            n_drop: 1
            hold_thresh: 8
          2:  # volatile_bull
            topk: 12
            n_drop: 2
            hold_thresh: 5
          3:  # volatile_bear
            topk: 8
            n_drop: 1
            hold_thresh: 10
"""
from __future__ import annotations

import logging
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class RegimeStrategySwitch:
    """Adjust TopkDropout parameters based on detected market regime."""

    # Default rules used when config does not specify a regime
    _DEFAULT_RULES: Dict[int, Dict[str, int]] = {
        0: {"topk": 15, "n_drop": 3, "hold_thresh": 5},   # calm_bull
        1: {"topk": 10, "n_drop": 1, "hold_thresh": 8},   # calm_bear
        2: {"topk": 12, "n_drop": 2, "hold_thresh": 5},   # volatile_bull
        3: {"topk": 8,  "n_drop": 1, "hold_thresh": 10},  # volatile_bear
    }

    _REGIME_NAMES = {
        0: "calm_bull",
        1: "calm_bear",
        2: "volatile_bull",
        3: "volatile_bear",
    }

    def __init__(self, rules: Optional[Dict[int, Dict[str, int]]] = None):
        self.rules = rules if rules is not None else dict(self._DEFAULT_RULES)

    # ── public API ──────────────────────────────────────────────────────────────

    @classmethod
    def from_config(cls, config: dict) -> Optional["RegimeStrategySwitch"]:
        """Build switch from config dict; return None if disabled."""
        cfg = config.get("strategy", {}).get("regime_switch", {})
        if not cfg.get("enabled", False):
            return None
        rules = cfg.get("rules")
        if rules is not None:
            # YAML keys are strings; cast to int for regime labels
            rules = {int(k): v for k, v in rules.items()}
        return cls(rules=rules)

    def detect_regime(self, price_data: pd.DataFrame) -> int:
        """Run RegimeFeatureEngine on *price_data* and return latest regime label.

        Returns 0 (calm_bull) when computation fails, so the strategy falls
        back to base parameters gracefully.
        """
        try:
            from quant_ex.features.regime_features import RegimeFeatureEngine
        except ImportError as exc:
            logger.warning("RegimeFeatureEngine not available: %s", exc)
            return 0

        try:
            engine = RegimeFeatureEngine()
            regime_df = engine.compute(price_data)
        except Exception as exc:
            logger.warning("Regime detection failed: %s", exc)
            return 0

        if regime_df is None or regime_df.empty or "regime_label" not in regime_df.columns:
            logger.warning("Regime detection returned empty/invalid data; using default")
            return 0

        # regime_label is constant per date; pick the latest date
        labels_by_date = (
            regime_df["regime_label"]
            .groupby(level="datetime")
            .first()
            .sort_index()
        )
        latest_label = int(labels_by_date.iloc[-1])
        return latest_label

    def adjust(self, base_params: Dict[str, int], regime_label: int) -> Dict[str, int]:
        """Return a copy of *base_params* overridden by regime-specific rules.

        Only keys present in the rule (topk, n_drop, hold_thresh) are
        overridden; missing keys keep their base value.
        """
        result = dict(base_params)
        override = self.rules.get(regime_label, {})
        if override:
            result.update(override)
            name = self._REGIME_NAMES.get(regime_label, f"unknown({regime_label})")
            logger.info(
                "RegimeSwitch: detected %s → params %s",
                name,
                {k: result[k] for k in ("topk", "n_drop", "hold_thresh") if k in result},
            )
        return result

    def adjust_cfg(self, cfg: Dict[str, any], regime_label: int) -> Dict[str, any]:
        """Convenience: adjust a daily-rebalance cfg dict in-place.

        *cfg* is expected to contain ``topk``, ``n_drop``, ``hold_thresh`` keys.
        """
        base = {
            "topk": cfg.get("topk", 15),
            "n_drop": cfg.get("n_drop", 3),
            "hold_thresh": cfg.get("hold_thresh", 5),
        }
        adjusted = self.adjust(base, regime_label)
        cfg.update(adjusted)
        return cfg
