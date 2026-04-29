"""Backtest execution engine wrapping qlib's backtest_daily."""
from __future__ import annotations
import hashlib
import logging
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


def stabilize_signal(
    pred: pd.Series,
    tie_breaker_epsilon: float = 1e-12,
) -> pd.Series:
    """Return prediction signal in deterministic order with stable tie breaks."""
    if pred is None or pred.empty:
        return pred

    signal = pred.dropna().copy()
    if not isinstance(signal.index, pd.MultiIndex):
        return signal.sort_index(kind="mergesort")

    names = list(signal.index.names)
    if "datetime" in names and "instrument" in names:
        datetime_level = names.index("datetime")
        instrument_level = names.index("instrument")
        frame = signal.rename("score").reset_index()
        frame["_instrument_key"] = frame["instrument"].astype(str)
        frame = frame.sort_values(
            ["datetime", "_instrument_key"],
            kind="mergesort",
        )
        if tie_breaker_epsilon and tie_breaker_epsilon > 0:
            offsets = frame["_instrument_key"].map(_stable_offset)
            frame["score"] = frame["score"].astype(float) + offsets * tie_breaker_epsilon
        frame = frame.drop(columns=["_instrument_key"])
        signal = frame.set_index(names)["score"]
        return signal.reorder_levels(names).sort_index(
            level=[datetime_level, instrument_level],
            kind="mergesort",
        )

    return signal.sort_index(kind="mergesort")


def _stable_offset(value: str) -> float:
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest()
    integer = int.from_bytes(digest, "big")
    return integer / ((1 << 64) - 1) - 0.5


class BacktestEngine:
    """
    Thin wrapper around qlib's backtest_daily.
    Accepts strategy_params dict so grid_search can swap parameters easily.
    """

    def __init__(self, config: dict):
        self.config = config
        bt = config.get("backtest", {})
        self._defaults = {
            "account":    bt.get("account", 1_000_000),
            "open_cost":  bt.get("open_cost", 0.0005),
            "close_cost": bt.get("close_cost", 0.0015),
            "min_cost":   bt.get("min_cost", 5),
        }

    # ── public ────────────────────────────────────────────────────────────────

    def run(
        self,
        pred: pd.Series,
        strategy_params: Optional[Dict[str, Any]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        account: Optional[float] = None,
        universe_filter=None,
        seed: int = 42,
    ) -> Tuple[pd.DataFrame, Dict]:
        """
        Run a single backtest.

        Args:
            pred:             Model predictions (instrument × datetime MultiIndex)
            strategy_params:  Dict with topk, n_drop, hold_thresh
            start_time:       Backtest start date
            end_time:         Backtest end date (default: today)
            account:          Initial capital
            universe_filter:  UniverseFilter instance (optional)

        Returns:
            (report_df, positions_dict)
        """
        from qlib.backtest.executor import SimulatorExecutor
        from qlib.contrib.strategy.signal_strategy import TopkDropoutStrategy
        from qlib.contrib.evaluate import backtest_daily

        if universe_filter is not None:
            pred = universe_filter.filter(pred)

        sp = strategy_params or {}
        strat_cfg = self.config.get("strategy", {}).get("topk_dropout", {})
        bt_cfg = self.config.get("backtest", {})
        topk       = sp.get("topk",       strat_cfg.get("topk",       10))
        n_drop     = sp.get("n_drop",     strat_cfg.get("n_drop",     3))
        hold_thresh = sp.get("hold_thresh", strat_cfg.get("hold_thresh", 5))
        method_sell = sp.get("method_sell", strat_cfg.get("method_sell", "bottom"))
        method_buy = sp.get("method_buy", strat_cfg.get("method_buy", "top"))
        tie_breaker_epsilon = bt_cfg.get("tie_breaker_epsilon", 1e-12)

        pred = stabilize_signal(pred, tie_breaker_epsilon=tie_breaker_epsilon)

        import random, numpy as np
        random.seed(seed)
        np.random.seed(seed)

        original_choice = None
        if method_sell == "random" or method_buy == "random":
            original_choice = np.random.choice

            def _choice_as_python_strings(a, *args, **kwargs):
                result = original_choice(a, *args, **kwargs)
                if isinstance(result, np.ndarray) and result.dtype.kind in {"O", "U", "S"}:
                    return [str(item) for item in result.tolist()]
                if isinstance(result, np.generic):
                    return result.item()
                return result

            np.random.choice = _choice_as_python_strings

        strategy = TopkDropoutStrategy(
            signal=pred,
            topk=topk,
            n_drop=n_drop,
            hold_thresh=hold_thresh,
            method_sell=method_sell,
            method_buy=method_buy,
        )

        executor = SimulatorExecutor(
            time_per_step="day",
            generate_portfolio_metrics=True,
            exchange_kwargs={
                "freq": "day",
                "deal_price": "close",
                "open_cost":  self._defaults["open_cost"],
                "close_cost": self._defaults["close_cost"],
                "min_cost":   self._defaults["min_cost"],
            },
        )

        bt_start = start_time or self.config.get("backtest", {}).get("start_time", "2024-01-01")
        bt_end   = end_time   or datetime.now().strftime("%Y-%m-%d")
        acct     = account    or self._defaults["account"]

        try:
            report, positions = backtest_daily(
                start_time=bt_start,
                end_time=bt_end,
                strategy=strategy,
                account=acct,
                benchmark=None,
                executor=executor,
            )
        finally:
            if original_choice is not None:
                np.random.choice = original_choice
        return report, positions
