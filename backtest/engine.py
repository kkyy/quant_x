"""Backtest execution engine wrapping qlib's backtest_daily."""
from __future__ import annotations
import logging
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


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
        topk       = sp.get("topk",       strat_cfg.get("topk",       10))
        n_drop     = sp.get("n_drop",     strat_cfg.get("n_drop",     3))
        hold_thresh = sp.get("hold_thresh", strat_cfg.get("hold_thresh", 5))

        strategy = TopkDropoutStrategy(
            signal=pred,
            topk=topk,
            n_drop=n_drop,
            hold_thresh=hold_thresh,
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

        report, positions = backtest_daily(
            start_time=bt_start,
            end_time=bt_end,
            strategy=strategy,
            account=acct,
            benchmark=None,
            executor=executor,
        )
        return report, positions
