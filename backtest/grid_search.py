"""Parameter grid search over TopkDropout strategy."""
from __future__ import annotations
import logging
from itertools import product
from typing import Any, Dict, List, Optional

import pandas as pd

from .metrics import compute_metrics

logger = logging.getLogger(__name__)


class GridSearchBacktest:
    """
    Enumerate (topk × n_drop × hold_thresh) combinations,
    run a backtest for each, and collect metrics.

    Example:
        searcher = GridSearchBacktest(engine, pred, config)
        results  = searcher.run({"topk": [5, 10, 15], "n_drop": [1, 3], "hold_thresh": [3, 5]})
        best     = searcher.best_params(results)
    """

    DEFAULT_GRID: Dict[str, List[Any]] = {
        "topk":        [5, 10, 15, 20],
        "n_drop":      [1, 3, 5],
        "hold_thresh": [3, 5, 10],
    }

    def __init__(self, engine, pred: pd.Series, config: dict):
        self.engine = engine
        self.pred = pred
        self.config = config

    # ── public ────────────────────────────────────────────────────────────────

    def run(
        self,
        param_grid: Optional[Dict[str, List[Any]]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        universe_filter=None,
    ) -> pd.DataFrame:
        """
        Run all parameter combinations.

        Returns:
            DataFrame sorted by sharpe descending.
        """
        grid = param_grid or self.DEFAULT_GRID
        keys = list(grid.keys())
        combos = list(product(*[grid[k] for k in keys]))
        logger.info(f"Grid search: {len(combos)} combinations")

        rows = []
        for i, combo in enumerate(combos, 1):
            params = dict(zip(keys, combo))
            logger.info(f"  [{i}/{len(combos)}] {params}")
            try:
                report, _ = self.engine.run(
                    pred=self.pred,
                    strategy_params=params,
                    start_time=start_time,
                    end_time=end_time,
                    universe_filter=universe_filter,
                )
                m = compute_metrics(report)
                rows.append({**params, **m})
                logger.info(
                    f"    Sharpe={m.get('sharpe', 0):.3f}  "
                    f"Ret={m.get('annual_return', 0):.2%}  "
                    f"DD={m.get('max_drawdown', 0):.2%}"
                )
            except Exception as e:
                logger.warning(f"    FAILED: {e}")
                rows.append({**params, "error": str(e)})

        df = pd.DataFrame(rows)
        if "sharpe" in df.columns:
            df = df.sort_values("sharpe", ascending=False).reset_index(drop=True)
        return df

    @staticmethod
    def best_params(results: pd.DataFrame) -> Dict[str, Any]:
        """Extract best parameter set from grid search DataFrame."""
        valid = results.dropna(subset=["sharpe"]) if "sharpe" in results.columns else results
        if valid.empty:
            return {}
        row = valid.iloc[0]
        return {
            "topk":        int(row.get("topk", 10)),
            "n_drop":      int(row.get("n_drop", 3)),
            "hold_thresh": int(row.get("hold_thresh", 5)),
        }
