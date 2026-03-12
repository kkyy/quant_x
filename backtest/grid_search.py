"""Parameter grid search over TopkDropout strategy."""
from __future__ import annotations
import logging
import multiprocessing
import os
from itertools import product
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from .metrics import compute_metrics

logger = logging.getLogger(__name__)


def _seed_worker(engine_config: dict, pred: pd.Series, params: dict,
                 start_time: Optional[str], end_time: Optional[str],
                 seed: int, result_queue) -> None:
    """Top-level worker executed in a fresh subprocess (spawn).

    PYTHONHASHSEED is already set in the environment by the parent process
    before spawning, so Python's hash randomization matches ``seed``.
    """
    import sys
    import pathlib
    sys.path.insert(0, str(pathlib.Path(__file__).parent.parent.parent))

    import qlib
    from quant_ex.backtest.engine import BacktestEngine
    from quant_ex.backtest.metrics import compute_metrics as _compute_metrics

    qlib.init(
        provider_uri=engine_config["qlib"]["provider_uri"],
        region=engine_config["qlib"]["region"],
    )
    engine = BacktestEngine(engine_config)
    try:
        report, _ = engine.run(
            pred=pred,
            strategy_params=params,
            start_time=start_time,
            end_time=end_time,
            seed=seed,
        )
        result_queue.put(_compute_metrics(report))
    except Exception as e:
        result_queue.put({"_error": str(e)})


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

    MULTI_SEEDS: List[int] = [42, 123, 2024, 7, 999]

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
        multi_seed: bool = False,
    ) -> pd.DataFrame:
        """
        Run all parameter combinations.

        Args:
            multi_seed: If True, run each combo with 5 built-in seeds in
                        separate subprocesses (so PYTHONHASHSEED differs per
                        seed) and report averaged metrics.

        Returns:
            DataFrame sorted by sharpe descending.
        """
        grid = param_grid or self.DEFAULT_GRID
        keys = list(grid.keys())
        combos = list(product(*[grid[k] for k in keys]))
        seeds = self.MULTI_SEEDS if multi_seed else [42]
        logger.info(
            f"Grid search: {len(combos)} combinations"
            + (f" × {len(seeds)} seeds" if multi_seed else "")
        )

        rows = []
        for i, combo in enumerate(combos, 1):
            params = dict(zip(keys, combo))
            logger.info(f"  [{i}/{len(combos)}] {params}")
            seed_metrics: List[Dict] = []
            for seed in seeds:
                try:
                    if multi_seed:
                        metrics = self._run_seed_subprocess(
                            params, start_time, end_time, seed
                        )
                    else:
                        report, _ = self.engine.run(
                            pred=self.pred,
                            strategy_params=params,
                            start_time=start_time,
                            end_time=end_time,
                            universe_filter=universe_filter,
                            seed=seed,
                        )
                        metrics = compute_metrics(report)
                    seed_metrics.append(metrics)
                except Exception as e:
                    logger.warning(f"    seed={seed} FAILED: {e}")

            if not seed_metrics:
                rows.append({**params, "error": "all seeds failed"})
                continue

            # Average numeric metrics across seeds
            numeric_keys = [k for k, v in seed_metrics[0].items()
                            if isinstance(v, (int, float))]
            m = {k: float(np.mean([sm[k] for sm in seed_metrics if k in sm]))
                 for k in numeric_keys}
            if multi_seed:
                m["sharpe_std"] = float(np.std([sm.get("sharpe", 0) for sm in seed_metrics]))
            rows.append({**params, **m})
            logger.info(
                f"    Sharpe={m.get('sharpe', 0):.3f}"
                + (f"±{m.get('sharpe_std', 0):.3f}" if multi_seed else "")
                + f"  Ret={m.get('annual_return', 0):.2%}"
                + f"  DD={m.get('max_drawdown', 0):.2%}"
            )

        df = pd.DataFrame(rows)
        if "sharpe" in df.columns:
            df = df.sort_values("sharpe", ascending=False).reset_index(drop=True)
        return df

    # ── private ───────────────────────────────────────────────────────────────

    def _run_seed_subprocess(
        self,
        params: dict,
        start_time: Optional[str],
        end_time: Optional[str],
        seed: int,
    ) -> dict:
        """Run one backtest in a fresh subprocess with PYTHONHASHSEED=seed.

        Setting os.environ["PYTHONHASHSEED"] here (in the parent) before
        calling ctx.Process() causes the spawned child to inherit that value.
        The child's Python interpreter reads PYTHONHASHSEED at startup, before
        any user code runs, so hash randomization is truly seeded to ``seed``.
        """
        os.environ["PYTHONHASHSEED"] = str(seed)
        os.environ["_QUANT_EX_SEED_WORKER"] = "1"  # suppress re-exec in child
        ctx = multiprocessing.get_context("spawn")
        q = ctx.Queue()
        p = ctx.Process(
            target=_seed_worker,
            args=(self.config, self.pred, params, start_time, end_time, seed, q),
        )
        p.start()
        p.join()

        if q.empty():
            raise RuntimeError(f"subprocess for seed={seed} returned no result")
        result = q.get_nowait()
        if isinstance(result, dict) and "_error" in result:
            raise RuntimeError(result["_error"])
        return result

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
