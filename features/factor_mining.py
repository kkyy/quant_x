"""
Automatic alpha factor mining.

Strategy:
- Define a library of expression templates (qlib expression language)
- Enumerate all parameter combinations
- Evaluate each factor's rank-IC and ICIR against forward returns
- Keep top-N factors by ICIR

Usage:
    miner = FactorMiner(price_data, label, top_n=20)
    results = miner.mine()
    miner.save_factors(results)

    # Later, load mined factors as a FactorPipeline-compatible provider:
    loader = MinedFactorLoader(path="./cache/mined_factors.json")
    factor_df = loader.compute(price_data)

Registered factor name: "mined"
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from itertools import product
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)


@dataclass
class FactorResult:
    name: str
    expression: str
    ic: float
    icir: float
    values: pd.Series = field(repr=False)


# ---------------------------------------------------------------------------
# Expression templates  (uses qlib expression engine)
# Each tuple: (name_template, expr_template, param_space)
# ---------------------------------------------------------------------------
TEMPLATES: List[Tuple[str, str, Dict]] = [
    # Momentum
    ("mom_{w}d",
     "Ref($close, 0) / Ref($close, {w}) - 1",
     {"w": [5, 10, 20, 40, 60]}),

    # Mean-reversion z-score
    ("mean_rev_{w}d",
     "($close - Mean($close, {w})) / (Std($close, {w}) + 1e-6)",
     {"w": [5, 10, 20]}),

    # Volume-price correlation (Amihud-like)
    ("vol_price_corr_{w}d",
     "Corr(Log($volume + 1), Log($close), {w})",
     {"w": [10, 20]}),

    # Turnover ratio (short vs long)
    ("turnover_ratio_{w}_{w2}d",
     "Mean($volume, {w}) / (Mean($volume, {w2}) + 1e-6)",
     {"w": [5], "w2": [20]}),

    # Price position in high-low range
    ("price_pos_{w}d",
     "($close - Min($low, {w})) / (Max($high, {w}) - Min($low, {w}) + 1e-6)",
     {"w": [10, 20]}),

    # Realised volatility
    ("rvol_{w}d",
     "Std(Log($close / Ref($close, 1) + 1e-6), {w})",
     {"w": [10, 20]}),

    # Intraday range
    ("hl_range_{w}d",
     "Mean(($high - $low) / ($close + 1e-6), {w})",
     {"w": [5, 10, 20]}),

    # Open-to-close return
    ("oc_ret_{w}d",
     "Mean(($close - $open) / ($open + 1e-6), {w})",
     {"w": [5, 10, 20]}),

    # Amount momentum (short vs long)
    ("amount_ratio_{w}_{w2}d",
     "Log(Mean($amount, {w}) / (Mean($amount, {w2}) + 1e-6) + 1e-6)",
     {"w": [5], "w2": [20]}),

    # RSI-like
    ("rsi_{w}d",
     "Mean(Greater($close - Ref($close, 1), 0), {w}) / "
     "(Mean(Abs($close - Ref($close, 1)), {w}) + 1e-6)",
     {"w": [14, 20]}),
]


# ── Factor Miner ──────────────────────────────────────────────────────────────

class FactorMiner:
    """
    Template-based alpha factor miner.

    Args:
        price_data: (instrument, datetime) MultiIndex price DataFrame
        label:      Forward-return label Series, same index
        top_n:      Keep top N factors by |ICIR|
        min_ic:     Minimum |mean IC| threshold
        min_icir:   Minimum |ICIR| threshold
    """

    def __init__(
        self,
        price_data: pd.DataFrame,
        label: pd.Series,
        top_n: int = 20,
        min_ic: float = 0.02,
        min_icir: float = 0.3,
    ):
        self.price_data = price_data
        self.label = label
        self.top_n = top_n
        self.min_ic = min_ic
        self.min_icir = min_icir

    # ── public ─────────────────────────────────────────────────────────────

    def mine(
        self, progress_cb: Optional[Callable[[int, int, str, float, float], None]] = None
    ) -> List[FactorResult]:
        """Run mining, return list of FactorResult sorted by |ICIR| desc."""
        total = sum(
            len(list(product(*[v if isinstance(v, list) else [v] for v in ps.values()])))
            for _, _, ps in TEMPLATES
        )
        logger.info(f"Mining {total} factor candidates …")

        results: List[FactorResult] = []
        count = 0

        for name_tpl, expr_tpl, param_space in TEMPLATES:
            keys = list(param_space.keys())
            vals = [param_space[k] if isinstance(param_space[k], list) else [param_space[k]]
                    for k in keys]

            for combo in product(*vals):
                params = dict(zip(keys, combo))
                name = name_tpl.format(**params)
                expr = expr_tpl.format(**params)
                count += 1

                fv = self._compute(expr)
                if fv is None:
                    if progress_cb:
                        progress_cb(count, total, name, 0.0, 0.0)
                    continue

                ic, icir = self._eval(fv)

                if progress_cb:
                    progress_cb(count, total, name, ic, icir)

                if abs(ic) >= self.min_ic and abs(icir) >= self.min_icir:
                    results.append(
                        FactorResult(name=name, expression=expr, ic=ic, icir=icir, values=fv)
                    )
                    logger.info(f"  ✅ {name} | IC={ic:.4f} ICIR={icir:.4f}")

        results.sort(key=lambda r: abs(r.icir), reverse=True)
        kept = results[: self.top_n]
        logger.info(f"Mining done. Valid={len(results)}, kept={len(kept)}")
        return kept

    def save_factors(
        self,
        results: List[FactorResult],
        path: str = "./cache/mined_factors.json",
    ):
        data = [
            {"name": r.name, "expression": r.expression, "ic": r.ic, "icir": r.icir}
            for r in results
        ]
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info(f"Saved {len(data)} factors → {path}")

    @staticmethod
    def load_factors(path: str = "./cache/mined_factors.json") -> List[dict]:
        p = Path(path)
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else []

    # ── private ─────────────────────────────────────────────────────────────

    def _compute(self, expr: str) -> Optional[pd.Series]:
        """Evaluate a qlib expression, return Series or None."""
        try:
            import qlib
            from qlib.config import C as _C
            if not getattr(_C, "provider_uri", None):
                logger.warning("qlib is not initialized; skipping expression '%s'", expr)
                return None
            from qlib.data import D
            datetimes = self.price_data.index.get_level_values("datetime")
            instruments = self.price_data.index.get_level_values("instrument").unique().tolist()
            df = D.features(
                instruments, [expr],
                start_time=str(datetimes.min())[:10],
                end_time=str(datetimes.max())[:10],
            )
            s = df.iloc[:, 0]
            s.index.names = ["instrument", "datetime"]
            return s
        except Exception as e:
            logger.debug(f"Failed to compute '{expr}': {e}")
            return None

    def _eval(self, factor: pd.Series) -> Tuple[float, float]:
        """Compute rank-IC and ICIR."""
        # Ensure index level order matches (factor uses instrument/datetime,
        # but label from dataset.prepare may use datetime/instrument)
        label = self.label
        if factor.index.names != label.index.names:
            label = label.reorder_levels(factor.index.names)
        aligned = pd.concat([factor, label], axis=1, join="inner").dropna()
        aligned.columns = ["f", "y"]

        if len(aligned) < 100:
            return 0.0, 0.0

        daily_ic = []
        for _, grp in aligned.groupby(level="datetime"):
            if len(grp) < 5:
                continue
            ic = grp["f"].rank().corr(grp["y"].rank())
            if not np.isnan(ic):
                daily_ic.append(ic)

        if not daily_ic:
            return 0.0, 0.0

        ic_s = pd.Series(daily_ic)
        mean_ic = ic_s.mean()
        icir = mean_ic / (ic_s.std() + 1e-8)
        return float(mean_ic), float(icir)


# ── MinedFactorLoader ─────────────────────────────────────────────────────────

@FactorRegistry.register("mined")
class MinedFactorLoader(BaseFactor):
    """
    Load previously mined factors from a JSON file and re-evaluate
    their qlib expressions against the given price data.

    Registered name: "mined"

    Parameters
    ----------
    path    : path to the JSON file produced by FactorMiner.save_factors()
    top_n   : maximum number of factors to load (sorted by |icir|)
    """

    def __init__(
        self,
        path: str = "./cache/mined_factors.json",
        top_n: Optional[int] = None,
    ):
        self.path = path
        self.top_n = top_n

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        factors = FactorMiner.load_factors(self.path)
        if not factors:
            logger.warning(f"MinedFactorLoader: no factors found at '{self.path}'")
            return None

        # Sort by |icir| and limit
        factors = sorted(factors, key=lambda x: abs(x.get("icir", 0)), reverse=True)
        if self.top_n is not None:
            factors = factors[: self.top_n]

        datetimes   = price_data.index.get_level_values("datetime")
        instruments = price_data.index.get_level_values("instrument").unique().tolist()
        start       = str(datetimes.min())[:10]
        end         = str(datetimes.max())[:10]

        parts: List[pd.Series] = []
        for f in factors:
            try:
                from qlib.config import C as _C
                if not getattr(_C, "provider_uri", None):
                    logger.warning("qlib is not initialized; skipping mined factor '%s'", f.get("name"))
                    continue
                from qlib.data import D
                df = D.features(
                    instruments, [f["expression"]],
                    start_time=start,
                    end_time=end,
                )
                s = df.iloc[:, 0]
                s.index.names = ["instrument", "datetime"]
                parts.append(s.rename(f["name"]))
                logger.debug(f"MinedFactorLoader: loaded '{f['name']}'")
            except Exception as exc:
                logger.warning(f"MinedFactorLoader: could not compute '{f['name']}': {exc}")

        if not parts:
            return None

        result = pd.concat(parts, axis=1)
        result.index.names = ["instrument", "datetime"]
        return result.sort_index()
