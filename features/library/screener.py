"""Factor quality evaluation and IC/ICIR-based screening.

FactorEvaluator
    Compute per-factor rank-IC, ICIR, and coverage stats.

FactorScreener
    Filter a factor DataFrame to keep only statistically valid factors,
    removing those with low predictive power or high inter-factor correlation.

Both operate on (instrument, datetime) MultiIndex DataFrames produced by
FactorPipeline.compute(), so they slot in between the pipeline and the model
trainer — no changes to either side required.

Example
-------
    evaluator = FactorEvaluator(forward_period=5)
    stats = evaluator.evaluate(factors, returns)   # → DataFrame of IC stats

    screener = FactorScreener(min_ic=0.02, min_icir=0.3)
    kept, report = screener.screen(factors, returns)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

logger = logging.getLogger(__name__)


# ── FactorEvaluator ───────────────────────────────────────────────────────────

class FactorEvaluator:
    """Compute rank-IC, ICIR, and coverage per factor column.

    Parameters
    ----------
    forward_period : int
        Number of trading days to shift labels forward.  The evaluator shifts
        *returns* backward by this amount so they align with factor dates.
        If ``forward_returns`` is already aligned (period=0), pass ``0``.
    """

    def __init__(self, forward_period: int = 0):
        self.forward_period = forward_period

    def evaluate(
        self,
        factors: pd.DataFrame,
        forward_returns: pd.Series,
    ) -> pd.DataFrame:
        """Return a stats DataFrame indexed by factor name.

        Parameters
        ----------
        factors : DataFrame
            (instrument, datetime) MultiIndex, one column per factor.
        forward_returns : Series
            Same index as *factors*.  If ``forward_period > 0``, the caller
            should pre-compute the shifted returns before passing them here
            (the evaluator assumes they are already aligned).

        Returns
        -------
        DataFrame with columns: ic_mean, ic_std, icir, coverage, n_dates
        """
        if factors is None or factors.empty:
            return pd.DataFrame()

        aligned = factors.copy()
        aligned["_ret"] = forward_returns
        aligned = aligned.dropna(subset=["_ret"])

        stats = {}
        for col in factors.columns:
            sub = aligned[[col, "_ret"]].dropna()
            if sub.empty:
                continue
            ic_series = self._ic_series(sub, col)
            n = len(ic_series)
            ic_mean = ic_series.mean()
            ic_std = ic_series.std()
            icir = ic_mean / ic_std if ic_std > 0 else 0.0
            cov = aligned[col].notna().mean()
            stats[col] = {
                "ic_mean": round(ic_mean, 6),
                "ic_std": round(ic_std, 6),
                "icir": round(icir, 4),
                "coverage": round(cov, 4),
                "n_dates": n,
            }

        return pd.DataFrame(stats).T

    def ic_series(
        self, factor: pd.Series, forward_returns: pd.Series
    ) -> pd.Series:
        """Return the per-date rank-IC time series for a single factor."""
        combined = pd.DataFrame({"f": factor, "_ret": forward_returns}).dropna()
        return self._ic_series(combined, "f")

    # ── internals ────────────────────────────────────────────────────────────

    @staticmethod
    def _ic_series(df: pd.DataFrame, factor_col: str) -> pd.Series:
        """Compute cross-sectional rank-IC per datetime."""
        if isinstance(df.index, pd.MultiIndex):
            date_level = df.index.get_level_values(1)
        else:
            date_level = df.index

        records = {}
        for dt in date_level.unique():
            if isinstance(df.index, pd.MultiIndex):
                slice_ = df[df.index.get_level_values(1) == dt]
            else:
                slice_ = df[df.index == dt]

            sub = slice_[[factor_col, "_ret"]].dropna()
            if len(sub) < 5:
                continue
            rho, _ = spearmanr(sub[factor_col], sub["_ret"])
            if not np.isnan(rho):
                records[dt] = rho

        return pd.Series(records)


# ── FactorScreener ────────────────────────────────────────────────────────────

@dataclass
class ScreenResult:
    """Per-factor screening decision with reason."""
    name: str
    ic_mean: float
    icir: float
    coverage: float
    kept: bool
    reason: str   # "ok" | "low_ic" | "low_icir" | "low_coverage" | "high_corr"


class FactorScreener:
    """Filter factors by quality thresholds and remove redundant correlated ones.

    Pipeline
    --------
    1. Drop factors below *min_coverage*.
    2. Drop factors below *min_ic* (|IC_mean|).
    3. Drop factors below *min_icir* (|ICIR|).
    4. Greedy correlation deduplication: sort survivors by |ICIR| descending,
       keep each factor only if its absolute pairwise correlation with all
       already-kept factors is below *max_corr*.

    Parameters
    ----------
    min_ic : float
        Minimum |IC_mean| threshold.
    min_icir : float
        Minimum |ICIR| threshold.
    max_corr : float
        Maximum Spearman rank-correlation allowed between kept factors.
        Set to ``1.0`` to disable deduplication.
    min_coverage : float
        Minimum valid-value fraction.
    evaluator : FactorEvaluator, optional
        Custom evaluator instance; defaults to ``FactorEvaluator()``.
    """

    def __init__(
        self,
        min_ic: float = 0.02,
        min_icir: float = 0.3,
        max_corr: float = 0.7,
        min_coverage: float = 0.3,
        evaluator: Optional[FactorEvaluator] = None,
    ):
        self.min_ic = min_ic
        self.min_icir = min_icir
        self.max_corr = max_corr
        self.min_coverage = min_coverage
        self.evaluator = evaluator or FactorEvaluator()

    def screen(
        self,
        factors: pd.DataFrame,
        forward_returns: pd.Series,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Apply full screening pipeline.

        Returns
        -------
        kept_factors : DataFrame
            Subset of *factors* that passed all screens.
        report : DataFrame
            Per-factor screening report (ic_mean, icir, coverage, kept, reason).
        """
        stats = self.evaluator.evaluate(factors, forward_returns)
        results: list[ScreenResult] = []
        survivors: list[str] = []

        # Threshold screens (steps 1-3)
        for col in factors.columns:
            if col not in stats.index:
                results.append(ScreenResult(col, 0, 0, 0, False, "no_data"))
                continue
            row = stats.loc[col]
            ic = row["ic_mean"]
            icir = row["icir"]
            cov = row["coverage"]

            if cov < self.min_coverage:
                results.append(ScreenResult(col, ic, icir, cov, False, "low_coverage"))
            elif abs(ic) < self.min_ic:
                results.append(ScreenResult(col, ic, icir, cov, False, "low_ic"))
            elif abs(icir) < self.min_icir:
                results.append(ScreenResult(col, ic, icir, cov, False, "low_icir"))
            else:
                survivors.append(col)
                results.append(ScreenResult(col, ic, icir, cov, True, "ok"))

        # Correlation deduplication (step 4)
        if survivors and self.max_corr < 1.0:
            survivors = self._dedup_corr(factors[survivors], stats, results)

        kept_factors = factors[survivors] if survivors else pd.DataFrame(index=factors.index)
        report = pd.DataFrame(
            [
                {
                    "name": r.name,
                    "ic_mean": r.ic_mean,
                    "icir": r.icir,
                    "coverage": r.coverage,
                    "kept": r.kept,
                    "reason": r.reason,
                }
                for r in results
            ]
        ).set_index("name")

        n_kept = report["kept"].sum()
        n_total = len(report)
        logger.info(f"FactorScreener: {n_kept}/{n_total} factors passed screening")
        return kept_factors, report

    # ── internals ────────────────────────────────────────────────────────────

    def _dedup_corr(
        self,
        survivors_df: pd.DataFrame,
        stats: pd.DataFrame,
        results: list[ScreenResult],
    ) -> list[str]:
        """Greedy max-|ICIR| correlation deduplication.  Mutates *results*."""
        if survivors_df.empty or len(survivors_df.columns) <= 1:
            return survivors_df.columns.tolist()

        # Sort by |ICIR| descending
        icir_vals = {c: abs(stats.loc[c, "icir"]) for c in survivors_df.columns if c in stats.index}
        ordered = sorted(icir_vals, key=icir_vals.get, reverse=True)

        kept: list[str] = []
        # Flatten to plain numeric for correlation (no MultiIndex needed)
        flat = survivors_df.reset_index(drop=True)

        for col in ordered:
            if not kept:
                kept.append(col)
                continue
            # Check correlation against every already-kept factor
            too_correlated = False
            for existing in kept:
                pair = flat[[col, existing]].dropna()
                if len(pair) < 10:
                    continue
                rho, _ = spearmanr(pair[col], pair[existing])
                if abs(rho) >= self.max_corr:
                    too_correlated = True
                    break

            if too_correlated:
                # Mark result entry as rejected
                for r in results:
                    if r.name == col and r.kept:
                        r.kept = False
                        r.reason = "high_corr"
                        break
            else:
                kept.append(col)

        return kept
