#!/usr/bin/env python3
"""Audit cached fundamental factors before using them in strategy training.

The script is intentionally cache-only: it reads existing CSV files under
``cache/valuation`` / ``cache/financial`` (or user supplied source dirs),
aligns them to qlib price dates, computes forward-return RankIC diagnostics
across several horizons, and writes reproducible reports under
``optimization_results/research_cycles``.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from quant_ex.data.loader import DataLoader
from quant_ex.features.library.screener import FactorEvaluator
from quant_ex.utils.config import load_config
from quant_ex.utils.logger import setup_logger

logger = setup_logger("fundamental_audit")


DEFAULT_SOURCES = {
    "valuation": "./cache/valuation",
    "financial": "./cache/financial",
}

DEFAULT_LAGS = {
    "valuation": 0,
    "financial": 45,
}


def ensure_hash_seed() -> None:
    """Re-exec script runs with deterministic hashing, but keep imports testable."""
    if os.environ.get("PYTHONHASHSEED") != "42":
        os.environ["PYTHONHASHSEED"] = "42"
        os.execv(sys.executable, [sys.executable] + sys.argv)


def parse_csv_ints(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def parse_key_values(value: Optional[str], defaults: Optional[dict] = None) -> dict:
    result = dict(defaults or {})
    if not value:
        return result
    for item in value.split(","):
        if not item.strip():
            continue
        if "=" not in item:
            raise ValueError(f"Expected key=value item, got: {item}")
        key, raw = item.split("=", 1)
        result[key.strip()] = raw.strip()
    return result


def load_cached_source(
    source_name: str,
    cache_dir: Path,
    lag_days: int = 0,
) -> tuple[pd.DataFrame, dict[str, str]]:
    """Load one per-stock cache directory into a MultiIndex DataFrame."""
    if not cache_dir.exists():
        logger.warning("Cache source missing: %s=%s", source_name, cache_dir)
        return pd.DataFrame(), {}

    frames = []
    for path in sorted(cache_dir.glob("*.csv")):
        try:
            df = pd.read_csv(path, index_col=[0, 1], parse_dates=[1])
            if df.empty:
                continue
            df.index.names = ["instrument", "datetime"]
            if lag_days:
                idx = df.index
                shifted_dates = idx.get_level_values("datetime") + pd.Timedelta(days=lag_days)
                df.index = pd.MultiIndex.from_arrays(
                    [idx.get_level_values("instrument"), shifted_dates],
                    names=["instrument", "datetime"],
                )
            frames.append(df.apply(pd.to_numeric, errors="coerce"))
        except Exception as exc:
            logger.debug("Failed to read %s: %s", path, exc)

    if not frames:
        logger.warning("Cache source empty: %s=%s", source_name, cache_dir)
        return pd.DataFrame(), {}

    data = pd.concat(frames).sort_index()
    source_map = {col: source_name for col in data.columns}
    return data, source_map


def load_cached_factors(
    sources: dict[str, str],
    source_lags: dict[str, int],
) -> tuple[pd.DataFrame, dict[str, str]]:
    """Load all configured cache sources and make duplicate columns explicit."""
    parts = []
    column_sources: dict[str, str] = {}
    seen: set[str] = set()

    for source_name, raw_dir in sources.items():
        data, source_map = load_cached_source(
            source_name,
            Path(raw_dir),
            lag_days=int(source_lags.get(source_name, 0)),
        )
        if data.empty:
            continue

        rename = {}
        for col in data.columns:
            final = col if col not in seen else f"{source_name}_{col}"
            rename[col] = final
            seen.add(final)
            column_sources[final] = source_map.get(col, source_name)
        parts.append(data.rename(columns=rename))

    if not parts:
        return pd.DataFrame(), {}
    return pd.concat(parts, axis=1).sort_index(), column_sources


def align_to_price_index(factors: pd.DataFrame, price_index: pd.MultiIndex) -> pd.DataFrame:
    """Forward-fill sparse fundamental observations onto daily price dates."""
    instruments = price_index.get_level_values("instrument").unique()
    price_dates = price_index.get_level_values("datetime").unique()
    factor_dates = factors.index.get_level_values("datetime").unique()
    all_dates = price_dates.union(factor_dates).sort_values()

    target = pd.MultiIndex.from_product(
        [instruments, all_dates], names=["instrument", "datetime"]
    )
    aligned = factors.reindex(target)
    aligned = aligned.groupby(level="instrument", group_keys=False).ffill()
    return aligned.reindex(price_index)


def compute_forward_returns(price_data: pd.DataFrame, horizon: int) -> pd.Series:
    """Compute T+1 to T+(horizon+1) forward return, matching qlib convention."""
    close = price_data["real_close"].sort_index()
    entry = close.groupby(level="instrument").shift(-1)
    exit_ = close.groupby(level="instrument").shift(-(horizon + 1))
    return (exit_ / entry - 1).rename(f"forward_return_{horizon}d")


def winsorize_by_date(factors: pd.DataFrame, lower: float, upper: float) -> pd.DataFrame:
    if lower <= 0 and upper >= 1:
        return factors

    def _clip(group: pd.DataFrame) -> pd.DataFrame:
        lo = group.quantile(lower)
        hi = group.quantile(upper)
        return group.clip(lower=lo, upper=hi, axis=1)

    return factors.groupby(level="datetime", group_keys=False).apply(_clip)


def transform_factors(
    factors: pd.DataFrame,
    mode: str,
    winsor_lower: float = 0.01,
    winsor_upper: float = 0.99,
) -> pd.DataFrame:
    """Apply daily cross-sectional transforms for audit comparability."""
    data = winsorize_by_date(factors, winsor_lower, winsor_upper)
    if mode == "raw":
        return data
    if mode == "rank":
        return data.groupby(level="datetime", group_keys=False).rank(pct=True)
    if mode == "zscore":
        grouped = data.groupby(level="datetime", group_keys=False)
        mean = grouped.transform("mean")
        std = grouped.transform("std").replace(0, pd.NA)
        return (data - mean) / std
    raise ValueError(f"Unknown transform: {mode}")


def yearly_sign_stability(
    factor: pd.Series,
    forward_returns: pd.Series,
    overall_ic: float,
    min_abs_ic: float,
) -> dict:
    evaluator = FactorEvaluator()
    frame = pd.DataFrame({"factor": factor, "ret": forward_returns}).dropna()
    if frame.empty or overall_ic == 0:
        return {"stable_years": 0, "positive_years": 0, "negative_years": 0, "year_count": 0}

    desired_sign = 1 if overall_ic > 0 else -1
    stable = positive = negative = year_count = 0
    years = frame.index.get_level_values("datetime").year.unique()
    for year in years:
        year_frame = frame[frame.index.get_level_values("datetime").year == year]
        if year_frame.empty:
            continue
        ic_series = evaluator.ic_series(year_frame["factor"], year_frame["ret"])
        if ic_series.empty:
            continue
        year_ic = ic_series.mean()
        year_count += 1
        if year_ic > 0:
            positive += 1
        elif year_ic < 0:
            negative += 1
        if year_ic * desired_sign >= min_abs_ic:
            stable += 1
    return {
        "stable_years": stable,
        "positive_years": positive,
        "negative_years": negative,
        "year_count": year_count,
    }


def evaluate_one(
    factors: pd.DataFrame,
    forward_returns: pd.Series,
    horizon: int,
    transform: str,
    column_sources: dict[str, str],
    min_stability_ic: float,
) -> pd.DataFrame:
    transformed = transform_factors(factors, transform)
    evaluator = FactorEvaluator()
    stats = evaluator.evaluate(transformed, forward_returns)
    if stats.empty:
        return stats

    rows = []
    for name, row in stats.iterrows():
        stability = yearly_sign_stability(
            transformed[name],
            forward_returns,
            overall_ic=float(row["ic_mean"]),
            min_abs_ic=min_stability_ic,
        )
        rows.append(
            {
                "factor": name,
                "source": column_sources.get(name, ""),
                "horizon": horizon,
                "transform": transform,
                "ic_mean": row["ic_mean"],
                "ic_std": row["ic_std"],
                "icir": row["icir"],
                "coverage": row["coverage"],
                "n_dates": int(row["n_dates"]),
                **stability,
            }
        )
    return pd.DataFrame(rows)


def select_factors(
    audit: pd.DataFrame,
    factors: pd.DataFrame,
    min_ic: float,
    min_icir: float,
    min_coverage: float,
    min_stable_years: int,
    max_corr: float,
) -> pd.DataFrame:
    if audit.empty:
        return audit

    selected = audit[
        (audit["ic_mean"].abs() >= min_ic)
        & (audit["icir"].abs() >= min_icir)
        & (audit["coverage"] >= min_coverage)
        & (audit["stable_years"] >= min_stable_years)
    ].copy()
    if selected.empty:
        selected["selected"] = []
        selected["reject_reason"] = []
        return selected

    selected = selected.sort_values(
        ["horizon", "transform", "icir"], key=lambda s: s.abs() if s.name == "icir" else s
    )
    selected["selected"] = True
    selected["reject_reason"] = "pass"

    # Correlation prune within each horizon/transform group.
    for (horizon, transform), group in selected.groupby(["horizon", "transform"]):
        transformed = transform_factors(factors[group["factor"].tolist()], transform)
        ordered = group.reindex(group["icir"].abs().sort_values(ascending=False).index)
        kept: list[str] = []
        for idx, row in ordered.iterrows():
            name = row["factor"]
            too_corr = False
            for existing in kept:
                pair = transformed[[name, existing]].dropna()
                if len(pair) < 10:
                    continue
                corr = pair[name].corr(pair[existing], method="spearman")
                if pd.notna(corr) and abs(corr) >= max_corr:
                    too_corr = True
                    break
            if too_corr:
                selected.loc[idx, "selected"] = False
                selected.loc[idx, "reject_reason"] = "high_corr"
            else:
                kept.append(name)

    return selected.sort_values(
        ["selected", "horizon", "transform", "icir"],
        ascending=[False, True, True, False],
    )


def build_output_paths(output_dir: Path, run_id: Optional[str]) -> dict[str, Path]:
    tag = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = output_dir / f"fundamental_audit_{tag}"
    return {
        "audit": stem.with_name(stem.name + "_metrics.csv"),
        "selected": stem.with_name(stem.name + "_selected.csv"),
        "coverage": stem.with_name(stem.name + "_coverage.csv"),
    }


def run_audit(args: argparse.Namespace) -> dict[str, Path]:
    config = load_config(args.config)
    market = args.market or config.get("market", {}).get("name", "csi300")
    start = args.start or config.get("training", {}).get("fit_start", "2015-01-01")
    end = args.end or datetime.now().strftime("%Y-%m-%d")

    sources = parse_key_values(args.sources, DEFAULT_SOURCES)
    source_lags = {
        key: int(value)
        for key, value in parse_key_values(args.source_lags, DEFAULT_LAGS).items()
    }

    logger.info("Loading price data: market=%s %s..%s", market, start, end)
    price_data = DataLoader(config).load_price_data(
        instruments=market,
        start_time=start,
        end_time=end,
    )

    logger.info("Loading cached factors: %s", sources)
    raw_factors, column_sources = load_cached_factors(sources, source_lags)
    if raw_factors.empty:
        raise RuntimeError("No cached fundamental factors were loaded")

    factors = align_to_price_index(raw_factors, price_data.index)
    coverage = (
        factors.notna()
        .mean()
        .rename("coverage")
        .reset_index()
        .rename(columns={"index": "factor"})
    )
    coverage["source"] = coverage["factor"].map(column_sources).fillna("")

    all_rows = []
    horizons = parse_csv_ints(args.horizons)
    transforms = [item.strip() for item in args.transforms.split(",") if item.strip()]
    for horizon in horizons:
        forward_returns = compute_forward_returns(price_data, horizon).reindex(factors.index)
        for transform in transforms:
            logger.info("Evaluating horizon=%sd transform=%s", horizon, transform)
            rows = evaluate_one(
                factors,
                forward_returns,
                horizon=horizon,
                transform=transform,
                column_sources=column_sources,
                min_stability_ic=args.min_stability_ic,
            )
            if not rows.empty:
                all_rows.append(rows)

    if not all_rows:
        raise RuntimeError("No factor audit rows were produced")

    audit = pd.concat(all_rows, ignore_index=True)
    selected = select_factors(
        audit,
        factors,
        min_ic=args.min_ic,
        min_icir=args.min_icir,
        min_coverage=args.min_coverage,
        min_stable_years=args.min_stable_years,
        max_corr=args.max_corr,
    )

    paths = build_output_paths(Path(args.output_dir), args.run_id)
    audit.sort_values(["horizon", "transform", "icir"], ascending=[True, True, False]).to_csv(
        paths["audit"], index=False
    )
    selected.to_csv(paths["selected"], index=False)
    coverage.sort_values("coverage", ascending=False).to_csv(paths["coverage"], index=False)

    logger.info("Audit metrics saved: %s", paths["audit"])
    logger.info("Selected factors saved: %s", paths["selected"])
    logger.info("Coverage report saved: %s", paths["coverage"])
    return paths


def main(argv: Optional[Iterable[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Cache-only fundamental factor IC audit")
    parser.add_argument("--config", type=str, default=None)
    parser.add_argument("--market", type=str, default=None)
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--horizons", type=str, default="1,5,10,20")
    parser.add_argument("--transforms", type=str, default="raw,rank,zscore")
    parser.add_argument(
        "--sources",
        type=str,
        default=None,
        help="Comma-separated source=cache_dir list. Default: valuation and financial caches.",
    )
    parser.add_argument(
        "--source-lags",
        type=str,
        default=None,
        help="Comma-separated source=days report lag. Default: valuation=0, financial=45.",
    )
    parser.add_argument("--min-ic", type=float, default=0.01)
    parser.add_argument("--min-icir", type=float, default=0.15)
    parser.add_argument("--min-coverage", type=float, default=0.60)
    parser.add_argument("--min-stable-years", type=int, default=4)
    parser.add_argument("--min-stability-ic", type=float, default=0.005)
    parser.add_argument("--max-corr", type=float, default=0.70)
    parser.add_argument("--output-dir", type=str, default="optimization_results/research_cycles")
    parser.add_argument("--run-id", type=str, default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)

    paths = run_audit(args)
    print("\nFundamental audit complete:")
    for name, path in paths.items():
        print(f"  {name}: {path}")


if __name__ == "__main__":
    ensure_hash_seed()
    main()
