"""Factor computation service with TTL cache."""
from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from quant_ex.data.loader import DataLoader
from quant_ex.features.base import FactorPipeline
from quant_ex.backtest.signal_diagnostics import compute_ic_decay, compute_rolling_ic
from quant_ex.utils.config import load_config

logger = logging.getLogger(__name__)

_config = load_config()
_ttl_cache: Dict[str, Tuple[float, Any]] = {}
_DEFAULT_TTL = 86400.0


def _cached(key: str, ttl: float, factory: Callable[[], Any]) -> Any:
    now = time.time()
    expiry, data = _ttl_cache.get(key, (0.0, None))
    if now < expiry:
        return data
    result = factory()
    _ttl_cache[key] = (now + ttl, result)
    return result


def _get_price_data(instruments=None, start=None, end=None):
    key = f"price:{instruments}:{start}:{end}"
    return _cached(key, _DEFAULT_TTL, lambda: _load_price(instruments, start, end))


def _load_price(instruments, start, end):
    from web.api.services.data_service import _qlib_loader
    loader = _qlib_loader()
    return loader.load_price_data(
        instruments=instruments or "csi500",
        start_time=start or "2020-01-01",
        end_time=end,
    )


def compute_factor_values(
    factor_names: List[str],
    symbols: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> Dict:
    cache_key = f"factors:{','.join(sorted(factor_names))}:{symbols}:{start}:{end}"
    return _cached(cache_key, _DEFAULT_TTL, lambda: _do_compute(factor_names, symbols, start, end))


def _do_compute(factor_names, symbols, start, end):
    price_data = _get_price_data(instruments=symbols, start=start, end=end)
    if price_data.empty:
        return {"factors": factor_names, "data": []}

    configs = [{"name": n} for n in factor_names]
    try:
        pipeline = FactorPipeline.from_config(configs)
        result = pipeline.compute(price_data)
    except Exception as e:
        logger.warning("FactorPipeline compute failed: %s", e)
        return {"factors": factor_names, "data": []}

    if result is None or result.empty:
        return {"factors": factor_names, "data": []}

    df = result.reset_index()
    if symbols and "instrument" in df.columns:
        df = df[df["instrument"].isin(symbols)]

    records = df.to_dict(orient="records")
    for row in records:
        if "datetime" in row:
            row["date"] = str(row["datetime"].date()) if hasattr(row["datetime"], "date") else str(row["datetime"])
            del row["datetime"]
        for k, v in row.items():
            if pd.isna(v):
                row[k] = None
            elif isinstance(v, (float, int)):
                row[k] = round(float(v), 6)

    return {"factors": factor_names, "data": records}


def compute_ic_analysis(
    factor_name: str,
    horizon: int = 5,
    window: int = 20,
) -> Dict:
    cache_key = f"ic:{factor_name}:{horizon}:{window}"
    return _cached(cache_key, _DEFAULT_TTL, lambda: _do_ic(factor_name, horizon, window))


def _do_ic(factor_name, horizon, window):
    price_data = _get_price_data()
    if price_data.empty:
        return {"factor": factor_name, "ic_mean": 0, "icir": 0, "decay": [], "rolling": []}

    configs = [{"name": factor_name}]
    try:
        pipeline = FactorPipeline.from_config(configs)
        result = pipeline.compute(price_data)
    except Exception as e:
        logger.warning("FactorPipeline compute failed for IC: %s", e)
        return {"factor": factor_name, "ic_mean": 0, "icir": 0, "decay": [], "rolling": []}

    if result is None or result.empty:
        return {"factor": factor_name, "ic_mean": 0, "icir": 0, "decay": [], "rolling": []}

    factor_cols = [c for c in result.columns if c not in ("instrument", "datetime")]
    if not factor_cols:
        return {"factor": factor_name, "ic_mean": 0, "icir": 0, "decay": [], "rolling": []}
    pred = result[factor_cols[0]]

    try:
        decay_df = compute_ic_decay(pred, price_data, horizons=[1, 2, 3, 5, 10, 15, 20])
        rolling_df = compute_rolling_ic(pred, price_data, horizon=horizon, window=window)
    except Exception as e:
        logger.warning("IC computation failed: %s", e)
        return {"factor": factor_name, "ic_mean": 0, "icir": 0, "decay": [], "rolling": []}

    decay_records = []
    if not decay_df.empty:
        for _, row in decay_df.iterrows():
            r = {}
            for k, v in row.items():
                r[k] = None if pd.isna(v) else round(float(v), 6) if isinstance(v, (float, int)) else v
            decay_records.append(r)

    rolling_records = []
    if not rolling_df.empty:
        for _, row in rolling_df.iterrows():
            r = {}
            for k, v in row.items():
                if k == "datetime":
                    r["date"] = str(v.date()) if hasattr(v, "date") else str(v)[:10]
                elif pd.isna(v):
                    r[k] = None
                elif isinstance(v, (float, int)):
                    r[k] = round(float(v), 6)
                else:
                    r[k] = v
            rolling_records.append(r)

    ic_mean = float(decay_df["mean_rank_ic"].mean()) if not decay_df.empty and "mean_rank_ic" in decay_df.columns else 0
    icir = float(decay_df["rank_icir"].mean()) if not decay_df.empty and "rank_icir" in decay_df.columns else 0

    return {
        "factor": factor_name,
        "ic_mean": round(ic_mean, 4),
        "icir": round(icir, 4),
        "decay": decay_records,
        "rolling": rolling_records,
    }
