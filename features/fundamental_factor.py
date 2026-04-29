"""FundamentalFactor — valuation and profitability data from akshare.

Fetches historical PE/PB/PS/dividend yield per stock using akshare's
``stock_a_lg_indicator`` API, caches per-stock CSVs locally, and aligns the
data to the price_data MultiIndex for use in the feature pipeline.

Supported metrics
-----------------
pe_ttm      Price-earnings ratio (trailing twelve months)
pb          Price-to-book ratio
ps_ttm      Price-to-sales ratio (TTM)
dyr         Dividend yield (%)

Notes
-----
- Data is forward-filled to daily frequency (announcements are sparse).
- Cache TTL default is 7 days; set ``cache_ttl_days=0`` to disable.
- Requires: ``pip install akshare``

Example config (model.yaml)
---------------------------
    features:
      factors:
        - name: fundamental
          metrics: [pe_ttm, pb]
          cache_dir: ./cache/fundamental
          cache_ttl_days: 7
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)

_SUPPORTED_METRICS = ["pe_ttm", "pb", "ps_ttm", "dyr"]

# akshare column name → our metric name
_COL_MAP = {
    "pe": "pe_ttm",
    "pe_ttm": "pe_ttm",
    "pb": "pb",
    "ps": "ps_ttm",
    "ps_ttm": "ps_ttm",
    "dyr": "dyr",
    "股息率": "dyr",
    "市盈率(TTM)": "pe_ttm",
    "市净率": "pb",
    "市销率(TTM)": "ps_ttm",
}


@FactorRegistry.register("fundamental")
class FundamentalFactor(BaseFactor):
    """Historical valuation factors fetched from akshare.

    Parameters
    ----------
    metrics : list[str], optional
        Subset of ``["pe_ttm", "pb", "ps_ttm", "dyr"]`` to compute.
        Defaults to ``["pe_ttm", "pb"]``.
    cache_dir : str
        Directory for per-stock CSV caches.
    cache_ttl_days : int
        Refresh cache files older than this many days.  0 = always refresh.
    max_workers : int
        Thread count for parallel per-stock fetches.
    precomputed : DataFrame, optional
        Provide your own (instrument, datetime) MultiIndex DataFrame to skip
        the akshare fetch entirely — useful for testing or custom data.
    """

    def __init__(
        self,
        metrics: Optional[List[str]] = None,
        cache_dir: str = "./cache/fundamental",
        cache_ttl_days: int = 7,
        max_workers: int = 4,
        precomputed: Optional[pd.DataFrame] = None,
    ):
        self.metrics = metrics or ["pe_ttm", "pb"]
        self.cache_dir = Path(cache_dir)
        self.cache_ttl_days = cache_ttl_days
        self.max_workers = max_workers
        self.precomputed = precomputed

    # ── BaseFactor interface ──────────────────────────────────────────────────

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        if self.precomputed is not None:
            return self._align(self.precomputed, price_data)

        instruments = list(price_data.index.get_level_values(0).unique())
        all_frames = self._fetch_all(instruments)
        if not all_frames:
            return None

        combined = pd.concat(all_frames)
        return self._align(combined, price_data)

    # ── internals ────────────────────────────────────────────────────────────

    def _align(self, data: pd.DataFrame, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Forward-fill data to match the price_data MultiIndex."""
        instruments = price_data.index.get_level_values(0).unique()
        dates = price_data.index.get_level_values(1).unique()

        target = pd.MultiIndex.from_product(
            [instruments, dates], names=["instrument", "datetime"]
        )
        aligned = data.reindex(target)
        # Forward-fill within each instrument (announcements are infrequent)
        aligned = (
            aligned.groupby(level=0, group_keys=False)
            .apply(lambda g: g.ffill())
        )
        aligned = aligned.reindex(price_data.index)

        # Keep only requested metrics that actually exist
        keep = [c for c in self.metrics if c in aligned.columns]
        if not keep:
            return None
        return aligned[keep]

    def _fetch_all(self, instruments: List[str]) -> List[pd.DataFrame]:
        """Fetch/load all instruments in parallel."""
        frames: List[pd.DataFrame] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {pool.submit(self._load_one, sym): sym for sym in instruments}
            for fut in as_completed(futures):
                sym = futures[fut]
                try:
                    df = fut.result()
                    if df is not None and not df.empty:
                        frames.append(df)
                except Exception as exc:
                    logger.debug(f"FundamentalFactor: {sym} failed: {exc}")
        return frames

    def _load_one(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Return cached or freshly fetched fundamental data for one stock."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = self.cache_dir / f"{qlib_symbol}.csv"

        if self._cache_valid(cache_file):
            return self._read_cache(cache_file, qlib_symbol)

        df = self._fetch_akshare(qlib_symbol)
        if df is not None and not df.empty:
            df.to_csv(cache_file)
        return df

    def _cache_valid(self, path: Path) -> bool:
        if not path.exists():
            return False
        if self.cache_ttl_days == 0:
            return False
        mtime = date.fromtimestamp(path.stat().st_mtime)
        return (date.today() - mtime).days < self.cache_ttl_days

    def _read_cache(self, path: Path, qlib_symbol: str) -> Optional[pd.DataFrame]:
        try:
            df = pd.read_csv(path, index_col=[0, 1], parse_dates=[1])
            df.index.names = ["instrument", "datetime"]
            return df
        except Exception as exc:
            logger.warning(f"FundamentalFactor: cache read failed {path}: {exc}")
            return None

    def _fetch_akshare(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Fetch historical valuation indicators from akshare.

        Uses ``ak.stock_a_lg_indicator(symbol=code)`` which returns a
        DataFrame with columns like pe_ttm, pb, ps_ttm, dyr indexed by date.
        """
        try:
            import akshare as ak
        except ImportError:
            logger.warning("FundamentalFactor: akshare not installed")
            return None

        code = qlib_symbol[2:]  # "SH600000" → "600000"
        try:
            raw = ak.stock_a_lg_indicator(symbol=code)
        except Exception as exc:
            logger.debug(f"FundamentalFactor: akshare fetch failed for {qlib_symbol}: {exc}")
            return None

        if raw is None or raw.empty:
            return None

        raw = raw.copy()
        # Normalise date column
        date_col = next(
            (c for c in raw.columns if "date" in c.lower() or "日期" in c),
            raw.columns[0],
        )
        raw[date_col] = pd.to_datetime(raw[date_col])
        raw = raw.set_index(date_col)
        raw.index.name = "datetime"

        # Normalise metric column names
        rename = {c: _COL_MAP[c] for c in raw.columns if c in _COL_MAP}
        raw = raw.rename(columns=rename)

        # Keep only supported metrics
        keep = [c for c in _SUPPORTED_METRICS if c in raw.columns]
        if not keep:
            return None
        raw = raw[keep]
        raw = raw.apply(pd.to_numeric, errors="coerce")

        # Build (instrument, datetime) MultiIndex
        raw.index = pd.MultiIndex.from_product(
            [[qlib_symbol], raw.index], names=["instrument", "datetime"]
        )
        return raw
