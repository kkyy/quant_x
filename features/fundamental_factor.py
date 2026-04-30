"""FundamentalFactor — valuation and profitability data from akshare.

Fetches historical PE/PB/PS/dividend yield per stock using akshare's
``stock_a_lg_indicator`` API, caches per-stock CSVs locally, and aligns the
data to the price_data MultiIndex for use in the feature pipeline.

For extended metrics (profitability, growth, cashflow), delegates to
``FinancialFetcher`` which uses Sina/EM financial analysis indicators.

Supported metrics
-----------------
**Valuation (akshare direct):**
pe_ttm      Price-earnings ratio (trailing twelve months)
pb          Price-to-book ratio
ps_ttm      Price-to-sales ratio (TTM)
dyr         Dividend yield (%)

**Profitability (via FinancialFetcher):**
roe         Return on equity (%)
roa         Return on assets (%)
gross_margin  Gross margin (%)
net_margin    Net margin (%)

**Growth (via FinancialFetcher):**
revenue_growth  Revenue growth (%)
profit_growth   Profit growth (%)

**Cashflow (via FinancialFetcher):**
ocf_to_np   Operating cash flow to net profit ratio (%)
fcf_yield   Free cash flow yield (%)

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
    # Extended metrics via group names:
        - name: fundamental
          metrics: [valuation, profitability, growth]
          include_change: true
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)

_SUPPORTED_METRICS = ["pe_ttm", "pb", "ps_ttm", "dyr"]

# ── Metric groups for extended fundamental factors ─────────────────────────
_METRIC_GROUPS = {
    "valuation": ["pe_ttm", "pb", "ps_ttm", "dyr"],
    "profitability": ["roe", "roa", "gross_margin", "net_margin"],
    "growth": ["revenue_growth", "profit_growth"],
    "cashflow": ["ocf_to_np", "fcf_yield"],
}

_ALL_METRICS: List[str] = []
for _group_metrics in _METRIC_GROUPS.values():
    _ALL_METRICS.extend(_group_metrics)

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
    """Historical valuation and extended fundamental factors.

    Parameters
    ----------
    metrics : list[str], optional
        Subset of metric names or group names to compute.
        Group names: ``"valuation"``, ``"profitability"``, ``"growth"``,
        ``"cashflow"``.
        Individual names: ``"pe_ttm"``, ``"pb"``, ``"roe"``, etc.
        Defaults to ``["valuation"]`` for backward compatibility.
    include_change : bool
        If True, add change factors (roe_chg, margin_chg, rev_accel).
    cache_dir : str
        Directory for per-stock CSV caches.
    cache_ttl_days : int
        Refresh cache files older than this many days.  0 = always refresh.
    max_workers : int
        Thread count for parallel per-stock fetches.
    precomputed : DataFrame, optional
        Provide your own (instrument, datetime) MultiIndex DataFrame to skip
        the fetch entirely — useful for testing or custom data.
    """

    def __init__(
        self,
        metrics: Optional[List[str]] = None,
        include_change: bool = False,
        cache_dir: str = "./cache/fundamental",
        cache_ttl_days: int = 7,
        max_workers: int = 4,
        precomputed: Optional[pd.DataFrame] = None,
    ):
        self.include_change = include_change
        self.cache_dir = Path(cache_dir)
        self.cache_ttl_days = cache_ttl_days
        self.max_workers = max_workers
        self.precomputed = precomputed

        # Expand group names to individual metrics; default = ["valuation"]
        raw_metrics = metrics if metrics is not None else ["valuation"]
        expanded: List[str] = []
        for m in raw_metrics:
            if m in _METRIC_GROUPS:
                expanded.extend(_METRIC_GROUPS[m])
            else:
                expanded.append(m)
        self.metrics = expanded

        # Determine fetch path: use FinancialFetcher when any non-valuation
        # metric is requested
        valuation_set = set(_METRIC_GROUPS["valuation"])
        non_valuation = [m for m in self.metrics if m not in valuation_set]
        self._use_fetcher = len(non_valuation) > 0

    # ── backward compat attribute ───────────────────────────────────────────

    def __setstate__(self, state):
        """Ensure old pickles get new attributes with safe defaults."""
        self.__dict__.update(state)
        self._ensure_runtime_defaults()

    def _ensure_runtime_defaults(self):
        """Fill in attributes added after initial release."""
        if not hasattr(self, "include_change"):
            self.include_change = False
        if not hasattr(self, "_use_fetcher"):
            self._use_fetcher = False

    # ── BaseFactor interface ──────────────────────────────────────────────────

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        if self.precomputed is not None:
            result = self._align(self.precomputed, price_data)
        elif self._use_fetcher:
            result = self._compute_via_fetcher(price_data)
        else:
            # Old direct-akshare path for valuation-only metrics
            instruments = list(price_data.index.get_level_values(0).unique())
            all_frames = self._fetch_all(instruments)
            if not all_frames:
                return None
            combined = pd.concat(all_frames)
            result = self._align(combined, price_data)

        if result is None:
            return None

        # Add change factors if requested
        if self.include_change:
            result = self._compute_change_factors(result, price_data)

        return result

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

    # ── Extended metrics via FinancialFetcher ─────────────────────────────────

    def _compute_via_fetcher(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Fetch extended fundamental metrics via FinancialFetcher.

        Combines valuation data from the old akshare path with financial
        statement data from FinancialFetcher.
        """
        instruments = list(price_data.index.get_level_values(0).unique())
        all_frames: List[pd.DataFrame] = []

        # Determine which metrics come from which source
        valuation_metrics = [m for m in self.metrics if m in _METRIC_GROUPS["valuation"]]
        extended_metrics = [m for m in self.metrics if m not in _METRIC_GROUPS["valuation"]]

        # Fetch valuation metrics via old akshare path if needed
        if valuation_metrics:
            valuation_frames = self._fetch_all(instruments)
            if valuation_frames:
                val_combined = pd.concat(valuation_frames)
                # Filter to only requested valuation columns
                val_cols = [c for c in valuation_metrics if c in val_combined.columns]
                if val_cols:
                    all_frames.append(val_combined[val_cols])

        # Fetch extended metrics via FinancialFetcher
        if extended_metrics:
            try:
                from ..data.fetchers.financial_fetcher import FinancialFetcher

                fetcher = FinancialFetcher(
                    cache_dir=str(self.cache_dir.parent / "financial"),
                    cache_ttl_days=self.cache_ttl_days,
                )
                dates = price_data.index.get_level_values(1)
                start_date = str(dates.min().date())
                end_date = str(dates.max().date())
                ext_data = fetcher.fetch(instruments, start_date, end_date)
                if ext_data is not None and not ext_data.empty:
                    ext_cols = [c for c in extended_metrics if c in ext_data.columns]
                    if ext_cols:
                        all_frames.append(ext_data[ext_cols])
            except Exception as exc:
                logger.warning(
                    f"FundamentalFactor: FinancialFetcher failed, "
                    f"extended metrics unavailable: {exc}"
                )

        if not all_frames:
            return None

        combined = pd.concat(all_frames, axis=1)
        return self._align(combined, price_data)

    def _compute_change_factors(
        self, data: pd.DataFrame, price_data: pd.DataFrame
    ) -> pd.DataFrame:
        """Compute period-over-period change factors.

        Adds:
        - roe_chg: ROE change (current - prior period)
        - margin_chg: gross_margin change
        - rev_accel: revenue_growth acceleration (current - prior)
        """
        result = data.copy()

        # Compute changes within each instrument group
        if "roe" in result.columns:
            result["roe_chg"] = result.groupby(level=0)["roe"].diff()

        if "gross_margin" in result.columns:
            result["margin_chg"] = result.groupby(level=0)["gross_margin"].diff()

        if "revenue_growth" in result.columns:
            result["rev_accel"] = result.groupby(level=0)["revenue_growth"].diff()

        return result
