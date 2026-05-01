"""Valuation data fetcher.

Primary: akshare stock_value_em(symbol="600519") — per-stock daily valuation history.
  Returns columns: 数据日期, 当日收盘价, 当日涨跌幅, 总市值, 流通市值, 总股本, 流通股本,
                   PE(TTM), PE(静), 市净率, PEG值, 市现率, 市销率

Fallback: akshare stock_a_lg_indicator(symbol=code) — legacy valuation path.
  Note: stock_a_lg_indicator may be broken/renamed in akshare 1.17.x+.

Cache strategy: one CSV per stock under cache/valuation/{SYMBOL}.csv
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

from .base import BaseDataFetcher

logger = logging.getLogger(__name__)

# stock_value_em Chinese column names → our English metric names
_VALUE_EM_COL_MAP = {
    "数据日期": "datetime",
    "当日收盘价": "close",
    "当日涨跌幅": "pct_change",
    "总市值": "market_cap",
    "流通市值": "float_market_cap",
    "总股本": "total_shares",
    "流通股本": "float_shares",
    "PE(TTM)": "pe_ttm",
    "PE(静)": "pe_static",
    "市净率": "pb",
    "PEG值": "peg",
    "市现率": "pcf",
    "市销率": "ps_ttm",
}

# All metrics we produce from stock_value_em (excluding date/close/pct_change)
_VALUE_EM_METRICS = [
    "market_cap",
    "float_market_cap",
    "total_shares",
    "float_shares",
    "pe_ttm",
    "pe_static",
    "pb",
    "peg",
    "pcf",
    "ps_ttm",
]

# Fallback: stock_a_lg_indicator column mapping
_LG_INDICATOR_COL_MAP = {
    "pe": "pe_ttm",
    "pe_ttm": "pe_ttm",
    "pb": "pb",
    "ps": "ps_ttm",
    "ps_ttm": "ps_ttm",
    "dyr": "dyr",
    "总市值": "market_cap",
}

_LG_METRICS = ["pe_ttm", "pb", "ps_ttm", "dyr", "market_cap"]


class ValuationFetcher(BaseDataFetcher):
    """Fetch and cache per-stock valuation data.

    Uses akshare ``stock_value_em`` (primary) with ``stock_a_lg_indicator``
    as fallback.
    """

    def __init__(
        self,
        cache_dir: str = "./cache/valuation",
        cache_ttl_days: int = 1,
        max_workers: int = 8,
    ):
        super().__init__(cache_dir=cache_dir, cache_ttl_days=cache_ttl_days)
        self.max_workers = max_workers

    def fetch(
        self, symbols: List[str], start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """Fetch valuation data for symbols in the given date range."""
        self.refresh_cache(symbols)
        return self._load_cached_range(symbols, start_date, end_date)

    def refresh_cache(self, symbols: List[str]) -> None:
        """Refresh per-stock cache files for the given symbols."""
        self._ensure_cache_dir()
        if len(symbols) <= 1:
            for sym in symbols:
                self._fetch_one(sym)
            return

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {pool.submit(self._fetch_one, sym): sym for sym in symbols}
            for future in as_completed(futures):
                sym = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    logger.warning(
                        "ValuationFetcher: refresh failed for %s: %s", sym, exc
                    )

    # ── Per-stock fetch ──────────────────────────────────────────────────────

    def _fetch_one(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Fetch (or read from cache) valuation data for a single stock."""
        self._ensure_cache_dir()
        cache_file = self.cache_dir / f"{qlib_symbol}.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        df = self._fetch_one_with_fallback(qlib_symbol)
        if df is not None and not df.empty:
            df.to_csv(cache_file)
            logger.debug("ValuationFetcher: cached data for %s", qlib_symbol)
        return df

    def _fetch_one_with_fallback(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Try primary API, then fallback."""
        # Primary: stock_value_em (bare 6-digit code)
        try:
            raw_primary = self._call_stock_value_em(qlib_symbol)
        except Exception as exc:
            logger.debug(
                "ValuationFetcher: stock_value_em failed for %s: %s",
                qlib_symbol,
                exc,
            )
            raw_primary = None

        primary_df = (
            self._normalize_stock_value_em(raw_primary, qlib_symbol)
            if raw_primary is not None
            else None
        )

        # Fallback: stock_a_lg_indicator (bare 6-digit code)
        try:
            raw_fallback = self._call_stock_a_lg_indicator(qlib_symbol)
        except Exception as exc:
            logger.debug(
                "ValuationFetcher: stock_a_lg_indicator fallback failed for %s: %s",
                qlib_symbol,
                exc,
            )
            raw_fallback = None

        fallback_df = (
            self._normalize_lg_indicator(raw_fallback, qlib_symbol)
            if raw_fallback is not None
            else None
        )

        # Merge: primary wins, fallback fills gaps
        if primary_df is not None:
            return self._merge_valuation_frames(primary_df, fallback_df)

        return fallback_df

    # ── Primary: stock_value_em ──────────────────────────────────────────────

    def _call_stock_value_em(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Call ak.stock_value_em(symbol=bare_code)."""
        import akshare as ak

        code = self.to_bare_code(qlib_symbol)
        return ak.stock_value_em(symbol=code)

    def _normalize_stock_value_em(
        self, raw: pd.DataFrame, qlib_symbol: str
    ) -> Optional[pd.DataFrame]:
        """Convert stock_value_em output to (instrument, datetime) MultiIndex."""
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        # Identify and rename the date column
        date_col = next(
            (c for c in df.columns if c in _VALUE_EM_COL_MAP and _VALUE_EM_COL_MAP[c] == "datetime"),
            next((c for c in df.columns if "日期" in str(c) or "date" in str(c).lower()), None),
        )
        if date_col is None:
            return None

        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col)
        df.index.name = "datetime"

        # Rename Chinese columns to English using the map
        rename = {}
        for c in df.columns:
            if c in _VALUE_EM_COL_MAP:
                target = _VALUE_EM_COL_MAP[c]
                if target != "datetime":  # skip the date column (already index)
                    rename[c] = target
        df = df.rename(columns=rename)

        # Keep only known metrics
        keep = [c for c in _VALUE_EM_METRICS if c in df.columns]
        if not keep:
            return None
        df = df[keep].apply(pd.to_numeric, errors="coerce")

        # Build (instrument, datetime) MultiIndex
        df.index = pd.MultiIndex.from_product(
            [[qlib_symbol], df.index], names=["instrument", "datetime"]
        )
        return df

    # ── Fallback: stock_a_lg_indicator ───────────────────────────────────────

    def _call_stock_a_lg_indicator(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Call ak.stock_a_lg_indicator(symbol=bare_code)."""
        import akshare as ak

        code = self.to_bare_code(qlib_symbol)
        return ak.stock_a_lg_indicator(symbol=code)

    def _normalize_lg_indicator(
        self, raw: pd.DataFrame, qlib_symbol: str
    ) -> Optional[pd.DataFrame]:
        """Convert stock_a_lg_indicator output to (instrument, datetime) MultiIndex."""
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        # Identify the date column
        date_col = next(
            (c for c in df.columns if "date" in c.lower() or "日期" in c),
            df.columns[0],
        )
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col)
        df.index.name = "datetime"

        # Rename columns using the map
        rename = {c: _LG_INDICATOR_COL_MAP[c] for c in df.columns if c in _LG_INDICATOR_COL_MAP}
        df = df.rename(columns=rename)

        # Keep only known metrics
        keep = [c for c in _LG_METRICS if c in df.columns]
        if not keep:
            return None
        df = df[keep].apply(pd.to_numeric, errors="coerce")

        # Build (instrument, datetime) MultiIndex
        df.index = pd.MultiIndex.from_product(
            [[qlib_symbol], df.index], names=["instrument", "datetime"]
        )
        return df

    # ── Merge primary + fallback ─────────────────────────────────────────────

    @staticmethod
    def _merge_valuation_frames(
        primary: pd.DataFrame, fallback: Optional[pd.DataFrame]
    ) -> pd.DataFrame:
        """Merge primary and fallback DataFrames.

        Primary columns take precedence. Fallback fills NaN gaps and adds
        columns not present in primary (e.g. dyr).
        """
        if fallback is None or fallback.empty:
            return primary

        result = primary.copy()
        fallback = fallback.reindex(result.index)
        for column in fallback.columns:
            if column not in result.columns:
                result[column] = fallback[column]
            else:
                result[column] = result[column].combine_first(fallback[column])
        return result

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _read_cache(self, path: Path) -> Optional[pd.DataFrame]:
        """Read a cached per-stock CSV file."""
        try:
            df = pd.read_csv(path, index_col=[0, 1], parse_dates=[1])
            df.index.names = ["instrument", "datetime"]
            return df
        except Exception as exc:
            logger.warning("ValuationFetcher: cache read failed %s: %s", path, exc)
            return None

    def _load_cached_range(
        self, symbols: List[str], start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """Load and concatenate cached per-stock files filtered by date range."""
        frames: List[pd.DataFrame] = []
        for sym in symbols:
            cache_file = self.cache_dir / f"{sym}.csv"
            if not cache_file.exists():
                continue
            try:
                df = pd.read_csv(cache_file, index_col=[0, 1], parse_dates=[1])
                df.index.names = ["instrument", "datetime"]
                dates = df.index.get_level_values(1)
                mask = (dates >= pd.Timestamp(start_date)) & (
                    dates <= pd.Timestamp(end_date)
                )
                if mask.any():
                    frames.append(df[mask])
            except Exception:
                continue
        if not frames:
            return None
        return pd.concat(frames)
