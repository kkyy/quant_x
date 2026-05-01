"""Dividend (分红) data fetcher.

Primary: akshare stock_history_dividend_detail(symbol="000001", indicator="分红", date="")
    Returns: 公告日期, 送股, 转增, 派息, 进度, 除权除息日, 股权登记日, 红股上市日

Fallback: akshare stock_fhps_detail_em(symbol="300073")
    East Money dividend detail, returns per-stock dividend history.

Cache strategy:
    cache/dividend/{SYMBOL}.csv  (1 file per stock)
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

# akshare Sina dividend column names → our English names
_SINA_DIV_COL_MAP = {
    "公告日期": "announcement_date",
    "送股": "bonus_shares",
    "转增": "conversion_shares",
    "派息": "cash_dividend",
    "进度": "progress",
    "除权除息日": "ex_date",
    "股权登记日": "record_date",
    "红股上市日": "bonus_listing_date",
}

# EM dividend column names → our English names
_EM_DIV_COL_MAP = {
    "公告日期": "announcement_date",
    "送股(股)": "bonus_shares",
    "转增(股)": "conversion_shares",
    "派息(元)": "cash_dividend",
    "进度": "progress",
    "除权除息日": "ex_date",
    "股权登记日": "record_date",
    "红股上市日": "bonus_listing_date",
    # EM variant column names
    "分红": "cash_dividend",
    "送转股": "bonus_shares",
    "派现": "cash_dividend_em",
}

# Columns we always keep in the output
_KEEP_COLS = [
    "announcement_date",
    "bonus_shares",
    "conversion_shares",
    "cash_dividend",
    "progress",
    "ex_date",
    "record_date",
]


class DividendFetcher(BaseDataFetcher):
    """Fetch and cache dividend history data."""

    def __init__(self, cache_dir: str = "./cache/dividend", cache_ttl_days: int = 30, max_workers: int = 8):
        super().__init__(cache_dir=cache_dir, cache_ttl_days=cache_ttl_days)
        self.max_workers = max_workers

    def fetch(self, symbols: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Fetch dividend data for symbols in date range.

        Returns DataFrame with (instrument, datetime) MultiIndex, or None.
        """
        self.refresh_cache(symbols)
        return self._load_cached_range(symbols, start_date, end_date)

    def refresh_cache(self, symbols: List[str]) -> None:
        """Refresh per-stock dividend cache files."""
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
                    logger.warning("DividendFetcher: refresh failed for %s: %s", sym, exc)

    # ── Per-stock fetch ────────────────────────────────────────────────────

    def _fetch_one(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Fetch dividend history for a single stock.

        Tries primary (Sina) source first, falls back to EM source.
        Caches result as cache/dividend/{SYMBOL}.csv.
        """
        self._ensure_cache_dir()
        cache_file = self.cache_dir / f"{qlib_symbol}.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        df = self._fetch_one_with_fallback(qlib_symbol)
        if df is not None and not df.empty:
            df.to_csv(cache_file)
            logger.debug("DividendFetcher: cached dividend data for %s", qlib_symbol)
        return df

    def _fetch_one_with_fallback(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Try primary Sina source, then EM fallback."""
        # Primary: Sina interface
        try:
            raw_sina = self._call_akshare_sina(qlib_symbol)
        except Exception as exc:
            logger.debug("DividendFetcher: Sina failed for %s: %s", qlib_symbol, exc)
            raw_sina = None

        sina_df = self._normalize_sina(raw_sina, qlib_symbol) if raw_sina is not None else None

        if sina_df is not None:
            return sina_df

        # Fallback: EM interface
        try:
            raw_em = self._call_akshare_em(qlib_symbol)
        except Exception as exc:
            logger.debug("DividendFetcher: EM fallback failed for %s: %s", qlib_symbol, exc)
            raw_em = None

        em_df = self._normalize_em(raw_em, qlib_symbol) if raw_em is not None else None
        return em_df

    def _call_akshare_sina(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Call akshare Sina dividend detail API.

        stock_history_dividend_detail returns history of dividend events.
        indicator="分红" for cash/stock dividends.
        date="" means return all available records.
        """
        import akshare as ak
        code = self.to_bare_code(qlib_symbol)
        return ak.stock_history_dividend_detail(symbol=code, indicator="分红", date="")

    def _call_akshare_em(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Call akshare EM dividend detail API (fallback)."""
        import akshare as ak
        code = self.to_bare_code(qlib_symbol)
        return ak.stock_fhps_detail_em(symbol=code)

    # ── Normalization ─────────────────────────────────────────────────────

    def _normalize_sina(self, raw: pd.DataFrame, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Normalize Sina dividend output to (instrument, datetime) MultiIndex.

        Uses ex_date (除权除息日) as the datetime index when available,
        falls back to announcement_date (公告日期).
        """
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        # Rename Chinese columns to English
        rename = {c: _SINA_DIV_COL_MAP[c] for c in df.columns if c in _SINA_DIV_COL_MAP}
        df = df.rename(columns=rename)

        # Parse date columns
        if "ex_date" in df.columns:
            df["ex_date"] = pd.to_datetime(df["ex_date"], errors="coerce")
        if "announcement_date" in df.columns:
            df["announcement_date"] = pd.to_datetime(df["announcement_date"], errors="coerce")
        if "record_date" in df.columns:
            df["record_date"] = pd.to_datetime(df["record_date"], errors="coerce")

        # Choose datetime: prefer ex_date, fall back to announcement_date
        if "ex_date" in df.columns and df["ex_date"].notna().any():
            df["datetime"] = df["ex_date"]
        elif "announcement_date" in df.columns and df["announcement_date"].notna().any():
            df["datetime"] = df["announcement_date"]
        else:
            logger.debug("DividendFetcher: no usable date column for %s", qlib_symbol)
            return None

        # Drop rows with no valid datetime
        df = df.dropna(subset=["datetime"])

        if df.empty:
            return None

        # Keep only the columns we need
        keep = [c for c in _KEEP_COLS if c in df.columns]
        if not keep:
            return None

        # Convert numeric columns
        for col in ["bonus_shares", "conversion_shares", "cash_dividend"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        # Build output
        result = df[keep + ["datetime"]].copy()
        result = result.set_index("datetime")
        result.index.name = "datetime"
        result.index = pd.MultiIndex.from_product(
            [[qlib_symbol], result.index], names=["instrument", "datetime"]
        )

        return result

    def _normalize_em(self, raw: pd.DataFrame, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Normalize EM dividend output to (instrument, datetime) MultiIndex.

        EM format may differ from Sina. We try to extract the same fields.
        """
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        # Rename Chinese columns using EM-specific map, then fall back to Sina map
        rename = {}
        for c in df.columns:
            if c in _EM_DIV_COL_MAP:
                rename[c] = _EM_DIV_COL_MAP[c]
            elif c in _SINA_DIV_COL_MAP:
                rename[c] = _SINA_DIV_COL_MAP[c]
        df = df.rename(columns=rename)

        # If we got cash_dividend_em but not cash_dividend, use it
        if "cash_dividend_em" in df.columns and "cash_dividend" not in df.columns:
            df["cash_dividend"] = df["cash_dividend_em"]
        if "cash_dividend_em" in df.columns:
            df = df.drop(columns=["cash_dividend_em"])

        # Parse date columns
        if "ex_date" in df.columns:
            df["ex_date"] = pd.to_datetime(df["ex_date"], errors="coerce")
        if "announcement_date" in df.columns:
            df["announcement_date"] = pd.to_datetime(df["announcement_date"], errors="coerce")
        if "record_date" in df.columns:
            df["record_date"] = pd.to_datetime(df["record_date"], errors="coerce")

        # EM may have a generic date column
        if "datetime" not in df.columns and "ex_date" not in df.columns and "announcement_date" not in df.columns:
            date_col = next(
                (c for c in df.columns if "日期" in str(c) or "date" in str(c).lower()),
                None,
            )
            if date_col is not None:
                df["announcement_date"] = pd.to_datetime(df[date_col], errors="coerce")

        # Choose datetime: prefer ex_date, fall back to announcement_date
        if "ex_date" in df.columns and df["ex_date"].notna().any():
            df["datetime"] = df["ex_date"]
        elif "announcement_date" in df.columns and df["announcement_date"].notna().any():
            df["datetime"] = df["announcement_date"]
        else:
            logger.debug("DividendFetcher: EM fallback no usable date for %s", qlib_symbol)
            return None

        df = df.dropna(subset=["datetime"])

        if df.empty:
            return None

        keep = [c for c in _KEEP_COLS if c in df.columns]
        if not keep:
            return None

        for col in ["bonus_shares", "conversion_shares", "cash_dividend"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        result = df[keep + ["datetime"]].copy()
        result = result.set_index("datetime")
        result.index.name = "datetime"
        result.index = pd.MultiIndex.from_product(
            [[qlib_symbol], result.index], names=["instrument", "datetime"]
        )

        return result

    # ── Helpers ────────────────────────────────────────────────────────────

    def _read_cache(self, path: Path) -> Optional[pd.DataFrame]:
        try:
            df = pd.read_csv(path, index_col=[0, 1], parse_dates=[1])
            df.index.names = ["instrument", "datetime"]
            return df
        except Exception as exc:
            logger.warning("DividendFetcher: cache read failed %s: %s", path, exc)
            return None

    def _load_cached_range(self, symbols: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Load and concatenate per-stock cache files in a date range."""
        frames = []
        for sym in symbols:
            cache_file = self.cache_dir / f"{sym}.csv"
            if not cache_file.exists():
                continue
            try:
                df = pd.read_csv(cache_file, index_col=[0, 1], parse_dates=[1])
                df.index.names = ["instrument", "datetime"]
                dates = df.index.get_level_values(1)
                mask = (dates >= pd.Timestamp(start_date)) & (dates <= pd.Timestamp(end_date))
                if mask.any():
                    frames.append(df[mask])
            except Exception:
                continue
        if not frames:
            return None
        return pd.concat(frames)
