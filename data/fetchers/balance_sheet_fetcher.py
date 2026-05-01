"""Balance sheet (资产负债表) data fetcher.

Primary: akshare stock_balance_sheet_by_report_em(symbol="600519.SH")
    Returns ~319 columns per reporting period. We keep a curated subset of ~12.

    NOTE: This API requires "code.exchange" format (e.g. "600519.SH", "000001.SZ",
    "920000.BJ"), NOT the bare 6-digit code.

Fallback: akshare stock_zcfz_em(date="20240331")
    Cross-sectional balance sheet for a single reporting date.
    Less comprehensive but available when the per-stock API fails.

Cache strategy:
    cache/balance_sheet/{SYMBOL}.csv  (1 file per stock)
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

# East Money balance sheet column names → our English metric names
# The EM API returns ~319 columns; we only keep these ~12 key items.
_BS_COL_MAP = {
    "报告期": "report_date",
    "营业总收入": "revenue",
    "TOTAL_OPERATE_INCOME": "revenue",
    "净利润": "net_profit",
    "PARENT_NETPROFIT": "net_profit",
    "总资产": "total_assets",
    "TOTAL_ASSETS": "total_assets",
    "所有者权益合计": "total_equity",
    "TOTAL_EQUITY": "total_equity",
    "归属于母公司所有者权益合计": "total_equity",
    "TOTAL_PARENT_EQUITY": "total_equity",
    "总负债": "total_liabilities",
    "TOTAL_LIABILITIES": "total_liabilities",
    "流动资产合计": "current_assets",
    "TOTAL_CURRENT_ASSETS": "current_assets",
    "流动负债合计": "current_liabilities",
    "TOTAL_CURRENT_LIAB": "current_liabilities",
    "存货": "inventory",
    "INVENTORY": "inventory",
    "商誉": "goodwill",
    "GOODWILL": "goodwill",
    "货币资金": "cash",
    "MONETARYFUNDS": "cash",
    "短期借款": "short_term_debt",
    "SHORT_LOAN": "short_term_debt",
    "长期借款": "long_term_debt",
    "LONG_LOAN": "long_term_debt",
}

# The ordered list of metric columns we keep in the output
_KEEP_COLS = [
    "revenue",
    "net_profit",
    "total_assets",
    "total_equity",
    "total_liabilities",
    "current_assets",
    "current_liabilities",
    "inventory",
    "goodwill",
    "cash",
    "short_term_debt",
    "long_term_debt",
]


class BalanceSheetFetcher(BaseDataFetcher):
    """Fetch and cache balance sheet data from akshare."""

    def __init__(
        self,
        cache_dir: str = "./cache/balance_sheet",
        cache_ttl_days: int = 30,
        max_workers: int = 4,
    ):
        super().__init__(cache_dir=cache_dir, cache_ttl_days=cache_ttl_days)
        self.max_workers = max_workers

    def fetch(
        self, symbols: List[str], start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """Fetch balance sheet data for symbols in date range.

        Returns DataFrame with (instrument, datetime) MultiIndex, or None.
        """
        self.refresh_cache(symbols)
        return self._load_cached_range(symbols, start_date, end_date)

    def refresh_cache(self, symbols: List[str]) -> None:
        """Refresh per-stock balance sheet cache files."""
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
                        "BalanceSheetFetcher: refresh failed for %s: %s", sym, exc
                    )

    # ── Per-stock fetch ────────────────────────────────────────────────────

    def _fetch_one(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Fetch balance sheet for a single stock.

        Tries primary (EM per-stock) source first, falls back to cross-sectional
        EM source. Caches result as cache/balance_sheet/{SYMBOL}.csv.
        """
        self._ensure_cache_dir()
        cache_file = self.cache_dir / f"{qlib_symbol}.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        df = self._fetch_one_with_fallback(qlib_symbol)
        if df is not None and not df.empty:
            df.to_csv(cache_file)
            logger.debug(
                "BalanceSheetFetcher: cached balance sheet for %s", qlib_symbol
            )
        return df

    def _fetch_one_with_fallback(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Try primary per-stock EM source, then cross-sectional fallback."""
        # Primary: per-stock EM API
        try:
            raw = self._call_akshare_em(qlib_symbol)
        except Exception as exc:
            logger.debug(
                "BalanceSheetFetcher: EM primary failed for %s: %s",
                qlib_symbol,
                exc,
            )
            raw = None

        df = self._normalize_balance_sheet(raw, qlib_symbol) if raw is not None else None

        if df is not None:
            return df

        # Fallback: cross-sectional EM API for the most recent reporting date
        try:
            raw_cs = self._call_akshare_cross_section(qlib_symbol)
        except Exception as exc:
            logger.debug(
                "BalanceSheetFetcher: EM cross-section fallback failed for %s: %s",
                qlib_symbol,
                exc,
            )
            raw_cs = None

        cs_df = (
            self._normalize_cross_section(raw_cs, qlib_symbol)
            if raw_cs is not None
            else None
        )
        return cs_df

    # ── API calls ──────────────────────────────────────────────────────────

    def _call_akshare_em(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Call akshare EM per-stock balance sheet API.

        stock_balance_sheet_by_report_em requires "code.exchange" format,
        e.g. "600519.SH", "000001.SZ", "920000.BJ".
        """
        import akshare as ak

        bare_code = self.to_bare_code(qlib_symbol)
        exchange = self.infer_exchange(bare_code)
        em_code = f"{bare_code}.{exchange}"
        return ak.stock_balance_sheet_by_report_em(symbol=em_code)

    def _call_akshare_cross_section(
        self, qlib_symbol: str
    ) -> Optional[pd.DataFrame]:
        """Call akshare EM cross-sectional balance sheet API (fallback).

        stock_zcfz_em returns data for ALL stocks at a given reporting date.
        We use the most recent quarter-end date.
        """
        import akshare as ak

        # Use the most recent quarter-end date
        today = date.today()
        quarter_end = self._most_recent_quarter_end(today)
        date_str = quarter_end.strftime("%Y%m%d")
        return ak.stock_zcfz_em(date=date_str)

    @staticmethod
    def _most_recent_quarter_end(ref: date) -> date:
        """Return the most recent quarter-end date on or before *ref*.

        Quarter ends: March 31, June 30, September 30, December 31.
        """
        year = ref.year
        candidates = [
            date(year, 3, 31),
            date(year, 6, 30),
            date(year, 9, 30),
            date(year - 1, 12, 31),
        ]
        for c in reversed(candidates):
            if c <= ref:
                return c
        # Should not reach here, but fallback to previous year Dec
        return date(year - 1, 12, 31)

    # ── Normalization ──────────────────────────────────────────────────────

    def _normalize_balance_sheet(
        self, raw: pd.DataFrame, qlib_symbol: str
    ) -> Optional[pd.DataFrame]:
        """Normalize EM per-stock balance sheet to (instrument, datetime) MultiIndex.

        The raw DataFrame has ~319 columns. We filter to the curated subset
        defined in _BS_COL_MAP / _KEEP_COLS.
        """
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        # Find the report-date column
        date_col = next(
            (
                c
                for c in df.columns
                if "报告期" in str(c) or "REPORT_DATE" in str(c) or "date" in str(c).lower()
            ),
            df.columns[0],
        )
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col])
        df = df.set_index(date_col)
        df.index.name = "datetime"

        # Rename EM columns to our English names
        rename = {c: _BS_COL_MAP[c] for c in df.columns if c in _BS_COL_MAP}
        df = df.rename(columns=rename)

        # Keep only the curated columns
        keep = [c for c in _KEEP_COLS if c in df.columns]
        if not keep:
            return None

        df = df[keep]
        df = df.apply(pd.to_numeric, errors="coerce")

        # Drop rows that are entirely NaN (all financial items missing)
        df = df.dropna(how="all")

        if df.empty:
            return None

        # Deduplicate index (some stocks have duplicate report periods)
        df = df[~df.index.duplicated(keep="last")]
        df = df.sort_index()

        # Build (instrument, datetime) MultiIndex
        df.index = pd.MultiIndex.from_product(
            [[qlib_symbol], df.index], names=["instrument", "datetime"]
        )
        return df

    def _normalize_cross_section(
        self, raw: pd.DataFrame, qlib_symbol: str
    ) -> Optional[pd.DataFrame]:
        """Normalize cross-sectional EM balance sheet for a single instrument.

        The cross-sectional API returns one row per stock for a single date.
        We extract the row matching our symbol and build the standard MultiIndex.
        """
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        # Find the stock-code column
        code_col = next(
            (
                c
                for c in df.columns
                if "代码" in str(c) or "code" in str(c).lower() or "股票代码" in str(c)
            ),
            None,
        )
        if code_col is None:
            logger.debug(
                "BalanceSheetFetcher: no stock code column in cross-section data"
            )
            return None

        bare_code = self.to_bare_code(qlib_symbol)

        # Filter to our stock (the cross-section code may be 6-digit bare or EM format)
        mask = df[code_col].astype(str).str.contains(bare_code)
        row = df[mask]
        if row.empty:
            return None

        row = row.iloc[[0]]  # Take first match

        # Find date column
        date_col = next(
            (
                c
                for c in row.columns
                if "报告期" in str(c) or "日期" in str(c) or "date" in str(c).lower()
            ),
            None,
        )

        if date_col is not None:
            dt = pd.to_datetime(row[date_col].iloc[0], errors="coerce")
        else:
            dt = pd.Timestamp("today").normalize()

        if pd.isna(dt):
            return None

        # Rename columns
        rename = {c: _BS_COL_MAP[c] for c in row.columns if c in _BS_COL_MAP}
        row = row.rename(columns=rename)

        keep = [c for c in _KEEP_COLS if c in row.columns]
        if not keep:
            return None

        row = row[keep]
        row = row.apply(pd.to_numeric, errors="coerce")
        row = row.dropna(how="all", axis=1)

        if row.empty:
            return None

        # Build (instrument, datetime) MultiIndex
        row.index = pd.MultiIndex.from_tuples(
            [(qlib_symbol, dt)], names=["instrument", "datetime"]
        )
        return row

    # ── EM code format helper ──────────────────────────────────────────────

    def build_em_code(self, qlib_symbol: str) -> str:
        """Build EM code format: "600519.SH", "000001.SZ", "920000.BJ".

        This is a public helper for testing the code format transformation.
        """
        bare_code = self.to_bare_code(qlib_symbol)
        exchange = self.infer_exchange(bare_code)
        return f"{bare_code}.{exchange}"

    # ── Cache helpers ──────────────────────────────────────────────────────

    def _read_cache(self, path: Path) -> Optional[pd.DataFrame]:
        try:
            df = pd.read_csv(path, index_col=[0, 1], parse_dates=[1])
            df.index.names = ["instrument", "datetime"]
            return df
        except Exception as exc:
            logger.warning(
                "BalanceSheetFetcher: cache read failed %s: %s", path, exc
            )
            return None

    def _load_cached_range(
        self, symbols: List[str], start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
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
