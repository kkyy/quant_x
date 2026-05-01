"""Earnings guidance (业绩预告) data fetcher.

Primary: akshare stock_yjyg_em(date="20250331")
    Bulk API — returns all earnings guidance for a given reporting period.

Raw columns (akshare):
    序号, 股票代码, 股票简称, 预测指标, 业绩变动, 预测数值,
    业绩变动幅度, 业绩变动原因, 预告类型, 上年同期值, 公告日期

Normalized columns:
    guidance_type_raw  — 预告类型 (Chinese text: 预增/预减/首亏/续亏/…)
    earnings_change_pct — 业绩变动幅度 (percentage, e.g. 50 means +50%)
    prior_value        — 上年同期值
    forecast_value     — 预测数值
    reporting_period   — reporting quarter end date (from the date parameter)

Cache strategy:
    cache/earnings_guidance/yjyg_{YYYYMMDD}.csv  (1 file per reporting period)
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

from .base import BaseDataFetcher

logger = logging.getLogger(__name__)

# akshare Chinese column names → our English names
_YJYG_COL_MAP = {
    "股票代码": "bare_code",
    "股票简称": "stock_name",
    "预测指标": "metric",
    "业绩变动": "change_desc",
    "预测数值": "forecast_value",
    "业绩变动幅度": "earnings_change_pct",
    "业绩变动原因": "change_reason",
    "预告类型": "guidance_type_raw",
    "上年同期值": "prior_value",
    "公告日期": "announcement_date",
}

# Columns we keep in the cached output
_KEEP_COLS = [
    "guidance_type_raw",
    "earnings_change_pct",
    "prior_value",
    "forecast_value",
    "reporting_period",
]


class EarningsGuidanceFetcher(BaseDataFetcher):
    """Fetch and cache earnings guidance data.

    This is a bulk API: ``ak.stock_yjyg_em(date="20250331")`` returns
    guidance for ALL stocks for a reporting period, so the ``symbols``
    parameter to ``refresh_cache()`` is accepted but ignored.

    Parameters
    ----------
    cache_dir : str
        Directory for per-quarter CSV caches.
    cache_ttl_days : int
        Refresh cache files older than this many days.  0 = always refresh.
    num_quarters : int
        Number of recent quarters to fetch when refreshing.
    """

    def __init__(
        self,
        cache_dir: str = "./cache/earnings_guidance",
        cache_ttl_days: int = 30,
        num_quarters: int = 8,
    ):
        super().__init__(cache_dir=cache_dir, cache_ttl_days=cache_ttl_days)
        self.num_quarters = num_quarters

    # ── Public interface ─────────────────────────────────────────────────────

    def fetch(
        self, symbols: List[str], start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """Fetch earnings guidance for symbols in date range.

        Returns DataFrame with (instrument, datetime) MultiIndex, or None.
        """
        self.refresh_cache(symbols)
        return self._load_cached_range(start_date, end_date)

    def refresh_cache(self, symbols: List[str]) -> None:
        """Refresh cache files for recent quarters.

        ``symbols`` is accepted for interface compatibility but ignored —
        the underlying API returns all stocks at once.
        """
        self._ensure_cache_dir()
        quarter_ends = self._recent_quarter_ends()
        for qdate in quarter_ends:
            try:
                self._fetch_quarter(qdate)
            except Exception as exc:
                logger.warning(
                    "EarningsGuidanceFetcher: refresh failed for %s: %s",
                    qdate, exc,
                )

    # ── Quarter fetching ────────────────────────────────────────────────────

    def _fetch_quarter(self, date_str: str) -> Optional[pd.DataFrame]:
        """Fetch and cache earnings guidance for one reporting period.

        Parameters
        ----------
        date_str : str
            Reporting period end date in YYYYMMDD format, e.g. "20250331".

        Returns
        -------
        DataFrame with (instrument, datetime) MultiIndex, or None.
        """
        self._ensure_cache_dir()
        cache_file = self.cache_dir / f"yjyg_{date_str}.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        try:
            import akshare as ak
            raw = ak.stock_yjyg_em(date=date_str)
        except Exception as exc:
            logger.debug(
                "EarningsGuidanceFetcher: akshare stock_yjyg_em failed "
                "for %s: %s", date_str, exc,
            )
            return None

        if raw is None or raw.empty:
            logger.debug(
                "EarningsGuidanceFetcher: no data returned for %s", date_str
            )
            return None

        df = self._normalize(raw, date_str)
        if df is not None and not df.empty:
            df.to_csv(cache_file)
            logger.debug(
                "EarningsGuidanceFetcher: cached %d rows for %s",
                len(df), date_str,
            )
        return df

    # ── Normalization ───────────────────────────────────────────────────────

    def _normalize(
        self, raw: pd.DataFrame, reporting_period: str
    ) -> Optional[pd.DataFrame]:
        """Normalize raw akshare output to (instrument, datetime) MultiIndex.

        Parameters
        ----------
        raw : DataFrame
            Raw output from ``ak.stock_yjyg_em``.
        reporting_period : str
            The reporting period end date (YYYYMMDD), stored as metadata.
        """
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        # Rename Chinese columns to English
        rename = {c: _YJYG_COL_MAP[c] for c in df.columns if c in _YJYG_COL_MAP}
        df = df.rename(columns=rename)

        # We need bare_code and announcement_date at minimum
        if "bare_code" not in df.columns or "announcement_date" not in df.columns:
            logger.debug(
                "EarningsGuidanceFetcher: missing required columns "
                "after rename (have: %s)", list(df.columns)
            )
            return None

        # Build instrument from bare_code
        df["bare_code"] = df["bare_code"].astype(str).str.strip()
        # Handle cases where code might include market prefix (e.g. "SH600519")
        df["bare_code"] = df["bare_code"].str.replace(
            r"^(SH|SZ|BJ)", "", regex=True
        )
        # Strip leading zeros but keep at least 6 digits
        df["bare_code"] = df["bare_code"].str.zfill(6)
        df["instrument"] = df["bare_code"].apply(self._build_instrument)

        # Parse announcement_date
        df["announcement_date"] = pd.to_datetime(
            df["announcement_date"], errors="coerce"
        )
        df = df.dropna(subset=["announcement_date"])

        if df.empty:
            return None

        # Parse numeric columns
        for col in ["earnings_change_pct", "prior_value", "forecast_value"]:
            if col in df.columns:
                # Handle percentage strings like "50%" or "50.00%"
                if df[col].dtype == object:
                    df[col] = (
                        df[col].astype(str)
                        .str.replace("%", "", regex=False)
                        .str.strip()
                    )
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Store reporting period as a date column
        try:
            df["reporting_period"] = pd.Timestamp(reporting_period)
        except Exception:
            df["reporting_period"] = pd.NaT

        # Build (instrument, datetime) MultiIndex
        df = df.set_index("announcement_date")
        df.index.name = "datetime"
        instruments = df.pop("instrument").values
        df.index = pd.MultiIndex.from_arrays(
            [instruments, df.index],
            names=["instrument", "datetime"],
        )

        # Keep only the columns we need
        keep = [c for c in _KEEP_COLS if c in df.columns]
        if not keep:
            return None
        df = df[keep]

        # Deduplicate: same instrument + same datetime → keep last
        df = df[~df.index.duplicated(keep="last")]

        return df

    @staticmethod
    def _build_instrument(bare_code: str) -> str:
        """Convert a bare code to qlib instrument format.

        Uses BaseDataFetcher.infer_exchange for the exchange mapping.
        """
        exchange = BaseDataFetcher.infer_exchange(bare_code)
        return f"{exchange}{bare_code}"

    # ── Quarter date generation ─────────────────────────────────────────────

    def _recent_quarter_ends(self) -> List[str]:
        """Return recent quarter-end dates in YYYYMMDD format.

        Generates ``num_quarters`` dates going backwards from the current
        quarter.  For example, if today is 2026-04-30 (Q2), and
        ``num_quarters=8``, returns dates for Q2 2026 through Q3 2024.
        """
        today = date.today()
        year = today.year
        month = today.month
        current_q = (month - 1) // 3  # 0-based: 0=Q1, 1=Q2, ...

        quarter_end_suffixes = ["0331", "0630", "0930", "1231"]
        results: List[str] = []

        for i in range(self.num_quarters):
            q = current_q - i
            y = year
            while q < 0:
                q += 4
                y -= 1
            results.append(f"{y}{quarter_end_suffixes[q]}")

        return results

    # ── Helpers ─────────────────────────────────────────────────────────────

    def _read_cache(self, path: Path) -> Optional[pd.DataFrame]:
        try:
            df = pd.read_csv(path, index_col=[0, 1], parse_dates=[1])
            df.index.names = ["instrument", "datetime"]
            return df
        except Exception as exc:
            logger.warning(
                "EarningsGuidanceFetcher: cache read failed %s: %s", path, exc
            )
            return None

    def _load_cached_range(
        self, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """Load and concatenate per-quarter cache files in a date range.

        Filters on the datetime (announcement date) column.
        """
        files = sorted(self.cache_dir.glob("yjyg_*.csv"))
        frames: List[pd.DataFrame] = []
        for f in files:
            try:
                df = pd.read_csv(f, index_col=[0, 1], parse_dates=[1])
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
        return pd.concat(frames).sort_index()
