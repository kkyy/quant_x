"""Institutional holdings (机构持仓) data fetcher.

Primary: akshare stock_report_fund_hold
  - symbol choices: "基金持仓", "QFII持仓", "社保持仓", "券商持仓", "保险持仓", "信托持仓"
  - date format: "YYYYMMDD" (quarter-end dates like 20240630, 20240331)

This fetcher focuses on the 3 main institutional types:
  - 基金持仓 (fund holdings)
  - QFII持仓 (QFII holdings)
  - 社保持仓 (social security fund holdings)

Cache strategy:
  - fund_hold_{YYYYMMDD}.csv   — fund holdings for a quarter
  - qfii_hold_{YYYYMMDD}.csv   — QFII holdings for a quarter
  - ss_hold_{YYYYMMDD}.csv     — social security holdings for a quarter
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .base import BaseDataFetcher

logger = logging.getLogger(__name__)

# Map from our short key → akshare symbol param
_HOLD_TYPE_MAP: Dict[str, str] = {
    "fund": "基金持仓",
    "qfii": "QFII持仓",
    "ss": "社保持仓",
}

# Column mapping for fund holdings: Chinese → English
_FUND_COL_MAP = {
    "持有基金家数": "fund_count",
    "持股总数": "hold_shares",
    "持股市值": "hold_mv",
    "持股变化": "hold_change",
    "持股变动数值": "hold_change_shares",
    "持股变动比例": "hold_change_pct",
}

# Column mapping for QFII / social security holdings
_QFII_SS_COL_MAP = {
    "持有机构家数": "inst_count",
    "持股总数": "hold_shares",
    "持股市值": "hold_mv",
    "持股变化": "hold_change",
    "持股变动数值": "hold_change_shares",
    "持股变动比例": "hold_change_pct",
}


class InstitutionalHoldFetcher(BaseDataFetcher):
    """Fetch and cache institutional holdings data (fund, QFII, social security)."""

    def __init__(
        self,
        cache_dir: str = "./cache/institutional",
        cache_ttl_days: int = 30,
        n_quarters: int = 8,
    ):
        super().__init__(cache_dir=cache_dir, cache_ttl_days=cache_ttl_days)
        self.n_quarters = n_quarters

    def fetch(
        self, symbols: List[str], start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """Fetch institutional holdings for symbols in date range.

        Returns DataFrame with (instrument, datetime) MultiIndex, or None.
        """
        self.refresh_cache(symbols)
        return self._load_cached_range(start_date, end_date)

    def refresh_cache(self, _symbols: List[str]) -> None:
        """Refresh cache for recent quarters across all 3 institutional types.

        _symbols is ignored — the API returns all stocks at once per quarter.
        """
        self._ensure_cache_dir()
        quarter_dates = self._recent_quarter_dates()
        for hold_type in _HOLD_TYPE_MAP:
            for qdate in quarter_dates:
                self._fetch_quarter(hold_type, qdate)

    # ── Per-quarter fetch ────────────────────────────────────────────────────

    def _fetch_quarter(
        self, hold_type: str, date_str: str
    ) -> Optional[pd.DataFrame]:
        """Fetch one institutional type for one quarter and cache it.

        Parameters
        ----------
        hold_type : str
            One of "fund", "qfii", "ss".
        date_str : str
            Quarter-end date in "YYYYMMDD" format, e.g. "20240630".
        """
        self._ensure_cache_dir()
        prefix = f"{hold_type}_hold"
        cache_file = self.cache_dir / f"{prefix}_{date_str}.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        ak_symbol = _HOLD_TYPE_MAP[hold_type]
        try:
            raw = self._call_akshare(ak_symbol, date_str)
        except Exception as exc:
            logger.warning(
                "InstitutionalHoldFetcher: %s/%s API failed: %s",
                hold_type,
                date_str,
                exc,
            )
            return None

        if raw is None or raw.empty:
            logger.debug(
                "InstitutionalHoldFetcher: no data for %s/%s", hold_type, date_str
            )
            return None

        df = self._normalize(raw, hold_type, date_str)
        if df is not None and not df.empty:
            df.to_csv(cache_file)
            logger.info(
                "InstitutionalHoldFetcher: cached %s/%s (%d stocks)",
                hold_type,
                date_str,
                len(df),
            )
        return df

    def _call_akshare(
        self, symbol: str, date_str: str
    ) -> Optional[pd.DataFrame]:
        """Call ak.stock_report_fund_hold(symbol=..., date=...)."""
        import akshare as ak

        return ak.stock_report_fund_hold(symbol=symbol, date=date_str)

    # ── Normalization ───────────────────────────────────────────────────────

    def _normalize(
        self, raw: pd.DataFrame, hold_type: str, date_str: str
    ) -> Optional[pd.DataFrame]:
        """Convert akshare output to (instrument, datetime) MultiIndex.

        Fund columns: 序号, 股票代码, 股票简称, 持有基金家数, 持股总数,
                      持股市值, 持股变化, 持股变动数值, 持股变动比例

        QFII/SS columns: 序号, 股票代码, 股票简称, 持有机构家数, 持股总数,
                         持股市值, 持股变化, 持股变动数值, 持股变动比例
        """
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        # Stock code → instrument
        code_col = next((c for c in df.columns if "代码" in str(c)), None)
        if code_col is None:
            logger.warning(
                "InstitutionalHoldFetcher: raw data missing 代码 column for %s/%s",
                hold_type,
                date_str,
            )
            return None
        df["instrument"] = df[code_col].astype(str).apply(self._code_to_instrument)

        # Quarter-end date as datetime
        quarter_dt = pd.Timestamp(date_str)
        df["datetime"] = quarter_dt

        # Select column mapping by type
        col_map = _FUND_COL_MAP if hold_type == "fund" else _QFII_SS_COL_MAP
        rename = {c: col_map[c] for c in df.columns if c in col_map}
        df = df.rename(columns=rename)

        keep = [c for c in col_map.values() if c in df.columns]
        if not keep:
            return None

        # Numeric conversion for numeric columns
        numeric_cols = [
            c for c in keep if c not in ("hold_change",)
        ]
        for c in numeric_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df = df.set_index(["instrument", "datetime"])
        df = df[keep].sort_index()
        return df

    # ── Cache loading ───────────────────────────────────────────────────────

    def _load_cached_range(
        self, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """Load and concatenate cached files in date range."""
        files = sorted(self.cache_dir.glob("*_hold_*.csv"))
        if not files:
            return None

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

    # ── Helpers ─────────────────────────────────────────────────────────────

    @staticmethod
    def _code_to_instrument(code: str) -> str:
        """Convert bare stock code to qlib instrument format.

        SSE codes:  6xxxxx, 9xxxxx (excl. 920xxx) -> SH
        BJ codes:   920xxx, 4xxxxx, 8xxxxx         -> BJ
        SZSE codes: 0xxxxx, 3xxxxx                  -> SZ
        """
        bare = str(code).strip()
        # Already prefixed
        if bare.startswith(("SH", "SZ", "BJ")):
            return bare
        # Pad to 6 digits if needed
        if len(bare) < 6:
            bare = bare.zfill(6)
        if bare.startswith("920"):
            return f"BJ{bare}"
        if bare.startswith(("6", "9")):
            return f"SH{bare}"
        if bare.startswith(("4", "8")):
            return f"BJ{bare}"
        return f"SZ{bare}"

    @staticmethod
    def _recent_quarter_dates(n: Optional[int] = None) -> List[str]:
        """Return recent quarter-end date strings like '20240630'.

        Yields up to *n* most recent quarter-ends before today.
        """
        if n is None:
            n = 8
        today = date.today()
        q_ends = [(3, 31), (6, 30), (9, 30), (12, 31)]
        results: List[str] = []
        for y in range(today.year, today.year - 4, -1):
            for m, d in reversed(q_ends):
                qd = date(y, m, d)
                if qd < today:
                    results.append(qd.strftime("%Y%m%d"))
                    if len(results) >= n:
                        return results
        return results

    def _read_cache(self, path: Path) -> Optional[pd.DataFrame]:
        """Read a cached CSV with (instrument, datetime) MultiIndex."""
        try:
            df = pd.read_csv(path, index_col=[0, 1], parse_dates=[1])
            df.index.names = ["instrument", "datetime"]
            return df
        except Exception as exc:
            logger.warning(
                "InstitutionalHoldFetcher: cache read failed %s: %s", path, exc
            )
            return None
