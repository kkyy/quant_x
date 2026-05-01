"""Stock pledge (股权质押) data fetcher.

Primary: akshare stock_gpzy_pledge_ratio_em(date=date_str)
    Returns all stocks with pledge data for a given trading day.
    Columns: 序号, 股票代码, 股票简称, 交易日期, 所属行业, 质押比例, 质押股数,
             质押市值, 质押笔数, 无限售股质押数, 限售股质押数, 近一年涨跌幅, 所属行业代码

Fallback: akshare stock_gpzy_pledge_ratio_detail_em()
    Per-stock detail data if bulk API fails.

Cache strategy:
    cache/pledge/pledge_{YYYYMMDD}.csv  (1 file per trading day)
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

from .base import BaseDataFetcher

logger = logging.getLogger(__name__)

# Chinese column names → English metric names
_COL_MAP = {
    "质押比例": "pledge_ratio",
    "质押股数": "pledge_shares",
    "质押市值": "pledge_mv",
    "无限售股质押数": "unlimited_pledge_shares",
    "限售股质押数": "limited_pledge_shares",
    "质押笔数": "pledge_count",
}


class PledgeFetcher(BaseDataFetcher):
    """Fetch and cache stock pledge (股权质押) data."""

    def __init__(self, cache_dir: str = "./cache/pledge", cache_ttl_days: int = 1):
        super().__init__(cache_dir=cache_dir, cache_ttl_days=cache_ttl_days)

    def fetch(self, symbols: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Not used directly — factors read from cache files."""
        self.refresh_cache(symbols)
        return self._load_cached_range(start_date, end_date)

    def refresh_cache(self, _symbols: List[str]) -> None:
        """Refresh pledge cache for today.

        _symbols is ignored — the pledge API returns all stocks at once.
        """
        today = date.today().strftime("%Y%m%d")
        self._fetch_pledge(today)

    # ── Bulk pledge data ─────────────────────────────────────────────────────

    def _fetch_pledge(self, date_str: str) -> Optional[pd.DataFrame]:
        """Fetch full-market pledge ratio data for one day."""
        self._ensure_cache_dir()
        cache_file = self.cache_dir / f"pledge_{date_str}.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        df = self._fetch_pledge_with_fallback(date_str)
        if df is not None and not df.empty:
            df.to_csv(cache_file)
            logger.info(f"PledgeFetcher: cached pledge data for {date_str} ({len(df)} stocks)")
        return df

    def _fetch_pledge_with_fallback(self, date_str: str) -> Optional[pd.DataFrame]:
        """Try primary bulk API, then per-stock fallback."""
        try:
            raw = self._call_akshare_bulk(date_str)
        except Exception as exc:
            logger.warning(f"PledgeFetcher: akshare bulk pledge failed: {exc}")
            raw = None

        if raw is not None:
            return self._normalize_pledge(raw)

        # Fallback: try per-stock detail API
        try:
            raw = self._call_akshare_detail()
        except Exception as exc:
            logger.warning(f"PledgeFetcher: akshare detail pledge fallback failed: {exc}")
            return None

        return self._normalize_detail(raw, date_str)

    def _call_akshare_bulk(self, date_str: str) -> Optional[pd.DataFrame]:
        """Call akshare bulk pledge ratio API."""
        import akshare as ak
        return ak.stock_gpzy_pledge_ratio_em(date=date_str)

    def _call_akshare_detail(self) -> Optional[pd.DataFrame]:
        """Call akshare per-stock pledge detail API (fallback)."""
        import akshare as ak
        return ak.stock_gpzy_pledge_ratio_detail_em()

    def _normalize_pledge(self, raw: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Convert akshare bulk pledge output to (instrument, datetime) MultiIndex."""
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        # Date column
        date_col = next((c for c in df.columns if "交易日期" in str(c)), None)
        if date_col is None:
            return None
        df[date_col] = pd.to_datetime(df[date_col])
        trade_date = df[date_col].iloc[0]

        # Code → qlib instrument
        code_col = next((c for c in df.columns if "股票代码" in str(c)), None)
        if code_col is None:
            return None
        df["instrument"] = df[code_col].apply(self._code_to_instrument)

        # Build output with English column names
        result = pd.DataFrame(index=df["instrument"])
        result["datetime"] = trade_date

        for cn_name, en_name in _COL_MAP.items():
            col = next((c for c in df.columns if cn_name in str(c)), None)
            if col is not None:
                result[en_name] = pd.to_numeric(df[col], errors="coerce")
            else:
                result[en_name] = 0.0

        result = result.reset_index().set_index(["instrument", "datetime"])
        return result

    def _normalize_detail(self, raw: pd.DataFrame, date_str: str) -> Optional[pd.DataFrame]:
        """Normalize per-stock pledge detail API output.

        The detail API (stock_gpzy_pledge_ratio_detail_em) may not have the
        same column structure as the bulk API.  We extract what we can.
        """
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        # Try to find code and date columns
        code_col = next((c for c in df.columns if "股票代码" in str(c) or "代码" in str(c)), None)
        if code_col is None:
            return None

        df["instrument"] = df[code_col].apply(self._code_to_instrument)

        # Use the provided date_str as the trade date
        trade_date = pd.Timestamp(date_str)

        result = pd.DataFrame(index=df["instrument"])
        result["datetime"] = trade_date

        for cn_name, en_name in _COL_MAP.items():
            col = next((c for c in df.columns if cn_name in str(c)), None)
            if col is not None:
                result[en_name] = pd.to_numeric(df[col], errors="coerce")
            else:
                result[en_name] = 0.0

        result = result.reset_index().set_index(["instrument", "datetime"])
        return result

    # ── Helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def _code_to_instrument(code: str) -> str:
        """Convert 6-digit code or prefixed code to qlib instrument.

        Follows NorthboundFetcher pattern with BJ exchange handling.
        """
        bare = str(code).strip()
        # Remove any leading zeros padded to 6 digits (e.g. "000567" stays)
        # Handle codes that might come with market prefix like "SH600000"
        if bare.startswith(("SH", "SZ", "BJ")):
            return bare
        if bare.startswith("920"):
            return f"BJ{bare}"
        if bare.startswith(("4", "8")):
            return f"BJ{bare}"
        exchange = "SH" if bare.startswith(("6", "9")) else "SZ"
        return f"{exchange}{bare}"

    def _read_cache(self, path: Path) -> Optional[pd.DataFrame]:
        try:
            df = pd.read_csv(path, index_col=[0, 1], parse_dates=[1])
            df.index.names = ["instrument", "datetime"]
            return df
        except Exception as exc:
            logger.warning(f"PledgeFetcher: cache read failed {path}: {exc}")
            return None

    def _load_cached_range(self, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Load and concatenate cached pledge files in a date range."""
        files = sorted(self.cache_dir.glob("pledge_*.csv"))
        if not files:
            return None
        frames = []
        for f in files:
            try:
                df = pd.read_csv(f, index_col=[0, 1], parse_dates=[1])
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
