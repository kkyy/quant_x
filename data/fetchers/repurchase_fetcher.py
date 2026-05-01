"""Share repurchase (回购) data fetcher.

Primary: akshare stock_repurchase_em()
    Returns all stocks with active/completed repurchase plans.
    No parameters needed — bulk API returns the full market.

Columns from akshare:
    序号, 股票代码, 股票简称, 最新价, 计划回购价格区间,
    计划回购数量区间-下限, 计划回购数量区间-上限,
    占公告前一日总股本比例-下限, 占公告前一日总股本比例-上限,
    计划回购金额区间-下限, 计划回购金额区间-上限,
    回购起始时间, 实施进度,
    已回购股份价格区间-下限, 已回购股份价格区间-上限,
    已回购股份数量, 已回购金额, 最新公告日期

Cache strategy:
    cache/repurchase/repurchase_{YYYYMMDD}.csv  (1 file per day)
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

from .base import BaseDataFetcher

logger = logging.getLogger(__name__)

# akshare EM repurchase column names → our English names
_EM_REPURCHASE_COL_MAP = {
    "股票代码": "code",
    "股票简称": "name",
    "最新价": "latest_price",
    "计划回购价格区间": "plan_price_range",
    "计划回购数量区间-下限": "plan_shares_lower",
    "计划回购数量区间-上限": "plan_shares_upper",
    "占公告前一日总股本比例-下限": "plan_pct_lower",
    "占公告前一日总股本比例-上限": "plan_pct_upper",
    "计划回购金额区间-下限": "plan_amount_lower",
    "计划回购金额区间-上限": "plan_amount_upper",
    "回购起始时间": "repurchase_start",
    "实施进度": "progress",
    "已回购股份价格区间-下限": "done_price_lower",
    "已回购股份价格区间-上限": "done_price_upper",
    "已回购股份数量": "done_shares",
    "已回购金额": "done_amount",
    "最新公告日期": "announcement_date",
}

# Columns we keep in the normalized output
_KEEP_COLS = [
    "plan_amount_lower",
    "plan_amount_upper",
    "plan_shares_lower",
    "plan_shares_upper",
    "plan_pct_lower",
    "plan_pct_upper",
    "done_amount",
    "done_shares",
    "progress",
    "announcement_date",
]


class RepurchaseFetcher(BaseDataFetcher):
    """Fetch and cache share repurchase plan data."""

    def __init__(self, cache_dir: str = "./cache/repurchase", cache_ttl_days: int = 1):
        super().__init__(cache_dir=cache_dir, cache_ttl_days=cache_ttl_days)

    def fetch(self, symbols: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Fetch repurchase data for symbols in date range.

        Returns DataFrame with (instrument, datetime) MultiIndex, or None.
        """
        self.refresh_cache(symbols)
        return self._load_cached_range(start_date, end_date)

    def refresh_cache(self, _symbols: List[str]) -> None:
        """Refresh repurchase cache for today.

        _symbols is ignored — the API returns all stocks with repurchase plans
        in a single bulk call.
        """
        today = date.today().strftime("%Y%m%d")
        self._fetch_repurchase(today)

    def _fetch_repurchase(self, date_str: str) -> Optional[pd.DataFrame]:
        """Fetch full-market repurchase plans and cache the result.

        Primary: ak.stock_repurchase_em()
        Fallback: None (single-source, EM is comprehensive)
        """
        self._ensure_cache_dir()
        cache_file = self.cache_dir / f"repurchase_{date_str}.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        try:
            raw = self._call_akshare_em()
        except Exception as exc:
            logger.warning("RepurchaseFetcher: akshare EM fetch failed: %s", exc)
            return None

        df = self._normalize(raw)
        if df is not None and not df.empty:
            df.to_csv(cache_file)
            logger.info(
                "RepurchaseFetcher: cached repurchase data for %s (%d stocks)",
                date_str,
                len(df),
            )
        return df

    def _call_akshare_em(self) -> Optional[pd.DataFrame]:
        """Call akshare EM repurchase API.

        stock_repurchase_em() takes no parameters and returns
        all stocks with repurchase plans.
        """
        import akshare as ak
        return ak.stock_repurchase_em()

    # ── Normalization ─────────────────────────────────────────────────────────

    def _normalize(self, raw: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Convert akshare repurchase output to (instrument, datetime) MultiIndex.

        Maps Chinese column names to English, converts bare stock codes to
        qlib instrument format, and parses date columns.
        """
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        # Rename Chinese columns to English
        rename = {c: _EM_REPURCHASE_COL_MAP[c] for c in df.columns if c in _EM_REPURCHASE_COL_MAP}
        df = df.rename(columns=rename)

        if "code" not in df.columns:
            logger.warning("RepurchaseFetcher: no stock code column found")
            return None

        # Convert bare code → qlib instrument (SH600519, SZ000001, BJ920000, etc.)
        df["instrument"] = df["code"].astype(str).apply(self._code_to_instrument)

        # Parse announcement_date
        if "announcement_date" in df.columns:
            df["announcement_date"] = pd.to_datetime(
                df["announcement_date"], errors="coerce"
            )

        # Use announcement_date as the datetime index; if missing, use today
        if "announcement_date" in df.columns and df["announcement_date"].notna().any():
            df["datetime"] = df["announcement_date"]
        else:
            df["datetime"] = pd.Timestamp(date.today())

        # Drop rows with no valid datetime
        df = df.dropna(subset=["datetime"])
        if df.empty:
            return None

        # Convert numeric columns
        numeric_cols = [
            "plan_amount_lower", "plan_amount_upper",
            "plan_shares_lower", "plan_shares_upper",
            "plan_pct_lower", "plan_pct_upper",
            "done_amount", "done_shares",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Keep only the columns we need
        keep = [c for c in _KEEP_COLS if c in df.columns]
        if not keep:
            return None

        result = df[keep + ["instrument", "datetime"]].copy()
        result = result.set_index(["instrument", "datetime"])
        result = result.sort_index()
        return result

    # ── Helpers ────────────────────────────────────────────────────────────────

    @staticmethod
    def _code_to_instrument(code: str) -> str:
        """Convert 6-digit code to qlib instrument.

        Uses the same mapping as BaseDataFetcher.infer_exchange:
        - 920xxx, 4xx, 8xx → BJ
        - 6xx, 9xx → SH
        - 0xx, 3xx → SZ
        """
        bare = str(code).strip()
        # Strip any existing exchange prefix
        if bare.startswith(("SH", "SZ", "BJ")):
            return bare
        exchange = BaseDataFetcher.infer_exchange(bare)
        return f"{exchange}{bare}"

    def _read_cache(self, path: Path) -> Optional[pd.DataFrame]:
        """Read a cached CSV file with (instrument, datetime) MultiIndex."""
        try:
            df = pd.read_csv(path, index_col=[0, 1], parse_dates=[1])
            df.index.names = ["instrument", "datetime"]
            return df
        except Exception as exc:
            logger.warning("RepurchaseFetcher: cache read failed %s: %s", path, exc)
            return None

    def _load_cached_range(self, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Load and concatenate cached repurchase files in a date range."""
        files = sorted(self.cache_dir.glob("repurchase_*.csv"))
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
