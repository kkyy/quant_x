"""Margin trading (融资融券) data fetcher.

Fetches daily margin trading detail from both SSE and SZSE via akshare,
normalises to a common schema, and caches per-day CSVs.

Primary sources (per exchange):
- SSE:  ak.stock_margin_detail_sse(date=YYYYMMDD)
- SZSE: ak.stock_margin_detail_szse(date=YYYYMMDD)

Fallback:
- ak.stock_margin_sse(start_date, end_date)  — aggregate SSE flow only

Cache strategy:
- Per-day file: cache/margin/margin_{YYYYMMDD}.csv
- Each file contains normalised SSE + SZSE rows with (instrument, datetime) MultiIndex
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

from .base import BaseDataFetcher

logger = logging.getLogger(__name__)

# Common schema columns after normalisation
_COMMON_COLUMNS = [
    "instrument",
    "datetime",
    "margin_balance",       # 融资余额(元)
    "margin_buy_amt",       # 融资买入额(元)
    "margin_repay_amt",     # 融资偿还额(元)
    "short_balance",        # 融券余量(股) — SSE reports in shares, SZSE in shares too
    "short_sell_vol",       # 融券卖出量(股)
    "short_repay_vol",      # 融券偿还量(股)
]


class MarginTradeFetcher(BaseDataFetcher):
    """Fetch and cache margin trading data from SSE and SZSE."""

    def __init__(self, cache_dir: str = "./cache/margin", cache_ttl_days: int = 1):
        super().__init__(cache_dir=cache_dir, cache_ttl_days=cache_ttl_days)

    def fetch(self, symbols: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Fetch margin data for symbols in date range.

        Returns DataFrame with (instrument, datetime) MultiIndex, or None.
        """
        self.refresh_cache(symbols)
        return self._load_cached_range(start_date, end_date)

    def refresh_cache(self, _symbols: List[str]) -> None:
        """Refresh margin cache for today.

        _symbols is ignored — the exchange APIs return all stocks at once.
        """
        today = date.today().strftime("%Y%m%d")
        self._fetch_margin(today)

    # ── Core fetch ──────────────────────────────────────────────────────────

    def _fetch_margin(self, date_str: str) -> Optional[pd.DataFrame]:
        """Fetch margin data for a single day, combining SSE + SZSE."""
        self._ensure_cache_dir()
        cache_file = self.cache_dir / f"margin_{date_str}.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        df = self._fetch_margin_with_fallback(date_str)
        if df is not None and not df.empty:
            df.to_csv(cache_file)
            logger.info(
                f"MarginTradeFetcher: cached margin data for {date_str} "
                f"({len(df)} stocks)"
            )
        return df

    def _fetch_margin_with_fallback(self, date_str: str) -> Optional[pd.DataFrame]:
        """Try SSE + SZSE; fallback to aggregate API if both fail."""
        parts: List[pd.DataFrame] = []

        # SSE
        try:
            sse_raw = self._call_sse(date_str)
            if sse_raw is not None and not sse_raw.empty:
                sse_norm = self._normalize_margin_sse(sse_raw, date_str)
                if sse_norm is not None and not sse_norm.empty:
                    parts.append(sse_norm)
        except Exception as exc:
            logger.warning(f"MarginTradeFetcher: SSE fetch failed for {date_str}: {exc}")

        # SZSE
        try:
            szse_raw = self._call_szse(date_str)
            if szse_raw is not None and not szse_raw.empty:
                szse_norm = self._normalize_margin_szse(szse_raw, date_str)
                if szse_norm is not None and not szse_norm.empty:
                    parts.append(szse_norm)
        except Exception as exc:
            logger.warning(f"MarginTradeFetcher: SZSE fetch failed for {date_str}: {exc}")

        if parts:
            combined = pd.concat(parts, ignore_index=True)
            combined = self._build_multiindex(combined)
            return combined

        # Fallback: aggregate SSE flow
        logger.info("MarginTradeFetcher: both exchange APIs failed, trying aggregate fallback")
        try:
            agg_raw = self._call_aggregate_sse(date_str)
            if agg_raw is not None and not agg_raw.empty:
                agg_norm = self._normalize_aggregate(agg_raw)
                if agg_norm is not None and not agg_norm.empty:
                    return agg_norm
        except Exception as exc:
            logger.warning(f"MarginTradeFetcher: aggregate fallback failed: {exc}")

        return None

    # ── API calls ───────────────────────────────────────────────────────────

    def _call_sse(self, date_str: str) -> Optional[pd.DataFrame]:
        """Call akshare SSE margin detail API.

        SSE columns: 标的证券代码, 信用交易日期, 融资余额(元), 融资买入额(元),
                     融资偿还额(元), 融券余量(股), 融券卖出量(股), 融券偿还量(股)
        """
        import akshare as ak
        return ak.stock_margin_detail_sse(date=date_str)

    def _call_szse(self, date_str: str) -> Optional[pd.DataFrame]:
        """Call akshare SZSE margin detail API.

        SZSE columns: 证券代码, 证券简称, 融资买入额(元), 融资余额(元),
                      融券卖出量, 融券余量, 融券余额(元), 融资融券余额(元)
        """
        import akshare as ak
        return ak.stock_margin_detail_szse(date=date_str)

    def _call_aggregate_sse(self, date_str: str) -> Optional[pd.DataFrame]:
        """Fallback: aggregate SSE margin flow (no per-stock detail)."""
        import akshare as ak
        start = date_str
        end = date_str
        return ak.stock_margin_sse(start_date=start, end_date=end)

    # ── Normalisation ───────────────────────────────────────────────────────

    def _normalize_margin_sse(
        self, raw: pd.DataFrame, date_str: str
    ) -> Optional[pd.DataFrame]:
        """Normalise SSE margin detail to common schema (flat, pre-MultiIndex).

        SSE raw columns:
            标的证券代码, 信用交易日期, 融资余额(元), 融资买入额(元),
            融资偿还额(元), 融券余量(股), 融券卖出量(股), 融券偿还量(股)
        """
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        # Find columns by known Chinese names
        code_col = next((c for c in df.columns if "标的证券代码" in str(c)), None)
        if code_col is None:
            logger.warning("MarginTradeFetcher: SSE data missing code column")
            return None

        col_map = {}
        for c in df.columns:
            cname = str(c)
            if "融资余额" in cname and "融券" not in cname:
                col_map[c] = "margin_balance"
            elif "融资买入额" in cname:
                col_map[c] = "margin_buy_amt"
            elif "融资偿还额" in cname:
                col_map[c] = "margin_repay_amt"
            elif "融券余量" in cname and "余额" not in cname:
                col_map[c] = "short_balance"
            elif "融券卖出量" in cname:
                col_map[c] = "short_sell_vol"
            elif "融券偿还量" in cname:
                col_map[c] = "short_repay_vol"

        df = df.rename(columns=col_map)

        # Build instrument from bare code
        df["instrument"] = df[code_col].astype(str).apply(self._code_to_instrument)
        df["datetime"] = pd.Timestamp(date_str)

        # Keep only common schema columns
        keep = [c for c in _COMMON_COLUMNS if c in df.columns]
        if not keep:
            return None
        df = df[keep]

        # Numeric conversion
        value_cols = [c for c in keep if c not in ("instrument", "datetime")]
        for c in value_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        return df

    def _normalize_margin_szse(
        self, raw: pd.DataFrame, date_str: str
    ) -> Optional[pd.DataFrame]:
        """Normalise SZSE margin detail to common schema (flat, pre-MultiIndex).

        SZSE raw columns:
            证券代码, 证券简称, 融资买入额(元), 融资余额(元),
            融券卖出量, 融券余量, 融券余额(元), 融资融券余额(元)

        Note: SZSE does not provide 融资偿还额 or 融券偿还量 per stock.
        """
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        code_col = next((c for c in df.columns if "证券代码" in str(c)), None)
        if code_col is None:
            logger.warning("MarginTradeFetcher: SZSE data missing code column")
            return None

        col_map = {}
        for c in df.columns:
            cname = str(c)
            if "融资余额" in cname and "融券" not in cname and "融资融券" not in cname:
                col_map[c] = "margin_balance"
            elif "融资买入额" in cname:
                col_map[c] = "margin_buy_amt"
            elif "融资偿还额" in cname:
                col_map[c] = "margin_repay_amt"
            elif "融券余量" in cname:
                col_map[c] = "short_balance"
            elif "融券卖出量" in cname:
                col_map[c] = "short_sell_vol"
            elif "融券偿还量" in cname:
                col_map[c] = "short_repay_vol"
            # SZSE "融券余额(元)" is a monetary value; we skip it
            # (our schema tracks shares, not yuan, for short side)
            # "融资融券余额(元)" is also skipped

        df = df.rename(columns=col_map)

        # Build instrument from bare code
        df["instrument"] = df[code_col].astype(str).apply(self._code_to_instrument)
        df["datetime"] = pd.Timestamp(date_str)

        # Keep only common schema columns
        keep = [c for c in _COMMON_COLUMNS if c in df.columns]
        if not keep:
            return None
        df = df[keep]

        # Numeric conversion
        value_cols = [c for c in keep if c not in ("instrument", "datetime")]
        for c in value_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        return df

    def _normalize_aggregate(self, raw: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Normalise aggregate SSE margin flow (limited columns)."""
        if raw is None or raw.empty:
            return None

        df = raw.copy()
        date_col = next(
            (c for c in df.columns if "日期" in str(c) or "date" in str(c).lower()),
            df.columns[0],
        )
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col)
        df.index.name = "datetime"

        # Aggregate data has no per-stock breakdown, so this is limited
        # We just return what we can — typically total market margin balance
        rename_map = {}
        for c in df.columns:
            cname = str(c)
            if "融资余额" in cname and "融券" not in cname:
                rename_map[c] = "margin_balance"

        df = df.rename(columns=rename_map)
        keep = [c for c in ["margin_balance"] if c in df.columns]
        if not keep:
            return None
        df = df[keep].apply(pd.to_numeric, errors="coerce")
        return df

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

    def _build_multiindex(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert flat DataFrame with 'instrument' and 'datetime' columns
        to (instrument, datetime) MultiIndex."""
        if "instrument" not in df.columns or "datetime" not in df.columns:
            return df
        df = df.set_index(["instrument", "datetime"])
        df = df.sort_index()
        return df

    def _read_cache(self, path: Path) -> Optional[pd.DataFrame]:
        """Read a cached margin CSV with (instrument, datetime) MultiIndex."""
        try:
            df = pd.read_csv(path, index_col=[0, 1], parse_dates=[1])
            df.index.names = ["instrument", "datetime"]
            return df
        except Exception as exc:
            logger.warning(f"MarginTradeFetcher: cache read failed {path}: {exc}")
            return None

    def _load_cached_range(self, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Load and concatenate cached margin files in a date range."""
        files = sorted(self.cache_dir.glob("margin_*.csv"))
        if not files:
            return None
        frames: List[pd.DataFrame] = []
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
        return pd.concat(frames).sort_index()
