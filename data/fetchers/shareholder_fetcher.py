"""Shareholder count (股东户数) data fetcher.

Primary bulk:   ak.stock_zh_a_gdhs(symbol="最新")  — all stocks' latest snapshot
Per-stock detail: ak.stock_zh_a_gdhs_detail_em(symbol="000001")  — historical time series
Fallback:       ak.stock_zh_a_gdhs(symbol="20240930")  — specific quarter-end snapshot

Cache strategy:
- Bulk snapshot:   cache/shareholder/gdhs_latest.csv
- Per-stock detail: cache/shareholder/{SYMBOL}.csv
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

# Bulk API column mapping: Chinese → English
_BULK_COL_MAP = {
    "股东户数-本次": "sh_count",
    "股东户数-增减比例": "sh_count_chg_pct",
    "户均持股数量": "shares_per_holder",
    "户均持股市值": "value_per_holder",
}

# Detail API column mapping: Chinese → English
_DETAIL_COL_MAP = {
    "股东户数": "sh_count",
    "户均持股数量": "shares_per_holder",
    "户均持股金额": "value_per_holder",
    "较上期变化": "sh_count_chg",
}


class ShareholderCountFetcher(BaseDataFetcher):
    """Fetch and cache shareholder count data."""

    def __init__(
        self,
        cache_dir: str = "./cache/shareholder",
        cache_ttl_days: int = 30,
        max_workers: int = 8,
    ):
        super().__init__(cache_dir=cache_dir, cache_ttl_days=cache_ttl_days)
        self.max_workers = max_workers

    def fetch(
        self, symbols: List[str], start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """Fetch shareholder data for symbols in date range.

        Returns DataFrame with (instrument, datetime) MultiIndex, or None.
        """
        self.refresh_cache(symbols)
        return self._load_cached_range(symbols, start_date, end_date)

    def refresh_cache(self, symbols: List[str]) -> None:
        """Two-tier refresh: bulk snapshot + per-stock detail for each symbol."""
        self._ensure_cache_dir()
        self._fetch_bulk()

        # Per-stock detail fetch (parallel)
        if len(symbols) <= 1:
            for sym in symbols:
                self._fetch_one_detail(sym)
            return

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {pool.submit(self._fetch_one_detail, sym): sym for sym in symbols}
            for future in as_completed(futures):
                sym = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    logger.warning(
                        "ShareholderCountFetcher: detail fetch failed for %s: %s",
                        sym,
                        exc,
                    )

    # ── Bulk snapshot ───────────────────────────────────────────────────────

    def _fetch_bulk(self) -> Optional[pd.DataFrame]:
        """Fetch bulk shareholder count snapshot for all stocks."""
        self._ensure_cache_dir()
        cache_file = self.cache_dir / "gdhs_latest.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        df = self._fetch_bulk_with_fallback()
        if df is not None and not df.empty:
            df.to_csv(cache_file)
            logger.info(
                "ShareholderCountFetcher: cached bulk snapshot (%d stocks)", len(df)
            )
        return df

    def _fetch_bulk_with_fallback(self) -> Optional[pd.DataFrame]:
        """Try '最新'; on failure, try specific quarter-end dates."""
        try:
            raw = self._call_akshare_bulk("最新")
        except Exception as exc:
            logger.warning(
                "ShareholderCountFetcher: bulk '最新' failed: %s", exc
            )
            raw = None

        if raw is not None:
            return self._normalize_bulk(raw)

        # Fallback: try recent quarter-end dates
        for qdate in self._recent_quarter_dates():
            try:
                raw = self._call_akshare_bulk(qdate)
            except Exception as exc:
                logger.debug(
                    "ShareholderCountFetcher: bulk fallback '%s' failed: %s",
                    qdate,
                    exc,
                )
                continue
            if raw is not None:
                return self._normalize_bulk(raw)

        logger.warning("ShareholderCountFetcher: all bulk fetch attempts failed")
        return None

    def _call_akshare_bulk(self, symbol: str) -> Optional[pd.DataFrame]:
        """Call ak.stock_zh_a_gdhs(symbol=...)."""
        import akshare as ak

        return ak.stock_zh_a_gdhs(symbol=symbol)

    @staticmethod
    def _recent_quarter_dates() -> List[str]:
        """Return recent quarter-end date strings like '20240930'.

        Yields up to 4 most recent quarter-ends before today.
        """
        today = date.today()
        quarters = []
        year = today.year
        month = today.month
        # Quarter ends: 0331, 0630, 0930, 1231
        q_ends = [(3, 31), (6, 30), (9, 30), (12, 31)]
        # Build list in reverse chronological order
        results = []
        for y in range(year, year - 3, -1):
            for m, d in reversed(q_ends):
                qd = date(y, m, d)
                if qd < today:
                    results.append(qd.strftime("%Y%m%d"))
                    if len(results) >= 4:
                        return results
        return results

    def _normalize_bulk(self, raw: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Convert bulk akshare output to (instrument, datetime) MultiIndex.

        Bulk columns: 代码, 名称, 最新价, 股东户数-本次, 股东户数-上次,
                      股东户数-增减, 股东户数-增减比例, 户均持股市值,
                      户均持股数量, 总市值, 公告日期
        """
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        # Code → instrument
        code_col = next((c for c in df.columns if "代码" in str(c)), None)
        if code_col is None:
            logger.warning("ShareholderCountFetcher: bulk data missing 代码 column")
            return None
        df["instrument"] = df[code_col].astype(str).apply(self._code_to_instrument)

        # Date → datetime
        date_col = next(
            (c for c in df.columns if "公告日期" in str(c) or "日期" in str(c)),
            None,
        )
        if date_col is None:
            logger.warning("ShareholderCountFetcher: bulk data missing date column")
            return None
        df["datetime"] = pd.to_datetime(df[date_col])

        # Map Chinese columns to English
        rename = {c: _BULK_COL_MAP[c] for c in df.columns if c in _BULK_COL_MAP}
        df = df.rename(columns=rename)

        keep = [c for c in _BULK_COL_MAP.values() if c in df.columns]
        if not keep:
            return None

        # Numeric conversion
        for c in keep:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df = df.set_index(["instrument", "datetime"])
        df = df[keep].sort_index()
        return df

    # ── Per-stock detail ────────────────────────────────────────────────────

    def _fetch_one_detail(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Fetch historical shareholder count for one stock."""
        self._ensure_cache_dir()
        cache_file = self.cache_dir / f"{qlib_symbol}.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        df = self._fetch_detail_with_fallback(qlib_symbol)
        if df is not None and not df.empty:
            df.to_csv(cache_file)
            logger.debug(
                "ShareholderCountFetcher: cached detail for %s", qlib_symbol
            )
        return df

    def _fetch_detail_with_fallback(
        self, qlib_symbol: str
    ) -> Optional[pd.DataFrame]:
        """Try akshare detail; on failure, return None."""
        try:
            raw = self._call_akshare_detail(qlib_symbol)
        except Exception as exc:
            logger.debug(
                "ShareholderCountFetcher: detail fetch failed for %s: %s",
                qlib_symbol,
                exc,
            )
            return None

        if raw is None:
            return None
        return self._normalize_detail(raw, qlib_symbol)

    def _call_akshare_detail(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Call ak.stock_zh_a_gdhs_detail_em(symbol=bare_code)."""
        import akshare as ak

        bare_code = self.to_bare_code(qlib_symbol)
        return ak.stock_zh_a_gdhs_detail_em(symbol=bare_code)

    def _normalize_detail(
        self, raw: pd.DataFrame, qlib_symbol: str
    ) -> Optional[pd.DataFrame]:
        """Convert detail akshare output to (instrument, datetime) MultiIndex.

        Detail columns: 股东户数, 户均持股数量, 户均持股金额, 截止日期, 较上期变化
        """
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        # Date → datetime
        date_col = next(
            (c for c in df.columns if "截止日期" in str(c) or "日期" in str(c)),
            None,
        )
        if date_col is None:
            logger.warning(
                "ShareholderCountFetcher: detail data missing date column for %s",
                qlib_symbol,
            )
            return None
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col)
        df.index.name = "datetime"

        # Map Chinese columns to English
        rename = {c: _DETAIL_COL_MAP[c] for c in df.columns if c in _DETAIL_COL_MAP}
        df = df.rename(columns=rename)

        keep = [c for c in _DETAIL_COL_MAP.values() if c in df.columns]
        if not keep:
            return None

        df = df[keep].apply(pd.to_numeric, errors="coerce")

        df.index = pd.MultiIndex.from_product(
            [[qlib_symbol], df.index], names=["instrument", "datetime"]
        )
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

    def _read_cache(self, path: Path) -> Optional[pd.DataFrame]:
        """Read a cached CSV with (instrument, datetime) MultiIndex."""
        try:
            df = pd.read_csv(path, index_col=[0, 1], parse_dates=[1])
            df.index.names = ["instrument", "datetime"]
            return df
        except Exception as exc:
            logger.warning(
                "ShareholderCountFetcher: cache read failed %s: %s", path, exc
            )
            return None

    def _load_cached_range(
        self, symbols: List[str], start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """Load and concatenate cached per-stock detail files in date range.

        Also includes the bulk snapshot (gdhs_latest.csv) for stocks that
        lack per-stock detail files.
        """
        frames: List[pd.DataFrame] = []

        # Per-stock detail files
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

        # Bulk snapshot (supplement for stocks without detail)
        bulk_file = self.cache_dir / "gdhs_latest.csv"
        if bulk_file.exists():
            try:
                bulk = pd.read_csv(bulk_file, index_col=[0, 1], parse_dates=[1])
                bulk.index.names = ["instrument", "datetime"]
                # Only add bulk rows for instruments not already in detail data
                if frames:
                    detail_insts = set()
                    for f in frames:
                        detail_insts.update(f.index.get_level_values(0).unique())
                    bulk_insts = bulk.index.get_level_values(0).unique()
                    missing_insts = [i for i in bulk_insts if i not in detail_insts]
                    if missing_insts:
                        bulk = bulk.loc[bulk.index.get_level_values(0).isin(missing_insts)]
                        dates = bulk.index.get_level_values(1)
                        mask = (dates >= pd.Timestamp(start_date)) & (
                            dates <= pd.Timestamp(end_date)
                        )
                        if mask.any():
                            frames.append(bulk[mask])
                else:
                    dates = bulk.index.get_level_values(1)
                    mask = (dates >= pd.Timestamp(start_date)) & (
                        dates <= pd.Timestamp(end_date)
                    )
                    if mask.any():
                        frames.append(bulk[mask])
            except Exception:
                pass

        if not frames:
            return None
        return pd.concat(frames).sort_index()
