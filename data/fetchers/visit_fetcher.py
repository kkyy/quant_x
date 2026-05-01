"""Institutional visit (机构调研) data fetcher.

Primary: akshare ``stock_jgdy_tj_em(date="20220101")`` — bulk, returns
aggregate institutional visit statistics for all stocks from the given date
onwards.

Fallback: akshare ``stock_jgdy_detail_em(date="20220101")`` — detailed
per-date visit records, used when the primary source fails.

Cache strategy:
- Visit snapshot: cache/visit/visits_{YYYYMMDD}.csv (1 file per fetch run)
  Contains all visits from ~1 year ago to today.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import List, Optional

import pandas as pd

from .base import BaseDataFetcher

logger = logging.getLogger(__name__)


class InstitutionalVisitFetcher(BaseDataFetcher):
    """Fetch and cache institutional visit (机构调研) data."""

    def __init__(self, cache_dir: str = "./cache/visit", cache_ttl_days: int = 7):
        super().__init__(cache_dir=cache_dir, cache_ttl_days=cache_ttl_days)

    def fetch(self, symbols: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Fetch institutional visit data for symbols in date range.

        Returns DataFrame with (instrument, datetime) MultiIndex, or None.
        """
        self.refresh_cache(symbols)
        return self._load_cached_range(start_date, end_date)

    def refresh_cache(self, _symbols: List[str]) -> None:
        """Refresh visit cache for today.

        _symbols is ignored — the visit API returns all stocks at once.
        """
        # Fetch visits from ~1 year ago to ensure enough history
        start = (date.today() - timedelta(days=365)).strftime("%Y%m%d")
        self._fetch_visits(start)

    # ── Visit snapshot ─────────────────────────────────────────────────────────

    def _fetch_visits(self, date_str: str) -> Optional[pd.DataFrame]:
        """Fetch full-market institutional visit statistics from date_str onwards."""
        self._ensure_cache_dir()
        today_str = date.today().strftime("%Y%m%d")
        cache_file = self.cache_dir / f"visits_{today_str}.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        df = self._fetch_visits_with_fallback(date_str)
        if df is not None and not df.empty:
            df.to_csv(cache_file)
            logger.info(
                f"InstitutionalVisitFetcher: cached visits from {date_str} "
                f"({len(df)} records)"
            )
        return df

    def _fetch_visits_with_fallback(self, date_str: str) -> Optional[pd.DataFrame]:
        """Try primary (EM bulk), then fallback (EM detail)."""
        try:
            raw = self._call_akshare_em_bulk(date_str)
        except Exception as exc:
            logger.warning(f"InstitutionalVisitFetcher: akshare EM bulk failed: {exc}")
            raw = None

        if raw is not None:
            return self._normalize_visits(raw)

        # Fallback: detailed per-date visit records
        logger.info("InstitutionalVisitFetcher: falling back to EM detail source")
        try:
            raw = self._call_akshare_em_detail(date_str)
        except Exception as exc:
            logger.warning(f"InstitutionalVisitFetcher: EM detail fallback failed: {exc}")
            return None

        if raw is not None:
            return self._normalize_detail_visits(raw)
        return None

    def _call_akshare_em_bulk(self, date_str: str) -> Optional[pd.DataFrame]:
        """Primary source: EM bulk visit statistics (all stocks from date)."""
        import akshare as ak
        return ak.stock_jgdy_tj_em(date=date_str)

    def _call_akshare_em_detail(self, date_str: str) -> Optional[pd.DataFrame]:
        """Fallback: EM detailed per-date visit records."""
        import akshare as ak
        return ak.stock_jgdy_detail_em(date=date_str)

    def _normalize_visits(self, raw: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Convert EM bulk visit output to (instrument, datetime) MultiIndex.

        Expected columns from stock_jgdy_tj_em:
            序号, 代码, 名称, 最新价, 涨跌幅, 接待机构数量, 接待方式,
            接待人员, 接待地点, 接待日期, 公告日期
        """
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        # ── Code → instrument ──────────────────────────────────────────────
        code_col = next((c for c in df.columns if "代码" in str(c)), None)
        if code_col is None:
            return None
        df["instrument"] = df[code_col].apply(self._code_to_instrument)

        # ── Parse visit date (接待日期) ────────────────────────────────────
        visit_date_col = next(
            (c for c in df.columns if "接待日期" in str(c)), None
        )
        if visit_date_col is None:
            # Try 接待日期 as a fallback alias
            visit_date_col = next(
                (c for c in df.columns if "调研日期" in str(c) or "日期" in str(c)),
                None,
            )
        if visit_date_col is None:
            return None
        df["datetime"] = pd.to_datetime(df[visit_date_col], errors="coerce")

        # ── Visitor count (接待机构数量) ──────────────────────────────────
        visitor_col = next(
            (c for c in df.columns if "接待机构数量" in str(c) or "机构数量" in str(c)),
            None,
        )
        df["visitor_count"] = (
            pd.to_numeric(df[visitor_col], errors="coerce").fillna(0).astype(int)
            if visitor_col
            else 0
        )

        # ── Announcement date (公告日期) ──────────────────────────────────
        ann_date_col = next(
            (c for c in df.columns if "公告日期" in str(c)), None
        )
        df["announcement_date"] = (
            pd.to_datetime(df[ann_date_col], errors="coerce")
            if ann_date_col
            else pd.NaT
        )

        # ── Visit method (接待方式) ──────────────────────────────────────
        method_col = next(
            (c for c in df.columns if "接待方式" in str(c)), None
        )
        df["visit_method"] = df[method_col] if method_col else ""

        # ── Build output DataFrame ────────────────────────────────────────
        result = df[["instrument", "datetime", "visitor_count", "announcement_date", "visit_method"]].copy()
        result = result.dropna(subset=["datetime"])
        result = result.set_index(["instrument", "datetime"])

        # Drop duplicate index entries (same stock, same visit date)
        result = result[~result.index.duplicated(keep="first")]

        return result

    def _normalize_detail_visits(self, raw: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Normalize EM detail visit output to same schema as _normalize_visits.

        The detail endpoint may have different column names but should contain
        at least stock code, date, and institution count.
        """
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        # ── Code → instrument ──────────────────────────────────────────────
        code_col = next((c for c in df.columns if "代码" in str(c)), None)
        if code_col is None:
            return None
        df["instrument"] = df[code_col].apply(self._code_to_instrument)

        # ── Parse date ────────────────────────────────────────────────────
        date_col = next(
            (c for c in df.columns if "日期" in str(c) or "date" in str(c).lower()),
            None,
        )
        if date_col is None:
            return None
        df["datetime"] = pd.to_datetime(df[date_col], errors="coerce")

        # ── Visitor count ─────────────────────────────────────────────────
        visitor_col = next(
            (c for c in df.columns if "机构" in str(c) and "数量" in str(c)),
            None,
        )
        if visitor_col is None:
            # If no explicit visitor count, count 1 per record
            df["visitor_count"] = 1
        else:
            df["visitor_count"] = (
                pd.to_numeric(df[visitor_col], errors="coerce").fillna(1).astype(int)
            )

        # ── Announcement date ─────────────────────────────────────────────
        ann_date_col = next(
            (c for c in df.columns if "公告" in str(c)), None
        )
        df["announcement_date"] = (
            pd.to_datetime(df[ann_date_col], errors="coerce")
            if ann_date_col
            else pd.NaT
        )

        # ── Visit method ──────────────────────────────────────────────────
        method_col = next(
            (c for c in df.columns if "方式" in str(c)), None
        )
        df["visit_method"] = df[method_col] if method_col else ""

        # ── Build output ──────────────────────────────────────────────────
        result = df[["instrument", "datetime", "visitor_count", "announcement_date", "visit_method"]].copy()
        result = result.dropna(subset=["datetime"])
        result = result.set_index(["instrument", "datetime"])
        result = result[~result.index.duplicated(keep="first")]

        return result

    # ── Helpers ────────────────────────────────────────────────────────────────

    @staticmethod
    def _code_to_instrument(code: str) -> str:
        """Convert 6-digit code or prefixed code to qlib instrument."""
        bare = str(code).strip()
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
            logger.warning(f"InstitutionalVisitFetcher: cache read failed {path}: {exc}")
            return None

    def _load_cached_range(self, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Load and concatenate cached visit files in a date range."""
        files = sorted(self.cache_dir.glob("visits_*.csv"))
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
