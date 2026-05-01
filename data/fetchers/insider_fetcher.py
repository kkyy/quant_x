"""Insider trade (股东增减持) data fetcher.

Primary: akshare stock_ggcg_em(symbol="全部") — bulk API returning ALL A-share
insider trades in one call.

Fallback: akshare stock_ggcg_em(symbol="增持") + stock_ggcg_em(symbol="减持")
separately, then concatenated.

Cache strategy:
- Single bulk file: cache/insider/insider_{YYYYMMDD}.csv
- Raw data is transaction-level (one row per trade); factor layer aggregates.
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

from .base import BaseDataFetcher

logger = logging.getLogger(__name__)

# akshare Chinese column names → our English column names
_COL_MAP = {
    "代码": "code",
    "名称": "name",
    "股东名称": "shareholder",
    "持股变动信息-增减": "direction_raw",
    "持股变动信息-变动数量": "shares_changed",
    "持股变动信息-占总股本比例": "pct_of_total",
    "持股变动信息-占流通股比例": "pct_of_float",
    "变动后持股情况-持股总数": "total_shares_after",
    "变动后持股情况-占总股本比例": "total_pct_after",
    "变动后持股情况-持流通股数": "float_shares_after",
    "变动后持股情况-占流通股比例": "float_pct_after",
    "变动开始日": "start_date",
    "变动截止日": "end_date",
    "公告日": "announcement_date",
}


class InsiderTradeFetcher(BaseDataFetcher):
    """Fetch and cache insider trade data for all A-shares."""

    def __init__(self, cache_dir: str = "./cache/insider", cache_ttl_days: int = 1):
        super().__init__(cache_dir=cache_dir, cache_ttl_days=cache_ttl_days)

    def fetch(self, symbols: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Refresh cache then load from cached files in the date range.

        The symbols parameter is ignored — the API returns all A-share insider
        trades at once.
        """
        self.refresh_cache(symbols)
        return self._load_cached_range(start_date, end_date)

    def refresh_cache(self, _symbols: List[str]) -> None:
        """Refresh insider trade cache for today.

        _symbols is ignored — the API returns all stocks at once.
        """
        today = date.today().strftime("%Y%m%d")
        self._fetch_insider(today)

    # ── Bulk fetch ──────────────────────────────────────────────────────────

    def _fetch_insider(self, date_str: str) -> Optional[pd.DataFrame]:
        """Fetch all insider trades and cache as a single CSV."""
        self._ensure_cache_dir()
        cache_file = self.cache_dir / f"insider_{date_str}.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        df = self._fetch_insider_with_fallback()
        if df is not None and not df.empty:
            df.to_csv(cache_file)
            logger.info(
                f"InsiderTradeFetcher: cached insider trades for {date_str} "
                f"({len(df)} transactions)"
            )
        return df

    def _fetch_insider_with_fallback(self) -> Optional[pd.DataFrame]:
        """Primary: bulk fetch. Fallback: separate buy + sell."""
        try:
            raw = self._call_akshare_bulk()
        except Exception as exc:
            logger.warning(f"InsiderTradeFetcher: akshare bulk fetch failed: {exc}")
            raw = None

        if raw is not None:
            return self._normalize_insider(raw)

        # Fallback: separate buy and sell
        logger.info("InsiderTradeFetcher: trying separate 增持/减持 fallback")
        try:
            return self._fetch_separate_buy_sell()
        except Exception as exc:
            logger.warning(
                f"InsiderTradeFetcher: separate buy/sell fallback failed: {exc}"
            )
            return None

    def _call_akshare_bulk(self) -> Optional[pd.DataFrame]:
        """Call akshare stock_ggcg_em with symbol="全部"."""
        import akshare as ak

        return ak.stock_ggcg_em(symbol="全部")

    def _fetch_separate_buy_sell(self) -> Optional[pd.DataFrame]:
        """Fallback: fetch 增持 and 减持 separately, then concatenate."""
        import akshare as ak

        buy_raw = ak.stock_ggcg_em(symbol="增持")
        sell_raw = ak.stock_ggcg_em(symbol="减持")

        frames = []
        if buy_raw is not None and not buy_raw.empty:
            frames.append(buy_raw)
        if sell_raw is not None and not sell_raw.empty:
            frames.append(sell_raw)

        if not frames:
            return None

        combined = pd.concat(frames, ignore_index=True)
        return self._normalize_insider(combined)

    # ── Normalization ───────────────────────────────────────────────────────

    def _normalize_insider(self, raw: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Convert akshare insider trade output to (instrument, datetime) MultiIndex.

        Key transformations:
        - 代码 → instrument (qlib format: SH600519, SZ000001, BJ920000)
        - 变动开始日 → datetime
        - 增减 → direction (增持=1, 减持=-1)
        - Keep numeric columns as floats
        """
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        # Rename Chinese columns to English
        rename = {c: _COL_MAP[c] for c in df.columns if c in _COL_MAP}
        df = df.rename(columns=rename)

        if "code" not in df.columns:
            logger.warning("InsiderTradeFetcher: no '代码' column found in raw data")
            return None

        # Convert code to qlib instrument format
        df["instrument"] = df["code"].apply(self._code_to_instrument)

        # Parse date columns
        if "start_date" in df.columns:
            df["datetime"] = pd.to_datetime(df["start_date"], errors="coerce")
        elif "end_date" in df.columns:
            df["datetime"] = pd.to_datetime(df["end_date"], errors="coerce")
        else:
            logger.warning(
                "InsiderTradeFetcher: no date column found in raw data"
            )
            return None

        # Drop rows with unparseable dates
        df = df.dropna(subset=["datetime"])

        # Encode direction: 增持 → 1, 减持 → -1
        if "direction_raw" in df.columns:
            df["direction"] = df["direction_raw"].map(
                lambda x: 1 if str(x).strip() == "增持" else (-1 if str(x).strip() == "减持" else 0)
            )
        else:
            df["direction"] = 0

        # Convert numeric columns
        for col in [
            "shares_changed",
            "pct_of_total",
            "pct_of_float",
            "total_shares_after",
            "total_pct_after",
            "float_shares_after",
            "float_pct_after",
        ]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Parse announcement_date
        if "announcement_date" in df.columns:
            df["announcement_date"] = pd.to_datetime(
                df["announcement_date"], errors="coerce"
            )

        # Select output columns
        output_cols = [
            "direction",
            "shares_changed",
            "pct_of_total",
            "pct_of_float",
            "shareholder",
            "total_shares_after",
            "total_pct_after",
            "float_shares_after",
            "float_pct_after",
            "announcement_date",
        ]
        keep = [c for c in output_cols if c in df.columns]
        result = df[["instrument", "datetime"] + keep].copy()

        # Build MultiIndex
        result = result.set_index(["instrument", "datetime"])
        result = result.sort_index()

        return result

    # ── Helpers ─────────────────────────────────────────────────────────────

    @staticmethod
    def _code_to_instrument(code: str) -> str:
        """Convert 6-digit code or prefixed code to qlib instrument.

        Follows the same exchange inference as BaseDataFetcher.infer_exchange:
        - 920xxx, 4xx, 8xx → BJ
        - 6xx, 9xx (excl. 920xxx) → SH
        - 0xx, 3xx → SZ
        """
        bare = str(code).strip()
        # Already has prefix
        if bare.startswith(("SH", "SZ", "BJ")):
            return bare
        # Ensure 6-digit
        if len(bare) < 6:
            bare = bare.zfill(6)
        if bare.startswith("920"):
            return f"BJ{bare}"
        if bare.startswith(("4", "8")):
            return f"BJ{bare}"
        if bare.startswith(("6", "9")):
            return f"SH{bare}"
        return f"SZ{bare}"

    def _read_cache(self, path: Path) -> Optional[pd.DataFrame]:
        """Read a cached CSV file with (instrument, datetime) MultiIndex."""
        try:
            df = pd.read_csv(path, index_col=[0, 1], parse_dates=[1])
            df.index.names = ["instrument", "datetime"]
            return df
        except Exception as exc:
            logger.warning(
                f"InsiderTradeFetcher: cache read failed {path}: {exc}"
            )
            return None

    def _load_cached_range(
        self, start_date: str, end_date: str
    ) -> Optional[pd.DataFrame]:
        """Load and concatenate cached insider files in a date range."""
        files = sorted(self.cache_dir.glob("insider_*.csv"))
        if not files:
            return None
        frames = []
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
        return pd.concat(frames)
