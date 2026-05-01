"""Northbound capital (沪深港通) data fetcher.

Primary: akshare (stock_hsgt_hold_stock_em, stock_hsgt_hist_em)
Fallback: East Money datacenter API (limited coverage)

Cache strategy:
- Holdings snapshot: cache/northbound/holdings_{date}.csv (1 per day)
- Historical flow: cache/northbound/hist_flow.csv (append daily)
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

from .base import BaseDataFetcher

logger = logging.getLogger(__name__)


class NorthboundFetcher(BaseDataFetcher):
    """Fetch and cache northbound capital data."""

    def __init__(self, cache_dir: str = "./cache/northbound", cache_ttl_days: int = 1):
        super().__init__(cache_dir=cache_dir, cache_ttl_days=cache_ttl_days)

    def fetch(self, symbols: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Not used directly — factors read from cache files."""
        self.refresh_cache(symbols)
        return self._load_cached_range(start_date, end_date)

    def refresh_cache(self, _symbols: List[str]) -> None:
        """Refresh holdings and flow cache for today.

        _symbols is ignored — the holdings API returns all stocks at once.
        """
        today = date.today().strftime("%Y-%m-%d")
        self._fetch_holdings(today)
        self._fetch_hist_flow()

    # ── Holdings snapshot ──────────────────────────────────────────────────

    def _fetch_holdings(self, date_str: str) -> Optional[pd.DataFrame]:
        """Fetch full-market northbound holdings for one day."""
        self._ensure_cache_dir()
        cache_file = self.cache_dir / f"holdings_{date_str}.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        df = self._fetch_holdings_with_fallback()
        if df is not None and not df.empty:
            df.to_csv(cache_file)
            logger.info(f"NorthboundFetcher: cached holdings for {date_str} ({len(df)} stocks)")
        return df

    def _fetch_holdings_with_fallback(self) -> Optional[pd.DataFrame]:
        try:
            raw = self._call_akshare_holdings()
        except Exception as exc:
            logger.warning(f"NorthboundFetcher: akshare holdings failed: {exc}")
            raw = None

        if raw is None:
            try:
                raw = self._call_eastmoney_holdings()
            except Exception as exc:
                logger.warning(f"NorthboundFetcher: eastmoney holdings fallback failed: {exc}")
                return None

        return self._normalize_holdings(raw)

    def _call_akshare_holdings(self) -> Optional[pd.DataFrame]:
        import akshare as ak
        return ak.stock_hsgt_hold_stock_em(market="北向", indicator="今日排行")

    def _call_eastmoney_holdings(self) -> Optional[pd.DataFrame]:
        # East Money datacenter API for northbound holdings
        # Limited implementation — returns None if unavailable
        logger.debug("NorthboundFetcher: East Money northbound holdings not yet supported")
        return None

    def _normalize_holdings(self, raw: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Convert akshare holdings output to (instrument, datetime) MultiIndex."""
        if raw is None or raw.empty:
            return None

        df = raw.copy()
        # Date column
        date_col = next((c for c in df.columns if "日期" in str(c)), None)
        if date_col is None:
            return None
        df[date_col] = pd.to_datetime(df[date_col])
        trade_date = df[date_col].iloc[0]

        # Code → qlib instrument
        code_col = next((c for c in df.columns if "代码" in str(c)), None)
        if code_col is None:
            return None
        df["instrument"] = df[code_col].apply(self._code_to_instrument)

        # Build output
        hold_pct_col = next((c for c in df.columns if "占流通股比" in str(c)), None)
        hold_mv_col = next((c for c in df.columns if "持股市值" in str(c) or "持股-市值" in str(c)), None)
        chg_pct_col = next((c for c in df.columns if "增持估计-占流通股比" in str(c)), None)

        result = pd.DataFrame(index=df["instrument"])
        result["datetime"] = trade_date
        result["nb_hold_pct"] = df[hold_pct_col].astype(float) if hold_pct_col else 0.0
        result["nb_hold_mv"] = df[hold_mv_col].astype(float) if hold_mv_col else 0.0
        result["nb_hold_pct_chg"] = df[chg_pct_col].astype(float) if chg_pct_col else 0.0

        # Net buy ratio: use the change in holding pct as a proxy
        # More precise: nb_hold_pct_chg * market_cap / turnover
        # Simplified: use the change directly
        result["nb_net_buy_ratio"] = result["nb_hold_pct_chg"]

        result = result.reset_index().set_index(["instrument", "datetime"])
        return result

    def _fetch_individual(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Fetch per-stock northbound history (on-demand, not cached in refresh_cache)."""
        self._ensure_cache_dir()
        cache_file = self.cache_dir / f"{qlib_symbol}_individual.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        code = self.to_bare_code(qlib_symbol)
        try:
            import akshare as ak
            raw = ak.stock_hsgt_individual_em(symbol=code)
        except Exception as exc:
            logger.warning(f"NorthboundFetcher: individual fetch failed for {qlib_symbol}: {exc}")
            return None

        if raw is None or raw.empty:
            return None

        df = raw.copy()
        date_col = next((c for c in df.columns if "日期" in str(c)), None)
        if date_col is None:
            return None
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col)
        df.index.name = "datetime"

        # Rename known columns
        rename_map = {}
        hold_pct_col = next((c for c in df.columns if "持股数量占A股百分比" in str(c)), None)
        if hold_pct_col:
            rename_map[hold_pct_col] = "nb_hold_pct"
        mv_col = next((c for c in df.columns if "持股市值" in str(c)), None)
        if mv_col:
            rename_map[mv_col] = "nb_hold_mv"
        chg_col = next((c for c in df.columns if "今日增持股数" in str(c)), None)
        if chg_col:
            rename_map[chg_col] = "nb_hold_chg"

        df = df.rename(columns=rename_map)
        keep = [c for c in ["nb_hold_pct", "nb_hold_mv", "nb_hold_chg"] if c in df.columns]
        if not keep:
            return None
        df = df[keep].apply(pd.to_numeric, errors="coerce")
        df.index = pd.MultiIndex.from_product(
            [[qlib_symbol], df.index], names=["instrument", "datetime"]
        )
        df.to_csv(cache_file)
        return df

    # ── Historical flow ────────────────────────────────────────────────────

    def _fetch_hist_flow(self) -> Optional[pd.DataFrame]:
        """Fetch historical northbound aggregate flow."""
        self._ensure_cache_dir()
        cache_file = self.cache_dir / "hist_flow.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        df = self._fetch_hist_flow_with_fallback()
        if df is not None and not df.empty:
            df.to_csv(cache_file)
            logger.info(f"NorthboundFetcher: cached hist_flow ({len(df)} days)")
        return df

    def _fetch_hist_flow_with_fallback(self) -> Optional[pd.DataFrame]:
        try:
            raw = self._call_akshare_hist_flow()
        except Exception as exc:
            logger.warning(f"NorthboundFetcher: akshare hist_flow failed: {exc}")
            return None

        if raw is None:
            return None
        return self._normalize_hist_flow(raw)

    def _call_akshare_hist_flow(self) -> Optional[pd.DataFrame]:
        import akshare as ak
        return ak.stock_hsgt_hist_em(symbol="北向资金")

    def _normalize_hist_flow(self, raw: pd.DataFrame) -> Optional[pd.DataFrame]:
        if raw is None or raw.empty:
            return None
        df = raw.copy()
        date_col = next((c for c in df.columns if "日期" in str(c) or "date" in str(c).lower()), df.columns[0])
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col)
        df.index.name = "datetime"

        rename_map = {}
        net_buy_col = next((c for c in df.columns if "净买额" in str(c)), None)
        if net_buy_col:
            rename_map[net_buy_col] = "nb_total_net_buy"
        buy_col = next((c for c in df.columns if "买入成交额" in str(c)), None)
        if buy_col:
            rename_map[buy_col] = "nb_total_buy"
        sell_col = next((c for c in df.columns if "卖出成交额" in str(c)), None)
        if sell_col:
            rename_map[sell_col] = "nb_total_sell"

        df = df.rename(columns=rename_map)
        keep = [c for c in ["nb_total_net_buy", "nb_total_buy", "nb_total_sell"] if c in df.columns]
        if not keep:
            return None
        df = df[keep].apply(pd.to_numeric, errors="coerce")
        return df

    # ── Helpers ────────────────────────────────────────────────────────────

    @staticmethod
    def _code_to_instrument(code: str) -> str:
        """Convert 6-digit code or prefixed code to qlib instrument."""
        bare = code.strip()
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
            logger.warning(f"NorthboundFetcher: cache read failed {path}: {exc}")
            return None

    def _load_cached_range(self, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Load and concatenate cached holdings files in a date range."""
        files = sorted(self.cache_dir.glob("holdings_*.csv"))
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
