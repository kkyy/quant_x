"""Financial fundamental data fetcher.

Primary: akshare stock_financial_analysis_indicator (Sina, 6-digit code)
Fallback: akshare stock_financial_analysis_indicator_em (East Money, code.SH format)

Also fetches cash flow statement for free cash flow computation.
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

# Sina Chinese column names → our English metric names
_SINA_COL_MAP = {
    "净资产收益率(%)": "roe",
    "加权净资产收益率(%)": "roe_weighted",
    "总资产利润率(%)": "roa",
    "资产报酬率(%)": "roa_alt",
    "销售毛利率(%)": "gross_margin",
    "销售净利率(%)": "net_margin",
    "主营业务收入增长率(%)": "revenue_growth",
    "净利润增长率(%)": "profit_growth",
    "摊薄每股收益(元)": "eps",
    "经营现金净流量与净利润的比率(%)": "ocf_to_np",
    "应收账款周转率(次)": "ar_turnover",
    "应收账款周转天数(天)": "ar_turn_days",
    "存货周转率(次)": "inventory_turnover",
    "存货周转天数(天)": "inventory_turn_days",
    "固定资产周转率(次)": "fixed_asset_turnover",
    "总资产周转率(次)": "asset_turnover",
    "总资产周转天数(天)": "asset_turn_days",
    "流动资产周转率(次)": "current_asset_turnover",
    "流动资产周转天数(天)": "current_asset_turn_days",
    "股东权益周转率(次)": "equity_turnover",
}

# All metrics we can produce from the Sina interface
_SINA_METRICS = list(set(_SINA_COL_MAP.values()))


class FinancialFetcher(BaseDataFetcher):
    """Fetch and cache financial fundamental data."""

    def __init__(self, cache_dir: str = "./cache/financial", cache_ttl_days: int = 7, max_workers: int = 8):
        super().__init__(cache_dir=cache_dir, cache_ttl_days=cache_ttl_days)
        self.max_workers = max_workers

    def fetch(self, symbols: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        self.refresh_cache(symbols)
        return self._load_cached_range(symbols, start_date, end_date)

    def refresh_cache(self, symbols: List[str]) -> None:
        self._ensure_cache_dir()
        if len(symbols) <= 1:
            for sym in symbols:
                self._fetch_indicators(sym)
            return

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {pool.submit(self._fetch_indicators, sym): sym for sym in symbols}
            for future in as_completed(futures):
                sym = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    logger.warning("FinancialFetcher: refresh failed for %s: %s", sym, exc)

    # ── Per-stock indicators ───────────────────────────────────────────────

    def _fetch_indicators(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        self._ensure_cache_dir()
        cache_file = self.cache_dir / f"{qlib_symbol}.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        df = self._fetch_indicators_with_fallback(qlib_symbol)
        if df is not None and not df.empty:
            df.to_csv(cache_file)
            logger.debug(f"FinancialFetcher: cached indicators for {qlib_symbol}")
        return df

    def _fetch_indicators_with_fallback(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        # Primary: Sina interface (6-digit code)
        try:
            raw_sina = self._call_akshare_sina(qlib_symbol)
        except Exception as exc:
            logger.debug(f"FinancialFetcher: Sina failed for {qlib_symbol}: {exc}")
            raw_sina = None

        sina_df = self._normalize_indicators(raw_sina, qlib_symbol) if raw_sina is not None else None

        # Fallback: EM interface (code.SH format)
        try:
            raw_em = self._call_akshare_em(qlib_symbol)
        except Exception as exc:
            logger.debug(f"FinancialFetcher: EM fallback failed for {qlib_symbol}: {exc}")
            raw_em = None

        em_df = self._normalize_em_indicators(raw_em, qlib_symbol) if raw_em is not None else None

        if sina_df is not None:
            return self._merge_indicator_frames(sina_df, em_df)

        return em_df

    @staticmethod
    def _merge_indicator_frames(primary: pd.DataFrame, fallback: Optional[pd.DataFrame]) -> pd.DataFrame:
        if fallback is None or fallback.empty:
            return primary

        result = primary.copy()
        fallback = fallback.reindex(result.index)
        for column in fallback.columns:
            if column not in result.columns:
                result[column] = fallback[column]
            else:
                result[column] = result[column].combine_first(fallback[column])
        ordered = [c for c in _SINA_METRICS if c in result.columns]
        return result[ordered]

    def _call_akshare_sina(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        import akshare as ak
        code = self.to_bare_code(qlib_symbol)
        start_year = str(date.today().year - 5)
        return ak.stock_financial_analysis_indicator(symbol=code, start_year=start_year)

    def _call_akshare_em(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        import akshare as ak
        # EM needs format like "600519.SH"
        code = self.to_bare_code(qlib_symbol)
        exchange = self.infer_exchange(code)
        em_code = f"{code}.{exchange}"
        return ak.stock_financial_analysis_indicator_em(symbol=em_code, indicator="按报告期")

    def _normalize_indicators(self, raw: pd.DataFrame, qlib_symbol: str) -> Optional[pd.DataFrame]:
        if raw is None or raw.empty:
            return None
        df = raw.copy()
        date_col = next((c for c in df.columns if "日期" in str(c) or "date" in str(c).lower()), df.columns[0])
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col)
        df.index.name = "datetime"

        # Rename Chinese columns to English
        rename = {c: _SINA_COL_MAP[c] for c in df.columns if c in _SINA_COL_MAP}
        df = df.rename(columns=rename)

        keep = [c for c in _SINA_METRICS if c in df.columns]
        if not keep:
            return None
        df = df[keep].apply(pd.to_numeric, errors="coerce")

        df.index = pd.MultiIndex.from_product(
            [[qlib_symbol], df.index], names=["instrument", "datetime"]
        )
        return df

    def _normalize_em_indicators(self, raw: pd.DataFrame, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Normalize EM format (English column names like ROEJQ, XSMLL, etc.)."""
        if raw is None or raw.empty:
            return None
        df = raw.copy()
        em_col_map = {
            "ROEJQ": "roe",
            "ZZCJLL": "roa",
            "XSMLL": "gross_margin",
            "XSJLL": "net_margin",
            "TOTALOPERATEREVETZ": "revenue_growth",
            "PARENTNETPROFITTZ": "profit_growth",
            "JYXJLYYSR": "ocf_to_np",
            "YSZKZZL": "ar_turnover",
            "YSZKZZTS": "ar_turn_days",
            "CHZZL": "inventory_turnover",
            "CHZZTS": "inventory_turn_days",
            "GDZCZZL": "fixed_asset_turnover",
            "ZZCZZL": "asset_turnover",
            "ZZCZZTS": "asset_turn_days",
            "LDZCZZL": "current_asset_turnover",
            "LDZCZZTS": "current_asset_turn_days",
            "GDQYZZL": "equity_turnover",
        }
        date_col = next((c for c in df.columns if "日期" in str(c) or "date" in str(c).lower() or "REPORT_DATE" in str(c)), df.columns[0])
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col)
        df.index.name = "datetime"

        rename = {c: em_col_map[c] for c in df.columns if c in em_col_map}
        df = df.rename(columns=rename)

        keep = [c for c in _SINA_METRICS if c in df.columns]
        if not keep:
            return None
        df = df[keep].apply(pd.to_numeric, errors="coerce")
        df.index = pd.MultiIndex.from_product(
            [[qlib_symbol], df.index], names=["instrument", "datetime"]
        )
        return df

    # ── Cash flow for FCF ─────────────────────────────────────────────────

    def _fetch_cash_flow(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        cache_file = self.cache_dir / f"{qlib_symbol}_cf.csv"
        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        try:
            import akshare as ak
            code = f"{self.infer_exchange(self.to_bare_code(qlib_symbol))}{self.to_bare_code(qlib_symbol)}"
            raw = ak.stock_cash_flow_sheet_by_report_em(symbol=code)
        except Exception as exc:
            logger.debug(f"FinancialFetcher: cash flow fetch failed for {qlib_symbol}: {exc}")
            return None

        if raw is None or raw.empty:
            return None
        raw.to_csv(cache_file)
        return raw

    @staticmethod
    def _compute_fcf(cf_df: pd.DataFrame) -> Optional[float]:
        """Compute free cash flow: operating CF - capex."""
        ocf_col = next((c for c in cf_df.columns if "经营活动产生的现金流量净额" in str(c)), None)
        capex_col = next((c for c in cf_df.columns if "购建固定资产无形资产和其他长期资产支付的现金" in str(c)), None)
        if ocf_col is None or capex_col is None:
            return None
        ocf = pd.to_numeric(cf_df[ocf_col].iloc[0], errors="coerce")
        capex = pd.to_numeric(cf_df[capex_col].iloc[0], errors="coerce")
        if pd.isna(ocf) or pd.isna(capex):
            return None
        return float(ocf - capex)

    # ── Helpers ────────────────────────────────────────────────────────────

    def _read_cache(self, path: Path) -> Optional[pd.DataFrame]:
        try:
            df = pd.read_csv(path, index_col=[0, 1], parse_dates=[1])
            df.index.names = ["instrument", "datetime"]
            return df
        except Exception as exc:
            logger.warning(f"FinancialFetcher: cache read failed {path}: {exc}")
            return None

    def _load_cached_range(self, symbols: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        frames = []
        for sym in symbols:
            cache_file = self.cache_dir / f"{sym}.csv"
            if not cache_file.exists():
                continue
            try:
                df = pd.read_csv(cache_file, index_col=[0, 1], parse_dates=[1])
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
