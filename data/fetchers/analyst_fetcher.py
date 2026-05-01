"""Analyst forecast consensus data fetcher.

Primary: akshare ``stock_profit_forecast_em(symbol="")`` — bulk, returns all
stocks with analyst coverage in a single API call.

Fallback: akshare ``stock_profit_forecast_ths(symbol=code, indicator="预测年报每股收益")``
— per-stock, slower, used when the primary source fails.

Cache strategy:
- Forecast snapshot: cache/analyst/forecast_{YYYYMMDD}.csv (1 per day)
"""
from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

from .base import BaseDataFetcher

logger = logging.getLogger(__name__)

# Column name mapping: Chinese API column → our English name
_RATING_COL_MAP = {
    "机构投资评级-买入": "buy_rating",
    "机构投资评级-增持": "outperform_rating",
    "机构投资评级-中性": "neutral_rating",
    "机构投资评级-减持": "underperform_rating",
    "机构投资评级-卖出": "sell_rating",
}


class AnalystForecastFetcher(BaseDataFetcher):
    """Fetch and cache analyst consensus forecast data."""

    def __init__(self, cache_dir: str = "./cache/analyst", cache_ttl_days: int = 3):
        super().__init__(cache_dir=cache_dir, cache_ttl_days=cache_ttl_days)

    def fetch(self, symbols: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Fetch analyst forecast data for symbols in date range.

        Returns DataFrame with (instrument, datetime) MultiIndex, or None.
        """
        self.refresh_cache(symbols)
        return self._load_cached_range(start_date, end_date)

    def refresh_cache(self, _symbols: List[str]) -> None:
        """Refresh forecast cache for today.

        _symbols is ignored — the forecast API returns all stocks at once.
        """
        today = date.today().strftime("%Y%m%d")
        self._fetch_forecast(today)

    # ── Forecast snapshot ─────────────────────────────────────────────────────

    def _fetch_forecast(self, date_str: str) -> Optional[pd.DataFrame]:
        """Fetch full-market analyst forecast snapshot for one day."""
        self._ensure_cache_dir()
        cache_file = self.cache_dir / f"forecast_{date_str}.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        df = self._fetch_forecast_with_fallback()
        if df is not None and not df.empty:
            df.to_csv(cache_file)
            logger.info(
                f"AnalystForecastFetcher: cached forecast for {date_str} "
                f"({len(df)} stocks)"
            )
        return df

    def _fetch_forecast_with_fallback(self) -> Optional[pd.DataFrame]:
        """Try primary (EM bulk), then fallback (THS per-stock)."""
        try:
            raw = self._call_akshare_em()
        except Exception as exc:
            logger.warning(f"AnalystForecastFetcher: akshare EM failed: {exc}")
            raw = None

        if raw is not None:
            return self._normalize_forecast(raw)

        # Fallback: THS per-stock (only for a limited set)
        logger.info("AnalystForecastFetcher: falling back to THS per-stock source")
        try:
            raw = self._call_akshare_ths_bulk()
        except Exception as exc:
            logger.warning(f"AnalystForecastFetcher: THS fallback failed: {exc}")
            return None

        if raw is not None:
            return self._normalize_ths_forecast(raw)
        return None

    def _call_akshare_em(self) -> Optional[pd.DataFrame]:
        """Primary source: EM bulk forecast (all stocks at once)."""
        import akshare as ak
        return ak.stock_profit_forecast_em(symbol="")

    def _call_akshare_ths_bulk(self, codes: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
        """Fallback: fetch THS forecast for a small list of bare codes.

        This is much slower (one API call per stock), so we limit to
        *codes* (defaults to empty — callers should pass a curated list).
        """
        if not codes:
            return None

        import akshare as ak

        frames = []
        for code in codes:
            try:
                raw = ak.stock_profit_forecast_ths(
                    symbol=code, indicator="预测年报每股收益"
                )
                if raw is not None and not raw.empty:
                    raw = raw.copy()
                    raw["代码"] = code
                    frames.append(raw)
            except Exception as exc:
                logger.debug(
                    f"AnalystForecastFetcher: THS fetch failed for {code}: {exc}"
                )

        if not frames:
            return None
        return pd.concat(frames, ignore_index=True)

    def _normalize_forecast(self, raw: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Convert EM bulk forecast output to (instrument, datetime) MultiIndex."""
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        # ── Code → instrument ──────────────────────────────────────────────
        code_col = next((c for c in df.columns if "代码" in str(c)), None)
        if code_col is None:
            return None
        df["instrument"] = df[code_col].apply(self._code_to_instrument)

        # ── Rename rating columns ──────────────────────────────────────────
        rename_map = {}
        for cn_name, en_name in _RATING_COL_MAP.items():
            col = next((c for c in df.columns if cn_name in str(c)), None)
            if col is not None:
                rename_map[col] = en_name

        # ── Report count ──────────────────────────────────────────────────
        report_col = next((c for c in df.columns if "研报数" in str(c)), None)
        if report_col is not None:
            rename_map[report_col] = "report_count"

        df = df.rename(columns=rename_map)

        # ── EPS forecast columns ──────────────────────────────────────────
        # Pattern: "2025预测每股收益", "2026预测每股收益", etc.
        eps_cols = {}
        for col in df.columns:
            m = re.match(r"(\d{4})预测每股收益", str(col))
            if m:
                year = int(m.group(1))
                eps_cols[year] = col

        current_year = date.today().year
        current_month = date.today().month

        # Determine "current" and "next" year for EPS forecasts.
        # If we're past mid-year (July+), current-year EPS is nearly realized;
        # the most informative "forward" is next-year. Before July, current-year
        # is still mostly a forecast, so use it as current_eps_forecast and
        # next-year as consensus_eps_forecast.
        if current_month >= 7:
            cy = current_year  # current year (nearly realized)
            ny = current_year + 1  # next year = forward
        else:
            cy = current_year  # current year (still forecast)
            ny = current_year + 1

        # Find the nearest available year columns
        # current_eps_forecast: use cy if available, else smallest year >= cy
        # consensus_eps_forecast: use ny if available, else smallest year > cy
        sorted_years = sorted(eps_cols.keys())

        current_eps_col = None
        consensus_eps_col = None

        for y in sorted_years:
            if y >= cy and current_eps_col is None:
                current_eps_col = eps_cols[y]
            if y >= ny and consensus_eps_col is None:
                consensus_eps_col = eps_cols[y]

        df["current_eps_forecast"] = (
            pd.to_numeric(df[current_eps_col], errors="coerce")
            if current_eps_col
            else pd.NA
        )
        df["consensus_eps_forecast"] = (
            pd.to_numeric(df[consensus_eps_col], errors="coerce")
            if consensus_eps_col
            else pd.NA
        )

        # ── Build output DataFrame ────────────────────────────────────────
        trade_date = pd.Timestamp(date.today())
        out_cols = [
            "report_count",
            "buy_rating",
            "outperform_rating",
            "neutral_rating",
            "underperform_rating",
            "sell_rating",
            "current_eps_forecast",
            "consensus_eps_forecast",
        ]
        # Keep only columns that exist, plus instrument for index
        keep_cols = [c for c in out_cols if c in df.columns]
        keep_cols = ["instrument"] + keep_cols

        result = df[keep_cols].copy()
        result["datetime"] = trade_date
        result = result.set_index(["instrument", "datetime"])

        # Convert all columns to numeric
        for c in result.columns:
            result[c] = pd.to_numeric(result[c], errors="coerce")

        return result

    def _normalize_ths_forecast(self, raw: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Normalize THS per-stock forecast output.

        THS returns columns like: 代码, 预测年份, 预测值, etc.
        We pivot to match the EM format as closely as possible.
        """
        if raw is None or raw.empty:
            return None

        df = raw.copy()

        code_col = next((c for c in df.columns if "代码" in str(c)), None)
        if code_col is None:
            return None
        df["instrument"] = df[code_col].apply(self._code_to_instrument)

        # THS returns per-year forecast rows; pivot to wide format
        year_col = next((c for c in df.columns if "年份" in str(c) or "预测年份" in str(c)), None)
        value_col = next((c for c in df.columns if "预测值" in str(c) or "每股收益" in str(c)), None)

        if year_col is None or value_col is None:
            # Cannot pivot — return minimal data
            trade_date = pd.Timestamp(date.today())
            result = df[["instrument"]].drop_duplicates()
            result["datetime"] = trade_date
            result = result.set_index(["instrument", "datetime"])
            return result

        df[year_col] = pd.to_numeric(df[year_col], errors="coerce")
        df[value_col] = pd.to_numeric(df[value_col], errors="coerce")

        # Pivot: instrument × year → EPS value
        pivot = df.pivot_table(
            index="instrument", columns=year_col, values=value_col, aggfunc="mean"
        )
        pivot.columns = [f"{int(c)}预测每股收益" for c in pivot.columns]
        pivot = pivot.reset_index()

        # Re-use the EM normalizer for EPS column detection
        # Build a minimal raw-like frame
        trade_date = pd.Timestamp(date.today())
        current_year = date.today().year
        ny = current_year + 1

        current_eps_col = None
        consensus_eps_col = None
        for col in pivot.columns:
            m = re.match(r"(\d{4})预测每股收益", str(col))
            if m:
                year = int(m.group(1))
                if year >= current_year and current_eps_col is None:
                    current_eps_col = col
                if year >= ny and consensus_eps_col is None:
                    consensus_eps_col = col

        result = pivot[["instrument"]].copy()
        result["report_count"] = pd.NA  # THS doesn't provide count in this endpoint
        result["buy_rating"] = pd.NA
        result["outperform_rating"] = pd.NA
        result["neutral_rating"] = pd.NA
        result["underperform_rating"] = pd.NA
        result["sell_rating"] = pd.NA
        result["current_eps_forecast"] = (
            pd.to_numeric(pivot[current_eps_col], errors="coerce")
            if current_eps_col
            else pd.NA
        )
        result["consensus_eps_forecast"] = (
            pd.to_numeric(pivot[consensus_eps_col], errors="coerce")
            if consensus_eps_col
            else pd.NA
        )
        result["datetime"] = trade_date
        result = result.set_index(["instrument", "datetime"])

        for c in result.columns:
            result[c] = pd.to_numeric(result[c], errors="coerce")

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
            logger.warning(f"AnalystForecastFetcher: cache read failed {path}: {exc}")
            return None

    def _load_cached_range(self, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Load and concatenate cached forecast files in a date range."""
        files = sorted(self.cache_dir.glob("forecast_*.csv"))
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
