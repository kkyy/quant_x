"""申万一级行业分类 fetcher.

Output: cache/sw1_industry_map.csv
Columns: instrument (SH/SZ/BJ prefix), sw1_name

Data source: akshare sw_index_third_cons (loops 31 SW Level-1 indices)
TTL default: 30 days (SW adjustments are infrequent)
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import List, Optional

import pandas as pd

from .base import BaseDataFetcher

logger = logging.getLogger(__name__)

_MAP_FILENAME = "sw1_industry_map.csv"


class SW1IndustryFetcher(BaseDataFetcher):
    """Fetch and cache the SW Level-1 industry → stock mapping."""

    def __init__(self, cache_dir: str = "./cache", cache_ttl_days: int = 30):
        super().__init__(cache_dir=cache_dir, cache_ttl_days=cache_ttl_days)

    # ── public helpers ────────────────────────────────────────────────────────

    @property
    def map_path(self) -> Path:
        return self.cache_dir / _MAP_FILENAME

    def get_map(self, force: bool = False) -> dict[str, str]:
        """Return {instrument: sw1_name} dict, refreshing cache if stale."""
        if force or not self._is_cache_fresh(self.map_path):
            self.refresh_cache([])
        if not self.map_path.exists():
            return {}
        df = pd.read_csv(self.map_path, dtype=str)
        return dict(zip(df["instrument"], df["sw1_name"]))

    # ── BaseDataFetcher interface ─────────────────────────────────────────────

    def fetch(self, symbols: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Return full mapping DataFrame (symbols / date range ignored)."""
        self.refresh_cache([])
        if not self.map_path.exists():
            return None
        return pd.read_csv(self.map_path, dtype=str)

    def refresh_cache(self, symbols: List[str]) -> None:
        """Download SW1 classification for all 31 industries and write map CSV."""
        if self._is_cache_fresh(self.map_path):
            logger.info("SW1 map cache is fresh (%s), skipping", self.map_path)
            return

        try:
            import akshare as ak
        except ImportError:
            logger.error("akshare not installed; cannot fetch SW1 industry map")
            return

        try:
            first = ak.sw_index_first_info()
        except Exception as exc:
            logger.error("Failed to fetch SW1 industry list: %s", exc)
            return

        rows: list[dict] = []
        total = len(first)
        for i, (_, row) in enumerate(first.iterrows(), 1):
            sym = row["行业代码"]
            name = row["行业名称"]
            logger.info("[%d/%d] Fetching %s (%s) …", i, total, name, sym)
            for attempt in range(3):
                try:
                    df = ak.sw_index_third_cons(symbol=sym)
                    for code in df["股票代码"].astype(str):
                        rows.append({"instrument": _norm(code), "sw1_name": name})
                    break
                except Exception as exc:
                    if attempt < 2:
                        logger.warning("  Retry %d for %s: %s", attempt + 1, sym, exc)
                        time.sleep(2)
                    else:
                        logger.error("  Gave up on %s: %s", sym, exc)
            time.sleep(0.5)

        if not rows:
            logger.error("SW1 fetch returned no data; cache not updated")
            return

        result = pd.DataFrame(rows).drop_duplicates("instrument")
        self._ensure_cache_dir()
        result.to_csv(self.map_path, index=False, encoding="utf-8-sig")
        logger.info("SW1 map saved: %d stocks → %s", len(result), self.map_path)


def _norm(code: str) -> str:
    """Normalize any code format to SH/SZ/BJ + 6-digit bare code."""
    import re
    digits = re.sub(r'\D', '', str(code))
    bare = digits[-6:].zfill(6) if len(digits) >= 6 else digits.zfill(6)
    if bare.startswith("92"):
        return "BJ" + bare
    if bare.startswith(("4", "8")):
        return "BJ" + bare
    if bare.startswith("6"):
        return "SH" + bare
    return "SZ" + bare
