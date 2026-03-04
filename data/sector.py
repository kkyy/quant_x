"""Sector (industry) data provider for A-shares using akshare + local cache."""
from __future__ import annotations
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class SectorDataProvider:
    """
    Provides sector (行业) membership for A-share stocks.

    Data source: akshare (东方财富行业分类)
    Fallback:    local cache (auto-refreshed every `sector_ttl_days` days)

    Sector map format: {"SH600000": "银行", "SZ000001": "银行", ...}
    """

    def __init__(self, config: dict):
        self.config = config
        cache_dir = config.get("data_cache", {}).get("dir", "./cache")
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._map: Optional[Dict[str, str]] = None

    # ── public API ────────────────────────────────────────────────────────────

    def get_map(self, force_refresh: bool = False) -> Dict[str, str]:
        """Return {instrument: sector_name} dict."""
        ttl = self.config.get("data_cache", {}).get("sector_ttl_days", 7)
        if not force_refresh and self._map is not None:
            return self._map
        if not force_refresh and self._cache_fresh(ttl):
            m = self._load_cache()
            if m:
                self._map = m
                logger.info(f"Sector map loaded from cache ({len(m)} stocks)")
                return self._map

        self._map = self._fetch() or self._load_cache() or {}
        return self._map

    def get_sector(self, instrument: str) -> str:
        return self.get_map().get(instrument, "Unknown")

    def get_series(self, instruments: list) -> "pd.Series":
        import pandas as pd
        m = self.get_map()
        return pd.Series({i: m.get(i, "Unknown") for i in instruments})

    # ── internals ─────────────────────────────────────────────────────────────

    def _cache_fresh(self, ttl_days: int) -> bool:
        ts_file = self.cache_dir / "sector_ts.txt"
        if not ts_file.exists():
            return False
        ts = datetime.fromisoformat(ts_file.read_text().strip())
        return (datetime.now() - ts).days < ttl_days

    def _load_cache(self) -> Optional[Dict[str, str]]:
        f = self.cache_dir / "sector_map.json"
        if f.exists():
            return json.loads(f.read_text(encoding="utf-8"))
        return None

    def _save_cache(self, m: Dict[str, str]):
        (self.cache_dir / "sector_map.json").write_text(
            json.dumps(m, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        (self.cache_dir / "sector_ts.txt").write_text(datetime.now().isoformat())

    def _fetch(self) -> Optional[Dict[str, str]]:
        """Fetch industry classification from akshare (东方财富)."""
        try:
            import akshare as ak
        except ImportError:
            logger.warning("akshare not installed. Run: pip install akshare")
            return None

        try:
            logger.info("Fetching sector data from akshare …")
            industry_list = ak.stock_board_industry_name_em()
            sector_map: Dict[str, str] = {}

            for _, row in industry_list.iterrows():
                name = row["板块名称"]
                try:
                    stocks = ak.stock_board_industry_cons_em(symbol=name)
                    for _, sr in stocks.iterrows():
                        code = str(sr["代码"])
                        if code.startswith("6"):
                            qcode = f"SH{code}"
                        elif code.startswith(("8", "4")):
                            qcode = f"BJ{code}"
                        else:
                            qcode = f"SZ{code}"
                        sector_map[qcode] = name
                except Exception as e:
                    logger.warning(f"  Skip sector '{name}': {e}")

            logger.info(
                f"Fetched {len(sector_map)} stocks in "
                f"{len(set(sector_map.values()))} sectors"
            )
            self._save_cache(sector_map)
            return sector_map

        except Exception as e:
            logger.error(f"akshare fetch failed: {e}")
            return None
