"""Sector (industry) data provider for A-shares.

Data source priority:
1. akshare (实时)
2. crawler/data/sector_stocks.json (本地离线，从 crawler 爬取）
3. local cache (last successful fetch)
"""
from __future__ import annotations
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from .utils import code_to_qlib_instrument as _code_to_qlib  # noqa: F401

logger = logging.getLogger(__name__)

# Path to the crawler data file (relative to this file: ../crawler/data/)
_CRAWLER_STOCKS_FILE = Path(__file__).parent.parent / "crawler" / "data" / "sector_stocks.json"


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

    def get_concept_map(self) -> Dict[str, str]:
        """
        Return {qlib_instrument: concept_name} from crawler's sector_stocks.json.

        Each stock is mapped to its first concept sector (BK code order).
        Returns empty dict if the data file does not exist.
        """
        if not _CRAWLER_STOCKS_FILE.exists():
            return {}
        try:
            data = json.loads(_CRAWLER_STOCKS_FILE.read_text(encoding="utf-8"))
            concept = data.get("concept", {})
            stock_concept: Dict[str, str] = {}
            for info in concept.values():
                name = info["name"]
                for stock in info["stocks"]:
                    code = stock["code"]
                    if code not in stock_concept:  # keep first (BK code order)
                        stock_concept[code] = name
            result = {_code_to_qlib(c): n for c, n in stock_concept.items()}
            logger.debug(f"Concept map: {len(result)} stocks, {len(concept)} concepts")
            return result
        except Exception as exc:
            logger.warning(f"concept map load failed: {exc}")
            return {}

    # ── internals ─────────────────────────────────────────────────────────────

    def _fetch(self) -> Optional[Dict[str, str]]:
        """Fetch industry classification: akshare first, crawler registry fallback."""
        result = self._fetch_akshare()
        if result:
            return result
        logger.info("Falling back to crawler/data/sector_stocks.json …")
        return self._fetch_from_registry()

    def _fetch_akshare(self) -> Optional[Dict[str, str]]:
        """Fetch industry classification from akshare (东方财富), using concurrent requests."""
        try:
            import akshare as ak
        except ImportError:
            logger.warning("akshare not installed, using crawler registry fallback")
            return None

        try:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            logger.info("Fetching sector data from akshare (concurrent) …")
            industry_list = ak.stock_board_industry_name_em()
            sector_names = industry_list["板块名称"].tolist()

            sector_map: Dict[str, str] = {}
            lock = __import__("threading").Lock()
            errors = []

            def _fetch_one(name: str):
                try:
                    stocks = ak.stock_board_industry_cons_em(symbol=name)
                    entries = {}
                    for _, sr in stocks.iterrows():
                        entries[_code_to_qlib(str(sr["代码"]))] = name
                    return entries
                except Exception as e:
                    logger.warning(f"  Skip sector '{name}': {e}")
                    return {}

            with ThreadPoolExecutor(max_workers=8) as pool:
                futures = {pool.submit(_fetch_one, name): name for name in sector_names}
                for future in as_completed(futures):
                    entries = future.result()
                    with lock:
                        sector_map.update(entries)

            logger.info(
                f"Fetched {len(sector_map)} stocks in "
                f"{len(set(sector_map.values()))} sectors"
            )
            self._save_cache(sector_map)
            return sector_map

        except Exception as e:
            logger.error(f"akshare fetch failed: {e}")
            return None

    def _fetch_from_registry(self) -> Optional[Dict[str, str]]:
        """Load industry sector map from crawler/data/sector_stocks.json (offline)."""
        if not _CRAWLER_STOCKS_FILE.exists():
            logger.warning(f"Crawler data not found: {_CRAWLER_STOCKS_FILE}")
            return None
        try:
            data = json.loads(_CRAWLER_STOCKS_FILE.read_text(encoding="utf-8"))
            industry = data.get("industry", {})
            sector_map: Dict[str, str] = {}
            for info in industry.values():
                name = info["name"]
                for stock in info["stocks"]:
                    sector_map[_code_to_qlib(stock["code"])] = name
            logger.info(
                f"Loaded {len(sector_map)} stocks in {len(industry)} sectors "
                "from crawler registry"
            )
            self._save_cache(sector_map)
            return sector_map
        except Exception as exc:
            logger.warning(f"Registry load failed: {exc}")
            return None
