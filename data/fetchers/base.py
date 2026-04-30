"""Base class for external data fetchers with caching support."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class BaseDataFetcher(ABC):
    """Abstract base for fetching and caching external data.

    Subclasses implement ``fetch()`` and ``refresh_cache()``.
    Caching uses file mtime vs TTL for freshness checks.
    """

    def __init__(self, cache_dir: str, cache_ttl_days: int = 7):
        self.cache_dir = Path(cache_dir)
        self.cache_ttl_days = cache_ttl_days

    @abstractmethod
    def fetch(self, symbols: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Fetch data for symbols in date range.

        Returns DataFrame with (instrument, datetime) MultiIndex, or None.
        """

    @abstractmethod
    def refresh_cache(self, symbols: List[str]) -> None:
        """Refresh cache files for the given symbols."""

    def _is_cache_fresh(self, path: Path) -> bool:
        if not path.exists():
            return False
        if self.cache_ttl_days == 0:
            return False
        mtime = date.fromtimestamp(path.stat().st_mtime)
        return (date.today() - mtime).days < self.cache_ttl_days

    def _ensure_cache_dir(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def to_bare_code(qlib_symbol: str) -> str:
        """SH600000 -> 600000"""
        return qlib_symbol[2:]

    @staticmethod
    def to_qlib_symbol(bare_code: str, exchange: str) -> str:
        """600000 + SH -> SH600000"""
        return f"{exchange}{bare_code}"

    @staticmethod
    def infer_exchange(bare_code: str) -> str:
        """Infer exchange from 6-digit code: 6/9->SH, 0/3->SZ."""
        if bare_code.startswith(("6", "9")):
            return "SH"
        return "SZ"
