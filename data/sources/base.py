"""Base class for supplementary OHLCV data sources."""
from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd


class BaseDataSource(ABC):
    """Supplementary data source that can fill gaps in qlib source CSVs.

    Implementations must return data in the same format as the Dolt-exported
    source CSVs so the existing normalize → dump_bin pipeline can process them
    without changes.

    Required output columns
    -----------------------
    tradedate : datetime64   trading date
    symbol    : str          qlib instrument (e.g. "SH600000")
    open      : float        opening price (unadjusted, yuan)
    high      : float
    low       : float
    close     : float        closing price (unadjusted, yuan)
    adjclose  : float        split-adjusted close; set to close when no splits
    volume    : float        volume in 手 (lots, 1 lot = 100 shares)
    amount    : float        turnover in yuan
    vwap      : float        volume-weighted avg price = amount/volume*10
    """

    SOURCE_COLUMNS = [
        "tradedate", "symbol", "high", "low", "open",
        "close", "adjclose", "volume", "amount", "vwap",
    ]

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable source name."""

    @abstractmethod
    def fetch(self, qlib_symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch daily OHLCV data for one stock.

        Parameters
        ----------
        qlib_symbol : str
            qlib instrument code, e.g. ``"SH600000"``.
        start_date : str
            Inclusive start date, ``"YYYY-MM-DD"``.
        end_date : str
            Inclusive end date, ``"YYYY-MM-DD"``.

        Returns
        -------
        pd.DataFrame
            Columns matching ``SOURCE_COLUMNS``.  Empty DataFrame when no data.
        """

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def to_bare_code(qlib_symbol: str) -> str:
        """``"SH600000"`` → ``"600000"``."""
        return qlib_symbol[2:]

    @staticmethod
    def to_exchange(qlib_symbol: str) -> str:
        """``"SH600000"`` → ``"SH"``."""
        return qlib_symbol[:2]

    @classmethod
    def _build_output(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Reorder/select columns; return empty frame on failure."""
        if df.empty:
            return pd.DataFrame(columns=cls.SOURCE_COLUMNS)
        missing = set(cls.SOURCE_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {missing}")
        return df[cls.SOURCE_COLUMNS].reset_index(drop=True)
