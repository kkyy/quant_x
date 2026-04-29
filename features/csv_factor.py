"""CsvFactor — load precomputed factor values from a CSV or Parquet file.

This is the universal "bring your own data" factor.  Any external alpha signal
(consensus estimates, alternative data, etc.) can be plugged into the pipeline
by saving it to a standard file format and pointing CsvFactor at it.

Expected file format
--------------------
One of:
  A)  Wide format: rows = dates, columns = instrument codes (qlib format, e.g. SH600000)
  B)  Long format: columns include *date_col*, *symbol_col*, and one or more
      factor-value columns.

The file is loaded once and cached in-memory.

Example config entry (model.yaml)
----------------------------------
    features:
      factors:
        - name: csv
          path: ./cache/consensus_roe.csv
          columns: [roe_ttm, analyst_score]
          date_col: date
          symbol_col: symbol
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)


@FactorRegistry.register("csv")
class CsvFactor(BaseFactor):
    """Load factor values from a CSV or Parquet file.

    Parameters
    ----------
    path : str
        Path to the data file.  Supported formats: ``.csv``, ``.csv.gz``,
        ``.parquet``, ``.feather``.
    columns : list[str], optional
        Subset of columns to keep.  All numeric columns are loaded when None.
    date_col : str
        Name of the date column in long format.  Ignored for wide format.
    symbol_col : str
        Name of the instrument column in long format.
    wide : bool
        ``True`` = rows are dates, columns are instruments.
        ``False`` (default) = long format with *date_col* and *symbol_col*.
    fill_method : str
        How to handle missing observations when aligning to *price_data*:
        ``"ffill"`` (default) or ``"zero"``.
    """

    def __init__(
        self,
        path: str,
        columns: Optional[List[str]] = None,
        date_col: str = "date",
        symbol_col: str = "symbol",
        wide: bool = False,
        fill_method: str = "ffill",
    ):
        self.path = Path(path)
        self.columns = columns
        self.date_col = date_col
        self.symbol_col = symbol_col
        self.wide = wide
        self.fill_method = fill_method
        self._data: Optional[pd.DataFrame] = None  # lazy loaded

    # ── BaseFactor interface ──────────────────────────────────────────────────

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Align stored factor data to *price_data*'s MultiIndex."""
        raw = self._load()
        if raw is None or raw.empty:
            return None

        instruments = price_data.index.get_level_values(0).unique()
        dates = price_data.index.get_level_values(1).unique()

        # Reindex to full (instrument × date) grid and forward-fill
        try:
            aligned = raw.reindex(
                pd.MultiIndex.from_product([instruments, dates], names=["instrument", "datetime"])
            )
        except Exception as exc:
            logger.warning(f"CsvFactor: reindex failed: {exc}")
            return None

        if self.fill_method == "ffill":
            aligned = (
                aligned.groupby(level=0, group_keys=False)
                .apply(lambda g: g.ffill())
            )
        else:
            aligned = aligned.fillna(0.0)

        # Only keep rows that appear in price_data
        aligned = aligned.reindex(price_data.index)
        return aligned if not aligned.empty else None

    # ── internals ────────────────────────────────────────────────────────────

    def _load(self) -> Optional[pd.DataFrame]:
        if self._data is not None:
            return self._data

        if not self.path.exists():
            logger.warning(f"CsvFactor: file not found: {self.path}")
            return None

        try:
            suffix = "".join(self.path.suffixes).lower()
            if ".parquet" in suffix:
                df = pd.read_parquet(self.path)
            elif ".feather" in suffix:
                df = pd.read_feather(self.path)
            else:
                df = pd.read_csv(self.path)
        except Exception as exc:
            logger.error(f"CsvFactor: cannot read {self.path}: {exc}")
            return None

        if self.wide:
            df = self._parse_wide(df)
        else:
            df = self._parse_long(df)

        if df is None or df.empty:
            return None

        # Apply column filter
        if self.columns:
            available = [c for c in self.columns if c in df.columns]
            if not available:
                logger.warning(
                    f"CsvFactor: none of {self.columns} found in {list(df.columns)}"
                )
                return None
            df = df[available]

        self._data = df
        logger.info(
            f"CsvFactor: loaded {df.shape[1]} columns, "
            f"{df.index.get_level_values(1).nunique()} dates from {self.path}"
        )
        return self._data

    def _parse_long(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Convert long-format DataFrame to (instrument, datetime) MultiIndex."""
        if self.date_col not in df.columns:
            logger.warning(f"CsvFactor: date column '{self.date_col}' not found")
            return None
        if self.symbol_col not in df.columns:
            logger.warning(f"CsvFactor: symbol column '{self.symbol_col}' not found")
            return None

        df = df.copy()
        df[self.date_col] = pd.to_datetime(df[self.date_col])
        df = df.set_index([self.symbol_col, self.date_col])
        df.index.names = ["instrument", "datetime"]
        return df.select_dtypes(include=["number"])

    def _parse_wide(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Convert wide-format (dates × instruments) to (instrument, datetime) MultiIndex."""
        # First column or index is the date
        if not isinstance(df.index, pd.DatetimeIndex):
            df = df.copy()
            df.index = pd.to_datetime(df.iloc[:, 0])
            df = df.iloc[:, 1:]
        df.index.name = "datetime"
        df.columns.name = "instrument"
        stacked = df.stack(future_stack=True).to_frame("value")
        stacked.index = stacked.index.swaplevel()
        stacked.index.names = ["instrument", "datetime"]
        return stacked
