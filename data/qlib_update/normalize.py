"""Normalize crowd-sourced A-share CSV files for qlib dump_bin."""
from __future__ import annotations

import argparse
import copy
import multiprocessing as mp
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def _ensure_qlib_paths() -> None:
    qlib_repo = os.environ.get("QLIB_REPO_DIR")
    candidates = []
    if qlib_repo:
        candidates.append(Path(qlib_repo).expanduser())

    project_root = Path(__file__).resolve().parents[2]
    candidates.append(project_root / "qlib_data" / "qlib")

    for repo in candidates:
        scripts = repo / "scripts"
        for path in (repo, scripts):
            if path.exists():
                path_str = str(path)
                if path_str not in sys.path:
                    sys.path.insert(0, path_str)


_ensure_qlib_paths()

try:
    from data_collector.base import BaseNormalize, Normalize
except ImportError as exc:  # pragma: no cover - depends on external qlib scripts path
    raise ImportError(
        "Cannot import qlib data_collector modules. Add <qlib_repo>/scripts to PYTHONPATH."
    ) from exc


class CrowdSourceNormalize(BaseNormalize):
    """Yahoo CN normalizer variant that keeps amount and adjusts vwap."""

    COLUMNS = ["open", "close", "high", "low", "vwap", "volume"]

    @staticmethod
    def calc_change(df: pd.DataFrame, last_close: float = None) -> pd.Series:
        close = df["close"].ffill()
        shifted = close.shift(1)
        if last_close is not None:
            shifted.iloc[0] = float(last_close)
        return close / shifted - 1

    def _get_calendar_list(self):
        return None

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = self._normalize_yahoo(df)
        df = self._adjusted_price(df)
        return self._manual_adj_data(df)

    def _normalize_yahoo(self, df: pd.DataFrame) -> pd.DataFrame:
        symbol = df.loc[df[self._symbol_field_name].first_valid_index(), self._symbol_field_name]
        columns = copy.deepcopy(self.COLUMNS)
        df = df.copy()
        df.set_index(self._date_field_name, inplace=True)
        df.index = pd.to_datetime(df.index).tz_localize(None)
        df = df[~df.index.duplicated(keep="first")]
        if self._calendar_list is not None:
            calendar = pd.DataFrame(index=self._calendar_list)
            df = df.reindex(
                calendar.loc[
                    pd.Timestamp(df.index.min()).date() :
                    pd.Timestamp(df.index.max()).date() + pd.Timedelta(hours=23, minutes=59)
                ].index
            )
        df.sort_index(inplace=True)
        df.loc[
            (df["volume"] <= 0) | np.isnan(df["volume"]),
            list(set(df.columns) - {self._symbol_field_name}),
        ] = np.nan

        count = 0
        while True:
            change_series = self.calc_change(df)
            mask = (change_series >= 89) & (change_series <= 111)
            if not mask.any():
                break
            existing_cols = [
                col for col in ["high", "close", "low", "open", "adjclose"]
                if col in df.columns
            ]
            df.loc[mask, existing_cols] = df.loc[mask, existing_cols] / 100
            count += 1
            if count >= 10:
                break

        df["change"] = self.calc_change(df)
        columns += ["change"]
        df.loc[(df["volume"] <= 0) | np.isnan(df["volume"]), columns] = np.nan
        df[self._symbol_field_name] = symbol
        df.index.names = [self._date_field_name]
        return df.reset_index()

    def _adjusted_price(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()
        df.set_index(self._date_field_name, inplace=True)
        if "adjclose" in df:
            df["factor"] = df["adjclose"] / df["close"]
            df["factor"] = df["factor"].ffill()
        else:
            df["factor"] = 1
        for col in self.COLUMNS:
            if col not in df.columns:
                continue
            if col == "volume":
                df[col] = df[col] / df["factor"]
            else:
                df[col] = df[col] * df["factor"]
        df.index.names = [self._date_field_name]
        return df.reset_index()

    def _manual_adj_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        result = df.copy()
        result.sort_values(self._date_field_name, inplace=True)
        result = result.set_index(self._date_field_name)
        first_close = result.loc[result["close"].first_valid_index():, "close"].iloc[0]
        for col in result.columns:
            if col in [self._symbol_field_name, "adjclose", "change", "amount"]:
                continue
            if col == "volume":
                result[col] = result[col] * first_close
            else:
                result[col] = result[col] / first_close
        result["amount"] = df.set_index(self._date_field_name)["amount"]
        return result.reset_index()


def normalize_crowd_source_data(
    source_dir: str,
    normalize_dir: str,
    max_workers: int = 1,
    date_field_name: str = "tradedate",
    symbol_field_name: str = "symbol",
) -> None:
    mp.set_start_method("spawn", force=True)
    normalizer = Normalize(
        source_dir=source_dir,
        target_dir=normalize_dir,
        normalize_class=CrowdSourceNormalize,
        max_workers=max_workers,
        date_field_name=date_field_name,
        symbol_field_name=symbol_field_name,
    )
    normalizer.normalize()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--normalize-dir", required=True)
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument("--date-field-name", default="tradedate")
    parser.add_argument("--symbol-field-name", default="symbol")
    args = parser.parse_args()

    normalize_crowd_source_data(
        source_dir=args.source_dir,
        normalize_dir=args.normalize_dir,
        max_workers=args.max_workers,
        date_field_name=args.date_field_name,
        symbol_field_name=args.symbol_field_name,
    )


if __name__ == "__main__":
    main()
