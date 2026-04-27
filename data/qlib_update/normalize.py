"""Normalize crowd-sourced A-share CSV files for qlib dump_bin."""
from __future__ import annotations

import argparse
import multiprocessing as mp

import pandas as pd

try:
    from data_collector.base import Normalize
    from data_collector.yahoo import collector as yahoo_collector
except ImportError as exc:  # pragma: no cover - depends on external qlib scripts path
    raise ImportError(
        "Cannot import qlib data_collector modules. Add <qlib_repo>/scripts to PYTHONPATH."
    ) from exc


class CrowdSourceNormalize(yahoo_collector.YahooNormalizeCN1d):
    """Yahoo CN normalizer variant that keeps amount and adjusts vwap."""

    COLUMNS = ["open", "close", "high", "low", "vwap", "volume"]

    def _manual_adj_data(self, df: pd.DataFrame) -> pd.DataFrame:
        result_df = super()._manual_adj_data(df)
        result_df["amount"] = df["amount"]
        return result_df


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

