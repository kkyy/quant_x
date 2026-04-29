"""Gap detector and filler for qlib source CSV directory.

After a Dolt dump, each stock's CSV in ``qlib_source/`` only contains data up
to the last Dolt commit.  If Dolt is stale (not updated for several days), the
gap filler fetches the missing trading days from a supplementary source
(akshare or East Money) and appends them to each CSV.

Typical usage
-------------
    from data.sources import GapFiller, AkshareSource

    filler = GapFiller(source_dir=paths.source_dir, data_source=AkshareSource())
    filler.fill(end_date="2025-04-29")
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from .base import BaseDataSource

logger = logging.getLogger(__name__)


def detect_source_cutoff(source_dir: Path, sample_size: int = 20) -> Optional[date]:
    """Return the latest ``tradedate`` found across a sample of source CSVs.

    Samples up to *sample_size* CSV files (favoring common SH/SZ stocks) to
    find the cutoff without reading all 6000+ files.

    Returns
    -------
    date or None
        Latest date present, or ``None`` if the directory is empty.
    """
    csv_files = list(source_dir.glob("*.csv"))
    if not csv_files:
        return None

    # Prefer liquid large-cap stocks for a fast representative sample
    preferred = [
        "SH600000.csv", "SH600519.csv", "SH601318.csv",
        "SZ000001.csv", "SZ000858.csv", "SZ300750.csv",
    ]
    candidates: list[Path] = []
    for name in preferred:
        p = source_dir / name
        if p.exists():
            candidates.append(p)

    # Top up with any files until we reach sample_size
    for f in csv_files:
        if len(candidates) >= sample_size:
            break
        if f not in candidates:
            candidates.append(f)

    latest: Optional[date] = None
    for f in candidates:
        try:
            df = pd.read_csv(f, usecols=["tradedate"], parse_dates=["tradedate"])
            if df.empty:
                continue
            d = df["tradedate"].max().date()
            if latest is None or d > latest:
                latest = d
        except Exception:
            continue

    return latest


class GapFiller:
    """Fill date gaps in qlib source CSVs from a supplementary data source.

    Parameters
    ----------
    source_dir : Path
        Directory containing per-stock source CSVs (``qlib_source/``).
    data_source : BaseDataSource
        Supplementary source to fetch missing data from.
    max_workers : int
        Thread parallelism when fetching multiple stocks.
    min_gap_days : int
        Only fill if the gap is at least this many calendar days (avoids
        spurious fills on weekends/holidays when Dolt is current).
    """

    def __init__(
        self,
        source_dir: Path,
        data_source: BaseDataSource,
        max_workers: int = 8,
        min_gap_days: int = 2,
    ):
        self.source_dir = Path(source_dir)
        self.data_source = data_source
        self.max_workers = max_workers
        self.min_gap_days = min_gap_days

    # ── public API ────────────────────────────────────────────────────────────

    def fill(self, end_date: Optional[str] = None) -> dict[str, int]:
        """Fill gaps for all CSVs in source_dir up to *end_date*.

        Parameters
        ----------
        end_date : str, optional
            Target end date ``"YYYY-MM-DD"``; defaults to today.

        Returns
        -------
        dict
            ``{"filled": N, "skipped": M, "errors": K}`` summary.
        """
        end_dt = date.fromisoformat(end_date) if end_date else date.today()
        cutoff = detect_source_cutoff(self.source_dir)

        if cutoff is None:
            logger.warning("source_dir is empty — nothing to fill")
            return {"filled": 0, "skipped": 0, "errors": 0}

        gap_days = (end_dt - cutoff).days
        if gap_days < self.min_gap_days:
            logger.info(
                f"Source data is current (cutoff={cutoff}, end={end_dt}, "
                f"gap={gap_days}d < min={self.min_gap_days}d) — skipping"
            )
            return {"filled": 0, "skipped": 0, "errors": 0}

        # start_date is one day after the cutoff
        start_dt = cutoff + timedelta(days=1)
        start_str = start_dt.isoformat()
        end_str = end_dt.isoformat()

        logger.info(
            f"Filling gap {start_str} → {end_str} "
            f"from [{self.data_source.name}] for source_dir={self.source_dir}"
        )

        csv_files = sorted(self.source_dir.glob("*.csv"))
        stats = {"filled": 0, "skipped": 0, "errors": 0}

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {
                pool.submit(self._fill_one, f, start_str, end_str): f
                for f in csv_files
            }
            for fut in as_completed(futures):
                result = fut.result()
                stats[result] += 1

        logger.info(
            f"Gap fill complete: {stats['filled']} filled, "
            f"{stats['skipped']} skipped, {stats['errors']} errors"
        )
        return stats

    # ── internals ─────────────────────────────────────────────────────────────

    def _fill_one(self, csv_path: Path, start_date: str, end_date: str) -> str:
        """Fill gap for a single stock CSV.  Returns ``"filled"``/``"skipped"``/``"errors"``."""
        qlib_symbol = csv_path.stem   # e.g. "SH600000"
        try:
            existing = pd.read_csv(csv_path, parse_dates=["tradedate"])
        except Exception as exc:
            logger.warning(f"[gap_filler] Cannot read {csv_path}: {exc}")
            return "errors"

        # Per-file cutoff: use its own latest date (some stocks may lag further)
        if not existing.empty:
            file_cutoff = existing["tradedate"].max()
            file_start = (file_cutoff + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            file_start = start_date

        if file_start > end_date:
            return "skipped"

        new_data = self.data_source.fetch(qlib_symbol, file_start, end_date)
        if new_data is None or new_data.empty:
            return "skipped"

        # Drop any overlapping dates (defensive)
        if not existing.empty:
            existing_dates = set(existing["tradedate"].dt.date)
            new_data = new_data[
                ~new_data["tradedate"].dt.date.isin(existing_dates)
            ]

        if new_data.empty:
            return "skipped"

        merged = pd.concat([existing, new_data], ignore_index=True)
        merged.sort_values("tradedate", inplace=True)
        merged.to_csv(csv_path, index=False)

        rows = len(new_data)
        logger.debug(f"[gap_filler] {qlib_symbol}: appended {rows} rows")
        return "filled"
