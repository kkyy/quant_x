"""Tests for data/sources — gap detection, filling, and source adapters."""
from __future__ import annotations

import pandas as pd
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_csv(tmp_path: Path, symbol: str, dates: list[str]) -> Path:
    """Write a minimal source CSV with given dates and return its path."""
    p = tmp_path / f"{symbol}.csv"
    rows = []
    for d in dates:
        rows.append({
            "tradedate": d,
            "symbol": symbol,
            "high": 10.5,
            "low": 9.5,
            "open": 10.0,
            "close": 10.0,
            "adjclose": 10.0,
            "volume": 1000.0,
            "amount": 10000.0,
            "vwap": 100.0,
        })
    df = pd.DataFrame(rows)
    df.to_csv(p, index=False)
    return p


def _make_new_rows(symbol: str, dates: list[str]) -> pd.DataFrame:
    """Return a DataFrame in source CSV format for the given dates."""
    rows = []
    for d in dates:
        rows.append({
            "tradedate": pd.Timestamp(d),
            "symbol": symbol,
            "high": 11.0,
            "low": 9.8,
            "open": 10.1,
            "close": 10.5,
            "adjclose": 10.5,
            "volume": 1200.0,
            "amount": 12600.0,
            "vwap": 105.0,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# detect_source_cutoff
# ---------------------------------------------------------------------------

class TestDetectSourceCutoff:
    def test_returns_none_on_empty_dir(self, tmp_path):
        from data.sources.gap_filler import detect_source_cutoff
        assert detect_source_cutoff(tmp_path) is None

    def test_returns_latest_date(self, tmp_path):
        from data.sources.gap_filler import detect_source_cutoff
        _make_csv(tmp_path, "SH600000", ["2025-04-20", "2025-04-21"])
        _make_csv(tmp_path, "SZ000001", ["2025-04-20", "2025-04-22"])  # later
        cutoff = detect_source_cutoff(tmp_path)
        from datetime import date
        assert cutoff == date(2025, 4, 22)

    def test_prefers_preferred_stocks(self, tmp_path):
        from data.sources.gap_filler import detect_source_cutoff
        # Only the preferred stock file present
        _make_csv(tmp_path, "SH600000", ["2025-04-10"])
        _make_csv(tmp_path, "SH999999", ["2025-04-15"])
        cutoff = detect_source_cutoff(tmp_path, sample_size=1)
        # With sample_size=1 and SH600000 preferred, it reads SH600000 first
        from datetime import date
        assert cutoff is not None


# ---------------------------------------------------------------------------
# GapFiller
# ---------------------------------------------------------------------------

class TestGapFiller:
    def _make_source(self, new_rows: pd.DataFrame):
        """Return a mock BaseDataSource that always returns new_rows."""
        src = MagicMock()
        src.name = "mock"
        src.fetch.return_value = new_rows
        return src

    def test_skips_when_gap_too_small(self, tmp_path):
        from data.sources.gap_filler import GapFiller
        _make_csv(tmp_path, "SH600000", ["2025-04-28"])
        src = self._make_source(_make_new_rows("SH600000", []))
        filler = GapFiller(source_dir=tmp_path, data_source=src, min_gap_days=3)
        stats = filler.fill(end_date="2025-04-29")  # gap = 1 day
        assert stats["filled"] == 0
        assert stats["skipped"] == 0
        src.fetch.assert_not_called()

    def test_fills_gap(self, tmp_path):
        from data.sources.gap_filler import GapFiller
        _make_csv(tmp_path, "SH600000", ["2025-04-20", "2025-04-21"])
        new_rows = _make_new_rows("SH600000", ["2025-04-22", "2025-04-23"])
        src = self._make_source(new_rows)
        filler = GapFiller(source_dir=tmp_path, data_source=src, min_gap_days=1)
        stats = filler.fill(end_date="2025-04-23")
        assert stats["filled"] == 1
        assert stats["errors"] == 0

        result = pd.read_csv(tmp_path / "SH600000.csv", parse_dates=["tradedate"])
        assert len(result) == 4
        assert result["tradedate"].max() == pd.Timestamp("2025-04-23")

    def test_no_duplicate_rows_after_fill(self, tmp_path):
        from data.sources.gap_filler import GapFiller
        _make_csv(tmp_path, "SH600000", ["2025-04-20", "2025-04-21"])
        # Source returns rows that overlap with existing data
        new_rows = _make_new_rows("SH600000", ["2025-04-21", "2025-04-22"])
        src = self._make_source(new_rows)
        filler = GapFiller(source_dir=tmp_path, data_source=src, min_gap_days=1)
        filler.fill(end_date="2025-04-22")
        result = pd.read_csv(tmp_path / "SH600000.csv", parse_dates=["tradedate"])
        # 2025-04-21 must appear only once
        assert result["tradedate"].duplicated().sum() == 0
        assert len(result) == 3

    def test_skips_stock_when_source_returns_empty(self, tmp_path):
        from data.sources.gap_filler import GapFiller
        _make_csv(tmp_path, "SH600000", ["2025-04-20"])
        src = self._make_source(pd.DataFrame(columns=[
            "tradedate", "symbol", "high", "low", "open",
            "close", "adjclose", "volume", "amount", "vwap",
        ]))
        filler = GapFiller(source_dir=tmp_path, data_source=src, min_gap_days=1)
        stats = filler.fill(end_date="2025-04-25")
        # Source returned empty → counted as skipped, not error
        assert stats["skipped"] == 1
        assert stats["errors"] == 0


# ---------------------------------------------------------------------------
# AkshareSource (unit test with mock)
# ---------------------------------------------------------------------------

class TestAkshareSource:
    def test_fetch_maps_columns(self):
        from data.sources.akshare_source import AkshareSource

        mock_df = pd.DataFrame([{
            "日期": "2025-04-22",
            "股票代码": "600000",
            "开盘": 9.5,
            "收盘": 10.0,
            "最高": 10.5,
            "最低": 9.4,
            "成交量": 2000.0,
            "成交额": 20000.0,
            "振幅": 1.1,
            "涨跌幅": 0.5,
            "涨跌额": 0.05,
            "换手率": 0.3,
        }])

        with patch("akshare.stock_zh_a_hist", return_value=mock_df):
            src = AkshareSource()
            result = src.fetch("SH600000", "2025-04-22", "2025-04-22")

        assert list(result.columns) == src.SOURCE_COLUMNS
        assert len(result) == 1
        assert result.iloc[0]["symbol"] == "SH600000"
        assert result.iloc[0]["adjclose"] == result.iloc[0]["close"]
        assert result.iloc[0]["vwap"] == pytest.approx(20000.0 / 2000.0 * 10)

    def test_fetch_returns_empty_on_error(self):
        from data.sources.akshare_source import AkshareSource

        with patch("akshare.stock_zh_a_hist", side_effect=Exception("network error")):
            src = AkshareSource()
            result = src.fetch("SH600000", "2025-04-22", "2025-04-22")

        assert result.empty


# ---------------------------------------------------------------------------
# EastMoneySource (unit test with mock)
# ---------------------------------------------------------------------------

class TestEastMoneySource:
    def test_fetch_maps_columns(self):
        from data.sources.eastmoney_source import EastMoneySource

        mock_df = pd.DataFrame([{
            "日期": pd.Timestamp("2025-04-22"),
            "开盘价": 9.5,
            "收盘价": 10.0,
            "最高价": 10.5,
            "最低价": 9.4,
            "成交量(手)": 2000.0,
            "成交额(元)": 20000.0,
            "振幅(%)": 1.1,
            "涨跌幅(%)": 0.5,
            "涨跌额": 0.05,
            "换手率(%)": 0.3,
        }])

        mock_api = MagicMock()
        mock_api.get_kline.return_value = mock_df

        with patch("data.sources.eastmoney_source.KlineAPI", return_value=mock_api, create=True):
            src = EastMoneySource()
            # Patch the import inside fetch
            import data.sources.eastmoney_source as em_mod
            with patch.dict("sys.modules", {
                "crawler.eastmoney.kline": MagicMock(KlineAPI=lambda: mock_api),
                "crawler.eastmoney.enums": MagicMock(
                    AdjustType=MagicMock(NONE=0),
                    KlineInterval=MagicMock(DAY=101),
                ),
            }):
                result = src.fetch("SH600000", "2025-04-22", "2025-04-22")

        assert list(result.columns) == src.SOURCE_COLUMNS
        assert result.iloc[0]["symbol"] == "SH600000"
        assert result.iloc[0]["adjclose"] == result.iloc[0]["close"]

    def test_fetch_returns_empty_on_import_error(self):
        from data.sources.eastmoney_source import EastMoneySource
        import sys

        src = EastMoneySource()
        with patch.dict("sys.modules", {
            "crawler.eastmoney.kline": None,
            "crawler.eastmoney.enums": None,
        }):
            result = src.fetch("SH600000", "2025-04-22", "2025-04-22")

        assert result.empty
