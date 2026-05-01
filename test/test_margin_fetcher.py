"""Tests for MarginTradeFetcher."""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch
from pathlib import Path

from quant_ex.data.fetchers.margin_fetcher import MarginTradeFetcher


@pytest.fixture
def fetcher(tmp_path):
    return MarginTradeFetcher(cache_dir=str(tmp_path / "margin"), cache_ttl_days=1)


# ── SSE raw data fixture ────────────────────────────────────────────────────

def _make_sse_raw():
    """Simulate akshare stock_margin_detail_sse output."""
    return pd.DataFrame({
        "标的证券代码": ["600519", "601318", "600036"],
        "信用交易日期": ["2026-04-29", "2026-04-29", "2026-04-29"],
        "融资余额(元)": [1.5e10, 8.0e9, 6.0e9],
        "融资买入额(元)": [5.0e8, 3.0e8, 2.0e8],
        "融资偿还额(元)": [4.5e8, 2.8e8, 1.9e8],
        "融券余量(股)": [200000, 150000, 100000],
        "融券卖出量(股)": [50000, 30000, 20000],
        "融券偿还量(股)": [40000, 25000, 15000],
    })


def _make_szse_raw():
    """Simulate akshare stock_margin_detail_szse output."""
    return pd.DataFrame({
        "证券代码": ["000001", "000002", "300001"],
        "证券简称": ["平安银行", "万科A", "特锐德"],
        "融资买入额(元)": [3.0e8, 2.5e8, 1.0e8],
        "融资余额(元)": [7.0e9, 5.5e9, 2.0e9],
        "融券卖出量": [40000, 35000, 10000],
        "融券余量": [120000, 80000, 30000],
        "融券余额(元)": [1.8e9, 1.2e9, 5.0e8],
        "融资融券余额(元)": [8.8e9, 6.7e9, 2.5e9],
    })


# ── Normalisation tests ─────────────────────────────────────────────────────

def test_normalize_margin_sse_column_mapping(fetcher):
    """SSE raw columns should map to common schema."""
    raw = _make_sse_raw()
    result = fetcher._normalize_margin_sse(raw, "20260429")
    assert result is not None
    assert "margin_balance" in result.columns
    assert "margin_buy_amt" in result.columns
    assert "margin_repay_amt" in result.columns
    assert "short_balance" in result.columns
    assert "short_sell_vol" in result.columns
    assert "short_repay_vol" in result.columns
    assert "instrument" in result.columns
    assert "datetime" in result.columns
    # Check instrument mapping
    instruments = result["instrument"].tolist()
    assert "SH600519" in instruments
    assert "SH601318" in instruments
    assert "SH600036" in instruments


def test_normalize_margin_szse_column_mapping(fetcher):
    """SZSE raw columns should map to common schema (missing repay columns)."""
    raw = _make_szse_raw()
    result = fetcher._normalize_margin_szse(raw, "20260429")
    assert result is not None
    assert "margin_balance" in result.columns
    assert "margin_buy_amt" in result.columns
    assert "short_balance" in result.columns
    assert "short_sell_vol" in result.columns
    # SZSE does not provide repay columns
    assert "margin_repay_amt" not in result.columns
    assert "short_repay_vol" not in result.columns
    # Check instrument mapping
    instruments = result["instrument"].tolist()
    assert "SZ000001" in instruments
    assert "SZ000002" in instruments
    assert "SZ300001" in instruments


# ── Combination test ────────────────────────────────────────────────────────

def test_combine_sse_szse(fetcher):
    """Both exchange APIs should produce combined output with all stocks."""
    with patch.object(fetcher, "_call_sse", return_value=_make_sse_raw()), \
         patch.object(fetcher, "_call_szse", return_value=_make_szse_raw()):
        result = fetcher._fetch_margin("20260429")
    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    # Should have both SH and SZ instruments
    instruments = result.index.get_level_values(0).unique().tolist()
    assert "SH600519" in instruments
    assert "SZ000001" in instruments
    # Total: 3 SSE + 3 SZSE = 6 stocks
    assert len(result) == 6


# ── Fetch and cache tests ───────────────────────────────────────────────────

def test_fetch_margin_returns_multiindex(fetcher):
    """fetch() should return a (instrument, datetime) MultiIndex DataFrame."""
    from datetime import date as _date
    today_str = _date.today().strftime("%Y-%m-%d")
    with patch.object(fetcher, "_call_sse", return_value=_make_sse_raw()), \
         patch.object(fetcher, "_call_szse", return_value=_make_szse_raw()):
        result = fetcher.fetch(
            symbols=["SH600519", "SZ000001"],
            start_date=today_str,
            end_date=today_str,
        )
    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    assert "margin_balance" in result.columns


def test_fetch_margin_caches_result(fetcher, tmp_path):
    """After fetch, a cache file should exist."""
    with patch.object(fetcher, "_call_sse", return_value=_make_sse_raw()), \
         patch.object(fetcher, "_call_szse", return_value=_make_szse_raw()):
        fetcher._fetch_margin("20260429")
    cache_file = tmp_path / "margin" / "margin_20260429.csv"
    assert cache_file.exists()


def test_fetch_margin_reads_cache(fetcher, tmp_path):
    """If cache is fresh, should read from cache and not call APIs."""
    # Pre-populate cache
    cached = pd.DataFrame({
        "margin_balance": [1.5e10],
        "margin_buy_amt": [5.0e8],
        "margin_repay_amt": [4.5e8],
        "short_balance": [200000.0],
        "short_sell_vol": [50000.0],
        "short_repay_vol": [40000.0],
    }, index=pd.MultiIndex.from_tuples(
        [("SH600519", pd.Timestamp("2026-04-29"))],
        names=["instrument", "datetime"],
    ))
    fetcher._ensure_cache_dir()
    cached.to_csv(tmp_path / "margin" / "margin_20260429.csv")

    with patch.object(fetcher, "_call_sse") as mock_sse, \
         patch.object(fetcher, "_call_szse") as mock_szse:
        result = fetcher._fetch_margin("20260429")
    mock_sse.assert_not_called()
    mock_szse.assert_not_called()
    assert result is not None
    assert "margin_balance" in result.columns


def test_fetch_margin_sse_only(fetcher):
    """If SZSE fails, should still return SSE data."""
    with patch.object(fetcher, "_call_sse", return_value=_make_sse_raw()), \
         patch.object(fetcher, "_call_szse", side_effect=Exception("SZSE error")):
        result = fetcher._fetch_margin("20260429")
    assert result is not None
    instruments = result.index.get_level_values(0).unique().tolist()
    assert "SH600519" in instruments
    # Should only have SSE stocks
    assert all(inst.startswith("SH") for inst in instruments)


def test_fetch_margin_szse_only(fetcher):
    """If SSE fails, should still return SZSE data."""
    with patch.object(fetcher, "_call_sse", side_effect=Exception("SSE error")), \
         patch.object(fetcher, "_call_szse", return_value=_make_szse_raw()):
        result = fetcher._fetch_margin("20260429")
    assert result is not None
    instruments = result.index.get_level_values(0).unique().tolist()
    assert "SZ000001" in instruments


def test_fetch_margin_both_fail_fallback(fetcher):
    """If both exchange APIs fail, should try aggregate fallback."""
    agg_df = pd.DataFrame({
        "日期": ["2026-04-29"],
        "融资余额(元)": [5.0e11],
    })
    with patch.object(fetcher, "_call_sse", side_effect=Exception("SSE error")), \
         patch.object(fetcher, "_call_szse", side_effect=Exception("SZSE error")), \
         patch.object(fetcher, "_call_aggregate_sse", return_value=agg_df):
        result = fetcher._fetch_margin("20260429")
    assert result is not None
    assert "margin_balance" in result.columns


def test_code_to_instrument_bj(fetcher):
    """BJ exchange codes should be correctly mapped."""
    assert MarginTradeFetcher._code_to_instrument("920001") == "BJ920001"
    assert MarginTradeFetcher._code_to_instrument("430001") == "BJ430001"
    assert MarginTradeFetcher._code_to_instrument("830001") == "BJ830001"


def test_code_to_instrument_sh(fetcher):
    """SSE codes should be correctly mapped."""
    assert MarginTradeFetcher._code_to_instrument("600519") == "SH600519"
    assert MarginTradeFetcher._code_to_instrument("601318") == "SH601318"


def test_code_to_instrument_sz(fetcher):
    """SZSE codes should be correctly mapped."""
    assert MarginTradeFetcher._code_to_instrument("000001") == "SZ000001"
    assert MarginTradeFetcher._code_to_instrument("300001") == "SZ300001"
