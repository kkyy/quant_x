"""Tests for ShareholderCountFetcher."""
import pytest
import pandas as pd
from unittest.mock import patch
from pathlib import Path

from quant_ex.data.fetchers.shareholder_fetcher import ShareholderCountFetcher


@pytest.fixture
def fetcher(tmp_path):
    return ShareholderCountFetcher(
        cache_dir=str(tmp_path / "shareholder"), cache_ttl_days=1
    )


# ── Bulk raw data fixtures ─────────────────────────────────────────────────

def _make_bulk_raw():
    """Simulate ak.stock_zh_a_gdhs(symbol="最新") output."""
    return pd.DataFrame({
        "代码": ["600519", "000001", "430001"],
        "名称": ["贵州茅台", "平安银行", "BH股份"],
        "最新价": [1800.0, 15.0, 8.5],
        "股东户数-本次": [120000, 350000, 15000],
        "股东户数-上次": [125000, 360000, 14500],
        "股东户数-增减": [-5000, -10000, 500],
        "股东户数-增减比例": [-4.0, -2.78, 3.45],
        "户均持股市值": [150000.0, 4300.0, 5600.0],
        "户均持股数量": [83.0, 510.0, 660.0],
        "总市值": [2.26e12, 2.9e11, 1.3e10],
        "公告日期": ["2026-03-31", "2026-03-31", "2026-03-31"],
    })


def _make_detail_raw():
    """Simulate ak.stock_zh_a_gdhs_detail_em(symbol="600519") output."""
    return pd.DataFrame({
        "股东户数": [120000, 125000, 130000],
        "户均持股数量": [83.0, 80.0, 76.0],
        "户均持股金额": [150000.0, 144000.0, 136800.0],
        "截止日期": ["2026-03-31", "2025-12-31", "2025-09-30"],
        "较上期变化": [-5000, -5000, 5000],
    })


# ── Normalisation tests ─────────────────────────────────────────────────────

def test_normalize_bulk_column_mapping(fetcher):
    """Bulk raw columns should map to English schema."""
    raw = _make_bulk_raw()
    result = fetcher._normalize_bulk(raw)
    assert result is not None
    assert "sh_count" in result.columns
    assert "sh_count_chg_pct" in result.columns
    assert "shares_per_holder" in result.columns
    assert "value_per_holder" in result.columns
    assert result.index.names == ["instrument", "datetime"]


def test_normalize_detail_column_mapping(fetcher):
    """Detail raw columns should map to English schema."""
    raw = _make_detail_raw()
    result = fetcher._normalize_detail(raw, "SH600519")
    assert result is not None
    assert "sh_count" in result.columns
    assert "shares_per_holder" in result.columns
    assert "value_per_holder" in result.columns
    assert "sh_count_chg" in result.columns
    assert result.index.names == ["instrument", "datetime"]


# ── MultiIndex tests ────────────────────────────────────────────────────────

def test_fetch_bulk_returns_multiindex(fetcher, tmp_path):
    """Bulk fetch should return (instrument, datetime) MultiIndex."""
    with patch.object(
        fetcher,
        "_call_akshare_bulk",
        return_value=_make_bulk_raw(),
    ):
        result = fetcher._fetch_bulk()
    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    # Check instrument mapping
    instruments = result.index.get_level_values(0).unique().tolist()
    assert "SH600519" in instruments
    assert "SZ000001" in instruments
    assert "BJ430001" in instruments


def test_fetch_bulk_caches_result(fetcher, tmp_path):
    """After bulk fetch, a cache file should exist."""
    with patch.object(
        fetcher,
        "_call_akshare_bulk",
        return_value=_make_bulk_raw(),
    ):
        fetcher._fetch_bulk()
    cache_file = tmp_path / "shareholder" / "gdhs_latest.csv"
    assert cache_file.exists()


def test_fetch_one_detail_returns_multiindex(fetcher, tmp_path):
    """Per-stock detail should return (instrument, datetime) MultiIndex."""
    with patch.object(
        fetcher,
        "_call_akshare_detail",
        return_value=_make_detail_raw(),
    ):
        result = fetcher._fetch_one_detail("SH600519")
    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    assert "SH600519" in result.index.get_level_values(0).unique()


# ── Fallback tests ──────────────────────────────────────────────────────────

def test_fallback_to_quarter_date(fetcher, tmp_path):
    """When '最新' fails, should try specific quarter dates."""
    quarter_raw = pd.DataFrame({
        "代码": ["600519"],
        "名称": ["贵州茅台"],
        "股东户数-本次": [120000],
        "股东户数-增减比例": [-4.0],
        "户均持股数量": [83.0],
        "户均持股市值": [150000.0],
        "公告日期": ["2025-09-30"],
    })

    call_count = {"n": 0}

    def mock_bulk(symbol):
        call_count["n"] += 1
        if symbol == "最新":
            raise Exception("API error for 最新")
        return quarter_raw

    with patch.object(fetcher, "_call_akshare_bulk", side_effect=mock_bulk):
        result = fetcher._fetch_bulk()
    assert result is not None
    assert "sh_count" in result.columns
    assert call_count["n"] > 1  # Should have tried fallback


def test_all_bulk_attempts_fail(fetcher, tmp_path):
    """When all bulk attempts fail, should return None."""
    with patch.object(
        fetcher,
        "_call_akshare_bulk",
        side_effect=Exception("API error"),
    ):
        result = fetcher._fetch_bulk()
    assert result is None


# ── Instrument mapping tests ────────────────────────────────────────────────

def test_code_to_instrument_bj(fetcher):
    """BJ exchange codes should be correctly mapped."""
    assert ShareholderCountFetcher._code_to_instrument("920001") == "BJ920001"
    assert ShareholderCountFetcher._code_to_instrument("430001") == "BJ430001"
    assert ShareholderCountFetcher._code_to_instrument("830001") == "BJ830001"


def test_code_to_instrument_sh(fetcher):
    """SSE codes should be correctly mapped."""
    assert ShareholderCountFetcher._code_to_instrument("600519") == "SH600519"
    assert ShareholderCountFetcher._code_to_instrument("601318") == "SH601318"


def test_code_to_instrument_sz(fetcher):
    """SZSE codes should be correctly mapped."""
    assert ShareholderCountFetcher._code_to_instrument("000001") == "SZ000001"
    assert ShareholderCountFetcher._code_to_instrument("300001") == "SZ300001"


# ── Cache read test ─────────────────────────────────────────────────────────

def test_fetch_bulk_reads_cache(fetcher, tmp_path):
    """If cache is fresh, should read from cache and not call APIs."""
    cached = pd.DataFrame(
        {
            "sh_count": [120000],
            "sh_count_chg_pct": [-4.0],
            "shares_per_holder": [83.0],
            "value_per_holder": [150000.0],
        },
        index=pd.MultiIndex.from_tuples(
            [("SH600519", pd.Timestamp("2026-03-31"))],
            names=["instrument", "datetime"],
        ),
    )
    fetcher._ensure_cache_dir()
    cached.to_csv(tmp_path / "shareholder" / "gdhs_latest.csv")

    with patch.object(
        fetcher, "_call_akshare_bulk"
    ) as mock_api:
        result = fetcher._fetch_bulk()
    mock_api.assert_not_called()
    assert result is not None
    assert "sh_count" in result.columns


# ── Load cached range test ──────────────────────────────────────────────────

def test_load_cached_range(fetcher, tmp_path):
    """_load_cached_range should filter by date and include per-stock + bulk."""
    fetcher._ensure_cache_dir()

    # Write per-stock detail
    detail = pd.DataFrame(
        {
            "sh_count": [130000, 125000],
            "shares_per_holder": [76.0, 80.0],
            "value_per_holder": [136800.0, 144000.0],
            "sh_count_chg": [5000, -5000],
        },
        index=pd.MultiIndex.from_tuples(
            [
                ("SH600519", pd.Timestamp("2025-09-30")),
                ("SH600519", pd.Timestamp("2025-12-31")),
            ],
            names=["instrument", "datetime"],
        ),
    )
    detail.to_csv(tmp_path / "shareholder" / "SH600519.csv")

    # Write bulk snapshot
    bulk = pd.DataFrame(
        {
            "sh_count": [350000],
            "sh_count_chg_pct": [-2.78],
            "shares_per_holder": [510.0],
            "value_per_holder": [4300.0],
        },
        index=pd.MultiIndex.from_tuples(
            [("SZ000001", pd.Timestamp("2026-03-31"))],
            names=["instrument", "datetime"],
        ),
    )
    bulk.to_csv(tmp_path / "shareholder" / "gdhs_latest.csv")

    result = fetcher._load_cached_range(
        ["SH600519", "SZ000001"], "2025-01-01", "2026-12-31"
    )
    assert result is not None
    instruments = result.index.get_level_values(0).unique().tolist()
    assert "SH600519" in instruments
    assert "SZ000001" in instruments
