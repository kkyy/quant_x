"""Tests for InstitutionalHoldFetcher."""
import pytest
import pandas as pd
from unittest.mock import patch
from pathlib import Path

from quant_ex.data.fetchers.institutional_fetcher import InstitutionalHoldFetcher


@pytest.fixture
def fetcher(tmp_path):
    return InstitutionalHoldFetcher(
        cache_dir=str(tmp_path / "institutional"), cache_ttl_days=1
    )


# ── Raw data fixtures ──────────────────────────────────────────────────────


def _make_fund_raw():
    """Simulate ak.stock_report_fund_hold(symbol="基金持仓", date="20240630") output."""
    return pd.DataFrame({
        "序号": [1, 2, 3],
        "股票代码": ["600519", "000001", "430001"],
        "股票简称": ["贵州茅台", "平安银行", "BH股份"],
        "持有基金家数": [150, 80, 5],
        "持股总数": [5000000.0, 3000000.0, 200000.0],
        "持股市值": [9000000000.0, 4500000000.0, 1600000.0],
        "持股变化": ["增持", "减持", "新进"],
        "持股变动数值": [500000.0, -200000.0, 200000.0],
        "持股变动比例": [11.11, -6.25, 100.0],
    })


def _make_qfii_raw():
    """Simulate ak.stock_report_fund_hold(symbol="QFII持仓", date="20240630") output."""
    return pd.DataFrame({
        "序号": [1, 2],
        "股票代码": ["600519", "000858"],
        "股票简称": ["贵州茅台", "五粮液"],
        "持有机构家数": [3, 1],
        "持股总数": [1000000.0, 500000.0],
        "持股市值": [1800000000.0, 800000000.0],
        "持股变化": ["新进", "增持"],
        "持股变动数值": [1000000.0, 200000.0],
        "持股变动比例": [100.0, 66.67],
    })


def _make_ss_raw():
    """Simulate ak.stock_report_fund_hold(symbol="社保持仓", date="20240630") output."""
    return pd.DataFrame({
        "序号": [1],
        "股票代码": ["601318"],
        "股票简称": ["中国平安"],
        "持有机构家数": [2],
        "持股总数": [800000.0],
        "持股市值": [400000000.0],
        "持股变化": ["增持"],
        "持股变动数值": [100000.0],
        "持股变动比例": [14.29],
    })


# ── Normalization tests ────────────────────────────────────────────────────


def test_normalize_fund_column_mapping(fetcher):
    """Fund raw columns should map to English schema."""
    raw = _make_fund_raw()
    result = fetcher._normalize(raw, "fund", "20240630")
    assert result is not None
    assert "fund_count" in result.columns
    assert "hold_shares" in result.columns
    assert "hold_mv" in result.columns
    assert "hold_change" in result.columns
    assert "hold_change_pct" in result.columns
    assert result.index.names == ["instrument", "datetime"]


def test_normalize_qfii_column_mapping(fetcher):
    """QFII raw columns should map to English schema."""
    raw = _make_qfii_raw()
    result = fetcher._normalize(raw, "qfii", "20240630")
    assert result is not None
    assert "inst_count" in result.columns
    assert "hold_shares" in result.columns
    assert "hold_mv" in result.columns
    assert "hold_change" in result.columns
    assert result.index.names == ["instrument", "datetime"]


def test_normalize_ss_column_mapping(fetcher):
    """Social security raw columns should map to English schema."""
    raw = _make_ss_raw()
    result = fetcher._normalize(raw, "ss", "20240630")
    assert result is not None
    assert "inst_count" in result.columns
    assert "hold_shares" in result.columns
    assert result.index.names == ["instrument", "datetime"]


# ── MultiIndex tests ───────────────────────────────────────────────────────


def test_fetch_quarter_returns_multiindex(fetcher, tmp_path):
    """_fetch_quarter should return (instrument, datetime) MultiIndex."""
    with patch.object(
        fetcher,
        "_call_akshare",
        return_value=_make_fund_raw(),
    ):
        result = fetcher._fetch_quarter("fund", "20240630")
    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    instruments = result.index.get_level_values(0).unique().tolist()
    assert "SH600519" in instruments
    assert "SZ000001" in instruments
    assert "BJ430001" in instruments


def test_fetch_quarter_caches_result(fetcher, tmp_path):
    """After _fetch_quarter, a cache file should exist."""
    with patch.object(
        fetcher,
        "_call_akshare",
        return_value=_make_fund_raw(),
    ):
        fetcher._fetch_quarter("fund", "20240630")
    cache_file = tmp_path / "institutional" / "fund_hold_20240630.csv"
    assert cache_file.exists()


def test_fetch_quarter_qfii_caches_result(fetcher, tmp_path):
    """QFII fetch should also create cache file."""
    with patch.object(
        fetcher,
        "_call_akshare",
        return_value=_make_qfii_raw(),
    ):
        fetcher._fetch_quarter("qfii", "20240630")
    cache_file = tmp_path / "institutional" / "qfii_hold_20240630.csv"
    assert cache_file.exists()


# ── Multiple quarters ──────────────────────────────────────────────────────


def test_multiple_quarters(fetcher, tmp_path):
    """Multiple quarter files should be loadable via _load_cached_range."""
    fetcher._ensure_cache_dir()

    # Write two quarter files manually
    q1 = pd.DataFrame(
        {
            "fund_count": [150, 80],
            "hold_shares": [5000000.0, 3000000.0],
            "hold_mv": [9000000000.0, 4500000000.0],
        },
        index=pd.MultiIndex.from_tuples(
            [
                ("SH600519", pd.Timestamp("2024-03-31")),
                ("SZ000001", pd.Timestamp("2024-03-31")),
            ],
            names=["instrument", "datetime"],
        ),
    )
    q2 = pd.DataFrame(
        {
            "fund_count": [160, 85],
            "hold_shares": [5500000.0, 3200000.0],
            "hold_mv": [10000000000.0, 4800000000.0],
        },
        index=pd.MultiIndex.from_tuples(
            [
                ("SH600519", pd.Timestamp("2024-06-30")),
                ("SZ000001", pd.Timestamp("2024-06-30")),
            ],
            names=["instrument", "datetime"],
        ),
    )
    q1.to_csv(tmp_path / "institutional" / "fund_hold_20240331.csv")
    q2.to_csv(tmp_path / "institutional" / "fund_hold_20240630.csv")

    result = fetcher._load_cached_range("2024-01-01", "2024-12-31")
    assert result is not None
    # Should have data from both quarters
    unique_dates = result.index.get_level_values(1).unique()
    assert pd.Timestamp("2024-03-31") in unique_dates
    assert pd.Timestamp("2024-06-30") in unique_dates


# ── Instrument mapping ─────────────────────────────────────────────────────


def test_code_to_instrument_sh(fetcher):
    """SSE codes should be correctly mapped."""
    assert InstitutionalHoldFetcher._code_to_instrument("600519") == "SH600519"


def test_code_to_instrument_sz(fetcher):
    """SZSE codes should be correctly mapped."""
    assert InstitutionalHoldFetcher._code_to_instrument("000001") == "SZ000001"


def test_code_to_instrument_bj(fetcher):
    """BJ exchange codes should be correctly mapped."""
    assert InstitutionalHoldFetcher._code_to_instrument("430001") == "BJ430001"
    assert InstitutionalHoldFetcher._code_to_instrument("920001") == "BJ920001"


# ── Cache read / freshness ─────────────────────────────────────────────────


def test_fetch_quarter_reads_cache(fetcher, tmp_path):
    """If cache is fresh, should read from cache and not call API."""
    cached = pd.DataFrame(
        {
            "fund_count": [150],
            "hold_shares": [5000000.0],
            "hold_mv": [9000000000.0],
        },
        index=pd.MultiIndex.from_tuples(
            [("SH600519", pd.Timestamp("2024-06-30"))],
            names=["instrument", "datetime"],
        ),
    )
    fetcher._ensure_cache_dir()
    cached.to_csv(tmp_path / "institutional" / "fund_hold_20240630.csv")

    with patch.object(fetcher, "_call_akshare") as mock_api:
        result = fetcher._fetch_quarter("fund", "20240630")
    mock_api.assert_not_called()
    assert result is not None
    assert "fund_count" in result.columns


def test_api_failure_returns_none(fetcher, tmp_path):
    """When API call fails, _fetch_quarter should return None."""
    with patch.object(
        fetcher,
        "_call_akshare",
        side_effect=Exception("API error"),
    ):
        result = fetcher._fetch_quarter("fund", "20240630")
    assert result is None


def test_empty_raw_returns_none(fetcher, tmp_path):
    """When API returns empty DataFrame, _fetch_quarter should return None."""
    with patch.object(
        fetcher,
        "_call_akshare",
        return_value=pd.DataFrame(),
    ):
        result = fetcher._fetch_quarter("fund", "20240630")
    assert result is None


# ── Quarter date generation ────────────────────────────────────────────────


def test_recent_quarter_dates():
    """Should generate valid quarter-end date strings."""
    dates = InstitutionalHoldFetcher._recent_quarter_dates(n=4)
    assert len(dates) <= 4
    for d in dates:
        assert len(d) == 8  # YYYYMMDD format
        # Should be a valid quarter-end month
        month = int(d[4:6])
        assert month in (3, 6, 9, 12)
