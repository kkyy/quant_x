import pytest
import numpy as np
import pandas as pd
from pathlib import Path
from unittest.mock import patch
from quant_ex.data.fetchers.repurchase_fetcher import RepurchaseFetcher


@pytest.fixture
def fetcher(tmp_path):
    return RepurchaseFetcher(cache_dir=str(tmp_path / "repurchase"), cache_ttl_days=1)


def _fake_akshare_raw(n=3):
    """Create a fake raw DataFrame simulating akshare stock_repurchase_em() output."""
    return pd.DataFrame({
        "序号": list(range(1, n + 1)),
        "股票代码": ["600519", "000001", "920001"][:n],
        "股票简称": ["贵州茅台", "平安银行", "北交测试"][:n],
        "最新价": [1800.0, 12.5, 10.0][:n],
        "计划回购价格区间": ["1700-1900", "11-13", "9-11"][:n],
        "计划回购数量区间-下限": [100000, 5000000, 100000][:n],
        "计划回购数量区间-上限": [200000, 10000000, 200000][:n],
        "占公告前一日总股本比例-下限": [0.01, 0.02, 0.01][:n],
        "占公告前一日总股本比例-上限": [0.02, 0.05, 0.02][:n],
        "计划回购金额区间-下限": [100000000, 50000000, 1000000][:n],
        "计划回购金额区间-上限": [200000000, 100000000, 2000000][:n],
        "回购起始时间": ["2025-01-01", "2025-03-01", "2025-06-01"][:n],
        "实施进度": ["实施中", "完成", "实施中"][:n],
        "已回购股份价格区间-下限": [1750.0, 11.5, 9.5][:n],
        "已回购股份价格区间-上限": [1850.0, 12.8, 10.5][:n],
        "已回购股份数量": [150000, 8000000, 50000][:n],
        "已回购金额": [150000000, 80000000, 500000][:n],
        "最新公告日期": ["2026-01-15", "2026-02-20", "2026-03-10"][:n],
    })


def test_fetch_repurchase_returns_multiindex(fetcher, tmp_path):
    """_fetch_repurchase should return a DataFrame with (instrument, datetime) MultiIndex."""
    fake_df = _fake_akshare_raw()
    with patch.object(fetcher, "_call_akshare_em", return_value=fake_df):
        result = fetcher._fetch_repurchase("20260430")
    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    assert "plan_amount_upper" in result.columns
    assert "done_amount" in result.columns
    assert "progress" in result.columns
    # Check instruments are correctly formatted
    instruments = result.index.get_level_values(0).unique().tolist()
    assert "SH600519" in instruments
    assert "SZ000001" in instruments
    assert "BJ920001" in instruments


def test_fetch_repurchase_caches_result(fetcher, tmp_path):
    """_fetch_repurchase should write a CSV cache file."""
    fake_df = _fake_akshare_raw(n=1)
    with patch.object(fetcher, "_call_akshare_em", return_value=fake_df):
        fetcher._fetch_repurchase("20260430")
    cache_file = tmp_path / "repurchase" / "repurchase_20260430.csv"
    assert cache_file.exists()


def test_fetch_repurchase_reads_cache(fetcher, tmp_path):
    """If cache is fresh, _fetch_repurchase should not call the API."""
    # Pre-populate cache
    fetcher._ensure_cache_dir()
    cached = pd.DataFrame({
        "plan_amount_upper": [200000000.0],
        "plan_amount_lower": [100000000.0],
        "done_amount": [150000000.0],
        "done_shares": [150000],
        "progress": ["实施中"],
        "announcement_date": [pd.Timestamp("2026-01-15")],
    }, index=pd.MultiIndex.from_tuples(
        [("SH600519", pd.Timestamp("2026-01-15"))],
        names=["instrument", "datetime"],
    ))
    cache_file = tmp_path / "repurchase" / "repurchase_20260430.csv"
    cached.to_csv(cache_file)

    with patch.object(fetcher, "_call_akshare_em") as mock_api:
        result = fetcher._fetch_repurchase("20260430")
    mock_api.assert_not_called()
    assert result is not None
    assert result["done_amount"].iloc[0] == 150000000.0


def test_normalize_column_mapping(fetcher):
    """_normalize should map Chinese column names to English."""
    raw = _fake_akshare_raw(n=1)
    result = fetcher._normalize(raw)
    assert result is not None
    # Verify all expected English columns are present
    for col in ["plan_amount_upper", "plan_amount_lower", "done_amount",
                "done_shares", "progress", "announcement_date"]:
        assert col in result.columns, f"Missing column: {col}"
    # Verify instrument format
    assert result.index.get_level_values(0)[0] == "SH600519"


def test_normalize_empty_returns_none(fetcher):
    """Empty DataFrame from source should return None."""
    raw = pd.DataFrame()
    result = fetcher._normalize(raw)
    assert result is None


def test_normalize_numeric_conversion(fetcher):
    """Numeric columns should be properly converted from strings."""
    raw = _fake_akshare_raw(n=1)
    result = fetcher._normalize(raw)
    assert result is not None
    # plan_amount_upper should be numeric
    val = result["plan_amount_upper"].iloc[0]
    assert isinstance(val, (int, float, np.floating, np.integer))


def test_api_failure_returns_none(fetcher, tmp_path):
    """If the API call fails, _fetch_repurchase should return None."""
    with patch.object(fetcher, "_call_akshare_em", side_effect=Exception("API error")):
        result = fetcher._fetch_repurchase("20260430")
    assert result is None


def test_code_to_instrument_various():
    """_code_to_instrument should handle various code formats."""
    assert RepurchaseFetcher._code_to_instrument("600519") == "SH600519"
    assert RepurchaseFetcher._code_to_instrument("000001") == "SZ000001"
    assert RepurchaseFetcher._code_to_instrument("920001") == "BJ920001"
    assert RepurchaseFetcher._code_to_instrument("430001") == "BJ430001"
    assert RepurchaseFetcher._code_to_instrument("830001") == "BJ830001"
    assert RepurchaseFetcher._code_to_instrument("300001") == "SZ300001"
    # Already prefixed
    assert RepurchaseFetcher._code_to_instrument("SH600519") == "SH600519"


def test_fetch_calls_refresh_and_loads(fetcher, tmp_path):
    """fetch() should call refresh_cache and return cached data."""
    fake_df = _fake_akshare_raw(n=2)
    with patch.object(fetcher, "_call_akshare_em", return_value=fake_df):
        result = fetcher.fetch(
            symbols=["SH600519", "SZ000001"],
            start_date="2026-01-01",
            end_date="2026-12-31",
        )
    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
