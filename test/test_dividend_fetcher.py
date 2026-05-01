import pytest
import pandas as pd
from unittest.mock import patch
from quant_ex.data.fetchers.dividend_fetcher import DividendFetcher


@pytest.fixture
def fetcher(tmp_path):
    return DividendFetcher(cache_dir=str(tmp_path / "dividend"), cache_ttl_days=30)


def test_fetch_one_returns_multiindex(fetcher, tmp_path):
    """_fetch_one should return a DataFrame with (instrument, datetime) MultiIndex."""
    fake_df = pd.DataFrame({
        "公告日期": ["2025-06-15", "2024-06-20"],
        "送股": [0.0, 0.0],
        "转增": [0.0, 0.0],
        "派息": [18.0, 16.5],
        "进度": ["实施方案", "实施方案"],
        "除权除息日": ["2025-07-01", "2024-07-05"],
        "股权登记日": ["2025-06-30", "2024-07-04"],
        "红股上市日": ["", ""],
    })
    with patch.object(fetcher, "_call_akshare_sina", return_value=fake_df):
        result = fetcher._fetch_one("SH600519")
    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    assert "cash_dividend" in result.columns
    assert "ex_date" in result.columns
    # Check values
    assert result["cash_dividend"].sum() == 34.5


def test_fetch_one_caches_result(fetcher, tmp_path):
    """_fetch_one should write a CSV cache file."""
    fake_df = pd.DataFrame({
        "公告日期": ["2025-06-15"],
        "送股": [0.0],
        "转增": [0.0],
        "派息": [18.0],
        "进度": ["实施方案"],
        "除权除息日": ["2025-07-01"],
        "股权登记日": ["2025-06-30"],
        "红股上市日": [""],
    })
    with patch.object(fetcher, "_call_akshare_sina", return_value=fake_df):
        fetcher._fetch_one("SH600519")
    cache_file = tmp_path / "dividend" / "SH600519.csv"
    assert cache_file.exists()


def test_fetch_one_reads_cache(fetcher, tmp_path):
    """If cache is fresh, _fetch_one should not call the API."""
    cached = pd.DataFrame({
        "cash_dividend": [18.0],
        "bonus_shares": [0.0],
        "conversion_shares": [0.0],
        "ex_date": [pd.Timestamp("2025-07-01")],
        "announcement_date": [pd.Timestamp("2025-06-15")],
        "record_date": [pd.Timestamp("2025-06-30")],
        "progress": ["实施方案"],
    }, index=pd.MultiIndex.from_tuples(
        [("SH600519", pd.Timestamp("2025-07-01"))],
        names=["instrument", "datetime"],
    ))
    fetcher._ensure_cache_dir()
    cached.to_csv(tmp_path / "dividend" / "SH600519.csv")

    with patch.object(fetcher, "_call_akshare_sina") as mock_api:
        result = fetcher._fetch_one("SH600519")
    mock_api.assert_not_called()
    assert result is not None
    assert result["cash_dividend"].iloc[0] == 18.0


def test_normalize_dividend_column_mapping(fetcher):
    """_normalize_sina should map Chinese column names to English."""
    raw = pd.DataFrame({
        "公告日期": ["2025-06-15"],
        "送股": [0.5],
        "转增": [0.3],
        "派息": [18.0],
        "进度": ["实施方案"],
        "除权除息日": ["2025-07-01"],
        "股权登记日": ["2025-06-30"],
        "红股上市日": [""],
    })
    result = fetcher._normalize_sina(raw, "SH600519")
    assert result is not None
    assert "cash_dividend" in result.columns
    assert "bonus_shares" in result.columns
    assert "conversion_shares" in result.columns
    assert "ex_date" in result.columns
    assert "announcement_date" in result.columns
    assert "record_date" in result.columns
    # Verify values are numeric for dividend columns
    assert result["cash_dividend"].iloc[0] == 18.0
    assert result["bonus_shares"].iloc[0] == 0.5
    assert result["conversion_shares"].iloc[0] == 0.3


def test_fallback_em_detail(fetcher, tmp_path):
    """If Sina source fails, _fetch_one should fall back to EM source."""
    fake_em_df = pd.DataFrame({
        "公告日期": ["2025-06-15"],
        "送股(股)": [0.0],
        "转增(股)": [0.0],
        "派息(元)": [18.0],
        "进度": ["实施方案"],
        "除权除息日": ["2025-07-01"],
        "股权登记日": ["2025-06-30"],
        "红股上市日": [""],
    })
    with patch.object(fetcher, "_call_akshare_sina", side_effect=Exception("sina error")), \
         patch.object(fetcher, "_call_akshare_em", return_value=fake_em_df):
        result = fetcher._fetch_one("SZ000001")
    assert result is not None
    assert "cash_dividend" in result.columns
    assert result["cash_dividend"].iloc[0] == 18.0


def test_both_sources_fail_returns_none(fetcher, tmp_path):
    """If both Sina and EM sources fail, _fetch_one should return None."""
    with patch.object(fetcher, "_call_akshare_sina", side_effect=Exception("sina error")), \
         patch.object(fetcher, "_call_akshare_em", side_effect=Exception("em error")):
        result = fetcher._fetch_one("SH600519")
    assert result is None


def test_empty_raw_returns_none(fetcher):
    """Empty DataFrame from source should return None."""
    raw = pd.DataFrame()
    result = fetcher._normalize_sina(raw, "SH600519")
    assert result is None
