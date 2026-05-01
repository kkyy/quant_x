import pytest
import pandas as pd
from unittest.mock import patch
from quant_ex.data.fetchers.pledge_fetcher import PledgeFetcher


@pytest.fixture
def fetcher(tmp_path):
    return PledgeFetcher(cache_dir=str(tmp_path / "pledge"), cache_ttl_days=1)


def test_fetch_pledge_returns_multiindex(fetcher, tmp_path):
    fake_df = pd.DataFrame({
        "序号": [1, 2],
        "股票代码": ["600519", "000001"],
        "股票简称": ["贵州茅台", "平安银行"],
        "交易日期": ["2026-04-29", "2026-04-29"],
        "所属行业": ["白酒", "银行"],
        "质押比例": [5.2, 3.1],
        "质押股数": [10000.0, 5000.0],
        "质押市值": [2000000.0, 50000.0],
        "质押笔数": [3, 2],
        "无限售股质押数": [10000.0, 5000.0],
        "限售股质押数": [0.0, 0.0],
        "近一年涨跌幅": [10.5, -5.2],
        "所属行业代码": [100, 200],
    })
    with patch("quant_ex.data.fetchers.pledge_fetcher.PledgeFetcher._call_akshare_bulk", return_value=fake_df):
        result = fetcher._fetch_pledge("20260429")
    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    assert "pledge_ratio" in result.columns
    assert "pledge_shares" in result.columns
    assert "pledge_mv" in result.columns
    assert "unlimited_pledge_shares" in result.columns
    assert "limited_pledge_shares" in result.columns


def test_fetch_pledge_caches_result(fetcher, tmp_path):
    fake_df = pd.DataFrame({
        "序号": [1],
        "股票代码": ["600519"],
        "股票简称": ["贵州茅台"],
        "交易日期": ["2026-04-29"],
        "所属行业": ["白酒"],
        "质押比例": [5.2],
        "质押股数": [10000.0],
        "质押市值": [2000000.0],
        "质押笔数": [3],
        "无限售股质押数": [10000.0],
        "限售股质押数": [0.0],
        "近一年涨跌幅": [10.5],
        "所属行业代码": [100],
    })
    with patch("quant_ex.data.fetchers.pledge_fetcher.PledgeFetcher._call_akshare_bulk", return_value=fake_df):
        fetcher._fetch_pledge("20260429")
    cache_file = tmp_path / "pledge" / "pledge_20260429.csv"
    assert cache_file.exists()


def test_fetch_pledge_reads_cache(fetcher, tmp_path):
    cached = pd.DataFrame({
        "pledge_ratio": [5.2],
        "pledge_shares": [10000.0],
        "pledge_mv": [2000000.0],
        "unlimited_pledge_shares": [10000.0],
        "limited_pledge_shares": [0.0],
        "pledge_count": [3],
    }, index=pd.MultiIndex.from_tuples(
        [("SH600519", pd.Timestamp("2026-04-29"))],
        names=["instrument", "datetime"],
    ))
    fetcher._ensure_cache_dir()
    cached.to_csv(tmp_path / "pledge" / "pledge_20260429.csv")

    with patch("quant_ex.data.fetchers.pledge_fetcher.PledgeFetcher._call_akshare_bulk") as mock_api:
        result = fetcher._fetch_pledge("20260429")
    mock_api.assert_not_called()
    assert result is not None
    assert "pledge_ratio" in result.columns


def test_normalize_pledge_column_mapping(fetcher, tmp_path):
    fake_df = pd.DataFrame({
        "序号": [1, 2, 3],
        "股票代码": ["600519", "000001", "430047"],
        "股票简称": ["贵州茅台", "平安银行", "诺思兰德"],
        "交易日期": ["2026-04-29", "2026-04-29", "2026-04-29"],
        "所属行业": ["白酒", "银行", "生物制品"],
        "质押比例": [5.2, 3.1, 10.0],
        "质押股数": [10000.0, 5000.0, 2000.0],
        "质押市值": [2000000.0, 50000.0, 30000.0],
        "质押笔数": [3, 2, 1],
        "无限售股质押数": [10000.0, 5000.0, 2000.0],
        "限售股质押数": [0.0, 0.0, 500.0],
        "近一年涨跌幅": [10.5, -5.2, 15.0],
        "所属行业代码": [100, 200, 300],
    })
    with patch("quant_ex.data.fetchers.pledge_fetcher.PledgeFetcher._call_akshare_bulk", return_value=fake_df):
        result = fetcher._fetch_pledge("20260429")
    assert result is not None
    # Verify Chinese→English column mapping
    assert "pledge_ratio" in result.columns
    assert "pledge_shares" in result.columns
    assert "pledge_mv" in result.columns
    assert "unlimited_pledge_shares" in result.columns
    assert "limited_pledge_shares" in result.columns
    # Original Chinese column names should NOT be present
    assert "质押比例" not in result.columns
    assert "质押股数" not in result.columns
    # Verify instrument format: SH for 6xx, SZ for 0xx, BJ for 4xx
    instruments = result.index.get_level_values(0).tolist()
    assert "SH600519" in instruments
    assert "SZ000001" in instruments
    assert "BJ430047" in instruments


def test_code_to_instrument_bj_exchange(fetcher):
    """BJ exchange: 920xxx, 4xx, 8xx codes."""
    assert PledgeFetcher._code_to_instrument("920001") == "BJ920001"
    assert PledgeFetcher._code_to_instrument("430047") == "BJ430047"
    assert PledgeFetcher._code_to_instrument("830799") == "BJ830799"


def test_code_to_instrument_sh_sz(fetcher):
    """SH for 6xx/9xx (excl. 920), SZ for 0xx/3xx."""
    assert PledgeFetcher._code_to_instrument("600519") == "SH600519"
    assert PledgeFetcher._code_to_instrument("900901") == "SH900901"
    assert PledgeFetcher._code_to_instrument("000001") == "SZ000001"
    assert PledgeFetcher._code_to_instrument("300001") == "SZ300001"


def test_code_to_instrument_already_prefixed(fetcher):
    """Codes already with exchange prefix pass through."""
    assert PledgeFetcher._code_to_instrument("SH600519") == "SH600519"
    assert PledgeFetcher._code_to_instrument("SZ000001") == "SZ000001"
    assert PledgeFetcher._code_to_instrument("BJ920001") == "BJ920001"


def test_fallback_to_detail_api(fetcher, tmp_path):
    """When bulk API fails, fall back to detail API."""
    detail_df = pd.DataFrame({
        "股票代码": ["600519"],
        "股票简称": ["贵州茅台"],
        "质押比例": [5.2],
        "质押股数": [10000.0],
        "质押市值": [2000000.0],
    })
    with patch("quant_ex.data.fetchers.pledge_fetcher.PledgeFetcher._call_akshare_bulk", side_effect=Exception("bulk error")), \
         patch("quant_ex.data.fetchers.pledge_fetcher.PledgeFetcher._call_akshare_detail", return_value=detail_df):
        result = fetcher._fetch_pledge("20260429")
    assert result is not None
    assert "pledge_ratio" in result.columns


def test_load_cached_range(fetcher, tmp_path):
    """Load multiple cached files and filter by date range."""
    fetcher._ensure_cache_dir()

    # Write two cache files with different dates
    df1 = pd.DataFrame({
        "pledge_ratio": [5.2],
    }, index=pd.MultiIndex.from_tuples(
        [("SH600519", pd.Timestamp("2026-04-28"))],
        names=["instrument", "datetime"],
    ))
    df2 = pd.DataFrame({
        "pledge_ratio": [5.5],
    }, index=pd.MultiIndex.from_tuples(
        [("SH600519", pd.Timestamp("2026-04-29"))],
        names=["instrument", "datetime"],
    ))
    df1.to_csv(tmp_path / "pledge" / "pledge_20260428.csv")
    df2.to_csv(tmp_path / "pledge" / "pledge_20260429.csv")

    result = fetcher._load_cached_range("2026-04-29", "2026-04-29")
    assert result is not None
    assert len(result) == 1
