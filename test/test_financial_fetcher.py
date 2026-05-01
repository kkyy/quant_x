import pytest
import pandas as pd
from unittest.mock import patch
from quant_ex.data.fetchers.financial_fetcher import FinancialFetcher


@pytest.fixture
def fetcher(tmp_path):
    return FinancialFetcher(cache_dir=str(tmp_path / "financial"), cache_ttl_days=7)


def test_fetch_indicators_returns_multiindex(fetcher, tmp_path):
    fake_df = pd.DataFrame({
        "日期": ["2025-03-31", "2025-06-30"],
        "净资产收益率(%)": [12.5, 13.0],
        "总资产利润率(%)": [5.2, 5.5],
        "销售毛利率(%)": [40.0, 41.0],
        "销售净利率(%)": [15.0, 15.5],
        "总资产周转率(次)": [0.5, 0.6],
        "主营业务收入增长率(%)": [10.0, 12.0],
        "净利润增长率(%)": [8.0, 9.0],
        "经营现金净流量与净利润的比率(%)": [80.0, 85.0],
    })
    with patch("quant_ex.data.fetchers.financial_fetcher.FinancialFetcher._call_akshare_sina", return_value=fake_df):
        result = fetcher._fetch_indicators("SH600519")
    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    assert "roe" in result.columns
    assert "gross_margin" in result.columns
    assert "asset_turnover" in result.columns


def test_fetch_indicators_caches_result(fetcher, tmp_path):
    fake_df = pd.DataFrame({
        "日期": ["2025-03-31"],
        "净资产收益率(%)": [12.5],
        "销售毛利率(%)": [40.0],
    })
    with patch("quant_ex.data.fetchers.financial_fetcher.FinancialFetcher._call_akshare_sina", return_value=fake_df):
        fetcher._fetch_indicators("SH600519")
    cache_file = tmp_path / "financial" / "SH600519.csv"
    assert cache_file.exists()


def test_fetch_indicators_reads_cache(fetcher, tmp_path):
    cached = pd.DataFrame({
        "roe": [12.5],
        "gross_margin": [40.0],
    }, index=pd.MultiIndex.from_tuples(
        [("SH600519", pd.Timestamp("2025-03-31"))],
        names=["instrument", "datetime"],
    ))
    fetcher._ensure_cache_dir()
    cached.to_csv(tmp_path / "financial" / "SH600519.csv")

    with patch("quant_ex.data.fetchers.financial_fetcher.FinancialFetcher._call_akshare_sina") as mock_api:
        result = fetcher._fetch_indicators("SH600519")
    mock_api.assert_not_called()
    assert result is not None


def test_normalize_indicators_column_mapping(fetcher):
    raw = pd.DataFrame({
        "日期": ["2025-03-31"],
        "净资产收益率(%)": [12.5],
        "总资产利润率(%)": [5.2],
        "销售毛利率(%)": [40.0],
        "销售净利率(%)": [15.0],
        "存货周转率(次)": [2.5],
        "主营业务收入增长率(%)": [10.0],
        "净利润增长率(%)": [8.0],
        "经营现金净流量与净利润的比率(%)": [80.0],
    })
    result = fetcher._normalize_indicators(raw, "SH600519")
    assert "roe" in result.columns
    assert "roa" in result.columns
    assert "revenue_growth" in result.columns
    assert "ocf_to_np" in result.columns
    assert "inventory_turnover" in result.columns


def test_merge_indicator_frames_uses_em_to_fill_missing_fields(fetcher):
    primary = pd.DataFrame(
        {
            "gross_margin": [pd.NA],
            "net_margin": [15.0],
        },
        index=pd.MultiIndex.from_tuples(
            [("SH600519", pd.Timestamp("2025-03-31"))],
            names=["instrument", "datetime"],
        ),
    )
    fallback = pd.DataFrame(
        {
            "gross_margin": [40.0],
            "asset_turnover": [0.7],
        },
        index=primary.index,
    )

    result = fetcher._merge_indicator_frames(primary, fallback)

    assert result.loc[("SH600519", pd.Timestamp("2025-03-31")), "gross_margin"] == 40.0
    assert result.loc[("SH600519", pd.Timestamp("2025-03-31")), "asset_turnover"] == 0.7


def test_fallback_to_em(fetcher, tmp_path):
    with patch("quant_ex.data.fetchers.financial_fetcher.FinancialFetcher._call_akshare_sina", side_effect=Exception("sina error")), \
         patch("quant_ex.data.fetchers.financial_fetcher.FinancialFetcher._call_akshare_em", return_value=None):
        result = fetcher._fetch_indicators("SH600519")
    assert result is None or isinstance(result, pd.DataFrame)


def test_compute_free_cash_flow(fetcher):
    cf_df = pd.DataFrame({
        "日期": ["2025-03-31"],
        "经营活动产生的现金流量净额": [100.0],
        "购建固定资产无形资产和其他长期资产支付的现金": [30.0],
    })
    result = fetcher._compute_fcf(cf_df)
    assert result == 70.0  # 100 - 30
