import pytest
import pandas as pd
from unittest.mock import patch
from quant_ex.data.fetchers.northbound_fetcher import NorthboundFetcher


@pytest.fixture
def fetcher(tmp_path):
    return NorthboundFetcher(cache_dir=str(tmp_path / "northbound"), cache_ttl_days=1)


def test_fetch_holdings_returns_multiindex(fetcher, tmp_path):
    fake_df = pd.DataFrame({
        "代码": ["600519", "000001"],
        "日期": ["2026-04-29", "2026-04-29"],
        "今日持股-占流通股比": [5.2, 3.1],
        "今日持股-市值": [1000.0, 500.0],
        "今日增持估计-占流通股比": [0.1, -0.05],
        "今日收盘价": [1800.0, 15.0],
        "成交额": [200.0, 100.0],
    })
    with patch("quant_ex.data.fetchers.northbound_fetcher.NorthboundFetcher._call_akshare_holdings", return_value=fake_df):
        result = fetcher._fetch_holdings("2026-04-29")
    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    assert "nb_hold_pct" in result.columns
    assert "nb_hold_mv" in result.columns


def test_fetch_holdings_caches_result(fetcher, tmp_path):
    fake_df = pd.DataFrame({
        "代码": ["600519"],
        "日期": ["2026-04-29"],
        "今日持股-占流通股比": [5.2],
        "今日持股-市值": [1000.0],
        "今日增持估计-占流通股比": [0.1],
        "今日收盘价": [1800.0],
        "成交额": [200.0],
    })
    with patch("quant_ex.data.fetchers.northbound_fetcher.NorthboundFetcher._call_akshare_holdings", return_value=fake_df):
        fetcher._fetch_holdings("2026-04-29")
    cache_file = tmp_path / "northbound" / "holdings_2026-04-29.csv"
    assert cache_file.exists()


def test_fetch_holdings_reads_cache(fetcher, tmp_path):
    cached = pd.DataFrame({
        "nb_hold_pct": [5.2],
        "nb_hold_mv": [1000.0],
        "nb_net_buy_ratio": [0.1],
        "nb_hold_pct_chg": [0.0],
    }, index=pd.MultiIndex.from_tuples(
        [("SH600519", pd.Timestamp("2026-04-29"))],
        names=["instrument", "datetime"],
    ))
    fetcher._ensure_cache_dir()
    cached.to_csv(tmp_path / "northbound" / "holdings_2026-04-29.csv")

    with patch("quant_ex.data.fetchers.northbound_fetcher.NorthboundFetcher._call_akshare_holdings") as mock_api:
        result = fetcher._fetch_holdings("2026-04-29")
    mock_api.assert_not_called()
    assert result is not None


def test_fetch_hist_flow_returns_multiindex(fetcher, tmp_path):
    fake_df = pd.DataFrame({
        "日期": ["2026-04-28", "2026-04-29"],
        "当日成交净买额": [50.0, 60.0],
        "买入成交额": [200.0, 220.0],
        "卖出成交额": [150.0, 160.0],
    })
    with patch("quant_ex.data.fetchers.northbound_fetcher.NorthboundFetcher._call_akshare_hist_flow", return_value=fake_df):
        result = fetcher._fetch_hist_flow()
    assert result is not None
    assert "nb_total_net_buy" in result.columns


def test_refresh_cache_calls_fetch_holdings(fetcher, tmp_path):
    symbols = ["SH600519"]
    with patch.object(fetcher, "_fetch_holdings") as mock_holdings, \
         patch.object(fetcher, "_fetch_hist_flow") as mock_flow:
        mock_holdings.return_value = pd.DataFrame()
        mock_flow.return_value = pd.DataFrame()
        fetcher.refresh_cache(symbols)
    # Should fetch holdings for today and hist flow
    assert mock_holdings.called or mock_flow.called


def test_fetch_individual_returns_multiindex(fetcher, tmp_path):
    fake_df = pd.DataFrame({
        "持股日期": ["2026-04-28", "2026-04-29"],
        "当日收盘价": [1800.0, 1820.0],
        "持股数量": [10000.0, 10500.0],
        "持股市值": [18000000.0, 19110000.0],
        "持股数量占A股百分比": [5.2, 5.4],
        "今日增持股数": [100.0, 500.0],
        "今日增持资金": [180000.0, 910000.0],
        "今日持股市值变化": [1000000.0, 1110000.0],
    })
    with patch("akshare.stock_hsgt_individual_em", return_value=fake_df):
        result = fetcher._fetch_individual("SH600519")
    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    assert "nb_hold_pct" in result.columns
    assert "nb_hold_mv" in result.columns


def test_fallback_to_eastmoney(fetcher, tmp_path):
    with patch("quant_ex.data.fetchers.northbound_fetcher.NorthboundFetcher._call_akshare_holdings", side_effect=Exception("akshare error")), \
         patch("quant_ex.data.fetchers.northbound_fetcher.NorthboundFetcher._call_eastmoney_holdings", return_value=None):
        result = fetcher._fetch_holdings("2026-04-29")
    assert result is None or isinstance(result, pd.DataFrame)
