"""Tests for InsiderTradeFetcher."""
import pytest
import pandas as pd
from unittest.mock import patch
from quant_ex.data.fetchers.insider_fetcher import InsiderTradeFetcher


@pytest.fixture
def fetcher(tmp_path):
    return InsiderTradeFetcher(cache_dir=str(tmp_path / "insider"), cache_ttl_days=1)


def _make_bulk_raw(n=4):
    """Build a fake akshare bulk response (transaction-level)."""
    return pd.DataFrame({
        "代码": ["600519", "000001", "920001", "300001"][:n],
        "名称": ["贵州茅台", "平安银行", "北交测试", "特锐德"][:n],
        "股东名称": ["股东A", "股东B", "股东C", "股东D"][:n],
        "持股变动信息-增减": ["增持", "减持", "增持", "减持"][:n],
        "持股变动信息-变动数量": [10000, -5000, 2000, -3000][:n],
        "持股变动信息-占总股本比例": [0.08, 0.025, 0.01, 0.015][:n],
        "持股变动信息-占流通股比例": [0.10, 0.03, 0.015, 0.02][:n],
        "变动后持股情况-持股总数": [50000, 30000, 10000, 20000][:n],
        "变动后持股情况-占总股本比例": [0.40, 0.15, 0.05, 0.10][:n],
        "变动后持股情况-持流通股数": [45000, 25000, 8000, 15000][:n],
        "变动后持股情况-占流通股比例": [0.36, 0.125, 0.04, 0.075][:n],
        "变动开始日": [
            "2026-04-25",
            "2026-04-26",
            "2026-04-27",
            "2026-04-28",
        ][:n],
        "变动截止日": [
            "2026-04-25",
            "2026-04-26",
            "2026-04-27",
            "2026-04-28",
        ][:n],
        "公告日": [
            "2026-04-26",
            "2026-04-27",
            "2026-04-28",
            "2026-04-29",
        ][:n],
    })


def test_fetch_insider_returns_multiindex(fetcher):
    """Normalized output should have (instrument, datetime) MultiIndex."""
    fake_raw = _make_bulk_raw()
    with patch.object(fetcher, "_call_akshare_bulk", return_value=fake_raw):
        result = fetcher._fetch_insider("20260429")
    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    # Verify instruments are in qlib format
    instruments = result.index.get_level_values(0).unique().tolist()
    assert "SH600519" in instruments
    assert "SZ000001" in instruments
    assert "BJ920001" in instruments
    assert "SZ300001" in instruments


def test_fetch_insider_caches_result(fetcher, tmp_path):
    """After fetch, a CSV file should exist in cache dir."""
    fake_raw = _make_bulk_raw(n=1)
    with patch.object(fetcher, "_call_akshare_bulk", return_value=fake_raw):
        fetcher._fetch_insider("20260429")
    cache_file = tmp_path / "insider" / "insider_20260429.csv"
    assert cache_file.exists()


def test_fetch_insider_reads_cache(fetcher, tmp_path):
    """If cache exists and is fresh, API should not be called."""
    cached = pd.DataFrame(
        {
            "direction": [1],
            "shares_changed": [10000.0],
            "pct_of_total": [0.08],
            "pct_of_float": [0.10],
        },
        index=pd.MultiIndex.from_tuples(
            [("SH600519", pd.Timestamp("2026-04-25"))],
            names=["instrument", "datetime"],
        ),
    )
    fetcher._ensure_cache_dir()
    cached.to_csv(tmp_path / "insider" / "insider_20260429.csv")

    with patch.object(
        fetcher, "_call_akshare_bulk"
    ) as mock_api:
        result = fetcher._fetch_insider("20260429")
    mock_api.assert_not_called()
    assert result is not None


def test_normalize_insider_column_mapping(fetcher):
    """Verify Chinese→English column mapping and direction encoding."""
    fake_raw = _make_bulk_raw()
    with patch.object(fetcher, "_call_akshare_bulk", return_value=fake_raw):
        result = fetcher._fetch_insider("20260429")
    assert result is not None

    # Direction encoding: 增持 → 1, 减持 → -1
    sh600519 = result.loc["SH600519"]
    assert (sh600519["direction"] == 1).all()

    sz000001 = result.loc["SZ000001"]
    assert (sz000001["direction"] == -1).all()

    bj920001 = result.loc["BJ920001"]
    assert (bj920001["direction"] == 1).all()

    # Verify key columns exist
    assert "shares_changed" in result.columns
    assert "pct_of_total" in result.columns
    assert "pct_of_float" in result.columns
    assert "shareholder" in result.columns


def test_fallback_separate_buy_sell(fetcher):
    """When bulk fetch fails, should fall back to separate 增持/减持 calls."""
    buy_raw = pd.DataFrame({
        "代码": ["600519"],
        "名称": ["贵州茅台"],
        "股东名称": ["股东A"],
        "持股变动信息-增减": ["增持"],
        "持股变动信息-变动数量": [10000],
        "持股变动信息-占总股本比例": [0.08],
        "持股变动信息-占流通股比例": [0.10],
        "变动后持股情况-持股总数": [50000],
        "变动后持股情况-占总股本比例": [0.40],
        "变动后持股情况-持流通股数": [45000],
        "变动后持股情况-占流通股比例": [0.36],
        "变动开始日": ["2026-04-25"],
        "变动截止日": ["2026-04-25"],
        "公告日": ["2026-04-26"],
    })

    sell_raw = pd.DataFrame({
        "代码": ["000001"],
        "名称": ["平安银行"],
        "股东名称": ["股东B"],
        "持股变动信息-增减": ["减持"],
        "持股变动信息-变动数量": [-5000],
        "持股变动信息-占总股本比例": [0.025],
        "持股变动信息-占流通股比例": [0.03],
        "变动后持股情况-持股总数": [30000],
        "变动后持股情况-占总股本比例": [0.15],
        "变动后持股情况-持流通股数": [25000],
        "变动后持股情况-占流通股比例": [0.125],
        "变动开始日": ["2026-04-26"],
        "变动截止日": ["2026-04-26"],
        "公告日": ["2026-04-27"],
    })

    with patch.object(
        fetcher, "_call_akshare_bulk", side_effect=Exception("bulk API error")
    ), patch("akshare.stock_ggcg_em", side_effect=[buy_raw, sell_raw]):
        result = fetcher._fetch_insider("20260429")

    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    # Should have both buy and sell entries
    instruments = result.index.get_level_values(0).unique().tolist()
    assert "SH600519" in instruments
    assert "SZ000001" in instruments

    # Verify direction encoding
    assert result.loc["SH600519", "direction"].iloc[0] == 1
    assert result.loc["SZ000001", "direction"].iloc[0] == -1


def test_bj_exchange_inference(fetcher):
    """BJ exchange codes (920xxx, 4xx, 8xx) should be correctly mapped."""
    assert fetcher._code_to_instrument("920001") == "BJ920001"
    assert fetcher._code_to_instrument("430001") == "BJ430001"
    assert fetcher._code_to_instrument("830001") == "BJ830001"
    assert fetcher._code_to_instrument("600519") == "SH600519"
    assert fetcher._code_to_instrument("000001") == "SZ000001"
    assert fetcher._code_to_instrument("300001") == "SZ300001"


def test_refresh_cache_ignores_symbols(fetcher):
    """refresh_cache should call _fetch_insider; symbols param is ignored."""
    with patch.object(fetcher, "_fetch_insider") as mock_fetch:
        mock_fetch.return_value = pd.DataFrame()
        fetcher.refresh_cache(["SH600519", "SZ000001"])
    mock_fetch.assert_called_once()
