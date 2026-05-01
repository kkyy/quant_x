import pytest
import pandas as pd
from unittest.mock import patch
from quant_ex.data.fetchers.valuation_fetcher import ValuationFetcher


@pytest.fixture
def fetcher(tmp_path):
    return ValuationFetcher(cache_dir=str(tmp_path / "valuation"), cache_ttl_days=7)


def test_fetch_one_returns_multiindex(fetcher, tmp_path):
    fake_df = pd.DataFrame({
        "数据日期": ["2025-03-31", "2025-04-01"],
        "当日收盘价": [1800.0, 1810.0],
        "当日涨跌幅": [0.5, 0.55],
        "总市值": [2.26e12, 2.27e12],
        "流通市值": [2.26e12, 2.27e12],
        "总股本": [1.256e9, 1.256e9],
        "流通股本": [1.256e9, 1.256e9],
        "PE(TTM)": [25.3, 25.5],
        "PE(静)": [24.1, 24.3],
        "市净率": [8.5, 8.6],
        "PEG值": [1.2, 1.3],
        "市现率": [15.2, 15.3],
        "市销率": [12.1, 12.2],
    })
    with patch.object(ValuationFetcher, "_call_stock_value_em", return_value=fake_df):
        result = fetcher._fetch_one("SH600519")
    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    assert "market_cap" in result.columns
    assert "pe_ttm" in result.columns
    assert "pb" in result.columns
    assert "peg" in result.columns
    assert "pcf" in result.columns
    assert "ps_ttm" in result.columns
    assert "float_market_cap" in result.columns
    assert "total_shares" in result.columns
    assert "float_shares" in result.columns


def test_fetch_one_caches_result(fetcher, tmp_path):
    fake_df = pd.DataFrame({
        "数据日期": ["2025-03-31"],
        "总市值": [2.26e12],
        "流通市值": [2.26e12],
        "总股本": [1.256e9],
        "流通股本": [1.256e9],
        "PE(TTM)": [25.3],
        "PE(静)": [24.1],
        "市净率": [8.5],
        "PEG值": [1.2],
        "市现率": [15.2],
        "市销率": [12.1],
    })
    with patch.object(ValuationFetcher, "_call_stock_value_em", return_value=fake_df):
        fetcher._fetch_one("SH600519")
    cache_file = tmp_path / "valuation" / "SH600519.csv"
    assert cache_file.exists()


def test_fetch_one_reads_cache(fetcher, tmp_path):
    cached = pd.DataFrame({
        "market_cap": [2.26e12],
        "pe_ttm": [25.3],
        "pb": [8.5],
    }, index=pd.MultiIndex.from_tuples(
        [("SH600519", pd.Timestamp("2025-03-31"))],
        names=["instrument", "datetime"],
    ))
    fetcher._ensure_cache_dir()
    cached.to_csv(tmp_path / "valuation" / "SH600519.csv")

    with patch.object(ValuationFetcher, "_call_stock_value_em") as mock_api:
        result = fetcher._fetch_one("SH600519")
    mock_api.assert_not_called()
    assert result is not None
    assert "market_cap" in result.columns


def test_normalize_valuation_column_mapping(fetcher):
    raw = pd.DataFrame({
        "数据日期": ["2025-03-31"],
        "总市值": [2.26e12],
        "流通市值": [2.26e12],
        "总股本": [1.256e9],
        "流通股本": [1.256e9],
        "PE(TTM)": [25.3],
        "PE(静)": [24.1],
        "市净率": [8.5],
        "PEG值": [1.2],
        "市现率": [15.2],
        "市销率": [12.1],
    })
    result = fetcher._normalize_stock_value_em(raw, "SH600519")
    assert result is not None
    # Verify Chinese → English column mapping
    assert "market_cap" in result.columns
    assert "float_market_cap" in result.columns
    assert "total_shares" in result.columns
    assert "float_shares" in result.columns
    assert "pe_ttm" in result.columns
    assert "pe_static" in result.columns
    assert "pb" in result.columns
    assert "peg" in result.columns
    assert "pcf" in result.columns
    assert "ps_ttm" in result.columns
    # Original Chinese column names should NOT be present
    assert "总市值" not in result.columns
    assert "市净率" not in result.columns


def test_fallback_lg_indicator(fetcher, tmp_path):
    fake_lg_df = pd.DataFrame({
        "date": ["2025-03-31", "2025-04-01"],
        "pe": [25.3, 25.5],
        "pb": [8.5, 8.6],
        "ps": [12.1, 12.2],
        "dyr": [1.5, 1.5],
        "总市值": [2.26e12, 2.27e12],
    })
    with patch.object(ValuationFetcher, "_call_stock_value_em", side_effect=Exception("primary error")), \
         patch.object(ValuationFetcher, "_call_stock_a_lg_indicator", return_value=fake_lg_df):
        result = fetcher._fetch_one("SH600519")
    assert result is not None
    assert "pe_ttm" in result.columns
    assert "pb" in result.columns
    assert "ps_ttm" in result.columns
    assert "dyr" in result.columns
    assert "market_cap" in result.columns


def test_merge_valuation_frames_fills_gaps(fetcher):
    primary = pd.DataFrame(
        {
            "market_cap": [2.26e12],
            "pe_ttm": [25.3],
            "pb": [8.5],
        },
        index=pd.MultiIndex.from_tuples(
            [("SH600519", pd.Timestamp("2025-03-31"))],
            names=["instrument", "datetime"],
        ),
    )
    fallback = pd.DataFrame(
        {
            "pe_ttm": [25.0],  # should NOT override primary
            "pb": [8.0],       # should NOT override primary
            "dyr": [1.5],      # should be added
        },
        index=primary.index,
    )

    result = ValuationFetcher._merge_valuation_frames(primary, fallback)

    # Primary values win for shared columns
    assert result.loc[("SH600519", pd.Timestamp("2025-03-31")), "pe_ttm"] == 25.3
    assert result.loc[("SH600519", pd.Timestamp("2025-03-31")), "pb"] == 8.5
    # Fallback-only column is added
    assert result.loc[("SH600519", pd.Timestamp("2025-03-31")), "dyr"] == 1.5


def test_load_cached_range(fetcher, tmp_path):
    """Load and filter cached per-stock files by date range."""
    fetcher._ensure_cache_dir()

    df1 = pd.DataFrame({
        "market_cap": [2.26e12, 2.27e12],
        "pe_ttm": [25.3, 25.5],
    }, index=pd.MultiIndex.from_tuples(
        [("SH600519", pd.Timestamp("2025-03-30")),
         ("SH600519", pd.Timestamp("2025-03-31"))],
        names=["instrument", "datetime"],
    ))
    df2 = pd.DataFrame({
        "market_cap": [5.0e10],
        "pe_ttm": [6.5],
    }, index=pd.MultiIndex.from_tuples(
        [("SZ000001", pd.Timestamp("2025-03-31"))],
        names=["instrument", "datetime"],
    ))
    df1.to_csv(tmp_path / "valuation" / "SH600519.csv")
    df2.to_csv(tmp_path / "valuation" / "SZ000001.csv")

    result = fetcher._load_cached_range(
        ["SH600519", "SZ000001"], "2025-03-31", "2025-03-31"
    )
    assert result is not None
    assert len(result) == 2
