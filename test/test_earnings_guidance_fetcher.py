import pytest
import pandas as pd
import types
from unittest.mock import patch
from quant_ex.data.fetchers.earnings_guidance_fetcher import EarningsGuidanceFetcher


@pytest.fixture
def fetcher(tmp_path):
    return EarningsGuidanceFetcher(
        cache_dir=str(tmp_path / "earnings_guidance"), cache_ttl_days=30
    )


def _fake_yjyg_raw():
    """Return a fake raw DataFrame mimicking akshare stock_yjyg_em output."""
    return pd.DataFrame({
        "序号": [1, 2, 3],
        "股票代码": ["600519", "000001", "920001"],
        "股票简称": ["贵州茅台", "平安银行", "BJ示例"],
        "预测指标": ["净利润", "净利润", "净利润"],
        "业绩变动": ["增长", "下降", "扭亏"],
        "预测数值": [500.0, 200.0, 50.0],
        "业绩变动幅度": [50.0, -20.0, None],
        "业绩变动原因": ["销量增长", "息差收窄", "成本控制"],
        "预告类型": ["预增", "预减", "扭亏"],
        "上年同期值": [333.3, 250.0, -30.0],
        "公告日期": ["2025-04-15", "2025-04-14", "2025-04-16"],
    })


def test_fetch_quarter_returns_multiindex(fetcher, tmp_path):
    """_fetch_quarter should return a DataFrame with (instrument, datetime) MultiIndex."""
    fake_raw = _fake_yjyg_raw()
    mock_ak = types.SimpleNamespace(stock_yjyg_em=lambda date: fake_raw)

    with patch.dict("sys.modules", {"akshare": mock_ak}):
        result = fetcher._fetch_quarter("20250331")

    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    assert "guidance_type_raw" in result.columns
    assert "earnings_change_pct" in result.columns


def test_fetch_quarter_caches_result(fetcher, tmp_path):
    """_fetch_quarter should write a CSV cache file."""
    fake_raw = _fake_yjyg_raw()
    mock_ak = types.SimpleNamespace(stock_yjyg_em=lambda date: fake_raw)

    with patch.dict("sys.modules", {"akshare": mock_ak}):
        result = fetcher._fetch_quarter("20250331")

    cache_file = tmp_path / "earnings_guidance" / "yjyg_20250331.csv"
    assert cache_file.exists()
    assert result is not None


def test_normalize_column_mapping(fetcher):
    """_normalize should map Chinese column names to English."""
    raw = _fake_yjyg_raw()
    result = fetcher._normalize(raw, "20250331")

    assert result is not None
    assert "guidance_type_raw" in result.columns
    assert "earnings_change_pct" in result.columns
    assert "prior_value" in result.columns
    assert "forecast_value" in result.columns
    assert "reporting_period" in result.columns


def test_normalize_instrument_format(fetcher):
    """_normalize should produce correct instrument format (SH/SZ/BJ prefix)."""
    raw = _fake_yjyg_raw()
    result = fetcher._normalize(raw, "20250331")

    assert result is not None
    instruments = result.index.get_level_values(0).unique().tolist()
    # 600519 → SH600519, 000001 → SZ000001, 920001 → BJ920001
    assert "SH600519" in instruments
    assert "SZ000001" in instruments
    assert "BJ920001" in instruments


def test_normalize_numeric_parsing(fetcher):
    """_normalize should parse numeric columns correctly."""
    raw = _fake_yjyg_raw()
    result = fetcher._normalize(raw, "20250331")

    assert result is not None
    # Check that earnings_change_pct is numeric
    assert pd.api.types.is_numeric_dtype(result["earnings_change_pct"])
    assert pd.api.types.is_numeric_dtype(result["prior_value"])
    assert pd.api.types.is_numeric_dtype(result["forecast_value"])


def test_normalize_empty_raw_returns_none(fetcher):
    """Empty DataFrame from source should return None."""
    raw = pd.DataFrame()
    result = fetcher._normalize(raw, "20250331")
    assert result is None


def test_normalize_missing_required_columns_returns_none(fetcher):
    """If required columns are missing, _normalize should return None."""
    raw = pd.DataFrame({"序号": [1], "股票简称": ["测试"]})
    result = fetcher._normalize(raw, "20250331")
    assert result is None


def test_recent_quarter_ends(fetcher):
    """_recent_quarter_ends should return the expected number of dates."""
    quarters = fetcher._recent_quarter_ends()
    assert len(quarters) == fetcher.num_quarters
    # All should be in YYYYMMDD format
    for q in quarters:
        assert len(q) == 8
        # Should end with valid quarter suffix
        assert q[-4:] in ("0331", "0630", "0930", "1231")


def test_type_encoding_in_factor(fetcher):
    """Verify that guidance_type_raw values will be mapped correctly by the factor."""
    raw = _fake_yjyg_raw()
    result = fetcher._normalize(raw, "20250331")

    assert result is not None
    # Check that guidance_type_raw contains expected Chinese values
    types = result["guidance_type_raw"].unique().tolist()
    assert "预增" in types
    assert "预减" in types
    assert "扭亏" in types


def test_build_instrument():
    """_build_instrument should map bare codes to correct exchanges."""
    assert EarningsGuidanceFetcher._build_instrument("600519") == "SH600519"
    assert EarningsGuidanceFetcher._build_instrument("000001") == "SZ000001"
    assert EarningsGuidanceFetcher._build_instrument("920001") == "BJ920001"
    assert EarningsGuidanceFetcher._build_instrument("430001") == "BJ430001"
    assert EarningsGuidanceFetcher._build_instrument("830001") == "BJ830001"
    assert EarningsGuidanceFetcher._build_instrument("300001") == "SZ300001"


def test_fetch_quarter_reads_cache(fetcher, tmp_path):
    """If cache is fresh, _fetch_quarter should not call the API."""
    # Write a pre-existing cache file
    fetcher._ensure_cache_dir()
    cached = pd.DataFrame(
        {"guidance_type_raw": ["预增"], "earnings_change_pct": [50.0]},
        index=pd.MultiIndex.from_tuples(
            [("SH600519", pd.Timestamp("2025-04-15"))],
            names=["instrument", "datetime"],
        ),
    )
    cache_file = tmp_path / "earnings_guidance" / "yjyg_20250331.csv"
    cached.to_csv(cache_file)

    # If akshare is imported it will fail, proving we didn't call it
    mock_ak = types.SimpleNamespace(
        stock_yjyg_em=lambda date: (_ for _ in ()).throw(Exception("should not be called"))
    )

    with patch.dict("sys.modules", {"akshare": mock_ak}):
        result = fetcher._fetch_quarter("20250331")

    assert result is not None
    assert result["guidance_type_raw"].iloc[0] == "预增"
