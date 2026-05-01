import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from quant_ex.data.fetchers.analyst_fetcher import AnalystForecastFetcher


@pytest.fixture
def fetcher(tmp_path):
    return AnalystForecastFetcher(cache_dir=str(tmp_path / "analyst"), cache_ttl_days=1)


def _make_em_raw(n=3):
    """Build a fake EM bulk forecast response."""
    codes = ["600519", "000001", "300750"][:n]
    names = ["贵州茅台", "平安银行", "宁德时代"][:n]
    data = {
        "代码": codes,
        "名称": names,
        "研报数": [30, 15, 25][:n],
        "机构投资评级-买入": [10, 5, 8][:n],
        "机构投资评级-增持": [8, 4, 7][:n],
        "机构投资评级-中性": [7, 3, 5][:n],
        "机构投资评级-减持": [3, 2, 3][:n],
        "机构投资评级-卖出": [2, 1, 2][:n],
        "2025预测每股收益": [55.0, 1.5, 12.0][:n],
        "2026预测每股收益": [60.0, 1.7, 14.0][:n],
        "2027预测每股收益": [65.0, 1.9, 16.0][:n],
    }
    return pd.DataFrame(data)


def test_fetch_forecast_returns_multiindex(fetcher, tmp_path):
    """Normalized forecast should have (instrument, datetime) MultiIndex."""
    fake_raw = _make_em_raw()
    with patch.object(fetcher, "_call_akshare_em", return_value=fake_raw):
        result = fetcher._fetch_forecast("20260430")
    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    assert "report_count" in result.columns
    assert "buy_rating" in result.columns
    assert "consensus_eps_forecast" in result.columns


def test_fetch_forecast_caches_result(fetcher, tmp_path):
    """After fetching, a cache file should exist on disk."""
    fake_raw = _make_em_raw(n=1)
    with patch.object(fetcher, "_call_akshare_em", return_value=fake_raw):
        fetcher._fetch_forecast("20260430")
    cache_file = tmp_path / "analyst" / "forecast_20260430.csv"
    assert cache_file.exists()


def test_fetch_forecast_reads_cache(fetcher, tmp_path):
    """If a fresh cache exists, the API should not be called."""
    cached = pd.DataFrame(
        {
            "report_count": [30],
            "buy_rating": [10],
            "outperform_rating": [8],
            "neutral_rating": [7],
            "underperform_rating": [3],
            "sell_rating": [2],
            "current_eps_forecast": [55.0],
            "consensus_eps_forecast": [60.0],
        },
        index=pd.MultiIndex.from_tuples(
            [("SH600519", pd.Timestamp("2026-04-30"))],
            names=["instrument", "datetime"],
        ),
    )
    fetcher._ensure_cache_dir()
    cached.to_csv(tmp_path / "analyst" / "forecast_20260430.csv")

    with patch.object(fetcher, "_call_akshare_em") as mock_api:
        result = fetcher._fetch_forecast("20260430")
    mock_api.assert_not_called()
    assert result is not None
    assert result.loc["SH600519", "report_count"].iloc[0] == 30


def test_normalize_forecast_column_mapping(fetcher, tmp_path):
    """Chinese column names should be renamed to English equivalents."""
    fake_raw = _make_em_raw(n=1)
    with patch.object(fetcher, "_call_akshare_em", return_value=fake_raw):
        result = fetcher._fetch_forecast("20260430")
    assert result is not None

    # Rating columns should be present with English names
    assert "buy_rating" in result.columns
    assert "outperform_rating" in result.columns
    assert "neutral_rating" in result.columns
    assert "underperform_rating" in result.columns
    assert "sell_rating" in result.columns
    assert "report_count" in result.columns

    # No Chinese column names should remain
    for col in result.columns:
        assert not any(ord(c) > 0x4E00 for c in col), f"Chinese column name found: {col}"


def test_instrument_code_mapping(fetcher, tmp_path):
    """Bare stock codes should be mapped to qlib instrument format."""
    fake_raw = _make_em_raw()
    with patch.object(fetcher, "_call_akshare_em", return_value=fake_raw):
        result = fetcher._fetch_forecast("20260430")
    assert result is not None
    instruments = result.index.get_level_values(0).tolist()
    assert "SH600519" in instruments
    assert "SZ000001" in instruments
    assert "SZ300750" in instruments


def test_eps_forecast_columns_populated(fetcher, tmp_path):
    """current_eps_forecast and consensus_eps_forecast should be numeric."""
    fake_raw = _make_em_raw(n=1)
    with patch.object(fetcher, "_call_akshare_em", return_value=fake_raw):
        result = fetcher._fetch_forecast("20260430")
    assert result is not None
    assert "current_eps_forecast" in result.columns
    assert "consensus_eps_forecast" in result.columns
    # Values should be numeric (not NaN for valid input)
    cur = result["current_eps_forecast"].iloc[0]
    fwd = result["consensus_eps_forecast"].iloc[0]
    assert pd.notna(cur)
    assert pd.notna(fwd)


def test_fallback_to_ths(fetcher, tmp_path):
    """When EM source fails, THS fallback should be attempted."""
    with patch.object(fetcher, "_call_akshare_em", side_effect=Exception("EM error")), \
         patch.object(fetcher, "_call_akshare_ths_bulk", return_value=None):
        result = fetcher._fetch_forecast("20260430")
    # THS returns None → _fetch_forecast_with_fallback returns None
    # but _fetch_forecast does not write empty cache
    assert result is None


def test_fetch_method_loads_cached_range(fetcher, tmp_path):
    """fetch() should refresh cache then load within date range."""
    cached = pd.DataFrame(
        {
            "report_count": [20],
            "buy_rating": [8],
            "outperform_rating": [5],
            "neutral_rating": [4],
            "underperform_rating": [2],
            "sell_rating": [1],
            "current_eps_forecast": [2.0],
            "consensus_eps_forecast": [2.3],
        },
        index=pd.MultiIndex.from_tuples(
            [("SZ000001", pd.Timestamp("2026-04-29"))],
            names=["instrument", "datetime"],
        ),
    )
    fetcher._ensure_cache_dir()
    cached.to_csv(tmp_path / "analyst" / "forecast_20260429.csv")

    with patch.object(fetcher, "refresh_cache"):
        result = fetcher.fetch(["SZ000001"], "2026-04-01", "2026-04-30")
    assert result is not None
    assert len(result) > 0
