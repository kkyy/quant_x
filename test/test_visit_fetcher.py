"""Tests for InstitutionalVisitFetcher."""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from quant_ex.data.fetchers.visit_fetcher import InstitutionalVisitFetcher


@pytest.fixture
def fetcher(tmp_path):
    return InstitutionalVisitFetcher(
        cache_dir=str(tmp_path / "visit"), cache_ttl_days=1
    )


def _make_bulk_raw(n=3):
    """Build a fake EM bulk visit statistics response.

    Simulates the output of ak.stock_jgdy_tj_em().
    """
    codes = ["600519", "000001", "300750"][:n]
    names = ["贵州茅台", "平安银行", "宁德时代"][:n]
    data = {
        "序号": list(range(1, n + 1)),
        "代码": codes,
        "名称": names,
        "最新价": [1800.0, 12.0, 200.0][:n],
        "涨跌幅": [1.5, -0.3, 2.1][:n],
        "接待机构数量": [50, 20, 35][:n],
        "接待方式": ["线上会议", "线下调研", "电话会议"][:n],
        "接待人员": ["董事长", "董秘", "总经理"][:n],
        "接待地点": ["贵阳", "深圳", "宁德"][:n],
        "接待日期": ["2026-04-28", "2026-04-28", "2026-04-29"][:n],
        "公告日期": ["2026-04-29", "2026-04-29", "2026-04-30"][:n],
    }
    return pd.DataFrame(data)


def test_fetch_visits_returns_multiindex(fetcher, tmp_path):
    """Normalized visit data should have (instrument, datetime) MultiIndex."""
    fake_raw = _make_bulk_raw()
    with patch.object(fetcher, "_call_akshare_em_bulk", return_value=fake_raw):
        result = fetcher._fetch_visits("20260401")
    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    assert "visitor_count" in result.columns
    assert "announcement_date" in result.columns
    assert "visit_method" in result.columns


def test_fetch_visits_caches_result(fetcher, tmp_path):
    """After fetching, a cache file should exist on disk."""
    fake_raw = _make_bulk_raw(n=1)
    with patch.object(fetcher, "_call_akshare_em_bulk", return_value=fake_raw):
        fetcher._fetch_visits("20260401")
    # Cache file is named visits_{YYYYMMDD}.csv where YYYYMMDD is today
    from datetime import date
    today_str = date.today().strftime("%Y%m%d")
    cache_file = tmp_path / "visit" / f"visits_{today_str}.csv"
    assert cache_file.exists()


def test_fetch_visits_reads_cache(fetcher, tmp_path):
    """If a fresh cache exists, the API should not be called."""
    cached = pd.DataFrame(
        {
            "visitor_count": [50],
            "announcement_date": [pd.Timestamp("2026-04-29")],
            "visit_method": ["线上会议"],
        },
        index=pd.MultiIndex.from_tuples(
            [("SH600519", pd.Timestamp("2026-04-28"))],
            names=["instrument", "datetime"],
        ),
    )
    fetcher._ensure_cache_dir()
    from datetime import date
    today_str = date.today().strftime("%Y%m%d")
    cached.to_csv(tmp_path / "visit" / f"visits_{today_str}.csv")

    with patch.object(fetcher, "_call_akshare_em_bulk") as mock_api:
        result = fetcher._fetch_visits("20260401")
    mock_api.assert_not_called()
    assert result is not None
    assert result.loc["SH600519", "visitor_count"].iloc[0] == 50


def test_normalize_column_mapping(fetcher, tmp_path):
    """Chinese column names should be normalized to English equivalents."""
    fake_raw = _make_bulk_raw()
    with patch.object(fetcher, "_call_akshare_em_bulk", return_value=fake_raw):
        result = fetcher._fetch_visits("20260401")
    assert result is not None

    # Our English column names should be present
    assert "visitor_count" in result.columns
    assert "announcement_date" in result.columns
    assert "visit_method" in result.columns

    # No Chinese column names should remain in the output columns
    for col in result.columns:
        assert not any(ord(c) > 0x4E00 for c in col), (
            f"Chinese column name found: {col}"
        )


def test_instrument_code_mapping(fetcher, tmp_path):
    """Bare stock codes should be mapped to qlib instrument format."""
    fake_raw = _make_bulk_raw()
    with patch.object(fetcher, "_call_akshare_em_bulk", return_value=fake_raw):
        result = fetcher._fetch_visits("20260401")
    assert result is not None
    instruments = result.index.get_level_values(0).tolist()
    assert "SH600519" in instruments
    assert "SZ000001" in instruments
    assert "SZ300750" in instruments


def test_visitor_count_is_numeric(fetcher, tmp_path):
    """visitor_count should be numeric (int)."""
    fake_raw = _make_bulk_raw(n=1)
    with patch.object(fetcher, "_call_akshare_em_bulk", return_value=fake_raw):
        result = fetcher._fetch_visits("20260401")
    assert result is not None
    assert pd.api.types.is_numeric_dtype(result["visitor_count"])


def test_fallback_to_detail(fetcher, tmp_path):
    """When EM bulk source fails, EM detail fallback should be attempted."""
    detail_raw = pd.DataFrame({
        "代码": ["600519"],
        "调研日期": ["2026-04-28"],
        "接待机构数量": [30],
        "公告日期": ["2026-04-29"],
        "接待方式": ["线下调研"],
    })

    with patch.object(
        fetcher, "_call_akshare_em_bulk", side_effect=Exception("EM bulk error")
    ), patch.object(
        fetcher, "_call_akshare_em_detail", return_value=detail_raw
    ):
        result = fetcher._fetch_visits("20260401")
    assert result is not None
    assert "visitor_count" in result.columns


def test_fetch_method_loads_cached_range(fetcher, tmp_path):
    """fetch() should refresh cache then load within date range."""
    cached = pd.DataFrame(
        {
            "visitor_count": [20],
            "announcement_date": [pd.Timestamp("2026-04-29")],
            "visit_method": ["电话会议"],
        },
        index=pd.MultiIndex.from_tuples(
            [("SZ000001", pd.Timestamp("2026-04-28"))],
            names=["instrument", "datetime"],
        ),
    )
    fetcher._ensure_cache_dir()
    cached.to_csv(tmp_path / "visit" / "visits_20260428.csv")

    with patch.object(fetcher, "refresh_cache"):
        result = fetcher.fetch(["SZ000001"], "2026-04-01", "2026-04-30")
    assert result is not None
    assert len(result) > 0
