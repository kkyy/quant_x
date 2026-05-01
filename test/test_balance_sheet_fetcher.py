import pytest
import pandas as pd
from unittest.mock import patch
from quant_ex.data.fetchers.balance_sheet_fetcher import (
    BalanceSheetFetcher,
    _BS_COL_MAP,
    _KEEP_COLS,
)


@pytest.fixture
def fetcher(tmp_path):
    return BalanceSheetFetcher(
        cache_dir=str(tmp_path / "balance_sheet"), cache_ttl_days=30
    )


def test_fetch_one_returns_multiindex(fetcher, tmp_path):
    """_fetch_one should return a DataFrame with (instrument, datetime) MultiIndex."""
    # Simulate the 319-column EM output (we provide a representative subset)
    fake_df = pd.DataFrame({
        "报告期": ["2025-03-31", "2024-12-31", "2024-09-30"],
        "TOTAL_OPERATE_INCOME": [1e9, 4e9, 3e9],
        "PARENT_NETPROFIT": [2e8, 8e8, 6e8],
        "TOTAL_ASSETS": [1e10, 9.5e9, 9e9],
        "TOTAL_PARENT_EQUITY": [4e9, 3.8e9, 3.6e9],
        "TOTAL_LIABILITIES": [6e9, 5.7e9, 5.4e9],
        "TOTAL_CURRENT_ASSETS": [3e9, 2.8e9, 2.6e9],
        "TOTAL_CURRENT_LIAB": [2e9, 1.9e9, 1.8e9],
        "INVENTORY": [5e8, 4.5e8, 4e8],
        "GOODWILL": [1e8, 1e8, 1e8],
        "MONETARYFUNDS": [2e8, 1.8e8, 1.6e8],
        "SHORT_LOAN": [3e8, 2.5e8, 2e8],
        "LONG_LOAN": [5e8, 4.5e8, 4e8],
        # And ~307 other columns we don't care about
        "RANDOM_OTHER_COL": [1, 2, 3],
    })
    with patch.object(fetcher, "_call_akshare_em", return_value=fake_df):
        result = fetcher._fetch_one("SH600519")
    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    assert "revenue" in result.columns
    assert "total_assets" in result.columns
    assert "total_equity" in result.columns
    # The random column should NOT be present
    assert "RANDOM_OTHER_COL" not in result.columns


def test_fetch_one_caches_result(fetcher, tmp_path):
    """_fetch_one should write a CSV cache file."""
    fake_df = pd.DataFrame({
        "报告期": ["2025-03-31"],
        "TOTAL_ASSETS": [1e10],
        "TOTAL_PARENT_EQUITY": [4e9],
        "TOTAL_LIABILITIES": [6e9],
        "TOTAL_CURRENT_ASSETS": [3e9],
        "TOTAL_CURRENT_LIAB": [2e9],
        "INVENTORY": [5e8],
        "MONETARYFUNDS": [2e8],
        "SHORT_LOAN": [3e8],
        "LONG_LOAN": [5e8],
    })
    with patch.object(fetcher, "_call_akshare_em", return_value=fake_df):
        fetcher._fetch_one("SH600519")
    cache_file = tmp_path / "balance_sheet" / "SH600519.csv"
    assert cache_file.exists()


def test_fetch_one_reads_cache(fetcher, tmp_path):
    """If cache is fresh, _fetch_one should not call the API."""
    cached = pd.DataFrame(
        {
            "revenue": [1e9],
            "net_profit": [2e8],
            "total_assets": [1e10],
            "total_equity": [4e9],
            "total_liabilities": [6e9],
            "current_assets": [3e9],
            "current_liabilities": [2e9],
            "inventory": [5e8],
            "goodwill": [1e8],
            "cash": [2e8],
            "short_term_debt": [3e8],
            "long_term_debt": [5e8],
        },
        index=pd.MultiIndex.from_tuples(
            [("SH600519", pd.Timestamp("2025-03-31"))],
            names=["instrument", "datetime"],
        ),
    )
    fetcher._ensure_cache_dir()
    cached.to_csv(tmp_path / "balance_sheet" / "SH600519.csv")

    with patch.object(fetcher, "_call_akshare_em") as mock_api:
        result = fetcher._fetch_one("SH600519")
    mock_api.assert_not_called()
    assert result is not None
    assert result["total_assets"].iloc[0] == 1e10


def test_normalize_column_mapping(fetcher):
    """_normalize_balance_sheet should filter 319 columns down to ~12 curated ones."""
    # Simulate a wide DataFrame with many columns
    many_cols = {f"COL_{i}": [1.0] for i in range(300)}
    # Include our target columns among the noise
    many_cols["报告期"] = ["2025-03-31"]
    many_cols["TOTAL_OPERATE_INCOME"] = [1e9]
    many_cols["PARENT_NETPROFIT"] = [2e8]
    many_cols["TOTAL_ASSETS"] = [1e10]
    many_cols["TOTAL_PARENT_EQUITY"] = [4e9]
    many_cols["TOTAL_LIABILITIES"] = [6e9]
    many_cols["TOTAL_CURRENT_ASSETS"] = [3e9]
    many_cols["TOTAL_CURRENT_LIAB"] = [2e9]
    many_cols["INVENTORY"] = [5e8]
    many_cols["GOODWILL"] = [1e8]
    many_cols["MONETARYFUNDS"] = [2e8]
    many_cols["SHORT_LOAN"] = [3e8]
    many_cols["LONG_LOAN"] = [5e8]

    raw = pd.DataFrame(many_cols)
    result = fetcher._normalize_balance_sheet(raw, "SH600519")
    assert result is not None
    # Should have exactly the curated columns, not 313
    assert set(result.columns) == set(_KEEP_COLS)
    # Should have exactly 1 row
    assert len(result) == 1


def test_em_code_format(fetcher):
    """build_em_code should produce correct code.exchange format."""
    assert fetcher.build_em_code("SH600519") == "600519.SH"
    assert fetcher.build_em_code("SZ000001") == "000001.SZ"
    assert fetcher.build_em_code("BJ920000") == "920000.BJ"
    assert fetcher.build_em_code("SH601318") == "601318.SH"
    assert fetcher.build_em_code("SZ300750") == "300750.SZ"
    assert fetcher.build_em_code("BJ430047") == "430047.BJ"


def test_fallback_cross_section(fetcher, tmp_path):
    """If primary EM source fails, should fall back to cross-sectional API."""
    fake_cs_df = pd.DataFrame({
        "股票代码": ["600519", "000001"],
        "报告期": ["2025-03-31", "2025-03-31"],
        "TOTAL_ASSETS": [1e10, 5e9],
        "TOTAL_PARENT_EQUITY": [4e9, 2e9],
        "TOTAL_LIABILITIES": [6e9, 3e9],
    })
    with patch.object(
        fetcher, "_call_akshare_em", side_effect=Exception("primary failed")
    ), patch.object(
        fetcher, "_call_akshare_cross_section", return_value=fake_cs_df
    ):
        result = fetcher._fetch_one("SH600519")
    assert result is not None
    assert result["total_assets"].iloc[0] == 1e10


def test_both_sources_fail_returns_none(fetcher, tmp_path):
    """If both primary and fallback sources fail, _fetch_one should return None."""
    with patch.object(
        fetcher, "_call_akshare_em", side_effect=Exception("primary error")
    ), patch.object(
        fetcher, "_call_akshare_cross_section", side_effect=Exception("cs error")
    ):
        result = fetcher._fetch_one("SH600519")
    assert result is None


def test_empty_raw_returns_none(fetcher):
    """Empty DataFrame from source should return None."""
    raw = pd.DataFrame()
    result = fetcher._normalize_balance_sheet(raw, "SH600519")
    assert result is None
