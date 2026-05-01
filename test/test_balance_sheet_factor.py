import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import patch
from quant_ex.features.balance_sheet_factor import (
    BalanceSheetFactor,
    _RATIO_METRICS,
    _ABSOLUTE_METRICS,
    _ALL_METRICS,
)


def _make_price_data(n_days=60, instruments=None, start_date="2026-01-01"):
    """Create a price_data DataFrame with (instrument, datetime) MultiIndex."""
    if instruments is None:
        instruments = ["SH600519", "SZ000001", "SH601318"]
    dates = pd.bdate_range(start_date, periods=n_days)
    idx = pd.MultiIndex.from_product(
        [instruments, dates], names=["instrument", "datetime"]
    )
    np.random.seed(42)
    return pd.DataFrame(
        {"real_close": np.random.uniform(10, 100, len(idx))}, index=idx
    )


def _make_balance_sheet_data(instruments=None, report_dates=None):
    """Create a balance sheet cache DataFrame with realistic financial values.

    Returns a DataFrame with (instrument, datetime) MultiIndex and the
    standard curated columns from BalanceSheetFetcher.
    """
    if instruments is None:
        instruments = ["SH600519", "SZ000001", "SH601318"]
    if report_dates is None:
        report_dates = [
            "2024-03-31",
            "2024-06-30",
            "2024-09-30",
            "2024-12-31",
            "2025-03-31",
        ]

    rows = []
    for inst in instruments:
        for i, rd in enumerate(report_dates):
            rows.append(
                {
                    "instrument": inst,
                    "datetime": pd.Timestamp(rd),
                    "revenue": 1e9 * (1 + 0.1 * i),
                    "net_profit": 2e8 * (1 + 0.05 * i),
                    "total_assets": 1e10 + i * 1e9,
                    "total_equity": 4e9 + i * 2e8,
                    "total_liabilities": 6e9 + i * 8e8,
                    "current_assets": 3e9 + i * 1e8,
                    "current_liabilities": 2e9 + i * 5e7,
                    "inventory": 5e8 + i * 2e7,
                    "goodwill": 1e8,
                    "cash": 2e8 + i * 1e7,
                    "short_term_debt": 3e8,
                    "long_term_debt": 5e8,
                }
            )

    df = pd.DataFrame(rows)
    df = df.set_index(["instrument", "datetime"])
    return df


def test_compute_returns_dataframe():
    """compute() should return a DataFrame with expected factor columns."""
    factor = BalanceSheetFactor(cache_dir="./cache/balance_sheet_test")
    price_data = _make_price_data(n_days=60, instruments=["SH600519", "SZ000001"])
    bs_data = _make_balance_sheet_data(instruments=["SH600519", "SZ000001"])

    with patch.object(factor, "_load_balance_sheet_cache", return_value=bs_data):
        result = factor.compute(price_data)
    assert result is not None
    # Should have all ratio columns
    for col in _RATIO_METRICS:
        assert col in result.columns, f"Missing ratio column: {col}"
    # Should have all absolute-value columns
    for col in _ABSOLUTE_METRICS:
        assert col in result.columns, f"Missing absolute column: {col}"


def test_align_to_price_data_index():
    """Result DataFrame should have the same index as price_data."""
    factor = BalanceSheetFactor(cache_dir="./cache/balance_sheet_test")
    price_data = _make_price_data(n_days=10, instruments=["SH600519"])
    bs_data = _make_balance_sheet_data(instruments=["SH600519"])

    with patch.object(factor, "_load_balance_sheet_cache", return_value=bs_data):
        result = factor.compute(price_data)
    assert result is not None
    assert result.index.equals(price_data.index)


def test_leverage_ratio():
    """Verify leverage_ratio = total_liabilities / total_equity."""
    factor = BalanceSheetFactor(
        cache_dir="./cache/balance_sheet_test", metrics=["leverage_ratio"]
    )
    instruments = ["SH600519"]
    dates = pd.bdate_range("2026-01-01", periods=10)
    idx = pd.MultiIndex.from_product(
        [instruments, dates], names=["instrument", "datetime"]
    )
    price_data = pd.DataFrame({"real_close": [50.0] * len(idx)}, index=idx)

    # Known values: total_liabilities=6e9, total_equity=4e9 → leverage_ratio=1.5
    bs_rows = [
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-03-31"),
            "revenue": 1e9,
            "net_profit": 2e8,
            "total_assets": 1e10,
            "total_equity": 4e9,
            "total_liabilities": 6e9,
            "current_assets": 3e9,
            "current_liabilities": 2e9,
            "inventory": 5e8,
            "goodwill": 1e8,
            "cash": 2e8,
            "short_term_debt": 3e8,
            "long_term_debt": 5e8,
        }
    ]
    bs_data = pd.DataFrame(bs_rows).set_index(["instrument", "datetime"])

    with patch.object(
        factor, "_load_balance_sheet_cache", return_value=bs_data
    ):
        result = factor.compute(price_data)

    assert result is not None
    # After forward-fill, all dates should have the same value
    expected = 6e9 / 4e9  # 1.5
    assert all(abs(result["leverage_ratio"].dropna() - expected) < 1e-6)


def test_current_ratio_and_quick_ratio():
    """Verify current_ratio and quick_ratio calculations.

    current_ratio = current_assets / current_liabilities
    quick_ratio = (current_assets - inventory) / current_liabilities
    """
    factor = BalanceSheetFactor(
        cache_dir="./cache/balance_sheet_test",
        metrics=["current_ratio", "quick_ratio"],
    )
    instruments = ["SH600519"]
    dates = pd.bdate_range("2026-01-01", periods=10)
    idx = pd.MultiIndex.from_product(
        [instruments, dates], names=["instrument", "datetime"]
    )
    price_data = pd.DataFrame({"real_close": [50.0] * len(idx)}, index=idx)

    # current_assets=3e9, current_liabilities=2e9, inventory=5e8
    bs_rows = [
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-03-31"),
            "revenue": 1e9,
            "net_profit": 2e8,
            "total_assets": 1e10,
            "total_equity": 4e9,
            "total_liabilities": 6e9,
            "current_assets": 3e9,
            "current_liabilities": 2e9,
            "inventory": 5e8,
            "goodwill": 1e8,
            "cash": 2e8,
            "short_term_debt": 3e8,
            "long_term_debt": 5e8,
        }
    ]
    bs_data = pd.DataFrame(bs_rows).set_index(["instrument", "datetime"])

    with patch.object(
        factor, "_load_balance_sheet_cache", return_value=bs_data
    ):
        result = factor.compute(price_data)

    assert result is not None
    expected_current = 3e9 / 2e9  # 1.5
    expected_quick = (3e9 - 5e8) / 2e9  # 1.25
    assert all(abs(result["current_ratio"].dropna() - expected_current) < 1e-6)
    assert all(abs(result["quick_ratio"].dropna() - expected_quick) < 1e-6)


def test_goodwill_to_equity():
    """Verify goodwill_to_equity = goodwill / total_equity."""
    factor = BalanceSheetFactor(
        cache_dir="./cache/balance_sheet_test", metrics=["goodwill_to_equity"]
    )
    instruments = ["SH600519"]
    dates = pd.bdate_range("2026-01-01", periods=10)
    idx = pd.MultiIndex.from_product(
        [instruments, dates], names=["instrument", "datetime"]
    )
    price_data = pd.DataFrame({"real_close": [50.0] * len(idx)}, index=idx)

    # goodwill=1e8, total_equity=4e9 → goodwill_to_equity=0.025
    bs_rows = [
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-03-31"),
            "revenue": 1e9,
            "net_profit": 2e8,
            "total_assets": 1e10,
            "total_equity": 4e9,
            "total_liabilities": 6e9,
            "current_assets": 3e9,
            "current_liabilities": 2e9,
            "inventory": 5e8,
            "goodwill": 1e8,
            "cash": 2e8,
            "short_term_debt": 3e8,
            "long_term_debt": 5e8,
        }
    ]
    bs_data = pd.DataFrame(bs_rows).set_index(["instrument", "datetime"])

    with patch.object(
        factor, "_load_balance_sheet_cache", return_value=bs_data
    ):
        result = factor.compute(price_data)

    assert result is not None
    expected = 1e8 / 4e9  # 0.025
    assert all(abs(result["goodwill_to_equity"].dropna() - expected) < 1e-6)


def test_absolute_value_factors():
    """Verify absolute-value factors (revenue, net_profit, total_assets, total_equity) are in output."""
    factor = BalanceSheetFactor(
        cache_dir="./cache/balance_sheet_test",
        metrics=["revenue", "net_profit", "total_assets", "total_equity"],
    )
    instruments = ["SH600519"]
    dates = pd.bdate_range("2026-01-01", periods=10)
    idx = pd.MultiIndex.from_product(
        [instruments, dates], names=["instrument", "datetime"]
    )
    price_data = pd.DataFrame({"real_close": [50.0] * len(idx)}, index=idx)

    bs_rows = [
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-03-31"),
            "revenue": 1.5e9,
            "net_profit": 3e8,
            "total_assets": 1.2e10,
            "total_equity": 4.5e9,
            "total_liabilities": 7.5e9,
            "current_assets": 3e9,
            "current_liabilities": 2e9,
            "inventory": 5e8,
            "goodwill": 1e8,
            "cash": 2e8,
            "short_term_debt": 3e8,
            "long_term_debt": 5e8,
        }
    ]
    bs_data = pd.DataFrame(bs_rows).set_index(["instrument", "datetime"])

    with patch.object(
        factor, "_load_balance_sheet_cache", return_value=bs_data
    ):
        result = factor.compute(price_data)

    assert result is not None
    assert "revenue" in result.columns
    assert "net_profit" in result.columns
    assert "total_assets" in result.columns
    assert "total_equity" in result.columns
    # Verify values are forward-filled correctly
    assert all(abs(result["revenue"].dropna() - 1.5e9) < 1.0)
    assert all(abs(result["net_profit"].dropna() - 3e8) < 1.0)
    assert all(abs(result["total_assets"].dropna() - 1.2e10) < 1.0)
    assert all(abs(result["total_equity"].dropna() - 4.5e9) < 1.0)


def test_net_debt_ratio():
    """Verify net_debt_ratio = (short_term_debt + long_term_debt - cash) / total_equity."""
    factor = BalanceSheetFactor(
        cache_dir="./cache/balance_sheet_test", metrics=["net_debt_ratio"]
    )
    instruments = ["SH600519"]
    dates = pd.bdate_range("2026-01-01", periods=10)
    idx = pd.MultiIndex.from_product(
        [instruments, dates], names=["instrument", "datetime"]
    )
    price_data = pd.DataFrame({"real_close": [50.0] * len(idx)}, index=idx)

    # short=3e8, long=5e8, cash=2e8, equity=4e9
    # net_debt = 3e8 + 5e8 - 2e8 = 6e8
    # net_debt_ratio = 6e8 / 4e9 = 0.15
    bs_rows = [
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-03-31"),
            "revenue": 1e9,
            "net_profit": 2e8,
            "total_assets": 1e10,
            "total_equity": 4e9,
            "total_liabilities": 6e9,
            "current_assets": 3e9,
            "current_liabilities": 2e9,
            "inventory": 5e8,
            "goodwill": 1e8,
            "cash": 2e8,
            "short_term_debt": 3e8,
            "long_term_debt": 5e8,
        }
    ]
    bs_data = pd.DataFrame(bs_rows).set_index(["instrument", "datetime"])

    with patch.object(
        factor, "_load_balance_sheet_cache", return_value=bs_data
    ):
        result = factor.compute(price_data)

    assert result is not None
    expected = (3e8 + 5e8 - 2e8) / 4e9  # 0.15
    assert all(abs(result["net_debt_ratio"].dropna() - expected) < 1e-6)


def test_division_by_zero_returns_nan():
    """When total_equity is zero, leverage_ratio should be NaN (not inf)."""
    factor = BalanceSheetFactor(
        cache_dir="./cache/balance_sheet_test", metrics=["leverage_ratio"]
    )
    instruments = ["SH600519"]
    dates = pd.bdate_range("2026-01-01", periods=10)
    idx = pd.MultiIndex.from_product(
        [instruments, dates], names=["instrument", "datetime"]
    )
    price_data = pd.DataFrame({"real_close": [50.0] * len(idx)}, index=idx)

    # total_equity=0 → should produce NaN, not inf
    bs_rows = [
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-03-31"),
            "revenue": 1e9,
            "net_profit": 2e8,
            "total_assets": 1e10,
            "total_equity": 0.0,
            "total_liabilities": 6e9,
            "current_assets": 3e9,
            "current_liabilities": 2e9,
            "inventory": 5e8,
            "goodwill": 1e8,
            "cash": 2e8,
            "short_term_debt": 3e8,
            "long_term_debt": 5e8,
        }
    ]
    bs_data = pd.DataFrame(bs_rows).set_index(["instrument", "datetime"])

    with patch.object(
        factor, "_load_balance_sheet_cache", return_value=bs_data
    ):
        result = factor.compute(price_data)

    assert result is not None
    assert result["leverage_ratio"].isna().all()


def test_include_change_factors():
    """include_change=True should add period-over-period change columns for ratios."""
    factor = BalanceSheetFactor(
        cache_dir="./cache/balance_sheet_test",
        metrics=["leverage_ratio"],
        include_change=True,
    )
    instruments = ["SH600519"]
    dates = pd.bdate_range("2026-01-01", periods=10)
    idx = pd.MultiIndex.from_product(
        [instruments, dates], names=["instrument", "datetime"]
    )
    price_data = pd.DataFrame({"real_close": [50.0] * len(idx)}, index=idx)

    # Two report periods with different leverage
    bs_rows = [
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-03-31"),
            "revenue": 1e9,
            "net_profit": 2e8,
            "total_assets": 1e10,
            "total_equity": 4e9,
            "total_liabilities": 6e9,  # leverage = 1.5
            "current_assets": 3e9,
            "current_liabilities": 2e9,
            "inventory": 5e8,
            "goodwill": 1e8,
            "cash": 2e8,
            "short_term_debt": 3e8,
            "long_term_debt": 5e8,
        },
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2024-12-31"),
            "revenue": 9e8,
            "net_profit": 1.8e8,
            "total_assets": 9.5e9,
            "total_equity": 3.8e9,
            "total_liabilities": 5.7e9,  # leverage = 1.5
            "current_assets": 2.8e9,
            "current_liabilities": 1.9e9,
            "inventory": 4.5e8,
            "goodwill": 1e8,
            "cash": 1.8e8,
            "short_term_debt": 2.5e8,
            "long_term_debt": 4.5e8,
        },
    ]
    bs_data = pd.DataFrame(bs_rows).set_index(["instrument", "datetime"])

    with patch.object(
        factor, "_load_balance_sheet_cache", return_value=bs_data
    ):
        result = factor.compute(price_data)

    assert result is not None
    assert "leverage_ratio_chg" in result.columns


def test_metrics_subset():
    """When metrics is a subset, only those columns should appear."""
    factor = BalanceSheetFactor(
        cache_dir="./cache/balance_sheet_test",
        metrics=["leverage_ratio", "revenue"],
    )
    price_data = _make_price_data(n_days=10, instruments=["SH600519"])
    bs_data = _make_balance_sheet_data(instruments=["SH600519"])

    with patch.object(factor, "_load_balance_sheet_cache", return_value=bs_data):
        result = factor.compute(price_data)

    assert result is not None
    assert "leverage_ratio" in result.columns
    assert "revenue" in result.columns
    # Other metrics should NOT be present
    assert "current_ratio" not in result.columns
    assert "net_profit" not in result.columns


def test_missing_data_returns_none():
    """If no balance sheet cache data, compute() should return None."""
    factor = BalanceSheetFactor(cache_dir="./cache/balance_sheet_test_empty")
    price_data = _make_price_data()

    with patch.object(factor, "_load_balance_sheet_cache", return_value=None):
        result = factor.compute(price_data)
    assert result is None


def test_setstate_backward_compat():
    """Old pickles without include_change/metrics get safe defaults."""
    factor = object.__new__(BalanceSheetFactor)
    state = {"cache_dir": Path("./cache/balance_sheet_test"), "cache_ttl_days": 30}
    factor.__setstate__(state)
    assert factor.include_change is False
    assert factor.metrics == list(_ALL_METRICS)
    assert factor.cache_ttl_days == 30


def test_setstate_backward_compat_missing_ttl():
    """Old pickles without cache_ttl_days get default."""
    factor = object.__new__(BalanceSheetFactor)
    state = {
        "cache_dir": Path("./cache/balance_sheet_test"),
        "include_change": True,
        "metrics": ["leverage_ratio"],
    }
    factor.__setstate__(state)
    assert factor.cache_ttl_days == 30
    assert factor.include_change is True
    assert factor.metrics == ["leverage_ratio"]


def test_multiple_instruments():
    """Factor should handle multiple instruments correctly."""
    factor = BalanceSheetFactor(cache_dir="./cache/balance_sheet_test")
    instruments = ["SH600519", "SZ000001"]
    price_data = _make_price_data(n_days=10, instruments=instruments)
    bs_data = _make_balance_sheet_data(instruments=instruments)

    with patch.object(factor, "_load_balance_sheet_cache", return_value=bs_data):
        result = factor.compute(price_data)

    assert result is not None
    # Each instrument should have data
    for inst in instruments:
        inst_data = result.loc[inst]
        assert not inst_data.empty
        assert inst_data["leverage_ratio"].notna().any()
