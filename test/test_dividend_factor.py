import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import patch
from quant_ex.features.dividend_factor import DividendFactor


def _make_price_data(n_days=60, instruments=None, start_date="2026-01-01"):
    """Create a price_data DataFrame with (instrument, datetime) MultiIndex."""
    if instruments is None:
        instruments = ["SH600519", "SZ000001", "SH601318"]
    dates = pd.bdate_range(start_date, periods=n_days)
    idx = pd.MultiIndex.from_product([instruments, dates], names=["instrument", "datetime"])
    # Use deterministic prices for reliable yield verification
    np.random.seed(42)
    return pd.DataFrame({"real_close": np.random.uniform(10, 100, len(idx))}, index=idx)


def _make_dividend_data(instruments, years=None):
    """Create a dividend cache DataFrame with multiple years of dividend events.

    Simulates dividend data with (instrument, datetime) MultiIndex.
    cash_dividend follows A-share convention: per 10 shares.
    """
    if years is None:
        years = [2024, 2025]

    rows = []
    for inst in instruments:
        for year in years:
            ex_date = pd.Timestamp(f"{year}-07-01")
            rows.append({
                "instrument": inst,
                "datetime": ex_date,
                "announcement_date": pd.Timestamp(f"{year}-06-15"),
                "bonus_shares": 0.0,
                "conversion_shares": 0.0,
                "cash_dividend": 10.0 + (year - 2024) * 2.0,  # 10, 12, 14, ...
                "progress": "实施方案",
                "ex_date": ex_date,
                "record_date": pd.Timestamp(f"{year}-06-30"),
            })

    df = pd.DataFrame(rows)
    df = df.set_index(["instrument", "datetime"])
    return df


def test_compute_returns_dataframe():
    """compute() should return a DataFrame with expected factor columns."""
    factor = DividendFactor(cache_dir="./cache/dividend_test", lookback_years=5)
    price_data = _make_price_data(n_days=60, instruments=["SH600519", "SZ000001"])
    dividend = _make_dividend_data(["SH600519", "SZ000001"], years=[2024, 2025])

    with patch.object(factor, "_load_dividend_cache", return_value=dividend):
        result = factor.compute(price_data)
    assert result is not None
    assert "div_yield_ttm" in result.columns
    assert "div_consistency" in result.columns
    assert "div_growth_rate" in result.columns


def test_align_to_price_data_index():
    """Result DataFrame should have the same index as price_data."""
    factor = DividendFactor(cache_dir="./cache/dividend_test", lookback_years=5)
    price_data = _make_price_data(n_days=10, instruments=["SH600519"])
    dividend = _make_dividend_data(["SH600519"], years=[2025])

    with patch.object(factor, "_load_dividend_cache", return_value=dividend):
        result = factor.compute(price_data)
    assert result is not None
    assert result.index.equals(price_data.index)


def test_div_yield_ttm():
    """Verify TTM yield calculation.

    cash_dividend is per 10 shares. A stock with cash_dividend=12 (per 10 shares)
    paying once in the last 12 months has per-share dividend = 12/10 = 1.2.
    If close_price = 50, yield = 1.2 / 50 = 0.024.
    """
    factor = DividendFactor(cache_dir="./cache/dividend_test", lookback_years=5)

    # Single instrument with known price
    instruments = ["SH600519"]
    dates = pd.bdate_range("2026-01-01", periods=30)
    idx = pd.MultiIndex.from_product([instruments, dates], names=["instrument", "datetime"])
    close_price = 50.0
    price_data = pd.DataFrame({"real_close": [close_price] * len(idx)}, index=idx)

    # One dividend event: cash_dividend = 12.0 (per 10 shares)
    div_rows = [{
        "instrument": "SH600519",
        "datetime": pd.Timestamp("2025-07-01"),
        "announcement_date": pd.Timestamp("2025-06-15"),
        "bonus_shares": 0.0,
        "conversion_shares": 0.0,
        "cash_dividend": 12.0,
        "progress": "实施方案",
        "ex_date": pd.Timestamp("2025-07-01"),
        "record_date": pd.Timestamp("2025-06-30"),
    }]
    dividend = pd.DataFrame(div_rows).set_index(["instrument", "datetime"])

    with patch.object(factor, "_load_dividend_cache", return_value=dividend):
        result = factor.compute(price_data)

    assert result is not None
    # After the dividend ex_date, the yield should be 12.0 / 10 / 50 = 0.024
    # Before the ex_date in the TTM window, it should also show the yield
    # (dividend is within TTM for all dates in Jan 2026 since July 2025 is within 12 months)
    expected_yield = 12.0 / 10.0 / close_price  # 0.024
    # Check at least one date has the correct yield
    assert any(abs(result["div_yield_ttm"] - expected_yield) < 1e-6)


def test_div_yield_ttm_multiple_events():
    """TTM yield should sum all dividends in the trailing 12 months."""
    factor = DividendFactor(cache_dir="./cache/dividend_test", lookback_years=5)

    instruments = ["SH600519"]
    dates = pd.bdate_range("2026-01-01", periods=10)
    idx = pd.MultiIndex.from_product([instruments, dates], names=["instrument", "datetime"])
    close_price = 50.0
    price_data = pd.DataFrame({"real_close": [close_price] * len(idx)}, index=idx)

    # Two dividend events in the past 12 months (interim + final)
    div_rows = [
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-07-01"),
            "announcement_date": pd.Timestamp("2025-06-15"),
            "bonus_shares": 0.0,
            "conversion_shares": 0.0,
            "cash_dividend": 6.0,
            "progress": "实施方案",
            "ex_date": pd.Timestamp("2025-07-01"),
            "record_date": pd.Timestamp("2025-06-30"),
        },
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-12-15"),
            "announcement_date": pd.Timestamp("2025-12-01"),
            "bonus_shares": 0.0,
            "conversion_shares": 0.0,
            "cash_dividend": 8.0,
            "progress": "实施方案",
            "ex_date": pd.Timestamp("2025-12-15"),
            "record_date": pd.Timestamp("2025-12-14"),
        },
    ]
    dividend = pd.DataFrame(div_rows).set_index(["instrument", "datetime"])

    with patch.object(factor, "_load_dividend_cache", return_value=dividend):
        result = factor.compute(price_data)

    assert result is not None
    # TTM yield = (6.0 + 8.0) / 10 / 50 = 0.028
    expected_yield = (6.0 + 8.0) / 10.0 / close_price
    assert any(abs(result["div_yield_ttm"] - expected_yield) < 1e-6)


def test_div_consistency():
    """div_consistency should count consecutive years with non-zero dividends."""
    factor = DividendFactor(cache_dir="./cache/dividend_test", lookback_years=5)

    instruments = ["SH600519"]
    dates = pd.bdate_range("2026-01-01", periods=10)
    idx = pd.MultiIndex.from_product([instruments, dates], names=["instrument", "datetime"])
    price_data = pd.DataFrame({"real_close": [50.0] * len(idx)}, index=idx)

    # Dividends in 2022, 2023, 2024, 2025 → 4 consecutive years
    div_rows = []
    for year in [2022, 2023, 2024, 2025]:
        div_rows.append({
            "instrument": "SH600519",
            "datetime": pd.Timestamp(f"{year}-07-01"),
            "announcement_date": pd.Timestamp(f"{year}-06-15"),
            "bonus_shares": 0.0,
            "conversion_shares": 0.0,
            "cash_dividend": 10.0,
            "progress": "实施方案",
            "ex_date": pd.Timestamp(f"{year}-07-01"),
            "record_date": pd.Timestamp(f"{year}-06-30"),
        })
    dividend = pd.DataFrame(div_rows).set_index(["instrument", "datetime"])

    with patch.object(factor, "_load_dividend_cache", return_value=dividend):
        result = factor.compute(price_data)

    assert result is not None
    # From 2026-01-01, looking back: 2025, 2024, 2023, 2022 are consecutive → 4
    assert (result["div_consistency"] == 4).any()


def test_div_consistency_broken_chain():
    """div_consistency should stop counting when a year is missing."""
    factor = DividendFactor(cache_dir="./cache/dividend_test", lookback_years=5)

    instruments = ["SH600519"]
    dates = pd.bdate_range("2026-01-01", periods=10)
    idx = pd.MultiIndex.from_product([instruments, dates], names=["instrument", "datetime"])
    price_data = pd.DataFrame({"real_close": [50.0] * len(idx)}, index=idx)

    # Dividends in 2021, 2023, 2024, 2025 (gap in 2022)
    div_rows = []
    for year in [2021, 2023, 2024, 2025]:
        div_rows.append({
            "instrument": "SH600519",
            "datetime": pd.Timestamp(f"{year}-07-01"),
            "announcement_date": pd.Timestamp(f"{year}-06-15"),
            "bonus_shares": 0.0,
            "conversion_shares": 0.0,
            "cash_dividend": 10.0,
            "progress": "实施方案",
            "ex_date": pd.Timestamp(f"{year}-07-01"),
            "record_date": pd.Timestamp(f"{year}-06-30"),
        })
    dividend = pd.DataFrame(div_rows).set_index(["instrument", "datetime"])

    with patch.object(factor, "_load_dividend_cache", return_value=dividend):
        result = factor.compute(price_data)

    assert result is not None
    # 2025→2024→2023 are consecutive (3), but 2022 is missing, so chain breaks
    assert (result["div_consistency"] == 3).any()


def test_div_growth_rate():
    """div_growth_rate should compute YoY growth correctly."""
    factor = DividendFactor(cache_dir="./cache/dividend_test", lookback_years=5)

    instruments = ["SH600519"]
    dates = pd.bdate_range("2026-01-01", periods=10)
    idx = pd.MultiIndex.from_product([instruments, dates], names=["instrument", "datetime"])
    price_data = pd.DataFrame({"real_close": [50.0] * len(idx)}, index=idx)

    # 2024: cash_dividend = 10.0, 2025: cash_dividend = 12.0 → growth = 20%
    div_rows = [
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2024-07-01"),
            "announcement_date": pd.Timestamp("2024-06-15"),
            "bonus_shares": 0.0,
            "conversion_shares": 0.0,
            "cash_dividend": 10.0,
            "progress": "实施方案",
            "ex_date": pd.Timestamp("2024-07-01"),
            "record_date": pd.Timestamp("2024-06-30"),
        },
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-07-01"),
            "announcement_date": pd.Timestamp("2025-06-15"),
            "bonus_shares": 0.0,
            "conversion_shares": 0.0,
            "cash_dividend": 12.0,
            "progress": "实施方案",
            "ex_date": pd.Timestamp("2025-07-01"),
            "record_date": pd.Timestamp("2025-06-30"),
        },
    ]
    dividend = pd.DataFrame(div_rows).set_index(["instrument", "datetime"])

    with patch.object(factor, "_load_dividend_cache", return_value=dividend):
        result = factor.compute(price_data)

    assert result is not None
    # growth = (12.0 - 10.0) / 10.0 = 0.2
    assert any(abs(result["div_growth_rate"] - 0.2) < 1e-6)


def test_div_growth_rate_capped():
    """div_growth_rate should be capped to [-1, 5] range."""
    factor = DividendFactor(cache_dir="./cache/dividend_test", lookback_years=5)

    instruments = ["SH600519"]
    dates = pd.bdate_range("2026-01-01", periods=10)
    idx = pd.MultiIndex.from_product([instruments, dates], names=["instrument", "datetime"])
    price_data = pd.DataFrame({"real_close": [50.0] * len(idx)}, index=idx)

    # Extreme growth: 2024: 1.0, 2025: 100.0 → uncapped = 99.0, capped = 5.0
    div_rows = [
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2024-07-01"),
            "announcement_date": pd.Timestamp("2024-06-15"),
            "bonus_shares": 0.0,
            "conversion_shares": 0.0,
            "cash_dividend": 1.0,
            "progress": "实施方案",
            "ex_date": pd.Timestamp("2024-07-01"),
            "record_date": pd.Timestamp("2024-06-30"),
        },
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-07-01"),
            "announcement_date": pd.Timestamp("2025-06-15"),
            "bonus_shares": 0.0,
            "conversion_shares": 0.0,
            "cash_dividend": 100.0,
            "progress": "实施方案",
            "ex_date": pd.Timestamp("2025-07-01"),
            "record_date": pd.Timestamp("2025-06-30"),
        },
    ]
    dividend = pd.DataFrame(div_rows).set_index(["instrument", "datetime"])

    with patch.object(factor, "_load_dividend_cache", return_value=dividend):
        result = factor.compute(price_data)

    assert result is not None
    assert result["div_growth_rate"].max() <= 5.0


def test_missing_data_returns_none():
    """If no dividend cache data, compute() should return None."""
    factor = DividendFactor(cache_dir="./cache/dividend_test_empty")
    price_data = _make_price_data()

    with patch.object(factor, "_load_dividend_cache", return_value=None):
        result = factor.compute(price_data)
    assert result is None


def test_instrument_without_dividends_gets_zero():
    """Instruments not in dividend data should get zero for yield and consistency."""
    factor = DividendFactor(cache_dir="./cache/dividend_test", lookback_years=5)
    price_data = _make_price_data(n_days=10, instruments=["SH600519", "SZ300001"])

    # Only SH600519 has dividend data
    div_rows = [{
        "instrument": "SH600519",
        "datetime": pd.Timestamp("2025-07-01"),
        "announcement_date": pd.Timestamp("2025-06-15"),
        "bonus_shares": 0.0,
        "conversion_shares": 0.0,
        "cash_dividend": 10.0,
        "progress": "实施方案",
        "ex_date": pd.Timestamp("2025-07-01"),
        "record_date": pd.Timestamp("2025-06-30"),
    }]
    dividend = pd.DataFrame(div_rows).set_index(["instrument", "datetime"])

    with patch.object(factor, "_load_dividend_cache", return_value=dividend):
        result = factor.compute(price_data)

    assert result is not None
    # SZ300001 is missing from dividend data — its values should be NaN after reindex
    # then forward-fill keeps NaN, but fillna(0) in compute sets them to 0
    sz_data = result.loc["SZ300001"]
    assert (sz_data["div_yield_ttm"] == 0).all()
    assert (sz_data["div_consistency"] == 0).all()


def test_setstate_backward_compat():
    """Old pickles without lookback_years get safe defaults."""
    # Create a bare object without __init__ (simulating unpickling from old pickle)
    factor = object.__new__(DividendFactor)
    state = {"cache_dir": Path("./cache/dividend_test"), "cache_ttl_days": 30}
    factor.__setstate__(state)
    assert factor.lookback_years == 5  # default
    assert factor.cache_ttl_days == 30


def test_setstate_backward_compat_missing_ttl():
    """Old pickles without cache_ttl_days get default."""
    factor = object.__new__(DividendFactor)
    state = {"cache_dir": Path("./cache/dividend_test"), "lookback_years": 3}
    factor.__setstate__(state)
    assert factor.cache_ttl_days == 30  # default
    assert factor.lookback_years == 3


def test_no_close_column_returns_none():
    """If price_data has no recognizable close column, compute() returns None."""
    factor = DividendFactor(cache_dir="./cache/dividend_test", lookback_years=5)
    dates = pd.bdate_range("2026-01-01", periods=10)
    idx = pd.MultiIndex.from_product([["SH600519"], dates], names=["instrument", "datetime"])
    price_data = pd.DataFrame({"some_other_col": [50.0] * len(idx)}, index=idx)

    div_rows = [{
        "instrument": "SH600519",
        "datetime": pd.Timestamp("2025-07-01"),
        "announcement_date": pd.Timestamp("2025-06-15"),
        "bonus_shares": 0.0,
        "conversion_shares": 0.0,
        "cash_dividend": 10.0,
        "progress": "实施方案",
        "ex_date": pd.Timestamp("2025-07-01"),
        "record_date": pd.Timestamp("2025-06-30"),
    }]
    dividend = pd.DataFrame(div_rows).set_index(["instrument", "datetime"])

    with patch.object(factor, "_load_dividend_cache", return_value=dividend):
        result = factor.compute(price_data)
    assert result is None
