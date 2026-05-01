"""Tests for MarginFactor."""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import patch

from quant_ex.features.margin_factor import MarginFactor


def _make_price_data(n_days=30, instruments=None):
    """Create a price_data DataFrame with (instrument, datetime) MultiIndex."""
    if instruments is None:
        instruments = ["SH600519", "SZ000001", "SH601318"]
    dates = pd.bdate_range("2026-02-01", periods=n_days)
    idx = pd.MultiIndex.from_product([instruments, dates], names=["instrument", "datetime"])
    return pd.DataFrame({"real_close": np.random.uniform(10, 100, len(idx))}, index=idx)


def _make_margin_data(instruments, n_days=30, start="2026-02-01"):
    """Create margin data with SSE and SZSE stocks, as if from cache."""
    dates = pd.bdate_range(start, periods=n_days)
    rows = []
    for inst in instruments:
        for i, dt in enumerate(dates):
            rows.append({
                "instrument": inst,
                "datetime": dt,
                "margin_balance": 1e10 + i * 1e8,
                "margin_buy_amt": 5e8 + i * 1e7,
                "margin_repay_amt": 4.5e8 + i * 5e6,
                "short_balance": 200000.0 + i * 1000,
                "short_sell_vol": 50000.0 + i * 500,
                "short_repay_vol": 40000.0 + i * 300,
            })
    df = pd.DataFrame(rows)
    df = df.set_index(["instrument", "datetime"])
    df.index.names = ["instrument", "datetime"]
    return df


# ── Core compute tests ──────────────────────────────────────────────────────

def test_compute_returns_dataframe():
    """compute() should return a DataFrame with expected columns."""
    factor = MarginFactor(windows=[5, 10], cache_dir="./cache/margin_test")
    price_data = _make_price_data(n_days=30)
    margin = _make_margin_data(list(price_data.index.get_level_values(0).unique()))

    with patch.object(factor, "_load_margin_cache", return_value=margin):
        result = factor.compute(price_data)
    assert result is not None
    assert isinstance(result, pd.DataFrame)
    # Raw columns
    assert "margin_balance" in result.columns
    assert "margin_buy_amt" in result.columns
    assert "short_balance" in result.columns
    assert "short_sell_vol" in result.columns
    # Change columns
    assert "margin_balance_chg_pct" in result.columns
    assert "short_sell_ratio" in result.columns
    assert "margin_balance_chg_5d" in result.columns
    assert "margin_balance_chg_10d" in result.columns


def test_align_to_price_data_index():
    """Result should be reindexed to exactly match price_data.index."""
    factor = MarginFactor(windows=[5], cache_dir="./cache/margin_test")
    price_data = _make_price_data(n_days=10, instruments=["SH600519", "SZ000001"])
    margin = _make_margin_data(
        list(price_data.index.get_level_values(0).unique()), n_days=10
    )

    with patch.object(factor, "_load_margin_cache", return_value=margin):
        result = factor.compute(price_data)
    assert result is not None
    assert result.index.equals(price_data.index)


def test_change_factors():
    """margin_balance_chg_pct and short_sell_ratio should be computed correctly."""
    factor = MarginFactor(
        windows=[5], include_change=True, cache_dir="./cache/margin_test"
    )
    instruments = ["SH600519"]
    price_data = _make_price_data(n_days=10, instruments=instruments)
    margin = _make_margin_data(instruments, n_days=10)

    with patch.object(factor, "_load_margin_cache", return_value=margin):
        result = factor.compute(price_data)
    assert result is not None

    # margin_balance_chg_pct: first row per instrument is NaN (no prior value)
    chg_pct = result["margin_balance_chg_pct"]
    assert pd.isna(chg_pct.iloc[0])

    # short_sell_ratio should be between 0 and 1 for non-zero denominator
    ratio = result["short_sell_ratio"].dropna()
    assert (ratio >= 0).all()
    assert (ratio <= 1).all()

    # margin_balance_chg_5d: first 5 rows should be NaN for diff(5)
    chg_5d = result["margin_balance_chg_5d"]
    assert pd.isna(chg_5d.iloc[:5]).all()


def test_include_change_false():
    """When include_change=False, no change columns should appear."""
    factor = MarginFactor(
        windows=[5, 10], include_change=False, cache_dir="./cache/margin_test"
    )
    price_data = _make_price_data(n_days=10)
    margin = _make_margin_data(list(price_data.index.get_level_values(0).unique()))

    with patch.object(factor, "_load_margin_cache", return_value=margin):
        result = factor.compute(price_data)
    assert result is not None
    assert "margin_balance_chg_pct" not in result.columns
    assert "short_sell_ratio" not in result.columns
    assert "margin_balance_chg_5d" not in result.columns


def test_missing_data_returns_none():
    """When cache is empty or missing, compute() should return None."""
    factor = MarginFactor(cache_dir="./cache/margin_nonexistent")
    price_data = _make_price_data()
    result = factor.compute(price_data)
    assert result is None


def test_short_sell_ratio_handles_zero_denominator():
    """When margin_balance + short_balance = 0, short_sell_ratio should be NaN."""
    factor = MarginFactor(windows=[5], cache_dir="./cache/margin_test")
    instruments = ["SH600519"]
    dates = pd.bdate_range("2026-02-01", periods=5)
    idx = pd.MultiIndex.from_product([instruments, dates], names=["instrument", "datetime"])

    # Create data with zero values for margin_balance and short_balance
    margin = pd.DataFrame({
        "margin_balance": [0.0, 0.0, 1e10, 1e10, 1e10],
        "margin_buy_amt": [0.0, 0.0, 5e8, 5e8, 5e8],
        "margin_repay_amt": [0.0, 0.0, 4e8, 4e8, 4e8],
        "short_balance": [0.0, 0.0, 200000.0, 200000.0, 200000.0],
        "short_sell_vol": [0.0, 0.0, 50000.0, 50000.0, 50000.0],
        "short_repay_vol": [0.0, 0.0, 40000.0, 40000.0, 40000.0],
    }, index=idx)

    with patch.object(factor, "_load_margin_cache", return_value=margin):
        result = factor.compute(price_data=_make_price_data(n_days=5, instruments=instruments))
    assert result is not None
    # First two rows have zero denominator → ratio should be NaN
    assert pd.isna(result["short_sell_ratio"].iloc[0])
    assert pd.isna(result["short_sell_ratio"].iloc[1])
    # Third row onward has valid denominator → ratio should be a finite number
    assert np.isfinite(result["short_sell_ratio"].iloc[2])


def test_backward_compat_setstate():
    """Old pickles missing new attributes should get safe defaults."""
    factor = MarginFactor(windows=[5, 10])
    # Simulate old pickle state missing 'include_change' and 'windows'
    state = factor.__dict__.copy()
    del state["include_change"]
    del state["windows"]

    new_factor = MarginFactor.__new__(MarginFactor)
    new_factor.__setstate__(state)
    assert new_factor.include_change is True
    assert new_factor.windows == [5, 10, 20]


def test_multiple_instruments_with_different_exchanges():
    """Factor should handle SH, SZ, BJ instruments together."""
    factor = MarginFactor(windows=[5], cache_dir="./cache/margin_test")
    instruments = ["SH600519", "SZ000001", "BJ430001"]
    price_data = _make_price_data(n_days=10, instruments=instruments)
    margin = _make_margin_data(instruments, n_days=10)

    with patch.object(factor, "_load_margin_cache", return_value=margin):
        result = factor.compute(price_data)
    assert result is not None
    assert result.index.equals(price_data.index)
    # All instruments should have data
    for inst in instruments:
        inst_data = result.loc[inst]
        assert not inst_data.empty
