import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch
from quant_ex.features.pledge_factor import PledgeFactor


def _make_price_data(n_days=60, instruments=None):
    if instruments is None:
        instruments = ["SH600519", "SZ000001", "SH601318"]
    dates = pd.bdate_range("2026-02-01", periods=n_days)
    idx = pd.MultiIndex.from_product([instruments, dates], names=["instrument", "datetime"])
    return pd.DataFrame({"real_close": np.random.uniform(10, 100, len(idx))}, index=idx)


def _make_pledge_data(instruments, date_str="2026-04-29"):
    n = len(instruments)
    data = {
        "pledge_ratio": [5.2, 3.1, 8.0][:n] if n <= 3 else [5.2 + i * 0.1 for i in range(n)],
        "pledge_shares": [10000.0, 5000.0, 20000.0][:n] if n <= 3 else [10000.0 + i * 100 for i in range(n)],
        "pledge_mv": [2000000.0, 50000.0, 3000000.0][:n] if n <= 3 else [2000000.0 + i * 10000 for i in range(n)],
        "unlimited_pledge_shares": [10000.0, 5000.0, 20000.0][:n] if n <= 3 else [10000.0 + i * 100 for i in range(n)],
        "limited_pledge_shares": [0.0, 0.0, 500.0][:n] if n <= 3 else [0.0] * n,
    }
    idx = pd.MultiIndex.from_tuples(
        [(inst, pd.Timestamp(date_str)) for inst in instruments],
        names=["instrument", "datetime"],
    )
    return pd.DataFrame(data, index=idx)


def test_compute_returns_dataframe():
    factor = PledgeFactor(cache_dir="./cache/pledge_test")
    price_data = _make_price_data()
    pledge = _make_pledge_data(list(price_data.index.get_level_values(0).unique()))

    with patch.object(factor, "_load_pledge_cache", return_value=pledge):
        result = factor.compute(price_data)
    assert result is not None
    assert "pledge_ratio" in result.columns
    assert "pledge_shares" in result.columns
    assert "pledge_mv" in result.columns
    assert "unlimited_pledge_shares" in result.columns
    assert "limited_pledge_shares" in result.columns


def test_align_to_price_data_index():
    factor = PledgeFactor(cache_dir="./cache/pledge_test")
    price_data = _make_price_data(n_days=10)
    pledge = _make_pledge_data(list(price_data.index.get_level_values(0).unique()))

    with patch.object(factor, "_load_pledge_cache", return_value=pledge):
        result = factor.compute(price_data)
    assert result is not None
    assert result.index.equals(price_data.index)


def test_change_factors():
    factor = PledgeFactor(cache_dir="./cache/pledge_test", include_change=True)
    price_data = _make_price_data(n_days=30, instruments=["SH600519"])
    # Create pledge data with time series
    dates = pd.bdate_range("2026-02-01", periods=30)
    idx = pd.MultiIndex.from_tuples(
        [("SH600519", d) for d in dates], names=["instrument", "datetime"]
    )
    pledge = pd.DataFrame({
        "pledge_ratio": np.linspace(3.0, 5.0, 30),
        "pledge_shares": np.linspace(5000, 8000, 30),
        "pledge_mv": np.linspace(500000, 800000, 30),
        "unlimited_pledge_shares": np.linspace(5000, 8000, 30),
        "limited_pledge_shares": np.zeros(30),
    }, index=idx)

    with patch.object(factor, "_load_pledge_cache", return_value=pledge):
        result = factor.compute(price_data)
    assert result is not None
    assert "pledge_ratio_chg" in result.columns


def test_change_factors_disabled():
    factor = PledgeFactor(cache_dir="./cache/pledge_test", include_change=False)
    price_data = _make_price_data(n_days=10)
    pledge = _make_pledge_data(list(price_data.index.get_level_values(0).unique()))

    with patch.object(factor, "_load_pledge_cache", return_value=pledge):
        result = factor.compute(price_data)
    assert result is not None
    assert "pledge_ratio_chg" not in result.columns


def test_missing_data_returns_none():
    factor = PledgeFactor(cache_dir="./cache/pledge_test_empty")
    price_data = _make_price_data()

    with patch.object(factor, "_load_pledge_cache", return_value=None):
        result = factor.compute(price_data)
    assert result is None


def test_stocks_without_pledge_get_zero():
    factor = PledgeFactor(cache_dir="./cache/pledge_test")
    price_data = _make_price_data(instruments=["SH600519", "SZ300001"])
    # Only SH600519 in pledge data
    pledge = _make_pledge_data(["SH600519"])

    with patch.object(factor, "_load_pledge_cache", return_value=pledge):
        result = factor.compute(price_data)
    assert result is not None
    # SZ300001 should have 0, not NaN
    sz_data = result.loc["SZ300001"]
    assert (sz_data["pledge_ratio"] == 0).all()


def test_setstate_backward_compat():
    """Old pickles without include_change get safe defaults."""
    factor = PledgeFactor(cache_dir="./cache/pledge_test", include_change=True)
    # Simulate unpickling from an old pickle that lacks include_change
    state = {"cache_dir": factor.cache_dir, "cache_ttl_days": 1}
    factor.__setstate__(state)
    assert factor.include_change is True


def test_setstate_backward_compat_missing_ttl():
    """Old pickles without cache_ttl_days get default."""
    factor = PledgeFactor(cache_dir="./cache/pledge_test")
    state = {"cache_dir": factor.cache_dir, "include_change": True}
    factor.__setstate__(state)
    assert factor.cache_ttl_days == 1
