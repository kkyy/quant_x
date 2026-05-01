import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import patch
from quant_ex.features.repurchase_factor import RepurchaseFactor


def _make_price_data(n_days=60, instruments=None, start_date="2026-01-01"):
    """Create a price_data DataFrame with (instrument, datetime) MultiIndex."""
    if instruments is None:
        instruments = ["SH600519", "SZ000001", "SH601318"]
    dates = pd.bdate_range(start_date, periods=n_days)
    idx = pd.MultiIndex.from_product([instruments, dates], names=["instrument", "datetime"])
    np.random.seed(42)
    return pd.DataFrame({"real_close": np.random.uniform(10, 100, len(idx))}, index=idx)


def _make_repurchase_data(instruments=None):
    """Create a repurchase cache DataFrame with (instrument, datetime) MultiIndex.

    Simulates repurchase plan data from RepurchaseFetcher cache.
    """
    if instruments is None:
        instruments = ["SH600519", "SZ000001"]

    rows = [
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2026-01-15"),
            "plan_amount_upper": 200000000.0,
            "plan_amount_lower": 100000000.0,
            "plan_shares_upper": 200000,
            "plan_shares_lower": 100000,
            "plan_pct_upper": 0.02,
            "plan_pct_lower": 0.01,
            "done_amount": 150000000.0,
            "done_shares": 150000,
            "progress": "实施中",
            "announcement_date": pd.Timestamp("2026-01-15"),
        },
        {
            "instrument": "SZ000001",
            "datetime": pd.Timestamp("2026-02-20"),
            "plan_amount_upper": 100000000.0,
            "plan_amount_lower": 50000000.0,
            "plan_shares_upper": 10000000,
            "plan_shares_lower": 5000000,
            "plan_pct_upper": 0.05,
            "plan_pct_lower": 0.02,
            "done_amount": 80000000.0,
            "done_shares": 8000000,
            "progress": "完成",
            "announcement_date": pd.Timestamp("2026-02-20"),
        },
    ]

    # Filter to only requested instruments
    rows = [r for r in rows if r["instrument"] in instruments]
    df = pd.DataFrame(rows)
    df = df.set_index(["instrument", "datetime"])
    return df


def test_compute_returns_dataframe():
    """compute() should return a DataFrame with expected factor columns."""
    factor = RepurchaseFactor(cache_dir="./cache/repurchase_test")
    price_data = _make_price_data(n_days=60, instruments=["SH600519", "SZ000001"])
    repurchase = _make_repurchase_data(instruments=["SH600519", "SZ000001"])

    with patch.object(factor, "_load_repurchase_cache", return_value=repurchase):
        result = factor.compute(price_data)
    assert result is not None
    assert "repurchase_completion_pct" in result.columns
    assert "repurchase_active" in result.columns


def test_repurchase_completion_pct():
    """Verify completion percentage calculation.

    SH600519: done_amount=150M, plan_amount_upper=200M → 150/200 = 0.75
    SZ000001: done_amount=80M, plan_amount_upper=100M → 80/100 = 0.80
    """
    factor = RepurchaseFactor(cache_dir="./cache/repurchase_test")
    instruments = ["SH600519", "SZ000001"]
    price_data = _make_price_data(n_days=10, instruments=instruments)
    repurchase = _make_repurchase_data(instruments=instruments)

    with patch.object(factor, "_load_repurchase_cache", return_value=repurchase):
        result = factor.compute(price_data)

    assert result is not None

    # SH600519: 150M / 200M = 0.75
    sh_vals = result.loc["SH600519", "repurchase_completion_pct"]
    # After forward-fill, some dates should have the value
    assert any(abs(sh_vals.dropna() - 0.75) < 1e-6)

    # SZ000001: 80M / 100M = 0.80
    sz_vals = result.loc["SZ000001", "repurchase_completion_pct"]
    assert any(abs(sz_vals.dropna() - 0.80) < 1e-6)


def test_repurchase_active_flag():
    """Active flag should be 1 for '实施中', 0 for '完成'.

    SH600519: progress='实施中' → active=1
    SZ000001: progress='完成' → active=0
    """
    factor = RepurchaseFactor(cache_dir="./cache/repurchase_test")
    instruments = ["SH600519", "SZ000001"]
    price_data = _make_price_data(n_days=10, instruments=instruments)
    repurchase = _make_repurchase_data(instruments=instruments)

    with patch.object(factor, "_load_repurchase_cache", return_value=repurchase):
        result = factor.compute(price_data)

    assert result is not None

    # SH600519: active=1
    sh_active = result.loc["SH600519", "repurchase_active"]
    assert (sh_active.dropna() == 1).any()

    # SZ000001: active=0
    sz_active = result.loc["SZ000001", "repurchase_active"]
    assert (sz_active.dropna() == 0).any()


def test_missing_data_returns_none():
    """If no repurchase cache data, compute() should return None."""
    factor = RepurchaseFactor(cache_dir="./cache/repurchase_test_empty")
    price_data = _make_price_data()

    with patch.object(factor, "_load_repurchase_cache", return_value=None):
        result = factor.compute(price_data)
    assert result is None


def test_instrument_without_repurchase_is_nan():
    """Instruments not in repurchase data should have NaN (not 0)."""
    factor = RepurchaseFactor(cache_dir="./cache/repurchase_test")
    # SH601318 has no repurchase data
    price_data = _make_price_data(
        n_days=10, instruments=["SH600519", "SH601318"]
    )
    repurchase = _make_repurchase_data(instruments=["SH600519"])

    with patch.object(factor, "_load_repurchase_cache", return_value=repurchase):
        result = factor.compute(price_data)

    assert result is not None
    # SH601318 should have NaN values (no repurchase plan)
    sh601318_completion = result.loc["SH601318", "repurchase_completion_pct"]
    assert sh601318_completion.isna().all()
    sh601318_active = result.loc["SH601318", "repurchase_active"]
    assert sh601318_active.isna().all()


def test_completion_pct_plan_amount_fallback():
    """When plan_amount_upper is missing, use average of upper and lower."""
    factor = RepurchaseFactor(cache_dir="./cache/repurchase_test")
    instruments = ["SH600519"]
    dates = pd.bdate_range("2026-01-01", periods=10)
    idx = pd.MultiIndex.from_product([instruments, dates], names=["instrument", "datetime"])
    price_data = pd.DataFrame({"real_close": [50.0] * len(idx)}, index=idx)

    # plan_amount_upper is NaN, but plan_amount_lower = 100M
    # → plan_amount = (0 + 100M) / 2 = 50M
    # done_amount = 25M → completion = 25/50 = 0.5
    repurchase = pd.DataFrame({
        "plan_amount_upper": [np.nan],
        "plan_amount_lower": [100000000.0],
        "done_amount": [25000000.0],
        "done_shares": [50000],
        "progress": ["实施中"],
        "announcement_date": [pd.Timestamp("2026-01-15")],
    }, index=pd.MultiIndex.from_tuples(
        [("SH600519", pd.Timestamp("2026-01-15"))],
        names=["instrument", "datetime"],
    ))

    with patch.object(factor, "_load_repurchase_cache", return_value=repurchase):
        result = factor.compute(price_data)

    assert result is not None
    vals = result.loc["SH600519", "repurchase_completion_pct"]
    assert any(abs(vals.dropna() - 0.5) < 1e-6)


def test_completion_pct_no_plan_amount_is_nan():
    """When both plan_amount_upper and plan_amount_lower are missing, completion is NaN."""
    factor = RepurchaseFactor(cache_dir="./cache/repurchase_test")
    instruments = ["SH600519"]
    dates = pd.bdate_range("2026-01-01", periods=10)
    idx = pd.MultiIndex.from_product([instruments, dates], names=["instrument", "datetime"])
    price_data = pd.DataFrame({"real_close": [50.0] * len(idx)}, index=idx)

    # Both plan amounts are NaN
    repurchase = pd.DataFrame({
        "plan_amount_upper": [np.nan],
        "plan_amount_lower": [np.nan],
        "done_amount": [25000000.0],
        "done_shares": [50000],
        "progress": ["实施中"],
        "announcement_date": [pd.Timestamp("2026-01-15")],
    }, index=pd.MultiIndex.from_tuples(
        [("SH600519", pd.Timestamp("2026-01-15"))],
        names=["instrument", "datetime"],
    ))

    with patch.object(factor, "_load_repurchase_cache", return_value=repurchase):
        result = factor.compute(price_data)

    assert result is not None
    vals = result.loc["SH600519", "repurchase_completion_pct"]
    assert vals.isna().all()


def test_result_reindexes_to_price_data():
    """Result DataFrame should have the same index as price_data."""
    factor = RepurchaseFactor(cache_dir="./cache/repurchase_test")
    price_data = _make_price_data(n_days=10, instruments=["SH600519"])
    repurchase = _make_repurchase_data(instruments=["SH600519"])

    with patch.object(factor, "_load_repurchase_cache", return_value=repurchase):
        result = factor.compute(price_data)

    assert result is not None
    assert result.index.equals(price_data.index)


def test_setstate_backward_compat():
    """Old pickles without cache_ttl_days get safe defaults."""
    factor = object.__new__(RepurchaseFactor)
    state = {"cache_dir": Path("./cache/repurchase_test")}
    factor.__setstate__(state)
    assert factor.cache_ttl_days == 1  # default
    assert isinstance(factor.cache_dir, Path)


def test_setstate_backward_compat_missing_dir():
    """Old pickles without cache_dir get safe default."""
    factor = object.__new__(RepurchaseFactor)
    state = {"cache_ttl_days": 7}
    factor.__setstate__(state)
    assert isinstance(factor.cache_dir, Path)
    assert factor.cache_ttl_days == 7
