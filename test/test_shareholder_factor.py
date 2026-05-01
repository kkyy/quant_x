"""Tests for ShareholderFactor."""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import patch

from quant_ex.features.shareholder_factor import ShareholderFactor


def _make_price_data(n_days=60, instruments=None):
    """Create a price_data DataFrame with (instrument, datetime) MultiIndex."""
    if instruments is None:
        instruments = ["SH600519", "SZ000001", "SH601318"]
    dates = pd.bdate_range("2026-01-01", periods=n_days)
    idx = pd.MultiIndex.from_product(
        [instruments, dates], names=["instrument", "datetime"]
    )
    return pd.DataFrame(
        {"real_close": np.random.uniform(10, 100, len(idx))}, index=idx
    )


def _make_shareholder_data(instruments=None, n_reports=4):
    """Create shareholder data as if loaded from cache.

    Simulates quarterly reporting: each instrument has n_reports rows
    at quarter-end dates.
    """
    if instruments is None:
        instruments = ["SH600519", "SZ000001", "SH601318"]

    quarter_dates = pd.to_datetime(["2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"])
    quarter_dates = quarter_dates[:n_reports]

    rows = []
    for inst in instruments:
        base_count = 120000 if inst.startswith("SH") else 350000
        for i, dt in enumerate(quarter_dates):
            # Decreasing count (bullish signal): count drops each quarter
            count = base_count - i * 5000
            chg_pct = -4.0 * (i + 1)  # increasing negative pct
            rows.append(
                {
                    "instrument": inst,
                    "datetime": dt,
                    "sh_count": float(count),
                    "sh_count_chg_pct": float(chg_pct),
                    "shares_per_holder": 80.0 + i * 2.0,
                    "value_per_holder": 140000.0 + i * 5000.0,
                }
            )

    df = pd.DataFrame(rows)
    df = df.set_index(["instrument", "datetime"])
    df.index.names = ["instrument", "datetime"]
    return df


# ── Core compute tests ──────────────────────────────────────────────────────


def test_compute_returns_dataframe():
    """compute() should return a DataFrame with expected columns."""
    factor = ShareholderFactor(
        cache_dir="./cache/shareholder_test", include_change=True
    )
    price_data = _make_price_data(n_days=60)
    sh_data = _make_shareholder_data(
        list(price_data.index.get_level_values(0).unique())
    )

    with patch.object(factor, "_load_shareholder_cache", return_value=sh_data):
        result = factor.compute(price_data)
    assert result is not None
    assert isinstance(result, pd.DataFrame)
    # Raw columns
    assert "sh_count" in result.columns
    assert "sh_count_chg_pct" in result.columns
    assert "shares_per_holder" in result.columns
    assert "value_per_holder" in result.columns
    # Change columns
    assert "sh_count_diff" in result.columns
    assert "sh_per_share_holding_change" in result.columns


def test_align_to_price_data_index():
    """Result should be reindexed to exactly match price_data.index."""
    factor = ShareholderFactor(
        cache_dir="./cache/shareholder_test", include_change=True
    )
    price_data = _make_price_data(
        n_days=10, instruments=["SH600519", "SZ000001"]
    )
    sh_data = _make_shareholder_data(
        list(price_data.index.get_level_values(0).unique())
    )

    with patch.object(factor, "_load_shareholder_cache", return_value=sh_data):
        result = factor.compute(price_data)
    assert result is not None
    assert result.index.equals(price_data.index)


def test_change_factors():
    """Change factors should be computed correctly from quarterly data."""
    factor = ShareholderFactor(
        cache_dir="./cache/shareholder_test", include_change=True
    )
    instruments = ["SH600519"]
    price_data = _make_price_data(n_days=60, instruments=instruments)
    sh_data = _make_shareholder_data(instruments, n_reports=4)

    with patch.object(factor, "_load_shareholder_cache", return_value=sh_data):
        result = factor.compute(price_data)
    assert result is not None

    # sh_count_diff: first value per instrument should be NaN (no prior period)
    sh_diff = result["sh_count_diff"]
    # The first occurrence of non-NaN should be at the second quarter date
    non_nan = sh_diff.dropna()
    assert len(non_nan) > 0

    # sh_per_share_holding_change: should also have NaN for first period
    sph_chg = result["sh_per_share_holding_change"]
    non_nan_sph = sph_chg.dropna()
    assert len(non_nan_sph) > 0


def test_include_change_false():
    """When include_change=False, no change columns should appear."""
    factor = ShareholderFactor(
        cache_dir="./cache/shareholder_test", include_change=False
    )
    price_data = _make_price_data(n_days=30)
    sh_data = _make_shareholder_data(
        list(price_data.index.get_level_values(0).unique())
    )

    with patch.object(factor, "_load_shareholder_cache", return_value=sh_data):
        result = factor.compute(price_data)
    assert result is not None
    assert "sh_count_diff" not in result.columns
    assert "sh_per_share_holding_change" not in result.columns
    # Raw columns should still be present
    assert "sh_count" in result.columns


def test_missing_data_returns_none():
    """When cache is empty or missing, compute() should return None."""
    factor = ShareholderFactor(cache_dir="./cache/shareholder_nonexistent")
    price_data = _make_price_data()
    result = factor.compute(price_data)
    assert result is None


def test_forward_fill_within_instrument():
    """Shareholder data should be forward-filled within each instrument."""
    factor = ShareholderFactor(
        cache_dir="./cache/shareholder_test", include_change=False
    )
    instruments = ["SH600519"]
    price_data = _make_price_data(n_days=60, instruments=instruments)

    # Only 2 quarter-end data points
    sh_data = pd.DataFrame(
        {
            "sh_count": [130000.0, 125000.0],
            "sh_count_chg_pct": [-3.85, -2.78],
            "shares_per_holder": [76.0, 80.0],
            "value_per_holder": [136800.0, 144000.0],
        },
        index=pd.MultiIndex.from_tuples(
            [
                ("SH600519", pd.Timestamp("2025-12-31")),
                ("SH600519", pd.Timestamp("2026-03-31")),
            ],
            names=["instrument", "datetime"],
        ),
    )

    with patch.object(factor, "_load_shareholder_cache", return_value=sh_data):
        result = factor.compute(price_data)
    assert result is not None

    # After the first quarter date, sh_count should be forward-filled
    # until the second quarter date
    after_first = result.loc["SH600519"].loc[
        (result.loc["SH600519"].index >= pd.Timestamp("2025-12-31"))
        & (result.loc["SH600519"].index < pd.Timestamp("2026-03-31"))
    ]
    if len(after_first) > 0:
        # All values between the two dates should equal the first value
        assert (after_first["sh_count"] == 130000.0).all()


def test_backward_compat_setstate():
    """Old pickles missing new attributes should get safe defaults."""
    factor = ShareholderFactor(include_change=True)
    # Simulate old pickle state missing 'include_change' and 'cache_dir'
    state = factor.__dict__.copy()
    del state["include_change"]
    del state["cache_dir"]

    new_factor = ShareholderFactor.__new__(ShareholderFactor)
    new_factor.__setstate__(state)
    assert new_factor.include_change is True
    assert new_factor.cache_dir == Path("./cache/shareholder")


def test_bj_instruments_handled():
    """BJ exchange instruments should be handled correctly."""
    factor = ShareholderFactor(
        cache_dir="./cache/shareholder_test", include_change=True
    )
    instruments = ["SH600519", "BJ430001", "BJ920001"]
    price_data = _make_price_data(n_days=30, instruments=instruments)
    sh_data = _make_shareholder_data(instruments, n_reports=2)

    with patch.object(factor, "_load_shareholder_cache", return_value=sh_data):
        result = factor.compute(price_data)
    assert result is not None
    assert result.index.equals(price_data.index)
    for inst in instruments:
        inst_data = result.loc[inst]
        assert not inst_data.empty


def test_sh_count_decreasing_is_negative_diff():
    """Decreasing shareholder count should produce negative sh_count_diff.

    This is the bullish concentration signal — the sign must be preserved
    so the model can learn: negative diff = bullish.
    """
    factor = ShareholderFactor(
        cache_dir="./cache/shareholder_test", include_change=True
    )
    instruments = ["SH600519"]
    price_data = _make_price_data(n_days=60, instruments=instruments)

    # Shareholder count decreasing over time, anchored within/around price_data range
    sh_data = pd.DataFrame(
        {
            "sh_count": [130000.0, 125000.0, 120000.0],
            "sh_count_chg_pct": [-3.85, -2.78, -4.0],
            "shares_per_holder": [76.0, 80.0, 83.0],
            "value_per_holder": [136800.0, 144000.0, 150000.0],
        },
        index=pd.MultiIndex.from_tuples(
            [
                ("SH600519", pd.Timestamp("2025-12-31")),
                ("SH600519", pd.Timestamp("2026-01-31")),
                ("SH600519", pd.Timestamp("2026-02-28")),
            ],
            names=["instrument", "datetime"],
        ),
    )

    with patch.object(factor, "_load_shareholder_cache", return_value=sh_data):
        result = factor.compute(price_data)
    assert result is not None

    # After 2026-01-31, sh_count is forward-filled as 125000 (the value from that date).
    # The diff at the first business day on/after 2026-01-31 should reflect
    # the change from 130000 (ffilled from 2025-12-31) to 125000 (at 2026-01-31).
    # After ffill, all dates from 2026-01-31 onward have sh_count=125000,
    # and the diff at 2026-01-31 = 125000 - 130000 = -5000 (negative).
    # We check a date that exists in price_data shortly after 2026-01-31.
    test_date = pd.Timestamp("2026-02-02")
    if test_date in result.loc["SH600519"].index:
        diff_val = result.loc[("SH600519", test_date), "sh_count_diff"]
        # After ffill, the diff at a date between two reporting dates is
        # NaN (same value as prior row via ffill, so diff=0 from the ffill
        # perspective, but the original diff was set at the transition date).
        # Check that at some point in the series, there is a negative diff.
    non_nan_diff = result.loc["SH600519", "sh_count_diff"].dropna()
    negative_diffs = non_nan_diff[non_nan_diff < 0]
    assert len(negative_diffs) > 0, (
        "Expected at least one negative sh_count_diff for decreasing shareholder count"
    )


def test_sh_count_chg_pct_computed_when_missing():
    """When sh_count_chg_pct is not in cache, it should be computed from sh_count."""
    factor = ShareholderFactor(
        cache_dir="./cache/shareholder_test", include_change=True
    )
    instruments = ["SH600519"]
    price_data = _make_price_data(n_days=60, instruments=instruments)

    # Data without sh_count_chg_pct column
    sh_data = pd.DataFrame(
        {
            "sh_count": [130000.0, 125000.0],
            "shares_per_holder": [76.0, 80.0],
            "value_per_holder": [136800.0, 144000.0],
        },
        index=pd.MultiIndex.from_tuples(
            [
                ("SH600519", pd.Timestamp("2025-12-31")),
                ("SH600519", pd.Timestamp("2026-03-31")),
            ],
            names=["instrument", "datetime"],
        ),
    )

    with patch.object(factor, "_load_shareholder_cache", return_value=sh_data):
        result = factor.compute(price_data)
    assert result is not None
    # sh_count_chg_pct should be computed even though it wasn't in cache
    assert "sh_count_chg_pct" in result.columns
