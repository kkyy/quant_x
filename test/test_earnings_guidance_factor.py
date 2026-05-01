import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import patch
from quant_ex.features.earnings_guidance_factor import EarningsGuidanceFactor


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


def _make_guidance_data(instruments=None):
    """Create a guidance cache DataFrame with various guidance types.

    Simulates yjyg cache data with (instrument, datetime) MultiIndex.
    """
    if instruments is None:
        instruments = ["SH600519", "SZ000001"]

    rows = [
        # SH600519: 预增 (strong increase)
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-04-15"),
            "guidance_type_raw": "预增",
            "earnings_change_pct": 50.0,
            "prior_value": 333.3,
            "forecast_value": 500.0,
            "reporting_period": pd.Timestamp("2025-03-31"),
        },
        # SZ000001: 预减 (decrease)
        {
            "instrument": "SZ000001",
            "datetime": pd.Timestamp("2025-04-14"),
            "guidance_type_raw": "预减",
            "earnings_change_pct": -20.0,
            "prior_value": 250.0,
            "forecast_value": 200.0,
            "reporting_period": pd.Timestamp("2025-03-31"),
        },
        # SH600519: another quarter, 首亏 (first loss)
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-01-10"),
            "guidance_type_raw": "续盈",
            "earnings_change_pct": 5.0,
            "prior_value": 300.0,
            "forecast_value": 315.0,
            "reporting_period": pd.Timestamp("2024-12-31"),
        },
        # Add more types
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-07-15"),
            "guidance_type_raw": "首亏",
            "earnings_change_pct": -120.0,
            "prior_value": 100.0,
            "forecast_value": -20.0,
            "reporting_period": pd.Timestamp("2025-06-30"),
        },
    ]

    df = pd.DataFrame(rows)
    df = df.set_index(["instrument", "datetime"])
    return df


def test_compute_returns_dataframe():
    """compute() should return a DataFrame with expected factor columns."""
    factor = EarningsGuidanceFactor(cache_dir="./cache/earnings_guidance_test")
    price_data = _make_price_data(
        n_days=60, instruments=["SH600519", "SZ000001"]
    )
    guidance = _make_guidance_data(["SH600519", "SZ000001"])

    with patch.object(factor, "_load_guidance_cache", return_value=guidance):
        result = factor.compute(price_data)

    assert result is not None
    assert "guidance_type" in result.columns
    assert "earnings_surprise_pct" in result.columns


def test_guidance_type_numeric_encoding():
    """Verify guidance type numeric encoding: 预增→3, 首亏→-3, etc."""
    factor = EarningsGuidanceFactor(cache_dir="./cache/earnings_guidance_test")

    # Build guidance data with specific types
    rows = [
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-04-15"),
            "guidance_type_raw": "预增",
            "earnings_change_pct": 50.0,
            "prior_value": 100.0,
            "forecast_value": 150.0,
        },
        {
            "instrument": "SZ000001",
            "datetime": pd.Timestamp("2025-04-15"),
            "guidance_type_raw": "略增",
            "earnings_change_pct": 10.0,
            "prior_value": 100.0,
            "forecast_value": 110.0,
        },
        {
            "instrument": "SH601318",
            "datetime": pd.Timestamp("2025-04-15"),
            "guidance_type_raw": "续盈",
            "earnings_change_pct": 2.0,
            "prior_value": 100.0,
            "forecast_value": 102.0,
        },
        {
            "instrument": "SZ300001",
            "datetime": pd.Timestamp("2025-04-15"),
            "guidance_type_raw": "扭亏",
            "earnings_change_pct": 150.0,
            "prior_value": -50.0,
            "forecast_value": 25.0,
        },
        {
            "instrument": "SH600000",
            "datetime": pd.Timestamp("2025-04-15"),
            "guidance_type_raw": "略减",
            "earnings_change_pct": -5.0,
            "prior_value": 100.0,
            "forecast_value": 95.0,
        },
        {
            "instrument": "SZ000002",
            "datetime": pd.Timestamp("2025-04-15"),
            "guidance_type_raw": "预减",
            "earnings_change_pct": -30.0,
            "prior_value": 100.0,
            "forecast_value": 70.0,
        },
        {
            "instrument": "BJ920001",
            "datetime": pd.Timestamp("2025-04-15"),
            "guidance_type_raw": "首亏",
            "earnings_change_pct": -120.0,
            "prior_value": 100.0,
            "forecast_value": -20.0,
        },
        {
            "instrument": "SH601398",
            "datetime": pd.Timestamp("2025-04-15"),
            "guidance_type_raw": "续亏",
            "earnings_change_pct": -50.0,
            "prior_value": -30.0,
            "forecast_value": -45.0,
        },
    ]

    guidance = pd.DataFrame(rows).set_index(["instrument", "datetime"])

    instruments = [
        "SH600519", "SZ000001", "SH601318", "SZ300001",
        "SH600000", "SZ000002", "BJ920001", "SH601398",
    ]
    dates = pd.bdate_range("2025-04-15", periods=5)
    idx = pd.MultiIndex.from_product(
        [instruments, dates], names=["instrument", "datetime"]
    )
    price_data = pd.DataFrame(
        {"real_close": np.random.uniform(10, 100, len(idx))}, index=idx
    )

    with patch.object(factor, "_load_guidance_cache", return_value=guidance):
        result = factor.compute(price_data)

    assert result is not None

    # Check encoding on the announcement date (2025-04-15)
    date_mask = result.index.get_level_values(1) == pd.Timestamp("2025-04-15")

    assert result.loc[("SH600519", pd.Timestamp("2025-04-15")), "guidance_type"] == 3  # 预增
    assert result.loc[("SZ000001", pd.Timestamp("2025-04-15")), "guidance_type"] == 2  # 略增
    assert result.loc[("SH601318", pd.Timestamp("2025-04-15")), "guidance_type"] == 1  # 续盈
    assert result.loc[("SZ300001", pd.Timestamp("2025-04-15")), "guidance_type"] == 2  # 扭亏
    assert result.loc[("SH600000", pd.Timestamp("2025-04-15")), "guidance_type"] == -1  # 略减
    assert result.loc[("SZ000002", pd.Timestamp("2025-04-15")), "guidance_type"] == -2  # 预减
    assert result.loc[("BJ920001", pd.Timestamp("2025-04-15")), "guidance_type"] == -3  # 首亏
    assert result.loc[("SH601398", pd.Timestamp("2025-04-15")), "guidance_type"] == -3  # 续亏


def test_earnings_surprise_pct():
    """Verify earnings_surprise_pct calculation.

    (forecast_value - prior_value) / |prior_value|
    SH600519: (500 - 333.3) / 333.3 ≈ 0.5002
    SZ000001: (200 - 250) / 250 = -0.2
    """
    factor = EarningsGuidanceFactor(cache_dir="./cache/earnings_guidance_test")
    price_data = _make_price_data(
        n_days=10, instruments=["SH600519", "SZ000001"], start_date="2025-04-15"
    )
    guidance = _make_guidance_data(["SH600519", "SZ000001"])

    with patch.object(factor, "_load_guidance_cache", return_value=guidance):
        result = factor.compute(price_data)

    assert result is not None

    # On the announcement date for SH600519 (2025-04-15)
    # The most recent guidance for SH600519 on 2025-04-15 is the one
    # announced on 2025-04-15 itself
    sh_data = result.loc["SH600519"]
    # Find the first date after 2025-04-15 where we have data
    valid_dates = sh_data.index[sh_data["guidance_type"].notna()]
    if len(valid_dates) > 0:
        surprise = sh_data.loc[valid_dates[0], "earnings_surprise_pct"]
        # (500 - 333.3) / 333.3 ≈ 0.5002
        assert abs(surprise - (500.0 - 333.3) / 333.3) < 0.01

    # SZ000001: (200 - 250) / 250 = -0.2
    sz_data = result.loc["SZ000001"]
    valid_dates = sz_data.index[sz_data["guidance_type"].notna()]
    if len(valid_dates) > 0:
        surprise = sz_data.loc[valid_dates[0], "earnings_surprise_pct"]
        assert abs(surprise - (-0.2)) < 0.01


def test_earnings_surprise_pct_fallback_to_change_pct():
    """When forecast_value or prior_value is missing, use earnings_change_pct."""
    factor = EarningsGuidanceFactor(cache_dir="./cache/earnings_guidance_test")

    rows = [
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-04-15"),
            "guidance_type_raw": "预增",
            "earnings_change_pct": 50.0,
            "prior_value": np.nan,
            "forecast_value": np.nan,
        },
    ]
    guidance = pd.DataFrame(rows).set_index(["instrument", "datetime"])

    instruments = ["SH600519"]
    dates = pd.bdate_range("2025-04-15", periods=5)
    idx = pd.MultiIndex.from_product(
        [instruments, dates], names=["instrument", "datetime"]
    )
    price_data = pd.DataFrame(
        {"real_close": [50.0] * len(idx)}, index=idx
    )

    with patch.object(factor, "_load_guidance_cache", return_value=guidance):
        result = factor.compute(price_data)

    assert result is not None
    # Should fall back to earnings_change_pct = 50.0
    val = result.loc[("SH600519", pd.Timestamp("2025-04-15")), "earnings_surprise_pct"]
    assert abs(val - 50.0) < 1e-6


def test_missing_data_returns_none():
    """If no guidance cache data, compute() should return None."""
    factor = EarningsGuidanceFactor(
        cache_dir="./cache/earnings_guidance_test_empty"
    )
    price_data = _make_price_data()

    with patch.object(factor, "_load_guidance_cache", return_value=None):
        result = factor.compute(price_data)
    assert result is None


def test_instrument_without_guidance_gets_nan():
    """Instruments not in guidance data should get NaN (not 0)."""
    factor = EarningsGuidanceFactor(cache_dir="./cache/earnings_guidance_test")
    price_data = _make_price_data(
        n_days=10, instruments=["SH600519", "SZ300001"], start_date="2025-04-15"
    )

    # Only SH600519 has guidance data
    rows = [
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-04-15"),
            "guidance_type_raw": "预增",
            "earnings_change_pct": 50.0,
            "prior_value": 100.0,
            "forecast_value": 150.0,
        },
    ]
    guidance = pd.DataFrame(rows).set_index(["instrument", "datetime"])

    with patch.object(factor, "_load_guidance_cache", return_value=guidance):
        result = factor.compute(price_data)

    assert result is not None
    # SZ300001 is missing from guidance data — its values should be NaN
    sz_data = result.loc["SZ300001"]
    assert sz_data["guidance_type"].isna().all()
    assert sz_data["earnings_surprise_pct"].isna().all()


def test_forward_fill_within_instrument():
    """Guidance should be forward-filled within each instrument."""
    factor = EarningsGuidanceFactor(cache_dir="./cache/earnings_guidance_test")

    instruments = ["SH600519"]
    dates = pd.bdate_range("2025-04-14", periods=5)
    idx = pd.MultiIndex.from_product(
        [instruments, dates], names=["instrument", "datetime"]
    )
    price_data = pd.DataFrame(
        {"real_close": [50.0] * len(idx)}, index=idx
    )

    # Guidance announced on 2025-04-15
    rows = [
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-04-15"),
            "guidance_type_raw": "预增",
            "earnings_change_pct": 50.0,
            "prior_value": 100.0,
            "forecast_value": 150.0,
        },
    ]
    guidance = pd.DataFrame(rows).set_index(["instrument", "datetime"])

    with patch.object(factor, "_load_guidance_cache", return_value=guidance):
        result = factor.compute(price_data)

    assert result is not None
    # Before announcement (2025-04-14): NaN
    assert pd.isna(result.loc[("SH600519", pd.Timestamp("2025-04-14")), "guidance_type"])
    # On announcement day and after: should be forward-filled
    assert result.loc[("SH600519", pd.Timestamp("2025-04-15")), "guidance_type"] == 3
    assert result.loc[("SH600519", pd.Timestamp("2025-04-16")), "guidance_type"] == 3
    assert result.loc[("SH600519", pd.Timestamp("2025-04-17")), "guidance_type"] == 3


def test_newer_guidance_overrides_older():
    """When multiple guidance announcements exist, the most recent one wins."""
    factor = EarningsGuidanceFactor(cache_dir="./cache/earnings_guidance_test")

    instruments = ["SH600519"]
    dates = pd.bdate_range("2025-04-14", periods=10)
    idx = pd.MultiIndex.from_product(
        [instruments, dates], names=["instrument", "datetime"]
    )
    price_data = pd.DataFrame(
        {"real_close": [50.0] * len(idx)}, index=idx
    )

    # Two guidance announcements: old (预增) and new (首亏)
    rows = [
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-04-15"),
            "guidance_type_raw": "预增",
            "earnings_change_pct": 50.0,
            "prior_value": 100.0,
            "forecast_value": 150.0,
        },
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-04-18"),
            "guidance_type_raw": "首亏",
            "earnings_change_pct": -120.0,
            "prior_value": 100.0,
            "forecast_value": -20.0,
        },
    ]
    guidance = pd.DataFrame(rows).set_index(["instrument", "datetime"])

    with patch.object(factor, "_load_guidance_cache", return_value=guidance):
        result = factor.compute(price_data)

    assert result is not None
    # Before first announcement: NaN
    assert pd.isna(result.loc[("SH600519", pd.Timestamp("2025-04-14")), "guidance_type"])
    # After first announcement, before second: 预增=3
    assert result.loc[("SH600519", pd.Timestamp("2025-04-16")), "guidance_type"] == 3
    # After second announcement: 首亏=-3
    assert result.loc[("SH600519", pd.Timestamp("2025-04-18")), "guidance_type"] == -3
    assert result.loc[("SH600519", pd.Timestamp("2025-04-21")), "guidance_type"] == -3


def test_setstate_backward_compat():
    """Old pickles without new attributes get safe defaults."""
    factor = object.__new__(EarningsGuidanceFactor)
    state = {"cache_dir": Path("./cache/earnings_guidance_test"), "cache_ttl_days": 30}
    factor.__setstate__(state)
    assert factor.cache_ttl_days == 30
    assert hasattr(factor, "_TYPE_MAP")
    assert factor._TYPE_MAP["预增"] == 3
    assert factor._TYPE_MAP["首亏"] == -3


def test_setstate_backward_compat_missing_ttl():
    """Old pickles without cache_ttl_days get default."""
    factor = object.__new__(EarningsGuidanceFactor)
    state = {"cache_dir": Path("./cache/earnings_guidance_test")}
    factor.__setstate__(state)
    assert factor.cache_ttl_days == 30  # default


def test_result_reindexes_to_price_data_index():
    """Result DataFrame should have the same index as price_data."""
    factor = EarningsGuidanceFactor(cache_dir="./cache/earnings_guidance_test")
    price_data = _make_price_data(
        n_days=10, instruments=["SH600519"], start_date="2025-04-15"
    )

    rows = [
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2025-04-15"),
            "guidance_type_raw": "预增",
            "earnings_change_pct": 50.0,
            "prior_value": 100.0,
            "forecast_value": 150.0,
        },
    ]
    guidance = pd.DataFrame(rows).set_index(["instrument", "datetime"])

    with patch.object(factor, "_load_guidance_cache", return_value=guidance):
        result = factor.compute(price_data)

    assert result is not None
    assert result.index.equals(price_data.index)


def test_bj_exchange_handled():
    """BJ exchange instruments (920xxx, 4xx, 8xx) should be handled correctly."""
    factor = EarningsGuidanceFactor(cache_dir="./cache/earnings_guidance_test")

    instruments = ["BJ920001", "BJ430001", "BJ830001"]
    dates = pd.bdate_range("2025-04-15", periods=5)
    idx = pd.MultiIndex.from_product(
        [instruments, dates], names=["instrument", "datetime"]
    )
    price_data = pd.DataFrame(
        {"real_close": [50.0] * len(idx)}, index=idx
    )

    rows = [
        {
            "instrument": "BJ920001",
            "datetime": pd.Timestamp("2025-04-15"),
            "guidance_type_raw": "预增",
            "earnings_change_pct": 30.0,
            "prior_value": 10.0,
            "forecast_value": 13.0,
        },
        {
            "instrument": "BJ430001",
            "datetime": pd.Timestamp("2025-04-15"),
            "guidance_type_raw": "续亏",
            "earnings_change_pct": -50.0,
            "prior_value": -5.0,
            "forecast_value": -7.5,
        },
        {
            "instrument": "BJ830001",
            "datetime": pd.Timestamp("2025-04-15"),
            "guidance_type_raw": "扭亏",
            "earnings_change_pct": 200.0,
            "prior_value": -10.0,
            "forecast_value": 10.0,
        },
    ]
    guidance = pd.DataFrame(rows).set_index(["instrument", "datetime"])

    with patch.object(factor, "_load_guidance_cache", return_value=guidance):
        result = factor.compute(price_data)

    assert result is not None
    assert result.loc[("BJ920001", pd.Timestamp("2025-04-15")), "guidance_type"] == 3
    assert result.loc[("BJ430001", pd.Timestamp("2025-04-15")), "guidance_type"] == -3
    assert result.loc[("BJ830001", pd.Timestamp("2025-04-15")), "guidance_type"] == 2
