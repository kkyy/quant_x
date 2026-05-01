"""Tests for InstitutionalFactor."""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import patch

from quant_ex.features.institutional_factor import InstitutionalFactor


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


def _make_fund_data(instruments=None, n_quarters=4):
    """Create fund holdings data as if loaded from cache.

    Simulates quarterly reporting with fund count and holding metrics.
    Quarter dates chosen to be within or before the price_data range
    (2026-01-01+) so that forward-fill carries them into the daily grid.
    """
    if instruments is None:
        instruments = ["SH600519", "SZ000001"]

    quarter_dates = pd.to_datetime(
        ["2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"]
    )
    quarter_dates = quarter_dates[:n_quarters]

    rows = []
    for inst in instruments:
        base_count = 150 if inst.startswith("SH") else 80
        for i, dt in enumerate(quarter_dates):
            count = base_count + i * 10
            rows.append(
                {
                    "instrument": inst,
                    "datetime": dt,
                    "fund_count": float(count),
                    "hold_shares": 5000000.0 + i * 500000,
                    "hold_mv": 9000000000.0 + i * 1000000000,
                    "hold_change": "增持" if i > 0 else "新进",
                    "hold_change_shares": 500000.0 if i > 0 else 0.0,
                    "hold_change_pct": 11.11 if i > 0 else 100.0,
                }
            )

    df = pd.DataFrame(rows)
    df = df.set_index(["instrument", "datetime"])
    df.index.names = ["instrument", "datetime"]
    return df


def _make_qfii_data(instruments=None, n_quarters=4):
    """Create QFII holdings data as if loaded from cache.

    QFII first enters with "新进" in Q2 2025, then holds/增持 in later quarters.
    Quarter dates within/around price_data range so transitions are detectable.
    """
    if instruments is None:
        instruments = ["SH600519", "SZ000858"]

    quarter_dates = pd.to_datetime(
        ["2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"]
    )
    quarter_dates = quarter_dates[:n_quarters]

    rows = []
    for inst in instruments:
        for i, dt in enumerate(quarter_dates):
            # QFII enters in the first quarter (新进), then holds
            if i == 0:
                change = "新进"
                count = 3
            else:
                change = "增持"
                count = 3 + i
            rows.append(
                {
                    "instrument": inst,
                    "datetime": dt,
                    "inst_count": float(count),
                    "hold_shares": 1000000.0 + i * 200000,
                    "hold_mv": 1800000000.0 + i * 300000000,
                    "hold_change": change,
                    "hold_change_shares": 1000000.0 if i == 0 else 200000.0,
                    "hold_change_pct": 100.0 if i == 0 else 20.0,
                }
            )

    df = pd.DataFrame(rows)
    df = df.set_index(["instrument", "datetime"])
    df.index.names = ["instrument", "datetime"]
    return df


def _make_ss_data(instruments=None, n_quarters=4):
    """Create social security holdings data as if loaded from cache."""
    if instruments is None:
        instruments = ["SH601318"]

    quarter_dates = pd.to_datetime(
        ["2025-06-30", "2025-09-30", "2025-12-31", "2026-03-31"]
    )
    quarter_dates = quarter_dates[:n_quarters]

    rows = []
    for inst in instruments:
        for i, dt in enumerate(quarter_dates):
            if i == 0:
                change = "新进"
                count = 2
            else:
                change = "增持"
                count = 2 + i
            rows.append(
                {
                    "instrument": inst,
                    "datetime": dt,
                    "inst_count": float(count),
                    "hold_shares": 800000.0 + i * 100000,
                    "hold_mv": 400000000.0 + i * 50000000,
                    "hold_change": change,
                    "hold_change_shares": 800000.0 if i == 0 else 100000.0,
                    "hold_change_pct": 100.0 if i == 0 else 14.29,
                }
            )

    df = pd.DataFrame(rows)
    df = df.set_index(["instrument", "datetime"])
    df.index.names = ["instrument", "datetime"]
    return df


# ── Core compute tests ──────────────────────────────────────────────────────


def test_compute_returns_dataframe():
    """compute() should return a DataFrame with expected columns."""
    factor = InstitutionalFactor(
        cache_dir="./cache/institutional_test", include_change=True
    )
    instruments = ["SH600519", "SZ000001", "SH601318"]
    price_data = _make_price_data(n_days=60, instruments=instruments)

    fund_data = _make_fund_data(["SH600519", "SZ000001"])
    qfii_data = _make_qfii_data(["SH600519"])
    ss_data = _make_ss_data(["SH601318"])

    with patch.object(factor, "_load_cache", side_effect=lambda t: {
        "fund": fund_data, "qfii": qfii_data, "ss": ss_data
    }.get(t)):
        result = factor.compute(price_data)

    assert result is not None
    assert isinstance(result, pd.DataFrame)
    # Raw columns
    assert "fund_hold_count" in result.columns
    assert "qfii_hold_flag" in result.columns
    assert "ss_hold_flag" in result.columns
    # Change columns
    assert "fund_hold_count_chg" in result.columns
    assert "qfii_new_entry" in result.columns
    assert "ss_new_entry" in result.columns


def test_fund_hold_count():
    """fund_hold_count should reflect the latest quarter's fund count."""
    factor = InstitutionalFactor(
        cache_dir="./cache/institutional_test", include_change=False
    )
    instruments = ["SH600519"]
    price_data = _make_price_data(n_days=60, instruments=instruments)

    fund_data = _make_fund_data(instruments, n_quarters=2)

    with patch.object(factor, "_load_cache", side_effect=lambda t: {
        "fund": fund_data, "qfii": None, "ss": None
    }.get(t)):
        result = factor.compute(price_data)

    assert result is not None
    assert "fund_hold_count" in result.columns
    # After the last quarter date, fund_hold_count should be forward-filled
    last_q_date = pd.Timestamp("2025-09-30")
    after_last = result.loc["SH600519"].loc[
        result.loc["SH600519"].index > last_q_date
    ]
    if len(after_last) > 0:
        # All values after the last quarter should be the same (ffilled)
        assert (
            after_last["fund_hold_count"] == after_last["fund_hold_count"].iloc[0]
        ).all()


def test_qfii_hold_flag():
    """qfii_hold_flag should be 1 for QFII-held stocks and 0 otherwise."""
    factor = InstitutionalFactor(
        cache_dir="./cache/institutional_test", include_change=False
    )
    instruments = ["SH600519", "SZ000001"]
    price_data = _make_price_data(n_days=60, instruments=instruments)

    # Only SH600519 has QFII
    qfii_data = _make_qfii_data(["SH600519"])

    with patch.object(factor, "_load_cache", side_effect=lambda t: {
        "fund": None, "qfii": qfii_data, "ss": None
    }.get(t)):
        result = factor.compute(price_data)

    assert result is not None
    assert "qfii_hold_flag" in result.columns
    # SH600519 should have qfii_hold_flag == 1 (at least for some dates)
    maotai_flag = result.loc["SH600519", "qfii_hold_flag"]
    assert (maotai_flag == 1.0).any()


def test_qfii_new_entry():
    """qfii_new_entry should flag the date QFII entered with '新进'."""
    factor = InstitutionalFactor(
        cache_dir="./cache/institutional_test", include_change=True
    )
    instruments = ["SH600519"]
    price_data = _make_price_data(n_days=120, instruments=instruments)

    # QFII data with "新进" at a quarter date inside the price_data range
    # so that the 0→1 transition is detectable via diff.
    # Price data starts 2026-01-01; put "新进" at 2026-02-28 (a reporting date
    # within the range) so the transition from 0→1 is visible.
    qfii_data = pd.DataFrame(
        {
            "inst_count": [3.0, 5.0],
            "hold_shares": [1000000.0, 1400000.0],
            "hold_mv": [1800000000.0, 2400000000.0],
            "hold_change": ["新进", "增持"],
            "hold_change_shares": [1000000.0, 400000.0],
            "hold_change_pct": [100.0, 40.0],
        },
        index=pd.MultiIndex.from_tuples(
            [
                ("SH600519", pd.Timestamp("2026-02-28")),
                ("SH600519", pd.Timestamp("2026-03-31")),
            ],
            names=["instrument", "datetime"],
        ),
    )

    with patch.object(factor, "_load_cache", side_effect=lambda t: {
        "fund": None, "qfii": qfii_data, "ss": None
    }.get(t)):
        result = factor.compute(price_data)

    assert result is not None
    assert "qfii_new_entry" in result.columns
    # The "新进" is at 2026-02-28, which is inside the price_data range.
    # Before that date, qfii_new_entry should be 0 (NaN → 0 after fillna).
    # On the first trading day on/after 2026-02-28, the new_entry flag
    # transitions from 0 to 1.
    new_entries = result.loc["SH600519", "qfii_new_entry"]
    assert (new_entries == 1.0).any()


def test_include_change_false():
    """When include_change=False, no change columns should appear."""
    factor = InstitutionalFactor(
        cache_dir="./cache/institutional_test", include_change=False
    )
    instruments = ["SH600519"]
    price_data = _make_price_data(n_days=60, instruments=instruments)

    fund_data = _make_fund_data(instruments)
    qfii_data = _make_qfii_data(instruments)

    with patch.object(factor, "_load_cache", side_effect=lambda t: {
        "fund": fund_data, "qfii": qfii_data, "ss": None
    }.get(t)):
        result = factor.compute(price_data)

    assert result is not None
    assert "fund_hold_count_chg" not in result.columns
    assert "qfii_new_entry" not in result.columns
    # Raw columns should still be present
    assert "fund_hold_count" in result.columns
    assert "qfii_hold_flag" in result.columns


def test_missing_data_returns_none():
    """When all caches are empty, compute() should return None."""
    factor = InstitutionalFactor(cache_dir="./cache/institutional_nonexistent")
    price_data = _make_price_data()
    result = factor.compute(price_data)
    assert result is None


def test_reindex_to_price_data():
    """Result should be reindexed to exactly match price_data.index."""
    factor = InstitutionalFactor(
        cache_dir="./cache/institutional_test", include_change=True
    )
    instruments = ["SH600519", "SZ000001"]
    price_data = _make_price_data(n_days=30, instruments=instruments)

    fund_data = _make_fund_data(instruments, n_quarters=2)

    with patch.object(factor, "_load_cache", side_effect=lambda t: {
        "fund": fund_data, "qfii": None, "ss": None
    }.get(t)):
        result = factor.compute(price_data)

    assert result is not None
    assert result.index.equals(price_data.index)


def test_forward_fill_within_instrument():
    """Quarterly data should be forward-filled within each instrument."""
    factor = InstitutionalFactor(
        cache_dir="./cache/institutional_test", include_change=False
    )
    instruments = ["SH600519"]
    price_data = _make_price_data(n_days=60, instruments=instruments)

    # Only 2 quarter-end data points
    fund_data = pd.DataFrame(
        {
            "fund_count": [150.0, 160.0],
            "hold_shares": [5000000.0, 5500000.0],
            "hold_mv": [9000000000.0, 10000000000.0],
        },
        index=pd.MultiIndex.from_tuples(
            [
                ("SH600519", pd.Timestamp("2025-12-31")),
                ("SH600519", pd.Timestamp("2026-03-31")),
            ],
            names=["instrument", "datetime"],
        ),
    )

    with patch.object(factor, "_load_cache", side_effect=lambda t: {
        "fund": fund_data, "qfii": None, "ss": None
    }.get(t)):
        result = factor.compute(price_data)

    assert result is not None
    # After the first quarter date but before the second, fund_hold_count should be 150
    after_first = result.loc["SH600519"].loc[
        (result.loc["SH600519"].index >= pd.Timestamp("2025-12-31"))
        & (result.loc["SH600519"].index < pd.Timestamp("2026-03-31"))
    ]
    if len(after_first) > 0:
        assert (after_first["fund_hold_count"] == 150.0).all()


def test_backward_compat_setstate():
    """Old pickles missing new attributes should get safe defaults."""
    factor = InstitutionalFactor(include_change=True)
    # Simulate old pickle state missing 'include_change' and 'cache_dir'
    state = factor.__dict__.copy()
    del state["include_change"]
    del state["cache_dir"]

    new_factor = InstitutionalFactor.__new__(InstitutionalFactor)
    new_factor.__setstate__(state)
    assert new_factor.include_change is True
    assert new_factor.cache_dir == Path("./cache/institutional")


def test_bj_instruments_handled():
    """BJ exchange instruments should be handled correctly."""
    factor = InstitutionalFactor(
        cache_dir="./cache/institutional_test", include_change=True
    )
    instruments = ["SH600519", "BJ430001", "BJ920001"]
    price_data = _make_price_data(n_days=30, instruments=instruments)

    fund_data = _make_fund_data(["SH600519", "BJ430001"], n_quarters=2)

    with patch.object(factor, "_load_cache", side_effect=lambda t: {
        "fund": fund_data, "qfii": None, "ss": None
    }.get(t)):
        result = factor.compute(price_data)

    assert result is not None
    assert result.index.equals(price_data.index)
    # SH600519 and BJ430001 should have data; BJ920001 may be NaN
    for inst in ["SH600519", "BJ430001"]:
        inst_data = result.loc[inst]
        assert not inst_data.empty


def test_ss_hold_flag():
    """ss_hold_flag should be 1 for social-security-held stocks."""
    factor = InstitutionalFactor(
        cache_dir="./cache/institutional_test", include_change=False
    )
    instruments = ["SH601318", "SZ000001"]
    price_data = _make_price_data(n_days=60, instruments=instruments)

    # Only SH601318 has social security
    ss_data = _make_ss_data(["SH601318"])

    with patch.object(factor, "_load_cache", side_effect=lambda t: {
        "fund": None, "qfii": None, "ss": ss_data
    }.get(t)):
        result = factor.compute(price_data)

    assert result is not None
    assert "ss_hold_flag" in result.columns
    # SH601318 should have ss_hold_flag == 1
    pingan_flag = result.loc["SH601318", "ss_hold_flag"]
    assert (pingan_flag == 1.0).any()


def test_fund_hold_count_chg():
    """fund_hold_count_chg should capture changes between quarters."""
    factor = InstitutionalFactor(
        cache_dir="./cache/institutional_test", include_change=True
    )
    instruments = ["SH600519"]
    price_data = _make_price_data(n_days=120, instruments=instruments)

    # Fund count increases from 150 to 160 between quarters
    fund_data = pd.DataFrame(
        {
            "fund_count": [150.0, 160.0],
            "hold_shares": [5000000.0, 5500000.0],
            "hold_mv": [9000000000.0, 10000000000.0],
        },
        index=pd.MultiIndex.from_tuples(
            [
                ("SH600519", pd.Timestamp("2025-12-31")),
                ("SH600519", pd.Timestamp("2026-03-31")),
            ],
            names=["instrument", "datetime"],
        ),
    )

    with patch.object(factor, "_load_cache", side_effect=lambda t: {
        "fund": fund_data, "qfii": None, "ss": None
    }.get(t)):
        result = factor.compute(price_data)

    assert result is not None
    assert "fund_hold_count_chg" in result.columns
    # After the second quarter date, the change should be 10 (160 - 150)
    chg = result.loc["SH600519", "fund_hold_count_chg"]
    non_nan = chg.dropna()
    # There should be at least one positive change (10)
    positive_changes = non_nan[non_nan > 0]
    assert len(positive_changes) > 0
