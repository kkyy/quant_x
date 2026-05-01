"""Tests for InsiderFactor."""
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch
from quant_ex.features.insider_factor import InsiderFactor


def _make_price_data(n_days=60, instruments=None):
    """Build a price_data DataFrame with (instrument, datetime) MultiIndex."""
    if instruments is None:
        instruments = ["SH600519", "SZ000001", "SH601318"]
    dates = pd.bdate_range("2026-02-01", periods=n_days)
    idx = pd.MultiIndex.from_product(
        [instruments, dates], names=["instrument", "datetime"]
    )
    return pd.DataFrame(
        {"real_close": np.random.uniform(10, 100, len(idx))}, index=idx
    )


def _make_insider_data(instruments=None, with_multiple_tx=False):
    """Build transaction-level insider data with (instrument, datetime) MultiIndex.

    By default produces one transaction per instrument/date. If with_multiple_tx
    is True, some (instrument, date) pairs will have multiple transactions to
    test aggregation.
    """
    if instruments is None:
        instruments = ["SH600519", "SZ000001"]

    rows = []
    dates = pd.bdate_range("2026-02-01", periods=10)

    for i, inst in enumerate(instruments):
        for j, dt in enumerate(dates):
            # Alternate buy/sell
            direction = 1 if (i + j) % 2 == 0 else -1
            pct = 0.05 if direction == 1 else 0.03
            shares = 10000.0 if direction == 1 else 8000.0
            pct_float = 0.06 if direction == 1 else 0.04

            rows.append(
                {
                    "instrument": inst,
                    "datetime": dt,
                    "direction": direction,
                    "shares_changed": shares,
                    "pct_of_total": pct,
                    "pct_of_float": pct_float,
                    "shareholder": f"股东_{i}_{j}",
                    "buy_count_dummy": 1 if direction == 1 else 0,
                    "sell_count_dummy": 1 if direction == -1 else 0,
                }
            )

            if with_multiple_tx and j == 0:
                # Add a second transaction on the same (instrument, date)
                rows.append(
                    {
                        "instrument": inst,
                        "datetime": dt,
                        "direction": 1,
                        "shares_changed": 5000.0,
                        "pct_of_total": 0.02,
                        "pct_of_float": 0.025,
                        "shareholder": f"股东_{i}_{j}_extra",
                        "buy_count_dummy": 1,
                        "sell_count_dummy": 0,
                    }
                )

    df = pd.DataFrame(rows)
    df = df.set_index(["instrument", "datetime"])
    return df


def test_compute_returns_dataframe():
    """compute() should return a DataFrame with factor columns."""
    factor = InsiderFactor(
        windows=[5, 20], cache_dir="./cache/insider_test", lookback_days=90
    )
    price_data = _make_price_data(instruments=["SH600519", "SZ000001"])
    insider_data = _make_insider_data(
        instruments=["SH600519", "SZ000001"]
    )

    with patch.object(factor, "_load_insider_cache", return_value=insider_data):
        result = factor.compute(price_data)

    assert result is not None
    assert "insider_net_buy_pct_5d" in result.columns
    assert "insider_net_buy_pct_20d" in result.columns
    assert "insider_buy_count_5d" in result.columns
    assert "insider_sell_count_5d" in result.columns
    assert "insider_buy_count_20d" in result.columns
    assert "insider_sell_count_20d" in result.columns


def test_align_to_price_data_index():
    """Result index should match price_data.index exactly."""
    factor = InsiderFactor(windows=[5], cache_dir="./cache/insider_test")
    price_data = _make_price_data(n_days=10, instruments=["SH600519"])
    insider_data = _make_insider_data(instruments=["SH600519"])

    with patch.object(factor, "_load_insider_cache", return_value=insider_data):
        result = factor.compute(price_data)

    assert result is not None
    assert result.index.equals(price_data.index)


def test_aggregation_from_transactions():
    """Transaction-level data should be correctly aggregated to (instrument, date)."""
    factor = InsiderFactor(windows=[5], cache_dir="./cache/insider_test")
    price_data = _make_price_data(n_days=10, instruments=["SH600519"])
    # Use data with multiple transactions per (instrument, date)
    insider_data = _make_insider_data(
        instruments=["SH600519"], with_multiple_tx=True
    )

    with patch.object(factor, "_load_insider_cache", return_value=insider_data):
        result = factor.compute(price_data)

    assert result is not None
    # First date should have aggregated values from 2 transactions
    # (one buy direction=1 with pct=0.05, one extra buy with pct=0.02)
    # Net buy pct for 5d window on first date = 0.05*1 + 0.02*1 = 0.07
    first_date = price_data.index.get_level_values(1).min()
    val = result.loc[("SH600519", first_date), "insider_net_buy_pct_5d"]
    assert abs(val - 0.07) < 1e-10


def test_net_buy_factor():
    """insider_net_buy_pct should correctly reflect signed insider activity."""
    factor = InsiderFactor(windows=[5], cache_dir="./cache/insider_test")
    price_data = _make_price_data(n_days=10, instruments=["SH600519"])

    # Build data: 3 consecutive buy days, then 2 sell days
    dates = pd.bdate_range("2026-02-01", periods=5)
    rows = []
    for i, dt in enumerate(dates[:3]):
        rows.append(
            {
                "instrument": "SH600519",
                "datetime": dt,
                "direction": 1,
                "shares_changed": 10000.0,
                "pct_of_total": 0.05,
                "pct_of_float": 0.06,
                "shareholder": f"买家{i}",
            }
        )
    for i, dt in enumerate(dates[3:]):
        rows.append(
            {
                "instrument": "SH600519",
                "datetime": dt,
                "direction": -1,
                "shares_changed": 8000.0,
                "pct_of_total": 0.03,
                "pct_of_float": 0.04,
                "shareholder": f"卖家{i}",
            }
        )

    insider_data = pd.DataFrame(rows).set_index(["instrument", "datetime"])

    with patch.object(factor, "_load_insider_cache", return_value=insider_data):
        result = factor.compute(price_data)

    assert result is not None
    # On the 5th day, the 5d rolling sum should be:
    # 0.05 + 0.05 + 0.05 + (-0.03) + (-0.03) = 0.09
    fifth_date = dates[4]
    val = result.loc[("SH600519", fifth_date), "insider_net_buy_pct_5d"]
    assert abs(val - 0.09) < 1e-10


def test_missing_data_returns_none():
    """When no cache data is available, compute() should return None."""
    factor = InsiderFactor(windows=[5], cache_dir="./cache/insider_test")
    price_data = _make_price_data(n_days=10)

    with patch.object(
        factor, "_load_insider_cache", return_value=None
    ):
        result = factor.compute(price_data)

    assert result is None


def test_empty_cache_returns_none():
    """When cache directory is empty, compute() should return None."""
    factor = InsiderFactor(
        windows=[5], cache_dir="./cache/insider_empty_test"
    )
    price_data = _make_price_data(n_days=10)

    # _load_insider_cache finds no files → returns None
    with patch.object(
        factor, "_load_insider_cache", return_value=None
    ):
        result = factor.compute(price_data)

    assert result is None


def test_stocks_without_insider_get_zero():
    """Instruments with no insider trades should get 0, not NaN."""
    factor = InsiderFactor(windows=[5], cache_dir="./cache/insider_test")
    price_data = _make_price_data(
        n_days=10, instruments=["SH600519", "SZ300001"]
    )
    # Only SH600519 has insider data
    insider_data = _make_insider_data(instruments=["SH600519"])

    with patch.object(factor, "_load_insider_cache", return_value=insider_data):
        result = factor.compute(price_data)

    assert result is not None
    # SZ300001 should have 0 values, not NaN
    sz_data = result.loc["SZ300001"]
    assert (sz_data == 0).all().all()


def test_backward_compat_setstate():
    """Old pickles missing new attributes should get safe defaults."""
    # Simulate unpickling: Python creates a bare object without calling
    # __init__, then calls __setstate__ with the pickled state dict.
    # An old pickle would not have lookback_days or windows.
    factor = object.__new__(InsiderFactor)
    state = {"cache_dir": "./cache/insider", "cache_ttl_days": 1}
    factor.__setstate__(state)
    assert factor.lookback_days == 90
    assert factor.windows == [5, 20, 60]
    assert factor.cache_ttl_days == 1


def test_aggregate_transactions_static():
    """Test the static _aggregate_transactions method directly."""
    # Build transaction-level data with known values
    rows = [
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2026-04-25"),
            "direction": 1,
            "shares_changed": 10000.0,
            "pct_of_total": 0.05,
            "pct_of_float": 0.06,
        },
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2026-04-25"),
            "direction": 1,
            "shares_changed": 5000.0,
            "pct_of_total": 0.02,
            "pct_of_float": 0.025,
        },
        {
            "instrument": "SH600519",
            "datetime": pd.Timestamp("2026-04-25"),
            "direction": -1,
            "shares_changed": 3000.0,
            "pct_of_total": 0.015,
            "pct_of_float": 0.02,
        },
    ]
    tx_data = pd.DataFrame(rows).set_index(["instrument", "datetime"])

    result = InsiderFactor._aggregate_transactions(tx_data)

    assert result is not None
    row = result.iloc[0]
    # net_buy_shares = 10000*1 + 5000*1 + 3000*(-1) = 12000
    assert abs(row["net_buy_shares"] - 12000.0) < 1e-10
    # pct_of_total_net = 0.05*1 + 0.02*1 + 0.015*(-1) = 0.055
    assert abs(row["pct_of_total_net"] - 0.055) < 1e-10
    # buy_count = 2 (two 增持)
    assert row["buy_count"] == 2
    # sell_count = 1 (one 减持)
    assert row["sell_count"] == 1
    # tx_count = 3
    assert row["tx_count"] == 3
