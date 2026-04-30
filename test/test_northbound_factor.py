import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch
from quant_ex.features.northbound_factor import NorthboundFactor


def _make_price_data(n_days=60, instruments=None):
    if instruments is None:
        instruments = ["SH600519", "SZ000001", "SH601318"]
    dates = pd.bdate_range("2026-02-01", periods=n_days)
    idx = pd.MultiIndex.from_product([instruments, dates], names=["instrument", "datetime"])
    return pd.DataFrame({"real_close": np.random.uniform(10, 100, len(idx))}, index=idx)


def _make_holdings_data(instruments, date_str="2026-04-29"):
    n = len(instruments)
    data = {
        "nb_hold_pct": [5.2, 3.1, 8.0][:n] if n <= 3 else [5.2 + i * 0.1 for i in range(n)],
        "nb_hold_mv": [1000.0, 500.0, 2000.0][:n] if n <= 3 else [1000.0 + i * 100 for i in range(n)],
        "nb_net_buy_ratio": [0.1, -0.05, 0.2][:n] if n <= 3 else [0.1 * ((-1) ** i) for i in range(n)],
        "nb_hold_pct_chg": [0.1, -0.05, 0.2][:n] if n <= 3 else [0.05 * ((-1) ** i) for i in range(n)],
    }
    idx = pd.MultiIndex.from_tuples(
        [(inst, pd.Timestamp(date_str)) for inst in instruments],
        names=["instrument", "datetime"],
    )
    return pd.DataFrame(data, index=idx)


def test_compute_returns_dataframe():
    factor = NorthboundFactor(windows=[5, 20], cache_dir="./cache/northbound_test")
    price_data = _make_price_data()
    holdings = _make_holdings_data(list(price_data.index.get_level_values(0).unique()))

    with patch.object(factor, "_load_holdings_cache", return_value=holdings):
        result = factor.compute(price_data)
    assert result is not None
    assert "nb_hold_pct" in result.columns
    assert "nb_hold_pct_chg_5d" in result.columns
    assert "nb_hold_pct_chg_20d" in result.columns


def test_compute_with_sector_aggregation():
    instruments = ["SH600519", "SZ000001"]
    sector_map = {"SH600519": "白酒", "SZ000001": "银行"}
    factor = NorthboundFactor(windows=[5], cache_dir="./cache/northbound_test", sector_map=sector_map)
    price_data = _make_price_data(instruments=instruments)
    holdings = _make_holdings_data(instruments)

    with patch.object(factor, "_load_holdings_cache", return_value=holdings):
        result = factor.compute(price_data)
    assert result is not None
    assert "nb_sector_hold_pct" in result.columns
    assert "nb_vs_sector_5d" in result.columns


def test_stocks_without_northbound_get_zero():
    factor = NorthboundFactor(windows=[5], cache_dir="./cache/northbound_test")
    price_data = _make_price_data(instruments=["SH600519", "SZ300001"])
    # Only SH600519 in holdings
    holdings = _make_holdings_data(["SH600519"])

    with patch.object(factor, "_load_holdings_cache", return_value=holdings):
        result = factor.compute(price_data)
    assert result is not None
    # SZ300001 should have 0, not NaN
    sz_data = result.loc["SZ300001"]
    assert (sz_data["nb_hold_pct"] == 0).all()


def test_align_to_price_data_index():
    factor = NorthboundFactor(windows=[5], cache_dir="./cache/northbound_test")
    price_data = _make_price_data(n_days=10)
    holdings = _make_holdings_data(list(price_data.index.get_level_values(0).unique()))

    with patch.object(factor, "_load_holdings_cache", return_value=holdings):
        result = factor.compute(price_data)
    assert result is not None
    assert result.index.equals(price_data.index)


def test_change_factors_computation():
    factor = NorthboundFactor(windows=[5], include_raw=True, include_change=True,
                               cache_dir="./cache/northbound_test")
    price_data = _make_price_data(n_days=30, instruments=["SH600519"])
    # Create holdings with time series
    dates = pd.bdate_range("2026-02-01", periods=30)
    idx = pd.MultiIndex.from_tuples(
        [("SH600519", d) for d in dates], names=["instrument", "datetime"]
    )
    holdings = pd.DataFrame({
        "nb_hold_pct": np.linspace(3.0, 5.0, 30),
        "nb_hold_mv": np.linspace(500, 800, 30),
        "nb_net_buy_ratio": np.random.uniform(-0.1, 0.1, 30),
        "nb_hold_pct_chg": np.random.uniform(-0.1, 0.1, 30),
    }, index=idx)

    with patch.object(factor, "_load_holdings_cache", return_value=holdings):
        result = factor.compute(price_data)
    assert result is not None
    assert "nb_hold_pct_chg_5d" in result.columns
