import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from quant_ex.features.valuation_factor import ValuationFactor


def _make_price_data(n_days=30, instruments=None):
    if instruments is None:
        instruments = ["SH600519", "SZ000001"]
    dates = pd.bdate_range("2026-02-01", periods=n_days)
    idx = pd.MultiIndex.from_product(
        [instruments, dates], names=["instrument", "datetime"]
    )
    return pd.DataFrame(
        {"real_close": np.random.uniform(10, 100, len(idx))}, index=idx
    )


def _make_valuation_data(instruments=None, n_days=30):
    """Create a synthetic valuation DataFrame with (instrument, datetime) MultiIndex."""
    if instruments is None:
        instruments = ["SH600519", "SZ000001"]
    dates = pd.bdate_range("2026-02-01", periods=n_days)
    idx = pd.MultiIndex.from_product(
        [instruments, dates], names=["instrument", "datetime"]
    )
    n = len(idx)
    return pd.DataFrame(
        {
            "market_cap": np.random.uniform(1e10, 1e12, n),
            "float_market_cap": np.random.uniform(5e9, 5e11, n),
            "total_shares": np.random.uniform(1e8, 1e10, n),
            "float_shares": np.random.uniform(5e7, 5e9, n),
            "pe_ttm": np.random.uniform(5, 50, n),
            "pe_static": np.random.uniform(4, 45, n),
            "pb": np.random.uniform(0.5, 10, n),
            "peg": np.random.uniform(0.3, 3.0, n),
            "pcf": np.random.uniform(5, 30, n),
            "ps_ttm": np.random.uniform(1, 20, n),
        },
        index=idx,
    )


def _write_valuation_cache(tmp_path, valuation_data):
    """Write per-stock valuation CSVs into cache dir, one per instrument."""
    cache_dir = tmp_path / "valuation"
    cache_dir.mkdir(parents=True, exist_ok=True)
    for inst in valuation_data.index.get_level_values(0).unique():
        stock_df = valuation_data.loc[inst].copy()
        stock_df.index = pd.MultiIndex.from_product(
            [[inst], stock_df.index], names=["instrument", "datetime"]
        )
        stock_df.to_csv(cache_dir / f"{inst}.csv")


def test_compute_returns_dataframe():
    valuation_data = _make_valuation_data()
    factor = ValuationFactor(precomputed=valuation_data)
    price_data = _make_price_data()
    result = factor.compute(price_data)
    assert result is not None
    assert isinstance(result, pd.DataFrame)
    assert "market_cap" in result.columns
    assert "pe_ttm" in result.columns
    assert "pb" in result.columns


def test_align_to_price_data_index():
    valuation_data = _make_valuation_data()
    factor = ValuationFactor(precomputed=valuation_data)
    price_data = _make_price_data()
    result = factor.compute(price_data)
    assert result is not None
    # Result should be reindexed to price_data.index
    assert result.index.equals(price_data.index)


def test_market_cap_in_output():
    """Verify absolute-value metrics (market_cap, float_market_cap, total_shares)
    are present in the output."""
    valuation_data = _make_valuation_data()
    factor = ValuationFactor(precomputed=valuation_data)
    price_data = _make_price_data()
    result = factor.compute(price_data)
    assert result is not None
    assert "market_cap" in result.columns
    assert "float_market_cap" in result.columns
    assert "total_shares" in result.columns
    assert "float_shares" in result.columns


def test_metrics_filter():
    """When metrics is specified, only those columns should be returned."""
    valuation_data = _make_valuation_data()
    factor = ValuationFactor(
        precomputed=valuation_data, metrics=["pe_ttm", "pb"]
    )
    price_data = _make_price_data()
    result = factor.compute(price_data)
    assert result is not None
    assert set(result.columns) == {"pe_ttm", "pb"}


def test_include_change():
    """When include_change=True, pe_ttm_chg, pb_chg, ps_ttm_chg, pcf_chg
    columns should be added."""
    valuation_data = _make_valuation_data()
    factor = ValuationFactor(
        precomputed=valuation_data,
        metrics=["pe_ttm", "pb", "ps_ttm", "pcf"],
        include_change=True,
    )
    price_data = _make_price_data()
    result = factor.compute(price_data)
    assert result is not None
    assert "pe_ttm_chg" in result.columns
    assert "pb_chg" in result.columns
    assert "ps_ttm_chg" in result.columns
    assert "pcf_chg" in result.columns


def test_include_change_with_single_instrument():
    """Change factors should work correctly with a single instrument."""
    valuation_data = _make_valuation_data(instruments=["SH600519"], n_days=10)
    factor = ValuationFactor(
        precomputed=valuation_data,
        metrics=["pe_ttm", "pb"],
        include_change=True,
    )
    price_data = _make_price_data(instruments=["SH600519"], n_days=10)
    result = factor.compute(price_data)
    assert result is not None
    assert "pe_ttm_chg" in result.columns
    # First row should be NaN (no prior period), later rows should have values
    first_chg = result["pe_ttm_chg"].iloc[0]
    assert pd.isna(first_chg)


def test_missing_data_returns_none():
    """When no cache files exist and no precomputed data, should return None."""
    factor = ValuationFactor(cache_dir="/nonexistent/path/valuation")
    price_data = _make_price_data()
    result = factor.compute(price_data)
    assert result is None


def test_backward_compat_setstate():
    """Old pickles should get default values for new attributes."""
    factor = ValuationFactor.__new__(ValuationFactor)
    # Simulate an old pickle with limited attributes
    factor.__dict__.update({
        "cache_dir": Path("./cache/valuation"),
    })
    factor.__setstate__(factor.__dict__)
    assert factor.include_change is False
    assert factor.metrics == [
        "market_cap", "float_market_cap", "total_shares", "float_shares",
        "pe_ttm", "pe_static", "pb", "peg", "pcf", "ps_ttm", "dyr",
    ]
    assert factor.precomputed is None
    assert factor.cache_ttl_days == 1


def test_load_from_cache_dir(tmp_path):
    """Factor should read per-stock CSVs from the cache directory."""
    valuation_data = _make_valuation_data(instruments=["SH600519"], n_days=10)
    _write_valuation_cache(tmp_path, valuation_data)

    factor = ValuationFactor(cache_dir=str(tmp_path / "valuation"))
    price_data = _make_price_data(instruments=["SH600519"], n_days=10)
    result = factor.compute(price_data)
    assert result is not None
    assert "market_cap" in result.columns
    assert "pe_ttm" in result.columns


def test_peg_and_pcf_in_output():
    """PEG and PCF metrics should be available (unique to ValuationFactor)."""
    valuation_data = _make_valuation_data()
    factor = ValuationFactor(
        precomputed=valuation_data, metrics=["peg", "pcf"]
    )
    price_data = _make_price_data()
    result = factor.compute(price_data)
    assert result is not None
    assert "peg" in result.columns
    assert "pcf" in result.columns


def test_forward_fill_within_instrument():
    """Valuation data with gaps should be forward-filled within each instrument."""
    # Create sparse data: only 3 dates for SH600519
    dates = pd.bdate_range("2026-02-01", periods=3)
    idx = pd.MultiIndex.from_tuples(
        [("SH600519", d) for d in dates],
        names=["instrument", "datetime"],
    )
    sparse_data = pd.DataFrame(
        {
            "market_cap": [1e12, 1.01e12, 1.02e12],
            "pe_ttm": [25.0, 25.5, 26.0],
        },
        index=idx,
    )

    # Price data has 10 days
    factor = ValuationFactor(precomputed=sparse_data, metrics=["market_cap", "pe_ttm"])
    price_data = _make_price_data(instruments=["SH600519"], n_days=10)
    result = factor.compute(price_data)
    assert result is not None
    # After forward-fill, later dates should have the last known value
    assert not result["market_cap"].iloc[-1] != result["market_cap"].iloc[-1]  # not NaN
