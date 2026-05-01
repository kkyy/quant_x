import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch
from quant_ex.features.analyst_factor import AnalystFactor


def _make_price_data(n_days=30, instruments=None):
    if instruments is None:
        instruments = ["SH600519", "SZ000001", "SH601318"]
    dates = pd.bdate_range("2026-02-02", periods=n_days)
    idx = pd.MultiIndex.from_product(
        [instruments, dates], names=["instrument", "datetime"]
    )
    return pd.DataFrame(
        {"real_close": np.random.uniform(10, 100, len(idx))}, index=idx
    )


def _make_analyst_data(instruments, date_str="2026-02-02"):
    """Build fake analyst forecast cache data with ratings and EPS forecasts.

    Default date_str is set to the first date of _make_price_data so that
    forward-fill propagates values to all subsequent dates.
    """
    n = len(instruments)
    # Vary the data so the factor computation is non-trivial
    report_counts = [30, 15, 25][:n] if n <= 3 else [10 + i * 5 for i in range(n)]
    buy_ratings = [10, 5, 8][:n] if n <= 3 else [3 + i for i in range(n)]
    outperform_ratings = [8, 4, 7][:n] if n <= 3 else [2 + i for i in range(n)]
    neutral_ratings = [7, 3, 5][:n] if n <= 3 else [1 + i for i in range(n)]
    underperform_ratings = [3, 2, 3][:n] if n <= 3 else [1 for _ in range(n)]
    sell_ratings = [2, 1, 2][:n] if n <= 3 else [1 for _ in range(n)]
    current_eps = [55.0, 1.5, 12.0][:n] if n <= 3 else [5.0 + i for i in range(n)]
    consensus_eps = [60.0, 1.7, 14.0][:n] if n <= 3 else [6.0 + i for i in range(n)]

    idx = pd.MultiIndex.from_tuples(
        [(inst, pd.Timestamp(date_str)) for inst in instruments],
        names=["instrument", "datetime"],
    )
    return pd.DataFrame(
        {
            "report_count": report_counts,
            "buy_rating": buy_ratings,
            "outperform_rating": outperform_ratings,
            "neutral_rating": neutral_ratings,
            "underperform_rating": underperform_ratings,
            "sell_rating": sell_ratings,
            "current_eps_forecast": current_eps,
            "consensus_eps_forecast": consensus_eps,
        },
        index=idx,
    )


def test_compute_returns_dataframe():
    factor = AnalystFactor(cache_dir="./cache/analyst_test")
    price_data = _make_price_data()
    forecast = _make_analyst_data(
        list(price_data.index.get_level_values(0).unique())
    )

    with patch.object(factor, "_load_forecast_cache", return_value=forecast):
        result = factor.compute(price_data)
    assert result is not None
    assert isinstance(result, pd.DataFrame)


def test_align_to_price_data_index():
    factor = AnalystFactor(cache_dir="./cache/analyst_test")
    price_data = _make_price_data(n_days=10)
    forecast = _make_analyst_data(
        list(price_data.index.get_level_values(0).unique())
    )

    with patch.object(factor, "_load_forecast_cache", return_value=forecast):
        result = factor.compute(price_data)
    assert result is not None
    assert result.index.equals(price_data.index)


def test_buy_rating_ratio():
    """buy_rating_ratio = (buy + outperform) / total_ratings."""
    factor = AnalystFactor(cache_dir="./cache/analyst_test")
    instruments = ["SH600519"]
    price_data = _make_price_data(n_days=5, instruments=instruments)
    forecast = _make_analyst_data(instruments)

    with patch.object(factor, "_load_forecast_cache", return_value=forecast):
        result = factor.compute(price_data)
    assert result is not None
    assert "buy_rating_ratio" in result.columns

    # Verify: (10 + 8) / (10 + 8 + 7 + 3 + 2) = 18 / 30 = 0.6
    ratio = result["buy_rating_ratio"].dropna().iloc[0]
    expected = (10 + 8) / (10 + 8 + 7 + 3 + 2)
    assert np.isclose(ratio, expected, atol=1e-6)


def test_analyst_coverage():
    """analyst_coverage should pass through report_count."""
    factor = AnalystFactor(cache_dir="./cache/analyst_test")
    instruments = ["SH600519"]
    price_data = _make_price_data(n_days=5, instruments=instruments)
    forecast = _make_analyst_data(instruments)

    with patch.object(factor, "_load_forecast_cache", return_value=forecast):
        result = factor.compute(price_data)
    assert result is not None
    assert "analyst_coverage" in result.columns

    # Should match the report_count from the forecast data
    coverage = result["analyst_coverage"].dropna().iloc[0]
    assert coverage == 30  # report_count for SH600519 in _make_analyst_data


def test_consensus_eps_growth():
    """consensus_eps_growth = (forward - current) / |current|."""
    factor = AnalystFactor(cache_dir="./cache/analyst_test")
    instruments = ["SH600519"]
    price_data = _make_price_data(n_days=5, instruments=instruments)
    forecast = _make_analyst_data(instruments)

    with patch.object(factor, "_load_forecast_cache", return_value=forecast):
        result = factor.compute(price_data)
    assert result is not None
    assert "consensus_eps_growth" in result.columns

    # Verify: (60.0 - 55.0) / |55.0| = 5.0 / 55.0 ≈ 0.0909
    growth = result["consensus_eps_growth"].dropna().iloc[0]
    expected = (60.0 - 55.0) / abs(55.0)
    assert np.isclose(growth, expected, atol=1e-6)


def test_missing_data_returns_nan():
    """Stocks without analyst coverage should have NaN, not 0."""
    factor = AnalystFactor(cache_dir="./cache/analyst_test")
    instruments = ["SH600519", "SZ300999"]  # SZ300999 not in forecast
    price_data = _make_price_data(n_days=5, instruments=instruments)
    # Only SH600519 has forecast data
    forecast = _make_analyst_data(["SH600519"])

    with patch.object(factor, "_load_forecast_cache", return_value=forecast):
        result = factor.compute(price_data)
    assert result is not None

    # SZ300999 should be NaN (not 0) for all analyst factors
    sz_data = result.loc["SZ300999"]
    for col in ["analyst_coverage", "buy_rating_ratio", "consensus_eps_growth"]:
        assert sz_data[col].isna().all(), f"{col} should be NaN for uncovered stock"


def test_no_cache_returns_none():
    """If no forecast cache data is available, compute should return None."""
    factor = AnalystFactor(cache_dir="./cache/analyst_test_nonexistent")
    price_data = _make_price_data()

    with patch.object(
        factor, "_load_forecast_cache", return_value=None
    ):
        result = factor.compute(price_data)
    assert result is None


def test_backward_compat_setstate():
    """Old pickles missing new attributes should get safe defaults."""
    factor = AnalystFactor(cache_dir="./cache/analyst_test")
    # Simulate an old pickle with missing attributes
    state = {"name": "analyst"}
    factor2 = AnalystFactor.__new__(AnalystFactor)
    factor2.__setstate__(state)
    assert hasattr(factor2, "cache_dir")
    assert hasattr(factor2, "cache_ttl_days")
    assert hasattr(factor2, "windows")
    assert factor2.cache_ttl_days == 3
    assert factor2.windows == [5, 20]


def test_eps_level_factors_present():
    """current_eps_forecast and consensus_eps_forecast should be in output as level factors."""
    factor = AnalystFactor(cache_dir="./cache/analyst_test")
    instruments = ["SH600519"]
    price_data = _make_price_data(n_days=5, instruments=instruments)
    forecast = _make_analyst_data(instruments)

    with patch.object(factor, "_load_forecast_cache", return_value=forecast):
        result = factor.compute(price_data)
    assert result is not None
    assert "current_eps_forecast" in result.columns
    assert "consensus_eps_forecast" in result.columns

    cur = result["current_eps_forecast"].dropna().iloc[0]
    fwd = result["consensus_eps_forecast"].dropna().iloc[0]
    assert np.isclose(cur, 55.0)
    assert np.isclose(fwd, 60.0)
