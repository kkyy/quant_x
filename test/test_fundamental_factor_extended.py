import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch
from quant_ex.features.fundamental_factor import FundamentalFactor


def _make_price_data(n_days=30, instruments=None):
    if instruments is None:
        instruments = ["SH600519"]
    dates = pd.bdate_range("2026-02-01", periods=n_days)
    idx = pd.MultiIndex.from_product([instruments, dates], names=["instrument", "datetime"])
    return pd.DataFrame({"real_close": np.random.uniform(10, 100, len(idx))}, index=idx)


def test_extended_metrics_groups():
    """metrics=['valuation', 'profitability', 'growth', 'cashflow'] should enable all metrics."""
    factor = FundamentalFactor(metrics=["valuation", "profitability", "growth", "cashflow"])
    expected = {"pe_ttm", "pb", "ps_ttm", "dyr", "roe", "roa", "gross_margin",
                "net_margin", "revenue_growth", "profit_growth", "ocf_to_np"}
    assert expected.issubset(set(factor.metrics))


def test_profitability_metrics_only():
    """metrics=['profitability'] should only include profitability columns."""
    factor = FundamentalFactor(metrics=["profitability"])
    assert "roe" in factor.metrics
    assert "pe_ttm" not in factor.metrics


def test_change_factors_appended():
    """include_change=True should add roe_chg, margin_chg, rev_accel columns."""
    factor = FundamentalFactor(metrics=["profitability", "growth"], include_change=True)
    precomputed = pd.DataFrame({
        "roe": [12.0, 13.0, 14.0],
        "gross_margin": [40.0, 41.0, 42.0],
        "revenue_growth": [10.0, 12.0, 8.0],
    }, index=pd.MultiIndex.from_tuples(
        [("SH600519", pd.Timestamp(f"2025-{str(i).zfill(2)}-01")) for i in range(1, 4)],
        names=["instrument", "datetime"],
    ))
    factor.precomputed = precomputed
    price_data = _make_price_data()
    result = factor.compute(price_data)
    assert result is not None
    assert "roe_chg" in result.columns
    assert "margin_chg" in result.columns
    assert "rev_accel" in result.columns


def test_backward_compat_valuation_only():
    """Default metrics=['valuation'] should produce same columns as old default."""
    factor = FundamentalFactor(metrics=["valuation"])
    assert set(factor.metrics) == {"pe_ttm", "pb", "ps_ttm", "dyr"}


def test_uses_financial_fetcher_for_extended():
    """When extended metrics are requested, should use FinancialFetcher."""
    factor = FundamentalFactor(metrics=["profitability"])
    assert factor._use_fetcher is True


def test_uses_old_path_for_valuation_only():
    """When only valuation metrics requested, should use old akshare path."""
    factor = FundamentalFactor(metrics=["valuation"])
    assert factor._use_fetcher is False
