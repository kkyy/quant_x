from pathlib import Path

import pandas as pd

import run_fundamental_audit as audit


def _price_data() -> pd.DataFrame:
    idx = pd.MultiIndex.from_product(
        [["SH600000", "SZ000001"], pd.date_range("2024-01-01", periods=5, freq="D")],
        names=["instrument", "datetime"],
    )
    return pd.DataFrame(
        {
            "real_close": [
                10,
                11,
                12,
                13,
                14,
                20,
                20,
                22,
                24,
                26,
            ]
        },
        index=idx,
    )


def test_load_cached_source_applies_release_lag(tmp_path: Path):
    cache_dir = tmp_path / "financial"
    cache_dir.mkdir()
    data = pd.DataFrame(
        {"roe": [10.0]},
        index=pd.MultiIndex.from_tuples(
            [("SH600000", pd.Timestamp("2024-03-31"))],
            names=["instrument", "datetime"],
        ),
    )
    data.to_csv(cache_dir / "SH600000.csv")

    loaded, sources = audit.load_cached_source("financial", cache_dir, lag_days=45)

    assert ("SH600000", pd.Timestamp("2024-05-15")) in loaded.index
    assert loaded.loc[("SH600000", pd.Timestamp("2024-05-15")), "roe"] == 10.0
    assert sources == {"roe": "financial"}


def test_align_to_price_index_forward_fills_within_instrument():
    price = _price_data()
    factors = pd.DataFrame(
        {"pb": [1.0, 2.0]},
        index=pd.MultiIndex.from_tuples(
            [
                ("SH600000", pd.Timestamp("2024-01-02")),
                ("SZ000001", pd.Timestamp("2024-01-03")),
            ],
            names=["instrument", "datetime"],
        ),
    )

    aligned = audit.align_to_price_index(factors, price.index)

    assert pd.isna(aligned.loc[("SH600000", pd.Timestamp("2024-01-01")), "pb"])
    assert aligned.loc[("SH600000", pd.Timestamp("2024-01-03")), "pb"] == 1.0
    assert pd.isna(aligned.loc[("SZ000001", pd.Timestamp("2024-01-02")), "pb"])
    assert aligned.loc[("SZ000001", pd.Timestamp("2024-01-04")), "pb"] == 2.0


def test_compute_forward_returns_matches_t_plus_one_entry_convention():
    price = _price_data()

    ret = audit.compute_forward_returns(price, horizon=1)

    assert ret.loc[("SH600000", pd.Timestamp("2024-01-01"))] == (12 / 11 - 1)
    assert ret.loc[("SZ000001", pd.Timestamp("2024-01-02"))] == (24 / 22 - 1)


def test_transform_factors_rank_is_daily_cross_sectional():
    idx = pd.MultiIndex.from_product(
        [["A", "B", "C"], [pd.Timestamp("2024-01-01")]],
        names=["instrument", "datetime"],
    )
    factors = pd.DataFrame({"quality": [3.0, 1.0, 2.0]}, index=idx)

    ranked = audit.transform_factors(factors, "rank", winsor_lower=0, winsor_upper=1)

    assert ranked.loc[("B", pd.Timestamp("2024-01-01")), "quality"] == 1 / 3
    assert ranked.loc[("A", pd.Timestamp("2024-01-01")), "quality"] == 1.0


def test_evaluate_one_returns_factor_diagnostics():
    price = _price_data()
    factors = pd.DataFrame(
        {"quality": range(len(price))},
        index=price.index,
    )
    returns = audit.compute_forward_returns(price, horizon=1)

    result = audit.evaluate_one(
        factors,
        returns,
        horizon=1,
        transform="rank",
        column_sources={"quality": "synthetic"},
        min_stability_ic=0.0,
    )

    assert set(["factor", "source", "horizon", "transform", "ic_mean", "coverage"]).issubset(
        result.columns
    )
    assert result.loc[0, "factor"] == "quality"
    assert result.loc[0, "source"] == "synthetic"
