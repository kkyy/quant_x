import pandas as pd

from quant_ex.signals.postprocess import postprocess_signal


def test_postprocess_signal_daily_rank():
    idx = pd.MultiIndex.from_tuples(
        [
            ("AAA", pd.Timestamp("2024-01-02")),
            ("BBB", pd.Timestamp("2024-01-02")),
            ("CCC", pd.Timestamp("2024-01-02")),
        ],
        names=["instrument", "datetime"],
    )
    pred = pd.Series([0.2, 0.8, 0.5], index=idx)
    config = {"signal": {"postprocess": {"enabled": True, "daily_transform": "rank"}}}

    ranked = postprocess_signal(pred, config=config)

    assert ranked.loc[("BBB", pd.Timestamp("2024-01-02"))] == 1.0
    assert ranked.loc[("AAA", pd.Timestamp("2024-01-02"))] == 1 / 3


def test_postprocess_signal_industry_neutralize_before_rank():
    idx = pd.MultiIndex.from_tuples(
        [
            ("AAA", pd.Timestamp("2024-01-02")),
            ("BBB", pd.Timestamp("2024-01-02")),
            ("CCC", pd.Timestamp("2024-01-02")),
            ("DDD", pd.Timestamp("2024-01-02")),
        ],
        names=["instrument", "datetime"],
    )
    pred = pd.Series([1.0, 3.0, 10.0, 14.0], index=idx)
    config = {
        "signal": {
            "postprocess": {
                "enabled": True,
                "daily_transform": "none",
                "industry_neutralize": True,
                "min_group_size": 2,
            }
        }
    }
    sector_map = {"AAA": "bank", "BBB": "bank", "CCC": "tech", "DDD": "tech"}

    neutral = postprocess_signal(pred, config=config, sector_map=sector_map)

    assert neutral.loc[("AAA", pd.Timestamp("2024-01-02"))] == -1.0
    assert neutral.loc[("BBB", pd.Timestamp("2024-01-02"))] == 1.0
    assert neutral.loc[("CCC", pd.Timestamp("2024-01-02"))] == -2.0
    assert neutral.loc[("DDD", pd.Timestamp("2024-01-02"))] == 2.0


def test_postprocess_signal_stock_vs_sector_filter():
    dates = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
    instruments = ["AAA", "BBB", "CCC", "DDD"]
    price_idx = pd.MultiIndex.from_product(
        [instruments, dates],
        names=["instrument", "datetime"],
    )
    close = [
        100.0, 110.0, 121.0,  # AAA: strong versus bank sector
        100.0, 101.0, 102.01,  # BBB: weak versus bank sector
        100.0, 100.0, 105.0,  # CCC: strong versus tech sector
        100.0, 100.0, 90.0,  # DDD: weak versus tech sector
    ]
    price_data = pd.DataFrame({"real_close": close}, index=price_idx)

    pred_idx = pd.MultiIndex.from_product(
        [instruments, [pd.Timestamp("2024-01-03")]],
        names=["instrument", "datetime"],
    )
    pred = pd.Series([0.4, 0.9, 0.3, 0.8], index=pred_idx)
    config = {
        "signal": {
            "postprocess": {
                "enabled": True,
                "daily_transform": "none",
                "stock_vs_sector_filter": {
                    "enabled": True,
                    "window": 2,
                    "keep_top_pct": 0.25,
                },
            }
        }
    }
    sector_map = {"AAA": "bank", "BBB": "bank", "CCC": "tech", "DDD": "tech"}

    filtered = postprocess_signal(
        pred,
        config=config,
        sector_map=sector_map,
        price_data=price_data,
    )

    assert set(filtered.index.get_level_values("instrument")) == {"AAA", "CCC"}


def test_postprocess_signal_stock_vs_sector_multiplicative_weight():
    """multiplicative_weight mode blends score with SVS rank instead of hard filtering."""
    dates = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
    instruments = ["AAA", "BBB", "CCC", "DDD"]
    price_idx = pd.MultiIndex.from_product(
        [instruments, dates],
        names=["instrument", "datetime"],
    )
    close = [
        100.0, 110.0, 121.0,  # AAA: strong versus bank sector
        100.0, 101.0, 102.01,  # BBB: weak versus bank sector
        100.0, 100.0, 105.0,  # CCC: strong versus tech sector
        100.0, 100.0, 90.0,  # DDD: weak versus tech sector
    ]
    price_data = pd.DataFrame({"real_close": close}, index=price_idx)

    pred_idx = pd.MultiIndex.from_product(
        [instruments, [pd.Timestamp("2024-01-03")]],
        names=["instrument", "datetime"],
    )
    pred = pd.Series([0.4, 0.9, 0.3, 0.8], index=pred_idx)
    config = {
        "signal": {
            "postprocess": {
                "enabled": True,
                "daily_transform": "none",
                "stock_vs_sector_filter": {
                    "enabled": True,
                    "window": 2,
                    "keep_top_pct": 0.5,
                    "mode": "multiplicative_weight",
                    "weight_strength": 0.5,
                },
            }
        }
    }
    sector_map = {"AAA": "bank", "BBB": "bank", "CCC": "tech", "DDD": "tech"}

    result = postprocess_signal(
        pred,
        config=config,
        sector_map=sector_map,
        price_data=price_data,
    )

    # Soft mode should NOT drop any stocks — all 4 remain
    assert set(result.index.get_level_values("instrument")) == {"AAA", "BBB", "CCC", "DDD"}

    # AAA (high SVS) and CCC (high SVS) should get a boost relative to
    # BBB and DDD which have low SVS ranks.  With weight_strength=0.5
    # and pred values 0.4, 0.9, 0.3, 0.8, the blended score order may
    # shift.  At minimum the result should be a ranked Series of length 4.
    assert len(result) == 4
    assert result.notna().all()


def test_postprocess_signal_stock_vs_sector_residual_add():
    """residual_add mode adds weighted SVS rank to the score instead of hard filtering."""
    dates = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
    instruments = ["AAA", "BBB", "CCC", "DDD"]
    price_idx = pd.MultiIndex.from_product(
        [instruments, dates],
        names=["instrument", "datetime"],
    )
    close = [
        100.0, 110.0, 121.0,  # AAA: strong versus bank sector
        100.0, 101.0, 102.01,  # BBB: weak versus bank sector
        100.0, 100.0, 105.0,  # CCC: strong versus tech sector
        100.0, 100.0, 90.0,  # DDD: weak versus tech sector
    ]
    price_data = pd.DataFrame({"real_close": close}, index=price_idx)

    pred_idx = pd.MultiIndex.from_product(
        [instruments, [pd.Timestamp("2024-01-03")]],
        names=["instrument", "datetime"],
    )
    pred = pd.Series([0.4, 0.9, 0.3, 0.8], index=pred_idx)
    config = {
        "signal": {
            "postprocess": {
                "enabled": True,
                "daily_transform": "none",
                "stock_vs_sector_filter": {
                    "enabled": True,
                    "window": 2,
                    "keep_top_pct": 0.5,
                    "mode": "residual_add",
                    "weight_strength": 0.3,
                },
            }
        }
    }
    sector_map = {"AAA": "bank", "BBB": "bank", "CCC": "tech", "DDD": "tech"}

    result = postprocess_signal(
        pred,
        config=config,
        sector_map=sector_map,
        price_data=price_data,
    )

    # Soft mode should NOT drop any stocks
    assert set(result.index.get_level_values("instrument")) == {"AAA", "BBB", "CCC", "DDD"}
    assert len(result) == 4
    assert result.notna().all()


def test_postprocess_signal_stock_vs_sector_hard_filter_default():
    """Default mode (no mode key in config) should behave as hard_filter."""
    dates = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
    instruments = ["AAA", "BBB", "CCC", "DDD"]
    price_idx = pd.MultiIndex.from_product(
        [instruments, dates],
        names=["instrument", "datetime"],
    )
    close = [
        100.0, 110.0, 121.0,
        100.0, 101.0, 102.01,
        100.0, 100.0, 105.0,
        100.0, 100.0, 90.0,
    ]
    price_data = pd.DataFrame({"real_close": close}, index=price_idx)

    pred_idx = pd.MultiIndex.from_product(
        [instruments, [pd.Timestamp("2024-01-03")]],
        names=["instrument", "datetime"],
    )
    pred = pd.Series([0.4, 0.9, 0.3, 0.8], index=pred_idx)
    # No "mode" key — should default to hard_filter
    config = {
        "signal": {
            "postprocess": {
                "enabled": True,
                "daily_transform": "none",
                "stock_vs_sector_filter": {
                    "enabled": True,
                    "window": 2,
                    "keep_top_pct": 0.25,
                },
            }
        }
    }
    sector_map = {"AAA": "bank", "BBB": "bank", "CCC": "tech", "DDD": "tech"}

    filtered = postprocess_signal(
        pred,
        config=config,
        sector_map=sector_map,
        price_data=price_data,
    )

    # Same as original test: hard filter keeps only top 25%
    assert set(filtered.index.get_level_values("instrument")) == {"AAA", "CCC"}
