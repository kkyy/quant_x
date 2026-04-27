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
