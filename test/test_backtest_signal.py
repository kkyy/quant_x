import pandas as pd

from quant_ex.backtest.engine import stabilize_signal


def test_stabilize_signal_sorts_index_and_breaks_ties_deterministically():
    idx = pd.MultiIndex.from_tuples(
        [
            ("BBB", pd.Timestamp("2024-01-03")),
            ("AAA", pd.Timestamp("2024-01-02")),
            ("BBB", pd.Timestamp("2024-01-02")),
            ("AAA", pd.Timestamp("2024-01-03")),
        ],
        names=["instrument", "datetime"],
    )
    pred = pd.Series([1.0, 1.0, 1.0, 1.0], index=idx)

    first = stabilize_signal(pred)
    second = stabilize_signal(pred.sample(frac=1, random_state=7))

    assert first.equals(second)
    assert list(first.index) == [
        ("AAA", pd.Timestamp("2024-01-02")),
        ("BBB", pd.Timestamp("2024-01-02")),
        ("AAA", pd.Timestamp("2024-01-03")),
        ("BBB", pd.Timestamp("2024-01-03")),
    ]
    assert first.loc[("AAA", pd.Timestamp("2024-01-02"))] != 1.0
