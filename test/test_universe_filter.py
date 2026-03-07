import pandas as pd

from quant_ex.data.universe import UniverseFilter


def test_min_price_filter_is_aligned_per_prediction_timestamp():
    pred_index = pd.MultiIndex.from_tuples(
        [
            ("AAA", pd.Timestamp("2024-01-02")),
            ("AAA", pd.Timestamp("2024-01-03")),
            ("BBB", pd.Timestamp("2024-01-02")),
            ("BBB", pd.Timestamp("2024-01-03")),
        ],
        names=["instrument", "datetime"],
    )
    pred = pd.Series([1.0, 2.0, 3.0, 4.0], index=pred_index)

    price_index = pd.MultiIndex.from_tuples(
        [
            ("AAA", pd.Timestamp("2024-01-02")),
            ("AAA", pd.Timestamp("2024-01-03")),
            ("BBB", pd.Timestamp("2024-01-02")),
            ("BBB", pd.Timestamp("2024-01-03")),
        ],
        names=["instrument", "datetime"],
    )
    price_data = pd.DataFrame(
        {"real_close": [9.0, 11.0, 12.0, 8.0]},
        index=price_index,
    )

    universe_filter = UniverseFilter({"universe_filter": {"min_price": 10}})

    filtered = universe_filter.filter(pred, price_data=price_data)

    assert list(filtered.index) == [
        ("AAA", pd.Timestamp("2024-01-03")),
        ("BBB", pd.Timestamp("2024-01-02")),
    ]