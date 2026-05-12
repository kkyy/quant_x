import pandas as pd

from quant_ex.backtest.grid_search import GridSearchBacktest


def test_grid_search_sorts_by_configured_rank_metric():
    searcher = GridSearchBacktest(
        engine=None,
        pred=pd.Series(dtype=float),
        config={"backtest": {"rank_metric": "information_ratio"}},
    )
    df = pd.DataFrame(
        [
            {"topk": 5, "sharpe": 2.0, "information_ratio": 0.1},
            {"topk": 10, "sharpe": 1.0, "information_ratio": 0.5},
        ]
    )

    sorted_df = searcher._sort_results(df)

    assert sorted_df.iloc[0]["topk"] == 10
