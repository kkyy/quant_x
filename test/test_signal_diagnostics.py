import pandas as pd

from quant_ex.backtest.signal_diagnostics import compute_signal_ic


def test_compute_signal_ic_uses_future_returns():
    dates = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"])
    instruments = [f"AAA{i:03d}" for i in range(10)]
    idx = pd.MultiIndex.from_product(
        [instruments, dates],
        names=["instrument", "datetime"],
    )
    prices = []
    for i in range(10):
        prices.extend([10.0, 10.0 + i, 11.0 + i])
    price = pd.DataFrame(
        {"real_close": prices},
        index=idx,
    )
    pred_idx = pd.MultiIndex.from_tuples(
        [(inst, dates[0]) for inst in instruments],
        names=["instrument", "datetime"],
    )
    pred = pd.Series(range(10), index=pred_idx)

    metrics = compute_signal_ic(pred, price, horizon=1)

    assert metrics["ic_days"] == 1
    assert metrics["rank_ic"] == 1.0
