import pandas as pd

from quant_ex.backtest.metrics import compute_metrics


def test_compute_metrics_uses_net_return_when_cost_column_exists():
    report = pd.DataFrame(
        {
            "return": [0.10, 0.00],
            "cost": [0.02, 0.01],
        },
        index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
    )

    metrics = compute_metrics(report, annual_factor=2)

    expected_cum = (1 + 0.08) * (1 - 0.01) - 1
    assert metrics["cum_return"] == round(expected_cum, 4)
