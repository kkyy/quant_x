from __future__ import annotations

import pandas as pd

from quant_ex.run_scheduled_rebalance import (
    _diff_positions,
    _next_trading_day,
    _previous_trading_day,
)


def test_diff_positions_classifies_buy_reduce_and_sell():
    previous = {
        "SH600001": {"shares": 300, "price": 10.0, "value": 3000.0},
        "SH600002": {"shares": 500, "price": 20.0, "value": 10000.0},
        "SH600003": {"shares": 200, "price": 30.0, "value": 6000.0},
    }
    target = {
        "SH600001": {"shares": 500, "price": 11.0, "value": 5500.0},
        "SH600002": {"shares": 300, "price": 21.0, "value": 6300.0},
        "SH600004": {"shares": 100, "price": 40.0, "value": 4000.0},
    }

    actions = {(item.action, item.instrument): item.shares for item in _diff_positions(previous, target)}

    assert actions[("buy", "SH600001")] == 200
    assert actions[("reduce", "SH600002")] == 200
    assert actions[("sell", "SH600003")] == 200
    assert actions[("buy", "SH600004")] == 100


def test_next_trading_day_uses_calendar():
    calendar = pd.to_datetime(["2026-04-27", "2026-04-28", "2026-04-30"]).tolist()

    next_day, exact = _next_trading_day(pd.Timestamp("2026-04-28"), calendar)

    assert next_day == "2026-04-30"
    assert exact is True


def test_previous_trading_day_uses_calendar():
    calendar = pd.to_datetime(["2026-04-27", "2026-04-28", "2026-04-30"]).tolist()

    previous_day, exact = _previous_trading_day(pd.Timestamp("2026-04-30"), calendar)

    assert previous_day == "2026-04-28"
    assert exact is True
