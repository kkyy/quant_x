from __future__ import annotations

import pandas as pd

from quant_ex.run_scheduled_rebalance import (
    _convert_snapshot_to_actual_prices,
    _diff_positions,
    _next_trading_day,
    _previous_trading_day,
    _resolve_cfg_start_date,
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


def test_resolve_cfg_start_date_signal_date_alias():
    cfg = {"start_date": "signal_date", "_start_date_raw": "signal_date"}
    calendar = pd.to_datetime(["2026-04-28", "2026-04-29", "2026-04-30"]).tolist()

    resolved = _resolve_cfg_start_date(cfg, pd.Timestamp("2026-04-29"), calendar)

    assert resolved["start_date"] == "2026-04-29"
    assert cfg["start_date"] == "signal_date"


def test_resolve_cfg_start_date_previous_trade_date_alias():
    cfg = {"start_date": "previous_trade_date", "_start_date_raw": "previous_trade_date"}
    calendar = pd.to_datetime(["2026-04-28", "2026-04-29", "2026-04-30"]).tolist()

    resolved = _resolve_cfg_start_date(cfg, pd.Timestamp("2026-04-29"), calendar)

    assert resolved["start_date"] == "2026-04-28"


def test_convert_snapshot_to_actual_prices_uses_target_value(monkeypatch):
    snapshot = {
        "SH600001": {"shares": 500.0, "price": 40.0, "value": 20000.0},
    }
    monkeypatch.setattr(
        "quant_ex.run_scheduled_rebalance._load_actual_close",
        lambda instrument, trade_date: 12.0,
    )

    converted = _convert_snapshot_to_actual_prices(snapshot, "2026-04-29")

    assert converted["SH600001"]["shares"] == 1600.0
    assert converted["SH600001"]["price"] == 12.0
    assert converted["SH600001"]["value"] == 19200.0
    assert converted["SH600001"]["raw_target_value"] == 20000.0
