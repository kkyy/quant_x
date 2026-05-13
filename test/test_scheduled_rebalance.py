from __future__ import annotations

import json

import pandas as pd

from quant_ex.run_scheduled_rebalance import (
    _compute_portfolio_pnl,
    _convert_snapshot_to_actual_prices,
    _diff_positions,
    _format_report,
    _load_executed_positions_from_cache,
    _next_trading_day,
    _positions_after_actions,
    _previous_trading_day,
    _resolve_cfg_start_date,
    RebalanceAction,
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
    """Legacy path (account_value=0): convert qlib adjusted value to actual shares."""
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


def test_convert_snapshot_to_actual_prices_equal_weight(monkeypatch):
    """When account_value is provided, allocate equal-weight using actual prices."""
    snapshot = {
        "SH600001": {"shares": 500.0, "price": 40.0, "value": 20000.0},
        "SH600002": {"shares": 300.0, "price": 50.0, "value": 15000.0},
    }
    monkeypatch.setattr(
        "quant_ex.run_scheduled_rebalance._load_actual_close",
        lambda instrument, trade_date: 12.0,
    )

    converted = _convert_snapshot_to_actual_prices(
        snapshot, "2026-04-29",
        account_value=100000, max_position_pct=0.0,
    )

    # 2 stocks, equal weight = 0.5 each
    # shares = int(100000 * 0.5 / 12.0 / 100) * 100 = 4100
    assert converted["SH600001"]["shares"] == 4100.0
    assert converted["SH600002"]["shares"] == 4100.0
    assert converted["SH600001"]["price"] == 12.0
    assert "raw_target_value" not in converted["SH600001"]


def test_convert_snapshot_to_actual_prices_respects_max_pct(monkeypatch):
    """When max_position_pct caps the weight per stock."""
    snapshot = {
        "SH600001": {"shares": 500.0, "price": 40.0, "value": 20000.0},
        "SH600002": {"shares": 300.0, "price": 50.0, "value": 15000.0},
    }
    monkeypatch.setattr(
        "quant_ex.run_scheduled_rebalance._load_actual_close",
        lambda instrument, trade_date: 12.0,
    )

    converted = _convert_snapshot_to_actual_prices(
        snapshot, "2026-04-29",
        account_value=100000, max_position_pct=0.2,
    )

    # min(1/2, 0.2) = 0.2, so 100000 * 0.2 = 20000 per stock
    # shares = int(20000 / 12.0 / 100) * 100 = 1600
    assert converted["SH600001"]["shares"] == 1600.0
    assert converted["SH600002"]["shares"] == 1600.0


def test_portfolio_pnl_uses_default_entry_date(monkeypatch):
    calendar = pd.to_datetime(["2026-04-30", "2026-05-07", "2026-05-08"]).tolist()
    prices = {
        ("SH600001", "2026-04-30"): 10.0,
        ("SH600001", "2026-05-07"): 11.0,
    }

    def fake_close(instrument, trade_date):
        if trade_date == "2026-05-08":
            return 12.0
        return prices.get((instrument, trade_date))

    monkeypatch.setattr("quant_ex.run_scheduled_rebalance._load_actual_close", fake_close)

    pnl = _compute_portfolio_pnl(
        {"SH600001": {"shares": 100, "price": 12.0, "value": 1200.0}},
        "2026-05-08",
        calendar,
        default_entry_date="2026-04-30",
    )

    assert pnl["cum_pnl"] == 200.0
    assert pnl["daily_pnl"] == 100.0
    assert pnl["per_stock"][0]["entry_date"] == "2026-04-30"


def test_format_report_shows_model_target_when_only_shares_change():
    cfg = {"market": "csi1000", "topk": 1, "n_drop": 1, "hold_thresh": 5, "start_date": "2026-04-30"}
    target = {"SH600001": {"shares": 100, "price": 10.0, "value": 1000.0}}
    model_target = {"SH600001": {"shares": 200, "price": 10.0, "value": 2000.0}}

    report = _format_report(
        trade_date="2026-05-08",
        next_trade_date="2026-05-11",
        latest_position_date="2026-05-08",
        cfg=cfg,
        target=target,
        actions=[],
        metrics={},
        mock=False,
        model_target=model_target,
    )

    assert "模型选股目标" in report
    assert "200股" in report


def test_load_executed_positions_from_legacy_cache_report(tmp_path, monkeypatch):
    payload = {
        "trade_date": "2026-05-12",
        "next_trade_date": "2026-05-13",
        "created_at": "2026-05-12T20:00:00",
        "strategy": {"market": "csi300"},
        "report": "\n".join(
            [
                "量化调仓信号",
                "目标持仓摘要:",
                "SH600115 中国东航 [航空运输]: 6700股 约29,748元",
                "SZ000651 格力电器 [空调]: 700股 约28,238元",
            ]
        ),
    }
    (tmp_path / "rebalance_2026-05-12.json").write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "quant_ex.run_scheduled_rebalance._load_actual_close",
        lambda instrument, trade_date: 5.0 if instrument == "SH600115" else 40.0,
    )

    positions = _load_executed_positions_from_cache(
        {"cache_dir": str(tmp_path), "market": "csi300"},
        "2026-05-13",
    )

    assert positions is not None
    assert positions["SH600115"]["shares"] == 6700.0
    assert positions["SH600115"]["price"] == 5.0
    assert positions["SH600115"]["value"] == 33500.0
    assert positions["SH600115"]["entry_date"] == "2026-05-13"
    assert positions["SZ000651"]["entry_date"] == "2026-05-13"


def test_load_executed_positions_prefers_structured_executed_positions(tmp_path, monkeypatch):
    payload = {
        "trade_date": "2026-05-12",
        "next_trade_date": "2026-05-13",
        "created_at": "2026-05-12T20:00:00",
        "strategy": {"market": "csi300"},
        "target_positions": {
            "SH600001": {"shares": 100, "price": 10.0, "value": 1000.0, "entry_date": "2026-05-13"},
        },
        "executed_positions": {
            "SH600002": {"shares": 200, "price": 8.0, "value": 1600.0, "entry_date": "2026-04-30"},
        },
    }
    (tmp_path / "rebalance_2026-05-12.json").write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )
    monkeypatch.setattr("quant_ex.run_scheduled_rebalance._load_actual_close", lambda instrument, trade_date: 9.0)

    positions = _load_executed_positions_from_cache(
        {"cache_dir": str(tmp_path), "market": "csi300"},
        "2026-05-13",
    )

    assert positions == {
        "SH600002": {
            "shares": 200.0,
            "price": 9.0,
            "value": 1800.0,
            "entry_date": "2026-04-30",
        }
    }


def test_positions_after_actions_keeps_filtered_small_diffs_as_previous():
    previous = {
        "SH600001": {"shares": 100, "price": 10.0, "value": 1000.0, "entry_date": "2026-04-30"},
        "SH600002": {"shares": 200, "price": 8.0, "value": 1600.0, "entry_date": "2026-04-30"},
    }
    target = {
        "SH600001": {"shares": 200, "price": 11.0, "value": 2200.0},
        "SH600003": {"shares": 100, "price": 20.0, "value": 2000.0},
    }
    actions = [RebalanceAction("buy", "SH600003", 100, 20.0, 2000.0)]

    positions = _positions_after_actions(previous, target, actions)

    assert positions["SH600001"]["shares"] == 100.0
    assert positions["SH600001"]["entry_date"] == "2026-04-30"
    assert positions["SH600003"]["shares"] == 100.0
    assert "SH600002" in positions
