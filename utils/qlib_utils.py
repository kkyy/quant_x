"""Qlib-specific utility functions."""
from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


def positions_to_shares_df(positions_dict: dict, lot_size: int = 100) -> pd.DataFrame:
    """Convert qlib backtest positions dict → tidy DataFrame."""
    records = []
    for date, data in positions_dict.items():
        pos = data.position
        account_value = pos.get("now_account_value")
        cash = pos.get("cash", 0)
        record = {
            "date": pd.to_datetime(date),
            "account_value": account_value,
            "position_value": account_value - cash,
        }
        for symbol, info in pos.items():
            if not isinstance(info, dict) or "amount" not in info:
                continue
            adj = (info["amount"] // lot_size) * lot_size
            record[symbol] = adj * info["price"]
        records.append(record)

    return pd.DataFrame(records).set_index("date").sort_index().fillna(0)


def compute_trade_signals(
    positions_df: pd.DataFrame,
    price_data: pd.DataFrame,
    min_shares: int = 10,
) -> str:
    """Compute buy/sell signals from position changes, return formatted string."""
    value_cols = [c for c in positions_df.columns if c not in ("account_value", "position_value")]

    positions_long = (
        positions_df.reset_index()
        .melt(id_vars=["date"], value_vars=value_cols, var_name="instrument", value_name="amount")
        .rename(columns={"date": "datetime"})
    )

    merged = positions_long.merge(
        price_data.reset_index()[["datetime", "instrument", "real_close"]],
        on=["datetime", "instrument"],
        how="left",
    )
    merged["shares"] = merged["amount"] / merged["real_close"]

    shares_pivot = merged.pivot(index="datetime", columns="instrument", values="shares").fillna(0)
    diff = shares_pivot.diff().fillna(0)

    lines = []
    for date, row in diff.iterrows():
        buys = row[row > min_shares]
        sells = row[row < -min_shares]
        if not buys.empty or not sells.empty:
            lines.append(f"\n📅 {date.strftime('%Y-%m-%d')} 交易信号:")
            for s, q in buys.items():
                lines.append(f"  ✅ 买入 {s}: {q:.0f} 股")
            for s, q in sells.items():
                lines.append(f"  ❌ 卖出 {s}: {-q:.0f} 股")

    return "\n".join(lines) if lines else "无交易信号"


def load_recorder_model(exp_name: str, recorder_id: str):
    """Load model from a qlib MLflow recorder."""
    from qlib.workflow import R
    recorder = R.get_recorder(recorder_id=recorder_id, experiment_name=exp_name)
    return recorder.load_object("trained_model")


def filter_pred_basic(pred: pd.Series, exclude_kcb: bool = True, exclude_list=None) -> pd.Series:
    """Quick filter on prediction series."""
    mask = pd.Series(True, index=pred.index)
    instrs = pred.index.get_level_values("instrument")

    if exclude_kcb:
        mask &= ~instrs.str.startswith("SH688")

    if exclude_list:
        mask &= ~instrs.isin(exclude_list)

    return pred[mask]
