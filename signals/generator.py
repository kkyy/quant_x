"""
Daily trading signal generation.

Workflow:
1. Load latest model from qlib recorder
2. Generate predictions on test segment
3. Apply universe filter
4. Compute target TopK positions
5. Diff against current positions → buy/sell signals
6. Format human-readable report
"""
from __future__ import annotations
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class SignalGenerator:
    """Generate and format daily trading signals."""

    def __init__(
        self,
        config: dict,
        data_loader,
        universe_filter=None,
        sector_provider=None,
    ):
        self.config = config
        self.data_loader = data_loader
        self.universe_filter = universe_filter
        self.sector_provider = sector_provider

    # ── public ────────────────────────────────────────────────────────────────

    def generate(
        self,
        model,
        dataset,
        current_positions: Optional[Dict[str, float]] = None,
        account_value: float = 1_000_000,
        trade_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate trading signals.

        Args:
            model:             Trained model (qlib or LGBMAlphaModel)
            dataset:           qlib DatasetH
            current_positions: {instrument: shares_held}
            account_value:     Current portfolio value in CNY
            trade_date:        Date string (default: today)

        Returns:
            Signal dict with keys: date, top_stocks, sector_info,
            target_positions, signals, account_value
        """
        trade_date = trade_date or datetime.now().strftime("%Y-%m-%d")

        pred = model.predict(dataset, segment="test")

        if self.universe_filter is not None:
            pred = self.universe_filter.filter(pred)

        latest_dt = pred.index.get_level_values("datetime").max()
        latest_pred = pred.xs(latest_dt, level="datetime")

        strat_cfg = self.config.get("strategy", {}).get("topk_dropout", {})
        topk = strat_cfg.get("topk", 10)
        top_stocks = latest_pred.nlargest(topk)

        prices = self._fetch_prices(list(top_stocks.index), trade_date)

        sector_info: Dict[str, str] = {}
        if self.sector_provider is not None:
            sm = self.sector_provider.get_map()
            sector_info = {i: sm.get(i, "") for i in top_stocks.index}

        target_positions = self._target_positions(top_stocks, prices, account_value)
        signals = self._diff_signals(target_positions, current_positions or {}, prices)

        return {
            "date": trade_date,
            "latest_prediction_date": str(latest_dt)[:10],
            "top_stocks": top_stocks.to_dict(),
            "sector_info": sector_info,
            "target_positions": target_positions,
            "signals": signals,
            "account_value": account_value,
        }

    def format_report(self, data: Dict[str, Any]) -> str:
        """Return a human-readable signal report string."""
        lines = [
            "╔" + "═" * 50 + "╗",
            "║  📊 量化选股信号报告" + " " * 30 + "║",
            "╠" + "═" * 50 + "╣",
            f"║  日期:   {data['date']}" + " " * (41 - len(data['date'])) + "║",
            f"║  数据截止: {data['latest_prediction_date']}" + " " * (39 - len(data['latest_prediction_date'])) + "║",
            f"║  账户估值: ¥{data['account_value']:,.0f}",
            "╠" + "═" * 50 + "╣",
            "║  目标持仓 (Top-K)",
            "╠" + "═" * 50 + "╣",
        ]

        for inst, info in data["target_positions"].items():
            sec = data.get("sector_info", {}).get(inst, "")
            sec_str = f"[{sec}]" if sec else ""
            lines.append(
                f"  {inst} {sec_str}\n"
                f"    评分={info['score']:.4f}  价格=¥{info['price']:.2f}"
                f"  目标={info['target_shares']}股  ≈¥{info['target_value']:,.0f}"
            )

        lines += ["", "─" * 52, "  🔔 交易信号", "─" * 52]

        buys    = [s for s in data["signals"] if s["action"] == "buy"]
        reduces = [s for s in data["signals"] if s["action"] == "reduce"]
        sells   = [s for s in data["signals"] if s["action"] == "sell"]

        if buys:
            lines.append("买入:")
            for s in buys:
                lines.append(f"  ✅ {s['instrument']}  +{s['shares']:.0f}股 @ ¥{s['price']:.2f}  ≈¥{s['value']:,.0f}")
        if reduces:
            lines.append("减仓:")
            for s in reduces:
                lines.append(f"  ⚖️  {s['instrument']}  -{s['shares']:.0f}股 @ ¥{s['price']:.2f}")
        if sells:
            lines.append("卖出 (不在持仓目标):")
            for s in sells:
                lines.append(f"  ❌ {s['instrument']}  -{s['shares']:.0f}股 @ ¥{s['price']:.2f}  ≈¥{s['value']:,.0f}")
        if not buys and not reduces and not sells:
            lines.append("  无交易信号（持仓不变）")

        return "\n".join(lines)

    # ── private ───────────────────────────────────────────────────────────────

    def _fetch_prices(self, instruments: List[str], date: str) -> Dict[str, float]:
        try:
            from datetime import timedelta
            start = (pd.Timestamp(date) - pd.Timedelta(days=10)).strftime("%Y-%m-%d")
            mkt = self.config.get("market", {}).get("name", "csi300")
            price_df = self.data_loader.load_price_data(
                instruments=mkt, start_time=start, end_time=date
            )
            latest = (
                price_df["real_close"]
                .reset_index()
                .sort_values("datetime")
                .groupby("instrument")["real_close"]
                .last()
            )
            return {i: float(latest.get(i, 0)) for i in instruments}
        except Exception as e:
            logger.warning(f"Price fetch failed: {e}")
            return {}

    def _target_positions(
        self,
        top_stocks: pd.Series,
        prices: Dict[str, float],
        account_value: float,
        lot_size: int = 100,
    ) -> Dict[str, Dict]:
        port_cfg = self.config.get("strategy", {}).get("portfolio", {})
        max_pct = port_cfg.get("max_position_pct", 0.25)
        n = len(top_stocks)
        weight = min(1.0 / n, max_pct) if n > 0 else 0

        positions = {}
        for inst, score in top_stocks.items():
            price = prices.get(inst, 0)
            if price <= 0:
                continue
            shares = int(account_value * weight / price / lot_size) * lot_size
            positions[inst] = {
                "score":         float(score),
                "price":         float(price),
                "target_shares": shares,
                "target_value":  shares * price,
                "weight":        weight,
            }
        return positions

    def _diff_signals(
        self,
        target: Dict[str, Dict],
        current: Dict[str, float],
        prices: Dict[str, float],
    ) -> List[Dict]:
        signals = []

        for inst, info in target.items():
            curr = current.get(inst, 0)
            diff = info["target_shares"] - curr
            if diff > 0:
                signals.append({"action": "buy",    "instrument": inst,
                                 "shares": diff,  "price": info["price"],
                                 "value": diff * info["price"], "score": info["score"]})
            elif diff < 0:
                signals.append({"action": "reduce", "instrument": inst,
                                 "shares": abs(diff), "price": info["price"],
                                 "value": abs(diff) * info["price"], "score": info["score"]})

        for inst, shares in current.items():
            if inst not in target and shares > 0:
                price = prices.get(inst, 0)
                signals.append({"action": "sell", "instrument": inst,
                                 "shares": shares, "price": price,
                                 "value": shares * price, "score": None})
        return signals
