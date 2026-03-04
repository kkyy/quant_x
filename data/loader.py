"""Data loading layer wrapping qlib."""
from __future__ import annotations
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class DataLoader:
    """Wraps qlib initialisation and data fetching with lazy init."""

    def __init__(self, config: dict):
        self.config = config
        self._initialized = False

    # ── qlib init ────────────────────────────────────────────────────────────

    def init_qlib(self):
        if self._initialized:
            return
        import qlib
        qlib.init(
            provider_uri=self.config["qlib"]["provider_uri"],
            region=self.config["qlib"]["region"],
        )
        self._initialized = True
        logger.info("qlib initialised")

    # ── price data ───────────────────────────────────────────────────────────

    def load_price_data(
        self,
        instruments: str = "csi300",
        start_time: str = "2015-01-01",
        end_time: Optional[str] = None,
        fields: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Return OHLCV DataFrame with (instrument, datetime) MultiIndex."""
        self.init_qlib()
        from qlib.data import D

        if fields is None:
            fields = [
                "$open", "$high", "$low", "$close",
                "$factor", "$adjclose", "$volume", "$change", "$amount",
            ]
        end_time = end_time or datetime.now().strftime("%Y-%m-%d")

        df = D.features(D.instruments(instruments), fields, start_time=start_time, end_time=end_time)
        df.index.names = ["instrument", "datetime"]
        df["real_close"] = df["$close"] / df["$factor"]
        return df

    # ── dataset ──────────────────────────────────────────────────────────────

    def build_dataset(
        self,
        segments: Dict[str, Tuple[str, str]],
        instruments: Optional[str] = None,
    ):
        """Build a qlib DatasetH from Alpha158 (or configured handler)."""
        self.init_qlib()
        from qlib.utils import init_instance_by_config
        from qlib.data.dataset import DatasetH

        instruments = instruments or self.config.get("market", {}).get("name", "csi300")
        model_cfg = self.config.get("model", {})
        handler_meta = model_cfg.get(
            "handler",
            {"class": "Alpha158", "module_path": "qlib.contrib.data.handler"},
        )

        train_seg = segments.get("train")
        all_ends = [v[1] for v in segments.values() if v[1]]
        all_starts = [v[0] for v in segments.values() if v[0]]

        handler_cfg = {
            "class": handler_meta["class"],
            "module_path": handler_meta["module_path"],
            "kwargs": {
                "start_time": min(all_starts),
                "end_time": max(all_ends),
                "fit_start_time": train_seg[0] if train_seg else min(all_starts),
                "fit_end_time": train_seg[1] if train_seg else min(all_ends),
                "instruments": instruments,
            },
        }

        hd = init_instance_by_config(handler_cfg)
        return DatasetH(handler=hd, segments=segments)

    # ── instruments ──────────────────────────────────────────────────────────

    def list_instruments(self, market: str = "csi300") -> List[str]:
        self.init_qlib()
        from qlib.data import D
        return list(D.instruments(market))
