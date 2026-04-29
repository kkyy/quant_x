"""Data loading layer wrapping qlib."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd

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
        instruments: Union[str, Sequence[str]] = "csi300",
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

        if isinstance(instruments, str):
            instrument_expr = D.instruments(instruments)
        else:
            instrument_expr = list(instruments)

        df = D.features(
            instrument_expr,
            fields,
            start_time=start_time,
            end_time=end_time,
        )
        df.index.names = ["instrument", "datetime"]
        df["real_close"] = df["$close"] / df["$factor"]
        return df

    # ── dataset ──────────────────────────────────────────────────────────────

    def build_dataset(
        self,
        segments: Dict[str, Tuple[str, str]],
        instruments: Optional[str] = None,
        label_horizon: Optional[int] = None,
    ):
        """Build a qlib DatasetH from Alpha158 (or configured handler).

        Parameters
        ----------
        segments       : dict of segment_name → (start_date, end_date)
        instruments    : market name or instrument list (default: config market.name)
        label_horizon  : CAP-12 — forward-return window in trading days.
                         When set, overrides the handler's label with an N-day
                         return: ``Ref($close, -(N+1)) / Ref($close, -1) - 1``.
                         ``None`` (default) uses the handler's built-in label
                         (Alpha158 default = 1-day return).
                         Can also be set via ``training.label_horizon`` in config.
        """
        self.init_qlib()
        from qlib.utils import init_instance_by_config
        from qlib.data.dataset import DatasetH

        instruments = instruments or self.config.get("market", {}).get("name", "csi300")
        model_cfg = self.config.get("model", {})
        handler_meta = model_cfg.get(
            "handler",
            {"class": "Alpha158", "module_path": "qlib.contrib.data.handler"},
        )

        # Resolve label_horizon: explicit arg > config key > None (handler default)
        if label_horizon is None:
            label_horizon = self.config.get("training", {}).get("label_horizon", None)
        if label_horizon is not None:
            label_horizon = int(label_horizon)

        train_seg = segments.get("train")
        all_ends = [v[1] for v in segments.values() if v[1]]
        all_starts = [v[0] for v in segments.values() if v[0]]

        handler_kwargs: dict = {
            "start_time": min(all_starts),
            "end_time": max(all_ends),
            "fit_start_time": train_seg[0] if train_seg else min(all_starts),
            "fit_end_time": train_seg[1] if train_seg else min(all_ends),
            "instruments": instruments,
        }

        # CAP-12: inject custom N-day label expression when label_horizon is set
        if label_horizon is not None and label_horizon >= 1:
            # Convention: Ref($close, -(N+1)) / Ref($close, -1) - 1
            # Matches Alpha158's built-in T+1 execution assumption:
            #   T+1 close as basis, T+(N+1) close as exit.
            label_expr = f"Ref($close, -{label_horizon + 1})/Ref($close, -1) - 1"
            handler_kwargs["label"] = [label_expr]
            logger.info(
                "CAP-12 multi-horizon label: horizon=%dd  expr=%s",
                label_horizon, label_expr,
            )

        handler_cfg = {
            "class": handler_meta["class"],
            "module_path": handler_meta["module_path"],
            "kwargs": handler_kwargs,
        }

        hd = init_instance_by_config(handler_cfg)
        return DatasetH(handler=hd, segments=segments)

    # ── instruments ──────────────────────────────────────────────────────────

    def list_instruments(self, market: str = "csi300") -> List[str]:
        self.init_qlib()
        from qlib.data import D
        return list(D.instruments(market))
