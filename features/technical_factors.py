"""
Technical indicator factors computed directly from OHLCV price data.

These factors do NOT require the qlib expression engine — they are computed
with pure pandas/numpy and can be used in any pipeline.

Registered name: "technical"

Factors produced
----------------
- ma_cross_{fast}_{slow}d   : fast MA / slow MA ratio (trend)
- rsi_{w}d                  : RSI-style momentum (0–1)
- bb_pos_{w}d               : Bollinger Band position (0=lower, 1=upper)
- atr_ratio_{w}d            : ATR / close (normalised volatility)
- obv_mom_{w}d              : OBV rolling momentum
- vwap_dev_{w}d             : deviation from rolling VWAP
"""
from __future__ import annotations

import logging
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)

# Column name aliases tried in order
_CLOSE_COLS = ("real_close", "$close", "close")
_HIGH_COLS  = ("real_high",  "$high",  "high")
_LOW_COLS   = ("real_low",   "$low",   "low")
_VOL_COLS   = ("real_volume","$volume","volume")
_OPEN_COLS  = ("real_open",  "$open",  "open")


def _pick_col(df: pd.DataFrame, candidates: Tuple[str, ...]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _pivot(price_df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Pivot (instrument, datetime) MultiIndex → datetime × instrument."""
    return price_df[col].unstack("instrument")


@FactorRegistry.register("technical")
class TechnicalFactorEngine(BaseFactor):
    """
    Compute technical indicator factors from price data.

    Parameters
    ----------
    ma_pairs    : list of (fast, slow) tuples for MA-cross factors
    rsi_windows : look-back windows for RSI-style factor
    bb_windows  : look-back windows for Bollinger Band position
    atr_windows : look-back windows for ATR ratio
    obv_windows : look-back windows for OBV momentum
    vwap_windows: look-back windows for VWAP deviation
    """

    def __init__(
        self,
        ma_pairs: Optional[List[Tuple[int, int]]] = None,
        rsi_windows: Optional[List[int]] = None,
        bb_windows: Optional[List[int]] = None,
        atr_windows: Optional[List[int]] = None,
        obv_windows: Optional[List[int]] = None,
        vwap_windows: Optional[List[int]] = None,
    ):
        self.ma_pairs    = ma_pairs    or [(5, 20), (10, 60)]
        self.rsi_windows = rsi_windows or [14, 20]
        self.bb_windows  = bb_windows  or [20]
        self.atr_windows = atr_windows or [14, 20]
        self.obv_windows = obv_windows or [10, 20]
        self.vwap_windows= vwap_windows or [5, 10, 20]

    # ── public ────────────────────────────────────────────────────────────────

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        close_col  = _pick_col(price_data, _CLOSE_COLS)
        high_col   = _pick_col(price_data, _HIGH_COLS)
        low_col    = _pick_col(price_data, _LOW_COLS)
        vol_col    = _pick_col(price_data, _VOL_COLS)

        if close_col is None:
            logger.warning("TechnicalFactorEngine: no close price column found, skipping.")
            return None

        close = _pivot(price_data, close_col)
        high  = _pivot(price_data, high_col)  if high_col  else None
        low   = _pivot(price_data, low_col)   if low_col   else None
        vol   = _pivot(price_data, vol_col)   if vol_col   else None

        pieces: List[pd.Series] = []

        # MA cross
        for fast, slow in self.ma_pairs:
            s = self._ma_cross(close, fast, slow)
            pieces.append(s.stack().rename(f"ma_cross_{fast}_{slow}d"))

        # RSI-style
        for w in self.rsi_windows:
            s = self._rsi(close, w)
            pieces.append(s.stack().rename(f"rsi_{w}d"))

        # Bollinger Band position
        for w in self.bb_windows:
            s = self._bb_position(close, w)
            pieces.append(s.stack().rename(f"bb_pos_{w}d"))

        # ATR ratio
        if high is not None and low is not None:
            for w in self.atr_windows:
                s = self._atr_ratio(close, high, low, w)
                pieces.append(s.stack().rename(f"atr_ratio_{w}d"))

        # OBV momentum
        if vol is not None:
            for w in self.obv_windows:
                s = self._obv_mom(close, vol, w)
                pieces.append(s.stack().rename(f"obv_mom_{w}d"))

        # VWAP deviation
        if vol is not None:
            for w in self.vwap_windows:
                s = self._vwap_dev(close, vol, w)
                pieces.append(s.stack().rename(f"vwap_dev_{w}d"))

        if not pieces:
            return None

        result = pd.concat(pieces, axis=1)
        result.index.names = ["datetime", "instrument"]
        return result.swaplevel().sort_index()

    # ── private factor functions ──────────────────────────────────────────────

    @staticmethod
    def _ma_cross(close: pd.DataFrame, fast: int, slow: int) -> pd.DataFrame:
        """fast_MA / slow_MA – 1."""
        fast_ma = close.rolling(fast).mean()
        slow_ma = close.rolling(slow).mean()
        return fast_ma / (slow_ma + 1e-8) - 1

    @staticmethod
    def _rsi(close: pd.DataFrame, window: int) -> pd.DataFrame:
        """RSI = mean(up) / (mean(up) + mean(down)), range [0, 1]."""
        delta = close.diff()
        up   = delta.clip(lower=0)
        down = (-delta).clip(lower=0)
        avg_up   = up.rolling(window).mean()
        avg_down = down.rolling(window).mean()
        return avg_up / (avg_up + avg_down + 1e-8)

    @staticmethod
    def _bb_position(close: pd.DataFrame, window: int) -> pd.DataFrame:
        """Position within Bollinger Bands: 0=lower band, 1=upper band."""
        ma  = close.rolling(window).mean()
        std = close.rolling(window).std()
        upper = ma + 2 * std
        lower = ma - 2 * std
        return (close - lower) / (upper - lower + 1e-8)

    @staticmethod
    def _atr_ratio(
        close: pd.DataFrame,
        high: pd.DataFrame,
        low: pd.DataFrame,
        window: int,
    ) -> pd.DataFrame:
        """Average True Range / close (normalised volatility)."""
        prev_close = close.shift(1)
        tr_dict = {}
        for inst in close.columns:
            h = high[inst] if inst in high.columns else pd.Series(np.nan, index=close.index)
            l = low[inst]  if inst in low.columns  else pd.Series(np.nan, index=close.index)
            c = close[inst]
            pc = c.shift(1)
            tr_dict[inst] = pd.concat(
                [h - l, (h - pc).abs(), (l - pc).abs()], axis=1
            ).max(axis=1)

        tr_df = pd.DataFrame(tr_dict)
        atr   = tr_df.rolling(window).mean()
        return atr / (close + 1e-8)

    @staticmethod
    def _obv_mom(close: pd.DataFrame, vol: pd.DataFrame, window: int) -> pd.DataFrame:
        """Rolling OBV momentum: OBV_t / OBV_{t-window}."""
        sign = np.sign(close.diff())
        obv  = (sign * vol).cumsum()
        return obv / (obv.shift(window).abs() + 1e-8) - 1

    @staticmethod
    def _vwap_dev(close: pd.DataFrame, vol: pd.DataFrame, window: int) -> pd.DataFrame:
        """Deviation from rolling VWAP: (close - vwap) / vwap."""
        vwap = (close * vol).rolling(window).sum() / (vol.rolling(window).sum() + 1e-8)
        return (close - vwap) / (vwap + 1e-8)
