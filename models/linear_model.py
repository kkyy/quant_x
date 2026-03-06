"""
Linear alpha models (Ridge / Lasso) — lightweight baselines.

Registered names
----------------
- "ridge"  → RidgeAlphaModel
- "lasso"  → LassoAlphaModel
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

from .base import BaseAlphaModel, ModelRegistry

logger = logging.getLogger(__name__)


@ModelRegistry.register("ridge")
class RidgeAlphaModel(BaseAlphaModel):
    """
    Ridge regression alpha model.

    Parameters
    ----------
    alpha       : regularisation strength (sklearn Ridge alpha)
    extra_factors: pre-computed factor DataFrame merged into features
    """

    def __init__(
        self,
        alpha: float = 1.0,
        extra_factors: Optional[pd.DataFrame] = None,
    ):
        from sklearn.linear_model import Ridge
        self._alpha = alpha
        self.extra_factors = extra_factors
        self.model = Ridge(alpha=alpha)
        self.feature_names_: Optional[list] = None

    # ── fit ──────────────────────────────────────────────────────────────────

    def fit(self, dataset, **kwargs) -> "RidgeAlphaModel":
        X_tr, y_tr = dataset.prepare("train", col_set=["feature", "label"], data_key="learn")
        y_tr = y_tr.squeeze()

        X_tr = self._merge_extra(X_tr, self.extra_factors).fillna(0)
        self.feature_names_ = X_tr.columns.tolist()

        self.model.fit(X_tr, y_tr)
        logger.info(
            f"RidgeAlphaModel fitted: alpha={self._alpha}, "
            f"features={len(self.feature_names_)}"
        )
        return self

    # ── predict ──────────────────────────────────────────────────────────────

    def predict(self, dataset, segment: str = "test") -> pd.Series:
        X = dataset.prepare(segment, col_set="feature", data_key="infer")
        X = (
            self._merge_extra(X, self.extra_factors)
            .reindex(columns=self.feature_names_, fill_value=0)
            .fillna(0)
        )
        preds = self.model.predict(X)
        return pd.Series(preds, index=X.index, name="score")

    # ── diagnostics ───────────────────────────────────────────────────────────

    def feature_importance(self, top_n: int = 30) -> pd.DataFrame:
        if self.feature_names_ is None:
            return pd.DataFrame(columns=["feature", "importance"])
        coef = np.abs(self.model.coef_)
        df = pd.DataFrame({"feature": self.feature_names_, "importance": coef})
        return df.sort_values("importance", ascending=False).head(top_n)


@ModelRegistry.register("lasso")
class LassoAlphaModel(BaseAlphaModel):
    """
    Lasso regression alpha model (built-in feature selection via L1).

    Parameters
    ----------
    alpha       : regularisation strength
    extra_factors: pre-computed factor DataFrame merged into features
    """

    def __init__(
        self,
        alpha: float = 0.01,
        extra_factors: Optional[pd.DataFrame] = None,
    ):
        from sklearn.linear_model import Lasso
        self._alpha = alpha
        self.extra_factors = extra_factors
        self.model = Lasso(alpha=alpha, max_iter=5000)
        self.feature_names_: Optional[list] = None

    def fit(self, dataset, **kwargs) -> "LassoAlphaModel":
        X_tr, y_tr = dataset.prepare("train", col_set=["feature", "label"], data_key="learn")
        y_tr = y_tr.squeeze()

        X_tr = self._merge_extra(X_tr, self.extra_factors).fillna(0)
        self.feature_names_ = X_tr.columns.tolist()

        self.model.fit(X_tr, y_tr)
        n_selected = int(np.sum(self.model.coef_ != 0))
        logger.info(
            f"LassoAlphaModel fitted: alpha={self._alpha}, "
            f"selected features={n_selected}/{len(self.feature_names_)}"
        )
        return self

    def predict(self, dataset, segment: str = "test") -> pd.Series:
        X = dataset.prepare(segment, col_set="feature", data_key="infer")
        X = (
            self._merge_extra(X, self.extra_factors)
            .reindex(columns=self.feature_names_, fill_value=0)
            .fillna(0)
        )
        preds = self.model.predict(X)
        return pd.Series(preds, index=X.index, name="score")

    def feature_importance(self, top_n: int = 30) -> pd.DataFrame:
        if self.feature_names_ is None:
            return pd.DataFrame(columns=["feature", "importance"])
        coef = np.abs(self.model.coef_)
        df = pd.DataFrame({"feature": self.feature_names_, "importance": coef})
        return df.sort_values("importance", ascending=False).head(top_n)
