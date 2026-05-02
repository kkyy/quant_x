"""
XGBoost alpha model.

Registered name: "xgb"

Requires: pip install xgboost
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from .base import BaseAlphaModel, ModelRegistry

logger = logging.getLogger(__name__)

_DEFAULT_PARAMS: Dict[str, Any] = {
    "n_estimators": 1000,
    "learning_rate": 0.05,
    "max_depth": 6,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.1,
    "reg_lambda": 1.0,
    "min_child_weight": 30,
    "tree_method": "hist",
    "verbosity": 0,
    "n_jobs": -1,
    "early_stopping_rounds": 50,
}


@ModelRegistry.register("xgb")
class XGBAlphaModel(BaseAlphaModel):
    """
    XGBoost alpha model.

    Drop-in compatible with BaseAlphaModel interface.
    Supports extra factor columns (sector / custom factors).

    Parameters
    ----------
    xgb_params    : XGBoost hyper-parameters (overrides defaults)
    extra_factors : pre-computed factor DataFrame merged into features
    """

    def __init__(
        self,
        xgb_params: Optional[Dict[str, Any]] = None,
        extra_factors: Optional[pd.DataFrame] = None,
    ):
        try:
            import xgboost  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "XGBoost is required for XGBAlphaModel. "
                "Install with: pip install xgboost"
            ) from exc

        self.xgb_params = {**_DEFAULT_PARAMS, **(xgb_params or {})}
        self.extra_factors = extra_factors
        self.model = None
        self.feature_names_: Optional[List[str]] = None

    # ── fit ──────────────────────────────────────────────────────────────────

    def fit(self, dataset, **kwargs) -> "XGBAlphaModel":
        import xgboost as xgb

        df_tr = dataset.prepare("train", col_set=["feature", "label"], data_key="learn")
        df_va = dataset.prepare("valid", col_set=["feature", "label"], data_key="learn")
        X_tr, y_tr = df_tr.xs("feature", axis=1, level=0), df_tr.xs("label", axis=1, level=0).squeeze()
        X_va, y_va = df_va.xs("feature", axis=1, level=0), df_va.xs("label", axis=1, level=0).squeeze()

        X_tr = self._merge_extra(X_tr, self.extra_factors)
        X_va = self._merge_extra(X_va, self.extra_factors)
        self.feature_names_ = X_tr.columns.tolist()

        params = {k: v for k, v in self.xgb_params.items()
                  if k not in ("n_estimators", "early_stopping_rounds")}
        n_est = self.xgb_params.get("n_estimators", 1000)
        early = self.xgb_params.get("early_stopping_rounds", 50)

        self.model = xgb.XGBRegressor(
            **params,
            n_estimators=n_est,
            early_stopping_rounds=early,
        )
        self.model.fit(
            X_tr, y_tr,
            eval_set=[(X_va, y_va)],
            verbose=100,
        )
        logger.info(
            f"XGBAlphaModel trained: best_iteration={self.model.best_iteration}, "
            f"features={len(self.feature_names_)}"
        )
        return self

    # ── predict ──────────────────────────────────────────────────────────────

    def predict(self, dataset, segment: str = "test") -> pd.Series:
        X = dataset.prepare(segment, col_set="feature", data_key="infer")
        X = (
            self._merge_extra(X, self.extra_factors)
            .reindex(columns=self.feature_names_, fill_value=np.nan)
        )
        preds = self.model.predict(X)
        return pd.Series(preds, index=X.index, name="score")

    # ── diagnostics ───────────────────────────────────────────────────────────

    def feature_importance(self, top_n: int = 30) -> pd.DataFrame:
        if self.model is None:
            return pd.DataFrame(columns=["feature", "importance"])
        imp = self.model.feature_importances_
        df = pd.DataFrame({"feature": self.feature_names_, "importance": imp})
        return df.sort_values("importance", ascending=False).head(top_n)
