"""
LightGBM model wrapper that extends qlib's Alpha158 features
with optional sector factors and auto-mined custom factors.

Registered name: "lgbm"
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
    "max_depth": 8,
    "num_leaves": 64,
    "min_child_samples": 50,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "reg_alpha": 0.1,
    "reg_lambda": 0.1,
    "verbose": -1,
    "n_jobs": -1,
}


@ModelRegistry.register("lgbm")
class LGBMAlphaModel(BaseAlphaModel):
    """
    Drop-in compatible with qlib model interface (.fit / .predict).

    Extra capabilities vs qlib's built-in LGBModel:
    - Merges extra_factors (sector / custom) into the feature matrix
    - Marks 'sector_id' as a categorical feature in LightGBM
    - Exposes feature importance

    Parameters
    ----------
    lgbm_params         : LightGBM hyper-parameters
    extra_factors       : pre-computed factor DataFrame (replaces old
                          sector_factors + custom_factors pair)
    categorical_features: column names to treat as categorical in LightGBM
    """

    def __init__(
        self,
        lgbm_params: Optional[Dict[str, Any]] = None,
        extra_factors: Optional[pd.DataFrame] = None,
        categorical_features: Optional[List[str]] = None,
        # legacy params kept for backward compatibility
        sector_factors: Optional[pd.DataFrame] = None,
        custom_factors: Optional[pd.DataFrame] = None,
    ):
        self.lgbm_params = {**_DEFAULT_PARAMS, **(lgbm_params or {})}
        self.categorical_features = categorical_features or ["sector_id"]
        self.model = None
        self.feature_names_: Optional[List[str]] = None

        # Build unified extra_factors from all sources
        parts = [df for df in (extra_factors, sector_factors, custom_factors)
                 if df is not None and not df.empty]
        if parts:
            merged = pd.concat(parts, axis=1)
            self.extra_factors: Optional[pd.DataFrame] = merged.loc[
                :, ~merged.columns.duplicated()
            ]
        else:
            self.extra_factors = None

    # ── fit ──────────────────────────────────────────────────────────────────

    def fit(self, dataset, **kwargs) -> "LGBMAlphaModel":
        import lightgbm as lgb

        X_tr, y_tr = dataset.prepare("train", col_set=["feature", "label"], data_key="learn")
        X_va, y_va = dataset.prepare("valid", col_set=["feature", "label"], data_key="learn")
        y_tr, y_va = y_tr.squeeze(), y_va.squeeze()

        X_tr = self._merge_extra(X_tr, self.extra_factors)
        X_va = self._merge_extra(X_va, self.extra_factors)
        self.feature_names_ = X_tr.columns.tolist()

        cat_feats = [f for f in self.categorical_features if f in self.feature_names_]

        params = {k: v for k, v in self.lgbm_params.items()
                  if k not in ("n_estimators", "early_stopping_rounds")}
        n_est = self.lgbm_params.get("n_estimators", 1000)
        early = self.lgbm_params.get("early_stopping_rounds", 50)

        tr_ds = lgb.Dataset(X_tr, label=y_tr,
                            categorical_feature=cat_feats or "auto",
                            free_raw_data=False)
        va_ds = lgb.Dataset(X_va, label=y_va,
                            categorical_feature=cat_feats or "auto",
                            reference=tr_ds, free_raw_data=False)

        self.model = lgb.train(
            params,
            tr_ds,
            num_boost_round=n_est,
            valid_sets=[va_ds],
            callbacks=[
                lgb.early_stopping(early, verbose=False),
                lgb.log_evaluation(100),
            ],
        )
        logger.info(
            f"LGBMAlphaModel trained: {self.model.num_trees()} trees, "
            f"{len(self.feature_names_)} features"
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
        imp = self.model.feature_importance(importance_type="gain")
        df = pd.DataFrame({"feature": self.model.feature_name(), "importance": imp})
        return df.sort_values("importance", ascending=False).head(top_n)
