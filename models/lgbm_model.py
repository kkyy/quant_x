"""
LightGBM model wrapper that extends qlib's Alpha158 features
with optional sector factors and auto-mined custom factors.
"""
from __future__ import annotations
import logging
import pickle
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class LGBMAlphaModel:
    """
    Drop-in compatible with qlib model interface (.fit / .predict).

    Extra capabilities vs qlib's built-in LGBModel:
    - Merges sector_factors and custom_factors into the feature matrix
    - Marks 'sector_id' as a categorical feature in LightGBM
    - Exposes feature importance
    """

    def __init__(
        self,
        lgbm_params: Optional[Dict[str, Any]] = None,
        sector_factors: Optional[pd.DataFrame] = None,
        custom_factors: Optional[pd.DataFrame] = None,
        categorical_features: Optional[List[str]] = None,
    ):
        self.lgbm_params = lgbm_params or {
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
        self.sector_factors = sector_factors
        self.custom_factors = custom_factors
        self.categorical_features = categorical_features or ["sector_id"]
        self.model = None
        self.feature_names_: Optional[List[str]] = None

    # ── fit ──────────────────────────────────────────────────────────────────

    def fit(self, dataset, **kwargs):
        import lightgbm as lgb

        X_tr, y_tr = dataset.prepare("train", col_set=["feature", "label"], data_key="learn")
        X_va, y_va = dataset.prepare("valid", col_set=["feature", "label"], data_key="learn")
        y_tr, y_va = y_tr.squeeze(), y_va.squeeze()

        X_tr = self._merge_extra(X_tr)
        X_va = self._merge_extra(X_va)
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
        logger.info(f"LGBMAlphaModel trained: {self.model.num_trees()} trees, "
                    f"{len(self.feature_names_)} features")
        return self

    # ── predict ──────────────────────────────────────────────────────────────

    def predict(self, dataset, segment: str = "test") -> pd.Series:
        X = dataset.prepare(segment, col_set="feature", data_key="infer")
        X = self._merge_extra(X).reindex(columns=self.feature_names_, fill_value=np.nan)
        preds = self.model.predict(X)
        return pd.Series(preds, index=X.index, name="score")

    # ── persistence ──────────────────────────────────────────────────────────

    def save(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(self, f)
        logger.info(f"Model saved → {path}")

    @classmethod
    def load(cls, path: str) -> "LGBMAlphaModel":
        with open(path, "rb") as f:
            return pickle.load(f)

    # ── diagnostics ──────────────────────────────────────────────────────────

    def feature_importance(self, top_n: int = 30) -> pd.DataFrame:
        if self.model is None:
            return pd.DataFrame()
        imp = self.model.feature_importance(importance_type="gain")
        df = pd.DataFrame({"feature": self.model.feature_name(), "importance": imp})
        return df.sort_values("importance", ascending=False).head(top_n)

    # ── private ──────────────────────────────────────────────────────────────

    def _merge_extra(self, X: pd.DataFrame) -> pd.DataFrame:
        parts = [X]
        for extra in (self.sector_factors, self.custom_factors):
            if extra is not None and not extra.empty:
                parts.append(extra.reindex(X.index))
        return pd.concat(parts, axis=1)
