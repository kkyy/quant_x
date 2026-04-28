"""
LightGBM model wrapper that extends qlib's Alpha158 features
with optional sector factors and auto-mined custom factors.

Registered name: "lgbm"
"""
from __future__ import annotations

import logging
import random
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
    # num_threads: -1 lets LightGBM use all logical cores via its own thread pool.
    # Use lgb.train() native param name; n_jobs is a sklearn alias and may be
    # silently ignored when calling lgb.train() directly.
    "num_threads": -1,
    "device_type": "cpu",  # change to "gpu" after building LightGBM with -DUSE_GPU=1
    "seed": 42,
    "feature_fraction_seed": 42,
    "bagging_seed": 42,
    "data_random_seed": 42,
    "extra_seed": 42,
    "deterministic": True,
    "force_col_wise": True,
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
        factor_pipeline=None,
        ensemble_seeds: Optional[List[int]] = None,
        # legacy params kept for backward compatibility
        sector_factors: Optional[pd.DataFrame] = None,
        custom_factors: Optional[pd.DataFrame] = None,
    ):
        self.lgbm_params = {**_DEFAULT_PARAMS, **(lgbm_params or {})}
        seed = int(self.lgbm_params.get("seed", 42))
        for key in ("feature_fraction_seed", "bagging_seed", "data_random_seed", "extra_seed"):
            self.lgbm_params.setdefault(key, seed)
        self.categorical_features = categorical_features or ["sector_id"]
        self.model = None
        self.models_: List[Any] = []
        self.feature_names_: Optional[List[str]] = None
        self.factor_pipeline = factor_pipeline
        self.ensemble_seeds = ensemble_seeds or [seed]

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

        seed = int(self.lgbm_params.get("seed", 42))
        random.seed(seed)
        np.random.seed(seed)

        df_tr = dataset.prepare("train", col_set=["feature", "label"], data_key="learn")
        df_va = dataset.prepare("valid", col_set=["feature", "label"], data_key="learn")
        X_tr, y_tr = df_tr["feature"], df_tr["label"].squeeze()
        X_va, y_va = df_va["feature"], df_va["label"].squeeze()

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

        self.models_ = []
        for i, ensemble_seed in enumerate(self.ensemble_seeds, 1):
            seed_params = {
                **params,
                "seed": ensemble_seed,
                "feature_fraction_seed": ensemble_seed,
                "bagging_seed": ensemble_seed,
                "data_random_seed": ensemble_seed,
                "extra_seed": ensemble_seed,
            }
            logger.info(
                "Training LGBM ensemble member %s/%s seed=%s",
                i,
                len(self.ensemble_seeds),
                ensemble_seed,
            )
            booster = lgb.train(
                seed_params,
                tr_ds,
                num_boost_round=n_est,
                valid_sets=[va_ds],
                callbacks=[
                    lgb.early_stopping(early, verbose=False),
                    lgb.log_evaluation(100),
                ],
            )
            self.models_.append(booster)

        self.model = self.models_[0] if self.models_ else None
        logger.info(
            "LGBMAlphaModel trained: %s model(s), %s features",
            len(self.models_),
            len(self.feature_names_),
        )
        return self

    # ── predict ──────────────────────────────────────────────────────────────

    def predict(self, dataset, segment: str = "test") -> pd.Series:
        X = dataset.prepare(segment, col_set="feature", data_key="infer")
        X = (
            self._merge_extra(X, self.extra_factors)
            .reindex(columns=self.feature_names_, fill_value=np.nan)
        )
        models = self._prediction_models()
        if not models:
            raise RuntimeError("LGBMAlphaModel is not fitted")
        preds = np.mean([m.predict(X) for m in models], axis=0)
        return pd.Series(preds, index=X.index, name="score")

    def __setstate__(self, state: Dict[str, Any]) -> None:
        """Keep older pickled LGBMAlphaModel objects usable after schema changes."""
        self.__dict__.update(state)
        self._ensure_runtime_defaults()

    def _ensure_runtime_defaults(self) -> None:
        seed = int(getattr(self, "lgbm_params", {}).get("seed", 42))
        if not hasattr(self, "models_"):
            self.models_ = []
        if not hasattr(self, "ensemble_seeds"):
            self.ensemble_seeds = [seed]
        if not hasattr(self, "categorical_features"):
            self.categorical_features = ["sector_id"]
        if not hasattr(self, "factor_pipeline"):
            self.factor_pipeline = None
        if not hasattr(self, "extra_factors"):
            self.extra_factors = None

    def _prediction_models(self) -> List[Any]:
        self._ensure_runtime_defaults()
        models = self.models_ or ([self.model] if self.model is not None else [])
        return [model for model in models if model is not None]

    def refresh_extra_factors(self, price_data: pd.DataFrame) -> None:
        """Recompute extra_factors using the stored pipeline and fresh price data.

        Call this in backtest when the test period extends beyond the training date,
        so that sector/technical factors are available for the full evaluation window.
        """
        if self.factor_pipeline is None:
            logger.debug("No factor_pipeline stored; skipping refresh.")
            return
        logger.info("Refreshing extra factors from stored pipeline …")
        new_factors = self.factor_pipeline.compute(price_data)
        if new_factors is not None and not new_factors.empty:
            self.extra_factors = new_factors
            logger.info(
                f"Extra factors refreshed: {len(new_factors.columns)} cols, "
                f"{len(new_factors)} rows"
            )
        else:
            logger.warning("Pipeline returned no factors; extra_factors unchanged.")

    # ── diagnostics ───────────────────────────────────────────────────────────

    def feature_importance(self, top_n: int = 30) -> pd.DataFrame:
        if self.model is None:
            return pd.DataFrame(columns=["feature", "importance"])
        models = self._prediction_models()
        imp = np.mean([m.feature_importance(importance_type="gain") for m in models], axis=0)
        df = pd.DataFrame({"feature": models[0].feature_name(), "importance": imp})
        return df.sort_values("importance", ascending=False).head(top_n)
