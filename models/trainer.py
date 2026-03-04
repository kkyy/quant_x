"""Model training orchestrator."""
from __future__ import annotations
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


class ModelTrainer:
    """
    Orchestrates model training.  Two modes:

    1. qlib_native=True  (default)
       Uses qlib's LGBModel + R (MLflow) experiment tracking.
       Identical to the workflow in your notebook.

    2. qlib_native=False
       Uses LGBMAlphaModel which supports sector / custom factors.
       Saves a .pkl; no MLflow tracking.
    """

    def __init__(self, config: dict, data_loader, sector_provider=None):
        self.config = config
        self.data_loader = data_loader
        self.sector_provider = sector_provider
        self.model_dir = Path(config.get("paths", {}).get("model_dir", "./models"))
        self.model_dir.mkdir(parents=True, exist_ok=True)

    # ── public API ────────────────────────────────────────────────────────────

    def train(
        self,
        qlib_native: bool = True,
        price_data: Optional[pd.DataFrame] = None,
        use_sector_factors: bool = True,
        experiment_name: Optional[str] = None,
    ) -> Tuple:
        """
        Train a model.

        Returns:
            (model, dataset, recorder_id_or_None)
        """
        exp_name = experiment_name or self.config.get("experiment", {}).get("name", "quant_ex_exp")
        training_cfg = self.config.get("training", {})
        today = datetime.now().strftime("%Y-%m-%d")
        market = self.config.get("market", {}).get("name", "csi300")

        segments = {
            "train": (training_cfg.get("fit_start", "2015-01-01"),
                      training_cfg.get("fit_end", "2021-12-31")),
            "valid": (training_cfg.get("valid_start", "2022-01-01"),
                      training_cfg.get("valid_end", "2023-12-31")),
            "test":  (training_cfg.get("test_start", "2024-01-01"), today),
        }

        dataset = self.data_loader.build_dataset(segments=segments, instruments=market)

        if qlib_native:
            return self._train_qlib(dataset, exp_name)
        else:
            return self._train_custom(dataset, price_data, use_sector_factors)

    # ── private ───────────────────────────────────────────────────────────────

    def _train_qlib(self, dataset, exp_name: str):
        from qlib.workflow import R
        from qlib.utils import init_instance_by_config

        lgb_cfg = self.config.get("model", {}).get("lightgbm", {})
        model_conf = {
            "class": "LGBModel",
            "module_path": "qlib.contrib.model.gbdt",
            "kwargs": {
                "loss": "mse",
                "colsample_bytree": lgb_cfg.get("colsample_bytree", 0.8),
                "learning_rate": lgb_cfg.get("learning_rate", 0.05),
                "subsample": lgb_cfg.get("subsample", 0.8),
                "lambda_l1": lgb_cfg.get("reg_alpha", 0.1),
                "lambda_l2": lgb_cfg.get("reg_lambda", 0.1),
                "max_depth": lgb_cfg.get("max_depth", 8),
                "num_leaves": lgb_cfg.get("num_leaves", 64),
                "num_threads": 20,
                "n_estimators": lgb_cfg.get("n_estimators", 1000),
                "verbose": -1,
                "early_stopping_rounds": lgb_cfg.get("early_stopping_rounds", 50),
                "eval_set_key": ["valid"],
            },
        }

        with R.start(experiment_name=exp_name):
            model = init_instance_by_config(model_conf)
            model.fit(dataset)
            R.save_objects(trained_model=model)
            rid = R.get_recorder().id

        logger.info(f"qlib model trained. Recorder ID: {rid}")
        logger.info(f"→ Set experiment.latest_recorder_id = \"{rid}\" in base.yaml")
        return model, dataset, rid

    def _train_custom(self, dataset, price_data, use_sector_factors: bool):
        from .lgbm_model import LGBMAlphaModel
        from ..features.sector_factors import SectorFactorEngine

        sector_factors = None
        if use_sector_factors and self.sector_provider is not None and price_data is not None:
            sector_map = self.sector_provider.get_map()
            cfg = self.config.get("model", {}).get("features", {})
            engine = SectorFactorEngine(
                sector_map,
                momentum_windows=cfg.get("sector_momentum_windows", [5, 10, 20, 60]),
                reversal_windows=cfg.get("sector_reversal_windows", [5, 20]),
            )
            logger.info("Computing sector factors …")
            sector_factors = engine.compute_all(price_data)

        lgb_cfg = self.config.get("model", {}).get("lightgbm", {})
        model = LGBMAlphaModel(lgbm_params=lgb_cfg, sector_factors=sector_factors)
        model.fit(dataset)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = self.model_dir / f"lgbm_{ts}.pkl"
        model.save(str(path))
        return model, dataset, None
