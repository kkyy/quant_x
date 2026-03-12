"""
Model abstraction layer.

BaseAlphaModel  – common interface every model must implement
ModelRegistry   – class registry for quick model lookup/instantiation

Usage
-----
# Register a new model (done via decorator in each model file):
@ModelRegistry.register("my_model")
class MyModel(BaseAlphaModel):
    ...

# Instantiate by name from config:
model = ModelRegistry.build("lgbm", lgbm_params={...})

# List all registered models:
print(ModelRegistry.list())
"""
from __future__ import annotations

import logging
import pickle
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional, Type

import pandas as pd

logger = logging.getLogger(__name__)


# ── Base class ────────────────────────────────────────────────────────────────

class BaseAlphaModel(ABC):
    """
    Common interface for all alpha models.

    Concrete subclasses must implement:
      - fit(dataset, **kwargs)   -> self
      - predict(dataset, segment) -> pd.Series of scores

    Optional overrides:
      - feature_importance(top_n) -> pd.DataFrame
      - save(path)
      - load(path)  (classmethod)
    """

    #: Short identifier used by ModelRegistry (override in subclass)
    name: str = "base"

    @abstractmethod
    def fit(self, dataset, **kwargs) -> "BaseAlphaModel":
        """Fit on the dataset's train/valid splits. Return self."""

    @abstractmethod
    def predict(self, dataset, segment: str = "test") -> pd.Series:
        """Return a score Series indexed by (instrument, datetime)."""

    # ── persistence (default: pickle) ────────────────────────────────────────

    def save(self, path: str) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(self, f)
        logger.info(f"Model saved → {path}")

    @classmethod
    def load(cls, path: str) -> "BaseAlphaModel":
        with open(path, "rb") as f:
            return pickle.load(f)

    # ── diagnostics ───────────────────────────────────────────────────────────

    def feature_importance(self, top_n: int = 30) -> pd.DataFrame:
        """Return a DataFrame with columns ['feature', 'importance']."""
        return pd.DataFrame(columns=["feature", "importance"])

    # ── helper ────────────────────────────────────────────────────────────────

    def _merge_extra(
        self, X: pd.DataFrame, extra_factors: Optional[pd.DataFrame]
    ) -> pd.DataFrame:
        """Concatenate extra factor columns to X, aligned by index."""
        if extra_factors is not None and not extra_factors.empty:
            ef = extra_factors
            # qlib DatasetH returns (datetime, instrument) order while our factor
            # pipeline produces (instrument, datetime) order – reorder if needed.
            if (
                isinstance(ef.index, pd.MultiIndex)
                and isinstance(X.index, pd.MultiIndex)
                and ef.index.names != X.index.names
                and set(ef.index.names) == set(X.index.names)
            ):
                ef = ef.reorder_levels(X.index.names)
            return pd.concat([X, ef.reindex(X.index)], axis=1)
        return X


# ── Registry ──────────────────────────────────────────────────────────────────

class ModelRegistry:
    """
    Global registry mapping model names to their classes.

    Models register themselves via the @ModelRegistry.register("name") decorator.
    """

    _registry: Dict[str, Type[BaseAlphaModel]] = {}

    @classmethod
    def register(cls, name: str):
        """Decorator: @ModelRegistry.register("lgbm")"""
        def decorator(model_cls: Type[BaseAlphaModel]) -> Type[BaseAlphaModel]:
            model_cls.name = name
            cls._registry[name] = model_cls
            logger.debug(f"ModelRegistry: registered '{name}' -> {model_cls.__name__}")
            return model_cls
        return decorator

    @classmethod
    def get(cls, name: str) -> Type[BaseAlphaModel]:
        if name not in cls._registry:
            raise KeyError(
                f"Model '{name}' not registered. "
                f"Available: {sorted(cls._registry)}"
            )
        return cls._registry[name]

    @classmethod
    def build(cls, name: str, **kwargs) -> BaseAlphaModel:
        """Instantiate a registered model by name."""
        return cls.get(name)(**kwargs)

    @classmethod
    def list(cls) -> list:
        return sorted(cls._registry.keys())
