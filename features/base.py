"""
Factor abstraction layer.

BaseFactor      – common interface every factor provider must implement
FactorRegistry  – class registry for quick factor lookup/instantiation
FactorPipeline  – compose multiple BaseFactor providers into one DataFrame

Usage
-----
# Register a custom factor (done via decorator):
@FactorRegistry.register("my_factor")
class MyFactor(BaseFactor):
    def compute(self, price_data: pd.DataFrame) -> pd.DataFrame:
        ...

# Build and run a pipeline from a config list:
pipeline = FactorPipeline.from_config([
    {"name": "technical", "windows": [5, 10, 20]},
    {"name": "mined", "path": "./cache/mined_factors.json"},
])
extra_features = pipeline.compute(price_data)

# List registered factors:
print(FactorRegistry.list())
"""
from __future__ import annotations

import inspect
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Type

import pandas as pd

logger = logging.getLogger(__name__)


# ── Base class ────────────────────────────────────────────────────────────────

class BaseFactor(ABC):
    """
    Common interface for all factor providers.

    Concrete subclasses must implement:
      - compute(price_data) -> pd.DataFrame

    The returned DataFrame must have a (instrument, datetime) MultiIndex
    and one or more factor columns.
    """

    #: Short identifier used by FactorRegistry (override in subclass)
    name: str = "base"

    @abstractmethod
    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Compute factor values from price_data.

        Parameters
        ----------
        price_data : DataFrame with (instrument, datetime) MultiIndex,
                     typically containing real_close, real_volume, etc.

        Returns
        -------
        DataFrame with same MultiIndex structure and factor columns,
        or None if computation is not possible.
        """


# ── Registry ──────────────────────────────────────────────────────────────────

class FactorRegistry:
    """
    Global registry mapping factor names to their classes.

    Factors register themselves via the @FactorRegistry.register("name") decorator.
    """

    _registry: Dict[str, Type[BaseFactor]] = {}

    @classmethod
    def register(cls, name: str):
        """Decorator: @FactorRegistry.register("sector")"""
        def decorator(factor_cls: Type[BaseFactor]) -> Type[BaseFactor]:
            factor_cls.name = name
            cls._registry[name] = factor_cls
            logger.debug(f"FactorRegistry: registered '{name}' -> {factor_cls.__name__}")
            return factor_cls
        return decorator

    @classmethod
    def get(cls, name: str) -> Type[BaseFactor]:
        if name not in cls._registry:
            raise KeyError(
                f"Factor '{name}' not registered. "
                f"Available: {sorted(cls._registry)}"
            )
        return cls._registry[name]

    @classmethod
    def build(cls, name: str, **kwargs) -> BaseFactor:
        """Instantiate a registered factor by name."""
        return cls.get(name)(**kwargs)

    @classmethod
    def list(cls) -> list:
        return sorted(cls._registry.keys())


# ── Pipeline ──────────────────────────────────────────────────────────────────

class FactorPipeline:
    """
    Compose multiple BaseFactor providers into a single feature DataFrame.

    Each factor is run independently; failures are logged and skipped.
    Results are concatenated column-wise on the shared index.

    Example
    -------
    pipeline = FactorPipeline([SectorFactor(sector_map), TechnicalFactor()])
    extra = pipeline.compute(price_data)
    """

    def __init__(self, factors: List[BaseFactor]):
        self.factors = factors

    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Run all factor providers and return concatenated DataFrame."""
        parts: List[pd.DataFrame] = []

        for factor in self.factors:
            try:
                df = factor.compute(price_data)
                if df is not None and not df.empty:
                    parts.append(df)
                    logger.debug(
                        f"Factor '{factor.name}' → {len(df.columns)} columns, "
                        f"{len(df)} rows"
                    )
            except Exception as exc:
                logger.warning(f"Factor '{factor.name}' failed: {exc}")

        if not parts:
            return None

        result = pd.concat(parts, axis=1)
        # Drop duplicate columns (same factor registered twice)
        result = result.loc[:, ~result.columns.duplicated()]
        return result

    @classmethod
    def from_config(cls, factor_configs: List[dict], **shared_kwargs) -> "FactorPipeline":
        """
        Build pipeline from a list of dicts, each with a 'name' key.

        shared_kwargs are passed to every factor's __init__ if they accept them.

        Example config list::

            [
                {"name": "sector", "momentum_windows": [5, 10, 20]},
                {"name": "technical"},
                {"name": "mined", "path": "./cache/mined_factors.json"},
            ]
        """
        factors: List[BaseFactor] = []
        for cfg in factor_configs:
            cfg = dict(cfg)  # copy so we don't mutate caller's data
            name = cfg.pop("name")
            # Filter shared_kwargs to only those accepted by this factor's __init__
            factor_cls = FactorRegistry.get(name)
            sig = inspect.signature(factor_cls.__init__)
            has_var_keyword = any(
                p.kind == inspect.Parameter.VAR_KEYWORD
                for p in sig.parameters.values()
            )
            if has_var_keyword:
                filtered_shared = shared_kwargs
            else:
                accepted = set(sig.parameters) - {"self"}
                filtered_shared = {k: v for k, v in shared_kwargs.items() if k in accepted}
            init_kwargs = {**filtered_shared, **cfg}
            try:
                factor = FactorRegistry.build(name, **init_kwargs)
                factors.append(factor)
            except (KeyError, TypeError) as exc:
                logger.warning(f"Could not build factor '{name}': {exc}")
        return cls(factors)

    def __repr__(self) -> str:
        names = [f.name for f in self.factors]
        return f"FactorPipeline({names})"
