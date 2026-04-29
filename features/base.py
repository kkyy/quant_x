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
        """Run all factor providers and return concatenated DataFrame.

        Factors with no shared mutable state are run in parallel threads
        (I/O-bound qlib calls benefit from thread-level concurrency).
        Falls back to serial execution on any threading failure.
        """
        if not self.factors:
            return None

        results: dict = {}  # index → (name, df)

        def _run(i: int, factor) -> tuple:
            try:
                df = factor.compute(price_data)
                return (i, factor.name, df)
            except Exception as exc:
                logger.warning(f"Factor '{factor.name}' failed: {exc}")
                return (i, factor.name, None)

        # Use threads: factors are mostly pandas/numpy or qlib I/O, not CPU-bound GIL work
        from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed
        with ThreadPoolExecutor(max_workers=min(len(self.factors), 4)) as pool:
            futures = {pool.submit(_run, i, f): i for i, f in enumerate(self.factors)}
            for future in _as_completed(futures):
                i, name, df = future.result()
                if df is not None and not df.empty:
                    results[i] = (name, df)
                    logger.debug(
                        f"Factor '{name}' → {len(df.columns)} columns, {len(df)} rows"
                    )

        if not results:
            return None

        parts = [results[i][1] for i in sorted(results)]
        result = pd.concat(parts, axis=1)
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

        Optional screener_config key (in shared_kwargs or any config entry)::

            screener_config = {"min_ic": 0.02, "min_icir": 0.3, "max_corr": 0.7}

        When provided a ``FactorScreener`` instance is built and stored in
        ``pipeline.screener``; call ``pipeline.compute_with_screening(price_data,
        forward_returns)`` to use it.
        """
        screener_config = shared_kwargs.pop("screener_config", None)
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
        pipeline = cls(factors)

        if screener_config is not None:
            try:
                from .library.screener import FactorScreener
                pipeline.screener = FactorScreener(**screener_config)
                logger.info("FactorPipeline: screener configured: %s", screener_config)
            except Exception as exc:
                logger.warning("FactorPipeline: could not build screener: %s", exc)
                pipeline.screener = None
        else:
            pipeline.screener = None

        return pipeline

    def compute_with_cleaning(
        self,
        price_data: pd.DataFrame,
        cleaner=None,
    ):
        """Run all factors then optionally apply a FactorCleaner.

        This is a non-breaking extension of ``compute()``.  The *cleaner*
        parameter accepts any object with a ``transform(df) → df`` method,
        typically ``features.library.FactorCleaner``.

        Parameters
        ----------
        price_data : DataFrame
            Input price data, same as for ``compute()``.
        cleaner : FactorCleaner, optional
            When provided, ``cleaner.transform(factors)`` is called after
            all factor columns are concatenated.  Pass ``None`` to skip
            cleaning (identical to calling ``compute()`` directly).

        Returns
        -------
        DataFrame or None
        """
        result = self.compute(price_data)
        if result is None or cleaner is None:
            return result
        try:
            return cleaner.transform(result)
        except Exception as exc:
            logger.warning(f"FactorCleaner failed, returning uncleaned factors: {exc}")
            return result

    def compute_with_screening(
        self,
        price_data: pd.DataFrame,
        forward_returns: "Optional[pd.Series]" = None,
        screener=None,
    ):
        """Run all factors then optionally apply a FactorScreener.

        Screening filters out low-IC / high-correlation factors before the
        feature matrix is passed to the model.  Since forward returns are
        required for IC evaluation, this method must be called from the
        training path (where labels are available).

        Parameters
        ----------
        price_data : DataFrame
            Input price data, same as for ``compute()``.
        forward_returns : pd.Series, optional
            Forward-return labels aligned to the same (instrument, datetime)
            MultiIndex as the factor DataFrame.  Required when *screener* is
            not None; ignored otherwise.
        screener : FactorScreener, optional
            When provided, ``screener.screen(factors, forward_returns)`` is
            called after factor computation.  The screening report is logged
            at INFO level.  Pass ``None`` to skip screening.

        Returns
        -------
        DataFrame or None
            Screened (or unscreened) factor DataFrame.
        """
        result = self.compute(price_data)
        if result is None or screener is None:
            return result
        if forward_returns is None:
            logger.warning(
                "FactorPipeline.compute_with_screening: screener provided but "
                "forward_returns is None; screening skipped"
            )
            return result
        try:
            kept, report = screener.screen(result, forward_returns)
            kept_cols = report.index[report["kept"]].tolist()
            logger.info(
                "FactorScreener report:\n%s",
                report.to_string(),
            )
            return kept if not kept.empty else None
        except Exception as exc:
            logger.warning(f"FactorScreener failed, returning unscreened factors: {exc}")
            return result

    def __repr__(self) -> str:
        names = [f.name for f in self.factors]
        return f"FactorPipeline({names})"
