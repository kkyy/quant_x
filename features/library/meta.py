"""FactorMeta and FactorLibrary — persistent catalog of all factors.

The library is a JSON-backed store that records every factor's:
  - how to build it (source, config dict)
  - quality metrics (IC, ICIR, coverage) from the last evaluation run
  - operational state (enabled/disabled, tags)

Workflow
--------
1.  Register a new factor in the library:

        lib = FactorLibrary()
        lib.add(FactorMeta(
            name="pb_ratio",
            source="fundamental",
            description="Price-to-book ratio",
            tags=["valuation"],
            config={"metrics": ["pb"]},
        ))

2.  After evaluation, write back quality metrics:

        lib.update_stats("pb_ratio", ic_mean=0.031, icir=0.74, coverage=0.93)

3.  Build a FactorPipeline from all enabled, high-quality entries:

        configs = lib.to_pipeline_configs(min_ic=0.02, min_icir=0.3)
        pipeline = FactorPipeline.from_config(configs)
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_DEFAULT_PATH = "./cache/factor_library.json"


@dataclass
class FactorMeta:
    """Metadata record for a single factor.

    Parameters
    ----------
    name : str
        Unique identifier; must match the column name produced by the factor.
    source : str
        Factor family: ``"technical"``, ``"sector"``, ``"mined"``,
        ``"csv"``, ``"fundamental"``, or any registered FactorRegistry key.
    description : str
        Human-readable description.
    tags : list[str]
        Free-form labels for filtering (e.g., ``["valuation", "momentum"]``).
    ic_mean : float or None
        Mean rank-IC from the last evaluation (positive = long-signal).
    icir : float or None
        IC information ratio (IC_mean / IC_std).
    coverage : float or None
        Fraction of (instrument × date) cells with valid values.
    enabled : bool
        Whether to include this factor when building a pipeline.
    created_at : str
        ISO-8601 timestamp of when this entry was first added.
    config : dict
        Kwargs passed to ``FactorRegistry.build(source, **config)`` to
        reconstruct the factor.  Must be JSON-serialisable.
    """

    name: str
    source: str
    description: str = ""
    tags: List[str] = field(default_factory=list)
    ic_mean: Optional[float] = None
    icir: Optional[float] = None
    coverage: Optional[float] = None
    enabled: bool = True
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    config: dict = field(default_factory=dict)

    def to_pipeline_config(self) -> dict:
        """Return a single entry suitable for ``FactorPipeline.from_config``."""
        return {"name": self.source, **self.config}

    def passes_quality(
        self,
        min_ic: Optional[float] = None,
        min_icir: Optional[float] = None,
        min_coverage: Optional[float] = None,
    ) -> bool:
        """Return True when all provided thresholds are satisfied.

        Factors that have not been evaluated yet (metrics are None) pass
        every threshold — they are given the benefit of the doubt.
        """
        if min_ic is not None and self.ic_mean is not None:
            if abs(self.ic_mean) < min_ic:
                return False
        if min_icir is not None and self.icir is not None:
            if abs(self.icir) < min_icir:
                return False
        if min_coverage is not None and self.coverage is not None:
            if self.coverage < min_coverage:
                return False
        return True


class FactorLibrary:
    """Persistent catalog of factors, backed by a JSON file.

    Parameters
    ----------
    path : str or Path
        Path to the JSON catalog file.  Created automatically on first save.

    Examples
    --------
    >>> lib = FactorLibrary("./cache/factor_library.json")
    >>> lib.add(FactorMeta(name="rsi_14d", source="technical",
    ...                    tags=["momentum"], config={"rsi_windows": [14]}))
    >>> lib.to_pipeline_configs(min_icir=0.3)
    [{"name": "technical", "rsi_windows": [14]}]
    """

    def __init__(self, path: str = _DEFAULT_PATH):
        self._path = Path(path)
        self._entries: Dict[str, FactorMeta] = {}
        if self._path.exists():
            self._load()

    # ── CRUD ─────────────────────────────────────────────────────────────────

    def add(self, meta: FactorMeta, overwrite: bool = False) -> None:
        """Add a factor entry.  Raises if name already exists unless overwrite=True."""
        if meta.name in self._entries and not overwrite:
            raise ValueError(
                f"Factor '{meta.name}' already in library. "
                "Pass overwrite=True to replace it."
            )
        self._entries[meta.name] = meta
        self.save()

    def get(self, name: str) -> Optional[FactorMeta]:
        """Return the FactorMeta for *name*, or None if not found."""
        return self._entries.get(name)

    def remove(self, name: str) -> None:
        """Delete an entry by name."""
        self._entries.pop(name, None)
        self.save()

    def enable(self, name: str) -> None:
        self._set_enabled(name, True)

    def disable(self, name: str) -> None:
        self._set_enabled(name, False)

    def _set_enabled(self, name: str, value: bool) -> None:
        if name not in self._entries:
            raise KeyError(f"Factor '{name}' not in library")
        self._entries[name].enabled = value
        self.save()

    # ── Stats update ─────────────────────────────────────────────────────────

    def update_stats(
        self,
        name: str,
        ic_mean: float,
        icir: float,
        coverage: float,
    ) -> None:
        """Write back evaluation results for a factor."""
        if name not in self._entries:
            raise KeyError(f"Factor '{name}' not in library")
        m = self._entries[name]
        m.ic_mean = round(ic_mean, 6)
        m.icir = round(icir, 4)
        m.coverage = round(coverage, 4)
        self.save()

    # ── Query ─────────────────────────────────────────────────────────────────

    def list(
        self,
        enabled_only: bool = True,
        tags: Optional[List[str]] = None,
        min_ic: Optional[float] = None,
        min_icir: Optional[float] = None,
        min_coverage: Optional[float] = None,
    ) -> List[FactorMeta]:
        """Return filtered list of factor entries.

        Parameters
        ----------
        enabled_only : bool
            Skip disabled factors.
        tags : list[str], optional
            Keep only factors that have ALL the listed tags.
        min_ic / min_icir / min_coverage : float, optional
            Quality thresholds (factors without stats always pass).
        """
        result = []
        for m in self._entries.values():
            if enabled_only and not m.enabled:
                continue
            if tags and not all(t in m.tags for t in tags):
                continue
            if not m.passes_quality(min_ic, min_icir, min_coverage):
                continue
            result.append(m)
        return result

    def to_pipeline_configs(
        self,
        enabled_only: bool = True,
        tags: Optional[List[str]] = None,
        min_ic: Optional[float] = None,
        min_icir: Optional[float] = None,
        min_coverage: Optional[float] = None,
    ) -> List[dict]:
        """Export as a list ready for ``FactorPipeline.from_config()``.

        Entries sharing the same *source* are NOT merged — each entry
        produces its own pipeline stage, which is the safest default.
        """
        return [
            m.to_pipeline_config()
            for m in self.list(
                enabled_only=enabled_only,
                tags=tags,
                min_ic=min_ic,
                min_icir=min_icir,
                min_coverage=min_coverage,
            )
        ]

    def summary(self) -> str:
        """Return a compact human-readable summary table."""
        lines = [
            f"{'name':<30} {'source':<14} {'IC':>7} {'ICIR':>7} {'cov':>5} {'on':>3}",
            "-" * 70,
        ]
        for m in sorted(self._entries.values(), key=lambda x: x.name):
            ic_s = f"{m.ic_mean:.4f}" if m.ic_mean is not None else "  n/a "
            icir_s = f"{m.icir:.3f}" if m.icir is not None else "  n/a"
            cov_s = f"{m.coverage:.2f}" if m.coverage is not None else " n/a"
            on_s = "✓" if m.enabled else "✗"
            lines.append(
                f"{m.name:<30} {m.source:<14} {ic_s:>7} {icir_s:>7} {cov_s:>5} {on_s:>3}"
            )
        return "\n".join(lines)

    # ── Persistence ───────────────────────────────────────────────────────────

    def save(self) -> None:
        """Write the catalog to disk (atomic via temp file)."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        data = {name: asdict(m) for name, m in self._entries.items()}
        tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(self._path)

    def _load(self) -> None:
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            self._entries = {name: FactorMeta(**v) for name, v in data.items()}
            logger.debug(f"FactorLibrary loaded {len(self._entries)} entries from {self._path}")
        except Exception as exc:
            logger.warning(f"FactorLibrary load failed ({self._path}): {exc}")
            self._entries = {}

    def __len__(self) -> int:
        return len(self._entries)

    def __repr__(self) -> str:
        return f"FactorLibrary(path={self._path}, entries={len(self._entries)})"
