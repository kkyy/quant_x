"""
Shared data-layer utilities.

Centralises stock-code conversion and sector_stocks.json loading so that
every module uses the same logic and the file is parsed at most once per
process (functools.lru_cache).
"""
from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import Dict

logger = logging.getLogger(__name__)

# Canonical path to the crawler's offline data file
_SECTOR_STOCKS_FILE = Path(__file__).parent.parent / "crawler" / "data" / "sector_stocks.json"
_STOCK_NAME_CACHE_FILE = Path(__file__).parent.parent / "cache" / "stock_name_map.json"


def code_to_qlib_instrument(code: str) -> str:
    """Convert a bare 6-digit A-share code to qlib instrument format.

    Rules (match qlib's own naming conventions):
      0xxxxx / 2xxxxx / 3xxxxx  →  SZ{code}
    920xxx / 4xxxxx / 8xxxxx  →  BJ{code}
    6xxxxx / 9xxxxx           →  SH{code}   (includes B-shares SH9xxxxx except 920xxx)
      4xxxxx / 8xxxxx           →  BJ{code}
      anything else             →  returned unchanged

    This is the single authoritative implementation.  Other modules that
    previously had their own copy (data/sector.py, data/universe.py,
    signals/generator.py) now delegate here.
    """
    code = str(code).strip()
    if len(code) != 6 or not code.isdigit():
        return code
    if code.startswith("920"):
        return f"BJ{code}"
    first = int(code[0])
    if first in (0, 2, 3):
        return f"SZ{code}"
    if first in (6, 9):
        return f"SH{code}"
    if first in (4, 8):
        return f"BJ{code}"
    return code


def normalize_qlib_instrument(value: str) -> str:
    """Normalize bare codes or prefixed instruments to the canonical qlib form."""
    value = str(value).strip()
    if len(value) == 6 and value.isdigit():
        return code_to_qlib_instrument(value)
    if len(value) == 8 and value[:2] in {"SH", "SZ", "BJ"} and value[2:].isdigit():
        return code_to_qlib_instrument(value[2:])
    return value


@lru_cache(maxsize=1)
def load_stock_names() -> Dict[str, str]:
    """Return {qlib_instrument: stock_name} loaded from sector_stocks.json.

    The result is cached for the lifetime of the process so multiple callers
    (UniverseFilter, SignalGenerator, run_scheduled_rebalance …) all share
    the same dict without re-reading the file.
    """
    if not _SECTOR_STOCKS_FILE.exists():
        logger.debug("sector_stocks.json not found; stock name map will be empty")
        return {}
    try:
        names: Dict[str, str] = {}
        data = json.loads(_SECTOR_STOCKS_FILE.read_text(encoding="utf-8"))
        for category in data.values():
            for sector in category.values():
                for stock in sector.get("stocks", []):
                    raw_code = stock.get("code", "")
                    name = stock.get("name", "")
                    if raw_code and name:
                        names[code_to_qlib_instrument(raw_code)] = name

        if _STOCK_NAME_CACHE_FILE.exists():
            try:
                cached_names = json.loads(_STOCK_NAME_CACHE_FILE.read_text(encoding="utf-8"))
                if isinstance(cached_names, dict):
                    names.update(
                        {
                            normalize_qlib_instrument(str(code)): str(name)
                            for code, name in cached_names.items()
                            if name
                        }
                    )
            except Exception as exc:
                logger.warning("Failed to load stock_name_map.json: %s", exc)

        logger.debug("Loaded %d stock names from sector_stocks.json", len(names))
        return names
    except Exception as exc:
        logger.warning("Failed to load sector_stocks.json: %s", exc)
        return {}
