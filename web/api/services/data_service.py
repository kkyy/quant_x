"""Backend data service: qlib lazy init + TTL cache + stock quotes & search."""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Lazy qlib singleton ───────────────────────────────────────────────────────

_loader: Optional[Any] = None


def _qlib_loader() -> Any:
    """Lazily create and cache the DataLoader singleton."""
    global _loader
    if _loader is not None:
        return _loader

    from quant_ex.data.loader import DataLoader
    from quant_ex.utils.config import load_config

    config = load_config()
    _loader = DataLoader(config)
    _loader.init_qlib()
    logger.info("DataLoader singleton created and qlib initialised")
    return _loader


# ── TTL cache ─────────────────────────────────────────────────────────────────

_ttl_cache: Dict[str, Tuple[float, Any]] = {}


def _cached(key: str, ttl: float, factory: Callable[[], Any]) -> Any:
    """Dict-based TTL cache. Calls factory() only when the entry is missing or stale."""
    now = time.time()
    expiry, data = _ttl_cache.get(key, (0.0, None))
    if now < expiry:
        return data

    result = factory()
    _ttl_cache[key] = (now + ttl, result)
    return result


# ── Public API ────────────────────────────────────────────────────────────────

def get_stock_quotes(
    symbol: str,
    start: str,
    end: str,
    fields: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Fetch OHLCV price data for a single stock from qlib.

    Parameters
    ----------
    symbol   : plain code ("600519") or qlib instrument ("SH600519")
    start    : start date string  (e.g. "2024-01-01")
    end      : end date string    (e.g. "2024-12-31")
    fields   : qlib field list (default: open/high/low/close/factor/adjclose/volume/change/amount)

    Returns
    -------
    {
        "symbol": "SH600519",
        "name":   "贵州茅台",
        "data":   [{"date": "2024-01-02", "open": 1650.0, ...}, ...]
    }
    """
    from quant_ex.data.utils import code_to_qlib_instrument, load_stock_names, normalize_qlib_instrument

    qlib_sym = normalize_qlib_instrument(symbol)
    cache_key = f"quotes:{qlib_sym}:{start}:{end}"

    def _load() -> Dict[str, Any]:
        loader = _qlib_loader()
        fields = fields or [
            "$open", "$high", "$low", "$close",
            "$factor", "$adjclose", "$volume", "$change", "$amount",
        ]

        df = loader.load_price_data(
            instruments=[qlib_sym],
            start_time=start,
            end_time=end,
            fields=fields,
        )

        # xs the instrument level when data is MultiIndex
        if isinstance(df.index, type(df.index)) and hasattr(df.index, "get_level_values"):
            if "instrument" in df.index.names:
                df = df.xs(qlib_sym, level="instrument")

        # Build stock name
        stock_names = load_stock_names()
        stock_name = stock_names.get(qlib_sym, "")

        # Strip '$' prefix from column names
        col_map = {c: c.lstrip("$") for c in df.columns}
        df = df.rename(columns=col_map)

        # Format datetime index
        records = []
        for dt, row in df.iterrows():
            dt_str = dt.strftime("%Y-%m-%d") if isinstance(dt, datetime) else str(dt)[:10]
            records.append({"date": dt_str, **row.to_dict()})

        return {"symbol": qlib_sym, "name": stock_name, "data": records}

    return _cached(cache_key, ttl=86400.0, factory=_load)


def search_stocks(q: str, limit: int = 10) -> List[Dict[str, str]]:
    """
    Case-insensitive fuzzy search across stock symbols and names.

    Parameters
    ----------
    q     : search query
    limit : max results (default 10)

    Returns
    -------
    [{"symbol": "SH600519", "name": "贵州茅台", "exchange": "SH"}, ...]
    """
    from quant_ex.data.utils import code_to_qlib_instrument, load_stock_names

    cache_key = f"search:{q.lower()}:{limit}"

    def _search() -> List[Dict[str, str]]:
        stock_names = load_stock_names()
        q_lower = q.lower().strip()
        if not q_lower:
            return []

        scored: List[Tuple[int, Dict[str, str]]] = []
        for qlib_sym, name in stock_names.items():
            sym_lower = qlib_sym.lower()
            name_lower = name.lower()
            score = 0

            # Exact symbol prefix match scores highest
            if sym_lower == q_lower:
                score = 100
            elif sym_lower.startswith(q_lower):
                score = 80
            elif q_lower in sym_lower:
                score = 50
            # Name match
            elif name_lower == q_lower:
                score = 90
            elif name_lower.startswith(q_lower):
                score = 60
            elif q_lower in name_lower:
                score = 30

            if score > 0:
                exchange = qlib_sym[:2] if len(qlib_sym) == 8 and qlib_sym[:2] in {"SH", "SZ", "BJ"} else ""
                scored.append((score, {"symbol": qlib_sym, "name": name, "exchange": exchange}))

        # Sort descending by score, then alphabetically
        scored.sort(key=lambda x: (-x[0], x[1]["symbol"]))
        return [item for _, item in scored[:limit]]

    return _cached(cache_key, ttl=300.0, factory=_search)
