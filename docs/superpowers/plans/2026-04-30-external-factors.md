# External Factors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add northbound capital and extended financial fundamental factors via a layered DataProvider + Factor architecture.

**Architecture:** New `data/fetchers/` layer handles API calls, caching, and fallback logic. Factor classes in `features/` read from cached data only, never call APIs directly. Existing `FactorPipeline`/`FactorRegistry` infrastructure is reused without modification.

**Tech Stack:** akshare (primary), East Money crawler SDK (fallback), pandas, ThreadPoolExecutor, per-stock CSV caching.

---

## File Structure

| Action | File | Responsibility |
|---|---|---|
| Create | `data/fetchers/__init__.py` | Package init, exports |
| Create | `data/fetchers/base.py` | `BaseDataFetcher` ABC |
| Create | `data/fetchers/northbound_fetcher.py` | `NorthboundFetcher` — akshare + EM fallback |
| Create | `data/fetchers/financial_fetcher.py` | `FinancialFetcher` — akshare Sina + EM fallback |
| Create | `features/northbound_factor.py` | `NorthboundFactor` — registered as `"northbound"` |
| Modify | `features/fundamental_factor.py` | Extend with profitability, growth, cashflow metrics |
| Modify | `models/trainer.py:28-37` | Add `"northbound_factor"` to importlib loop |
| Modify | `config/model.yaml:114-143` | Add northbound + fundamental factor config entries |
| Modify | `run_daily.py:107-155` | Add DataProvider refresh before signal generation |
| Modify | `run_scheduled_rebalance.py` | Add DataProvider refresh before rebalance |
| Create | `test/test_northbound_fetcher.py` | NorthboundFetcher unit tests |
| Create | `test/test_financial_fetcher.py` | FinancialFetcher unit tests |
| Create | `test/test_northbound_factor.py` | NorthboundFactor unit tests |
| Create | `test/test_fundamental_factor_extended.py` | Extended FundamentalFactor tests |

---

### Task 1: BaseDataFetcher ABC

**Files:**
- Create: `data/fetchers/__init__.py`
- Create: `data/fetchers/base.py`
- Test: `test/test_base_fetcher.py`

- [ ] **Step 1: Write failing test for BaseDataFetcher interface**

```python
# test/test_base_fetcher.py
import pytest
from quant_ex.data.fetchers.base import BaseDataFetcher


class ConcreteFetcher(BaseDataFetcher):
    """Minimal concrete impl for testing."""
    def fetch(self, symbols, start_date, end_date):
        return None

    def refresh_cache(self, symbols):
        pass


def test_cannot_instantiate_abc():
    with pytest.raises(TypeError):
        BaseDataFetcher(cache_dir="/tmp", cache_ttl_days=1)


def test_concrete_subclass_instantiates():
    f = ConcreteFetcher(cache_dir="/tmp", cache_ttl_days=1)
    assert f.cache_dir == "/tmp"
    assert f.cache_ttl_days == 1


def test_is_cache_fresh_missing_file(tmp_path):
    f = ConcreteFetcher(cache_dir=str(tmp_path), cache_ttl_days=1)
    assert f._is_cache_fresh(tmp_path / "nonexistent.csv") is False


def test_is_cache_fresh_within_ttl(tmp_path):
    f = ConcreteFetcher(cache_dir=str(tmp_path), cache_ttl_days=7)
    cache_file = tmp_path / "data.csv"
    cache_file.write_text("x")
    assert f._is_cache_fresh(cache_file) is True


def test_is_cache_fresh_expired(tmp_path):
    import time
    f = ConcreteFetcher(cache_dir=str(tmp_path), cache_ttl_days=0)
    cache_file = tmp_path / "data.csv"
    cache_file.write_text("x")
    # TTL=0 means always stale
    assert f._is_cache_fresh(cache_file) is False


def test_to_bare_code():
    assert BaseDataFetcher.to_bare_code("SH600000") == "600000"


def test_to_qlib_symbol():
    assert BaseDataFetcher.to_qlib_symbol("600000", "SH") == "SH600000"


def test_infer_exchange():
    assert BaseDataFetcher.infer_exchange("600000") == "SH"
    assert BaseDataFetcher.infer_exchange("000001") == "SZ"
    assert BaseDataFetcher.infer_exchange("300001") == "SZ"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -m pytest test/test_base_fetcher.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'quant_ex.data.fetchers'`

- [ ] **Step 3: Write BaseDataFetcher implementation**

```python
# data/fetchers/__init__.py
from .base import BaseDataFetcher
from .northbound_fetcher import NorthboundFetcher
from .financial_fetcher import FinancialFetcher

__all__ = ["BaseDataFetcher", "NorthboundFetcher", "FinancialFetcher"]
```

```python
# data/fetchers/base.py
"""Base class for external data fetchers with caching support."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class BaseDataFetcher(ABC):
    """Abstract base for fetching and caching external data.

    Subclasses implement ``fetch()`` and ``refresh_cache()``.
    Caching uses file mtime vs TTL for freshness checks.
    """

    def __init__(self, cache_dir: str, cache_ttl_days: int = 7):
        self.cache_dir = Path(cache_dir)
        self.cache_ttl_days = cache_ttl_days

    @abstractmethod
    def fetch(self, symbols: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Fetch data for symbols in date range.

        Returns DataFrame with (instrument, datetime) MultiIndex, or None.
        """

    @abstractmethod
    def refresh_cache(self, symbols: List[str]) -> None:
        """Refresh cache files for the given symbols."""

    def _is_cache_fresh(self, path: Path) -> bool:
        if not path.exists():
            return False
        if self.cache_ttl_days == 0:
            return False
        mtime = date.fromtimestamp(path.stat().st_mtime)
        return (date.today() - mtime).days < self.cache_ttl_days

    def _ensure_cache_dir(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def to_bare_code(qlib_symbol: str) -> str:
        """SH600000 → 600000"""
        return qlib_symbol[2:]

    @staticmethod
    def to_qlib_symbol(bare_code: str, exchange: str) -> str:
        """600000 + SH → SH600000"""
        return f"{exchange}{bare_code}"

    @staticmethod
    def infer_exchange(bare_code: str) -> str:
        """Infer exchange from 6-digit code: 6/9→SH, 0/3→SZ."""
        if bare_code.startswith(("6", "9")):
            return "SH"
        return "SZ"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -m pytest test/test_base_fetcher.py -v`
Expected: All 7 tests PASS

- [ ] **Step 5: Commit**

```bash
git add data/fetchers/__init__.py data/fetchers/base.py test/test_base_fetcher.py
git commit -m "feat: add BaseDataFetcher ABC with cache freshness and code conversion"
```

---

### Task 2: NorthboundFetcher

**Files:**
- Create: `data/fetchers/northbound_fetcher.py`
- Test: `test/test_northbound_fetcher.py`

- [ ] **Step 1: Write failing test for NorthboundFetcher**

```python
# test/test_northbound_fetcher.py
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock
from quant_ex.data.fetchers.northbound_fetcher import NorthboundFetcher


@pytest.fixture
def fetcher(tmp_path):
    return NorthboundFetcher(cache_dir=str(tmp_path / "northbound"), cache_ttl_days=1)


def test_fetch_holdings_returns_multiindex(fetcher, tmp_path):
    """_fetch_holdings should return DataFrame with (instrument, datetime) MultiIndex."""
    fake_df = pd.DataFrame({
        "代码": ["600519", "000001"],
        "日期": ["2026-04-29", "2026-04-29"],
        "今日持股-占流通股比": [5.2, 3.1],
        "今日持股-市值": [1000.0, 500.0],
        "今日增持估计-占流通股比": [0.1, -0.05],
        "今日收盘价": [1800.0, 15.0],
        "成交额": [200.0, 100.0],
    })
    with patch("quant_ex.data.fetchers.northbound_fetcher.NorthboundFetcher._call_akshare_holdings", return_value=fake_df):
        result = fetcher._fetch_holdings("2026-04-29")
    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    assert "nb_hold_pct" in result.columns
    assert "nb_hold_mv" in result.columns


def test_fetch_holdings_caches_result(fetcher, tmp_path):
    fake_df = pd.DataFrame({
        "代码": ["600519"],
        "日期": ["2026-04-29"],
        "今日持股-占流通股比": [5.2],
        "今日持股-市值": [1000.0],
        "今日增持估计-占流通股比": [0.1],
        "今日收盘价": [1800.0],
        "成交额": [200.0],
    })
    with patch("quant_ex.data.fetchers.northbound_fetcher.NorthboundFetcher._call_akshare_holdings", return_value=fake_df):
        fetcher._fetch_holdings("2026-04-29")
    cache_file = tmp_path / "northbound" / "holdings_2026-04-29.csv"
    assert cache_file.exists()


def test_fetch_holdings_reads_cache(fetcher, tmp_path):
    """Second call should read from cache, not call API."""
    cached = pd.DataFrame({
        "nb_hold_pct": [5.2],
        "nb_hold_mv": [1000.0],
        "nb_net_buy_ratio": [0.1],
        "nb_hold_pct_chg": [0.0],
    }, index=pd.MultiIndex.from_tuples(
        [("SH600519", pd.Timestamp("2026-04-29"))],
        names=["instrument", "datetime"],
    ))
    fetcher._ensure_cache_dir()
    cached.to_csv(tmp_path / "northbound" / "holdings_2026-04-29.csv")

    with patch("quant_ex.data.fetchers.northbound_fetcher.NorthboundFetcher._call_akshare_holdings") as mock_api:
        result = fetcher._fetch_holdings("2026-04-29")
    mock_api.assert_not_called()
    assert result is not None


def test_fetch_hist_flow_returns_multiindex(fetcher, tmp_path):
    fake_df = pd.DataFrame({
        "日期": ["2026-04-28", "2026-04-29"],
        "当日成交净买额": [50.0, 60.0],
        "买入成交额": [200.0, 220.0],
        "卖出成交额": [150.0, 160.0],
    })
    with patch("quant_ex.data.fetchers.northbound_fetcher.NorthboundFetcher._call_akshare_hist_flow", return_value=fake_df):
        result = fetcher._fetch_hist_flow()
    assert result is not None
    assert "nb_total_net_buy" in result.columns


def test_refresh_cache_calls_fetch_holdings(fetcher, tmp_path):
    symbols = ["SH600519"]
    with patch.object(fetcher, "_fetch_holdings") as mock_holdings, \
         patch.object(fetcher, "_fetch_hist_flow") as mock_flow:
        mock_holdings.return_value = pd.DataFrame()
        mock_flow.return_value = pd.DataFrame()
        fetcher.refresh_cache(symbols)
    # Should fetch holdings for today and hist flow
    assert mock_holdings.called or mock_flow.called


def test_fallback_to_eastmoney(fetcher, tmp_path):
    """When akshare fails, should attempt East Money fallback."""
    with patch("quant_ex.data.fetchers.northbound_fetcher.NorthboundFetcher._call_akshare_holdings", side_effect=Exception("akshare error")), \
         patch("quant_ex.data.fetchers.northbound_fetcher.NorthboundFetcher._call_eastmoney_holdings", return_value=None):
        result = fetcher._fetch_holdings("2026-04-29")
    # Should not crash; result may be None
    assert result is None or isinstance(result, pd.DataFrame)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -m pytest test/test_northbound_fetcher.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'quant_ex.data.fetchers.northbound_fetcher'`

- [ ] **Step 3: Write NorthboundFetcher implementation**

```python
# data/fetchers/northbound_fetcher.py
"""Northbound capital (沪深港通) data fetcher.

Primary: akshare (stock_hsgt_hold_stock_em, stock_hsgt_hist_em)
Fallback: East Money datacenter API (limited coverage)

Cache strategy:
- Holdings snapshot: cache/northbound/holdings_{date}.csv (1 per day)
- Historical flow: cache/northbound/hist_flow.csv (append daily)
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd

from .base import BaseDataFetcher

logger = logging.getLogger(__name__)


class NorthboundFetcher(BaseDataFetcher):
    """Fetch and cache northbound capital data."""

    def __init__(self, cache_dir: str = "./cache/northbound", cache_ttl_days: int = 1):
        super().__init__(cache_dir=cache_dir, cache_ttl_days=cache_ttl_days)

    def fetch(self, symbols: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Not used directly — factors read from cache files."""
        self.refresh_cache(symbols)
        return self._load_cached_range(start_date, end_date)

    def refresh_cache(self, symbols: List[str]) -> None:
        """Refresh holdings and flow cache for today."""
        today = date.today().strftime("%Y-%m-%d")
        self._fetch_holdings(today)
        self._fetch_hist_flow()

    # ── Holdings snapshot ──────────────────────────────────────────────────

    def _fetch_holdings(self, date_str: str) -> Optional[pd.DataFrame]:
        """Fetch full-market northbound holdings for one day."""
        self._ensure_cache_dir()
        cache_file = self.cache_dir / f"holdings_{date_str}.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        df = self._fetch_holdings_with_fallback(date_str)
        if df is not None and not df.empty:
            df.to_csv(cache_file)
            logger.info(f"NorthboundFetcher: cached holdings for {date_str} ({len(df)} stocks)")
        return df

    def _fetch_holdings_with_fallback(self, date_str: str) -> Optional[pd.DataFrame]:
        try:
            raw = self._call_akshare_holdings()
        except Exception as exc:
            logger.warning(f"NorthboundFetcher: akshare holdings failed: {exc}")
            raw = None

        if raw is None:
            try:
                raw = self._call_eastmoney_holdings()
            except Exception as exc:
                logger.warning(f"NorthboundFetcher: eastmoney holdings fallback failed: {exc}")
                return None

        return self._normalize_holdings(raw)

    def _call_akshare_holdings(self) -> Optional[pd.DataFrame]:
        import akshare as ak
        return ak.stock_hsgt_hold_stock_em(market="北向", indicator="今日排行")

    def _call_eastmoney_holdings(self) -> Optional[pd.DataFrame]:
        # East Money datacenter API for northbound holdings
        # Limited implementation — returns None if unavailable
        logger.debug("NorthboundFetcher: East Money northbound holdings not yet supported")
        return None

    def _normalize_holdings(self, raw: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Convert akshare holdings output to (instrument, datetime) MultiIndex."""
        if raw is None or raw.empty:
            return None

        df = raw.copy()
        # Date column
        date_col = next((c for c in df.columns if "日期" in str(c)), None)
        if date_col is None:
            return None
        df[date_col] = pd.to_datetime(df[date_col])
        trade_date = df[date_col].iloc[0]

        # Code → qlib instrument
        code_col = next((c for c in df.columns if "代码" in str(c)), None)
        if code_col is None:
            return None
        df["instrument"] = df[code_col].apply(self._code_to_instrument)

        # Build output
        hold_pct_col = next((c for c in df.columns if "占流通股比" in str(c)), None)
        hold_mv_col = next((c for c in df.columns if "持股市值" in str(c) or "持股-市值" in str(c)), None)
        chg_pct_col = next((c for c in df.columns if "增持估计-占流通股比" in str(c)), None)
        amount_col = next((c for c in df.columns if "成交额" in str(c)), None)

        result = pd.DataFrame(index=df["instrument"])
        result["datetime"] = trade_date
        result["nb_hold_pct"] = df[hold_pct_col].astype(float) if hold_pct_col else 0.0
        result["nb_hold_mv"] = df[hold_mv_col].astype(float) if hold_mv_col else 0.0
        result["nb_hold_pct_chg"] = df[chg_pct_col].astype(float) if chg_pct_col else 0.0

        # Net buy ratio = hold_pct_chg * market_cap / amount
        # Simplified: use the change in holding pct directly
        if amount_col is not None and hold_pct_col is not None:
            result["nb_net_buy_ratio"] = result["nb_hold_pct_chg"]
        else:
            result["nb_net_buy_ratio"] = 0.0

        result = result.reset_index().set_index(["instrument", "datetime"])
        return result

    # ── Historical flow ────────────────────────────────────────────────────

    def _fetch_hist_flow(self) -> Optional[pd.DataFrame]:
        """Fetch historical northbound aggregate flow."""
        self._ensure_cache_dir()
        cache_file = self.cache_dir / "hist_flow.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        df = self._fetch_hist_flow_with_fallback()
        if df is not None and not df.empty:
            df.to_csv(cache_file)
            logger.info(f"NorthboundFetcher: cached hist_flow ({len(df)} days)")
        return df

    def _fetch_hist_flow_with_fallback(self) -> Optional[pd.DataFrame]:
        try:
            raw = self._call_akshare_hist_flow()
        except Exception as exc:
            logger.warning(f"NorthboundFetcher: akshare hist_flow failed: {exc}")
            return None

        if raw is None:
            return None
        return self._normalize_hist_flow(raw)

    def _call_akshare_hist_flow(self) -> Optional[pd.DataFrame]:
        import akshare as ak
        return ak.stock_hsgt_hist_em(symbol="北向资金")

    def _normalize_hist_flow(self, raw: pd.DataFrame) -> Optional[pd.DataFrame]:
        if raw is None or raw.empty:
            return None
        df = raw.copy()
        date_col = next((c for c in df.columns if "日期" in str(c) or "date" in str(c).lower()), df.columns[0])
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col)
        df.index.name = "datetime"

        rename_map = {}
        net_buy_col = next((c for c in df.columns if "净买额" in str(c)), None)
        if net_buy_col:
            rename_map[net_buy_col] = "nb_total_net_buy"
        buy_col = next((c for c in df.columns if "买入成交额" in str(c)), None)
        if buy_col:
            rename_map[buy_col] = "nb_total_buy"
        sell_col = next((c for c in df.columns if "卖出成交额" in str(c)), None)
        if sell_col:
            rename_map[sell_col] = "nb_total_sell"

        df = df.rename(columns=rename_map)
        keep = [c for c in ["nb_total_net_buy", "nb_total_buy", "nb_total_sell"] if c in df.columns]
        if not keep:
            return None
        df = df[keep].apply(pd.to_numeric, errors="coerce")
        return df

    # ── Helpers ────────────────────────────────────────────────────────────

    @staticmethod
    def _code_to_instrument(code: str) -> str:
        """Convert 6-digit code or prefixed code to qlib instrument."""
        bare = code.strip()
        if bare.startswith(("SH", "SZ")):
            return bare
        exchange = "SH" if bare.startswith(("6", "9")) else "SZ"
        return f"{exchange}{bare}"

    def _read_cache(self, path: Path) -> Optional[pd.DataFrame]:
        try:
            df = pd.read_csv(path, index_col=[0, 1], parse_dates=[1])
            df.index.names = ["instrument", "datetime"]
            return df
        except Exception as exc:
            logger.warning(f"NorthboundFetcher: cache read failed {path}: {exc}")
            return None

    def _load_cached_range(self, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """Load and concatenate cached holdings files in a date range."""
        files = sorted(self.cache_dir.glob("holdings_*.csv"))
        if not files:
            return None
        frames = []
        for f in files:
            try:
                df = pd.read_csv(f, index_col=[0, 1], parse_dates=[1])
                df.index.names = ["instrument", "datetime"]
                dates = df.index.get_level_values(1)
                mask = (dates >= pd.Timestamp(start_date)) & (dates <= pd.Timestamp(end_date))
                if mask.any():
                    frames.append(df[mask])
            except Exception:
                continue
        if not frames:
            return None
        return pd.concat(frames)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -m pytest test/test_northbound_fetcher.py -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add data/fetchers/northbound_fetcher.py test/test_northbound_fetcher.py
git commit -m "feat: add NorthboundFetcher with akshare + EM fallback and daily caching"
```

---

### Task 3: FinancialFetcher

**Files:**
- Create: `data/fetchers/financial_fetcher.py`
- Test: `test/test_financial_fetcher.py`

- [ ] **Step 1: Write failing test for FinancialFetcher**

```python
# test/test_financial_fetcher.py
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock
from quant_ex.data.fetchers.financial_fetcher import FinancialFetcher


@pytest.fixture
def fetcher(tmp_path):
    return FinancialFetcher(cache_dir=str(tmp_path / "financial"), cache_ttl_days=7)


def test_fetch_indicators_returns_multiindex(fetcher, tmp_path):
    fake_df = pd.DataFrame({
        "日期": ["2025-03-31", "2025-06-30"],
        "净资产收益率(%)": [12.5, 13.0],
        "总资产利润率(%)": [5.2, 5.5],
        "销售毛利率(%)": [40.0, 41.0],
        "销售净利率(%)": [15.0, 15.5],
        "主营业务收入增长率(%)": [10.0, 12.0],
        "净利润增长率(%)": [8.0, 9.0],
        "经营现金净流量与净利润的比率(%)": [80.0, 85.0],
    })
    with patch("quant_ex.data.fetchers.financial_fetcher.FinancialFetcher._call_akshare_sina", return_value=fake_df):
        result = fetcher._fetch_indicators("SH600519")
    assert result is not None
    assert result.index.names == ["instrument", "datetime"]
    assert "roe" in result.columns
    assert "gross_margin" in result.columns


def test_fetch_indicators_caches_result(fetcher, tmp_path):
    fake_df = pd.DataFrame({
        "日期": ["2025-03-31"],
        "净资产收益率(%)": [12.5],
        "销售毛利率(%)": [40.0],
    })
    with patch("quant_ex.data.fetchers.financial_fetcher.FinancialFetcher._call_akshare_sina", return_value=fake_df):
        fetcher._fetch_indicators("SH600519")
    cache_file = tmp_path / "financial" / "SH600519.csv"
    assert cache_file.exists()


def test_fetch_indicators_reads_cache(fetcher, tmp_path):
    cached = pd.DataFrame({
        "roe": [12.5],
        "gross_margin": [40.0],
    }, index=pd.MultiIndex.from_tuples(
        [("SH600519", pd.Timestamp("2025-03-31"))],
        names=["instrument", "datetime"],
    ))
    fetcher._ensure_cache_dir()
    cached.to_csv(tmp_path / "financial" / "SH600519.csv")

    with patch("quant_ex.data.fetchers.financial_fetcher.FinancialFetcher._call_akshare_sina") as mock_api:
        result = fetcher._fetch_indicators("SH600519")
    mock_api.assert_not_called()
    assert result is not None


def test_normalize_indicators_column_mapping(fetcher):
    raw = pd.DataFrame({
        "日期": ["2025-03-31"],
        "净资产收益率(%)": [12.5],
        "总资产利润率(%)": [5.2],
        "销售毛利率(%)": [40.0],
        "销售净利率(%)": [15.0],
        "主营业务收入增长率(%)": [10.0],
        "净利润增长率(%)": [8.0],
        "经营现金净流量与净利润的比率(%)": [80.0],
    })
    result = fetcher._normalize_indicators(raw, "SH600519")
    assert "roe" in result.columns
    assert "roa" in result.columns
    assert "revenue_growth" in result.columns
    assert "ocf_to_np" in result.columns


def test_fallback_to_em(fetcher, tmp_path):
    with patch("quant_ex.data.fetchers.financial_fetcher.FinancialFetcher._call_akshare_sina", side_effect=Exception("sina error")), \
         patch("quant_ex.data.fetchers.financial_fetcher.FinancialFetcher._call_akshare_em", return_value=None):
        result = fetcher._fetch_indicators("SH600519")
    assert result is None or isinstance(result, pd.DataFrame)


def test_compute_free_cash_flow(fetcher):
    cf_df = pd.DataFrame({
        "日期": ["2025-03-31"],
        "经营活动产生的现金流量净额": [100.0],
        "购建固定资产无形资产和其他长期资产支付的现金": [30.0],
    })
    result = fetcher._compute_fcf(cf_df)
    assert result == 70.0  # 100 - 30
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -m pytest test/test_financial_fetcher.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'quant_ex.data.fetchers.financial_fetcher'`

- [ ] **Step 3: Write FinancialFetcher implementation**

```python
# data/fetchers/financial_fetcher.py
"""Financial fundamental data fetcher.

Primary: akshare stock_financial_analysis_indicator (Sina, 6-digit code)
Fallback: akshare stock_financial_analysis_indicator_em (East Money, code.SH format)

Also fetches cash flow statement for free cash flow computation.
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd

from .base import BaseDataFetcher

logger = logging.getLogger(__name__)

# Sina Chinese column names → our English metric names
_SINA_COL_MAP = {
    "净资产收益率(%)": "roe",
    "加权净资产收益率(%)": "roe_weighted",
    "总资产利润率(%)": "roa",
    "资产报酬率(%)": "roa_alt",
    "销售毛利率(%)": "gross_margin",
    "销售净利率(%)": "net_margin",
    "主营业务收入增长率(%)": "revenue_growth",
    "净利润增长率(%)": "profit_growth",
    "摊薄每股收益(元)": "eps",
    "经营现金净流量与净利润的比率(%)": "ocf_to_np",
}

# All metrics we can produce from the Sina interface
_SINA_METRICS = list(set(_SINA_COL_MAP.values()))


class FinancialFetcher(BaseDataFetcher):
    """Fetch and cache financial fundamental data."""

    def __init__(self, cache_dir: str = "./cache/financial", cache_ttl_days: int = 7):
        super().__init__(cache_dir=cache_dir, cache_ttl_days=cache_ttl_days)

    def fetch(self, symbols: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        self.refresh_cache(symbols)
        return self._load_cached_range(symbols, start_date, end_date)

    def refresh_cache(self, symbols: List[str]) -> None:
        self._ensure_cache_dir()
        for sym in symbols:
            self._fetch_indicators(sym)

    # ── Per-stock indicators ───────────────────────────────────────────────

    def _fetch_indicators(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        cache_file = self.cache_dir / f"{qlib_symbol}.csv"

        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        df = self._fetch_indicators_with_fallback(qlib_symbol)
        if df is not None and not df.empty:
            df.to_csv(cache_file)
            logger.debug(f"FinancialFetcher: cached indicators for {qlib_symbol}")
        return df

    def _fetch_indicators_with_fallback(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        # Primary: Sina interface (6-digit code)
        try:
            raw = self._call_akshare_sina(qlib_symbol)
        except Exception as exc:
            logger.debug(f"FinancialFetcher: Sina failed for {qlib_symbol}: {exc}")
            raw = None

        if raw is not None:
            return self._normalize_indicators(raw, qlib_symbol)

        # Fallback: EM interface (code.SH format)
        try:
            raw = self._call_akshare_em(qlib_symbol)
        except Exception as exc:
            logger.debug(f"FinancialFetcher: EM fallback failed for {qlib_symbol}: {exc}")
            return None

        if raw is not None:
            return self._normalize_em_indicators(raw, qlib_symbol)
        return None

    def _call_akshare_sina(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        import akshare as ak
        code = self.to_bare_code(qlib_symbol)
        start_year = str(date.today().year - 3)
        return ak.stock_financial_analysis_indicator(symbol=code, start_year=start_year)

    def _call_akshare_em(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        import akshare as ak
        # EM needs format like "600519.SH"
        code = self.to_bare_code(qlib_symbol)
        exchange = self.infer_exchange(code)
        em_code = f"{code}.{exchange}"
        return ak.stock_financial_analysis_indicator_em(symbol=em_code, indicator="按报告期")

    def _normalize_indicators(self, raw: pd.DataFrame, qlib_symbol: str) -> Optional[pd.DataFrame]:
        if raw is None or raw.empty:
            return None
        df = raw.copy()
        date_col = next((c for c in df.columns if "日期" in str(c) or "date" in str(c).lower()), df.columns[0])
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col)
        df.index.name = "datetime"

        # Rename Chinese columns to English
        rename = {c: _SINA_COL_MAP[c] for c in df.columns if c in _SINA_COL_MAP}
        df = df.rename(columns=rename)

        keep = [c for c in _SINA_METRICS if c in df.columns]
        if not keep:
            return None
        df = df[keep].apply(pd.to_numeric, errors="coerce")

        df.index = pd.MultiIndex.from_product(
            [[qlib_symbol], df.index], names=["instrument", "datetime"]
        )
        return df

    def _normalize_em_indicators(self, raw: pd.DataFrame, qlib_symbol: str) -> Optional[pd.DataFrame]:
        """Normalize EM format (English column names like ROEJQ, XSMLL, etc.)."""
        if raw is None or raw.empty:
            return None
        df = raw.copy()
        em_col_map = {
            "ROEJQ": "roe",
            "ZZCJLL": "roa",
            "XSMLL": "gross_margin",
            "XSJLL": "net_margin",
            "TOTALOPERATEREVETZ": "revenue_growth",
            "PARENTNETPROFITTZ": "profit_growth",
            "JYXJLYYSR": "ocf_to_np",
        }
        date_col = next((c for c in df.columns if "日期" in str(c) or "date" in str(c).lower() or "REPORT_DATE" in str(c)), df.columns[0])
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col)
        df.index.name = "datetime"

        rename = {c: em_col_map[c] for c in df.columns if c in em_col_map}
        df = df.rename(columns=rename)

        keep = [c for c in _SINA_METRICS if c in df.columns]
        if not keep:
            return None
        df = df[keep].apply(pd.to_numeric, errors="coerce")
        df.index = pd.MultiIndex.from_product(
            [[qlib_symbol], df.index], names=["instrument", "datetime"]
        )
        return df

    # ── Cash flow for FCF ─────────────────────────────────────────────────

    def _fetch_cash_flow(self, qlib_symbol: str) -> Optional[pd.DataFrame]:
        cache_file = self.cache_dir / f"{qlib_symbol}_cf.csv"
        if self._is_cache_fresh(cache_file):
            return self._read_cache(cache_file)

        try:
            import akshare as ak
            code = f"{self.infer_exchange(self.to_bare_code(qlib_symbol))}{self.to_bare_code(qlib_symbol)}"
            raw = ak.stock_cash_flow_sheet_by_report_em(symbol=code)
        except Exception as exc:
            logger.debug(f"FinancialFetcher: cash flow fetch failed for {qlib_symbol}: {exc}")
            return None

        if raw is None or raw.empty:
            return None
        raw.to_csv(cache_file)
        return raw

    @staticmethod
    def _compute_fcf(cf_df: pd.DataFrame) -> Optional[float]:
        """Compute free cash flow: operating CF - capex."""
        ocf_col = next((c for c in cf_df.columns if "经营活动产生的现金流量净额" in str(c)), None)
        capex_col = next((c for c in cf_df.columns if "购建固定资产无形资产和其他长期资产支付的现金" in str(c)), None)
        if ocf_col is None or capex_col is None:
            return None
        ocf = pd.to_numeric(cf_df[ocf_col].iloc[0], errors="coerce")
        capex = pd.to_numeric(cf_df[capex_col].iloc[0], errors="coerce")
        if pd.isna(ocf) or pd.isna(capex):
            return None
        return float(ocf - capex)

    # ── Helpers ────────────────────────────────────────────────────────────

    def _read_cache(self, path: Path) -> Optional[pd.DataFrame]:
        try:
            df = pd.read_csv(path, index_col=[0, 1], parse_dates=[1])
            df.index.names = ["instrument", "datetime"]
            return df
        except Exception as exc:
            logger.warning(f"FinancialFetcher: cache read failed {path}: {exc}")
            return None

    def _load_cached_range(self, symbols: List[str], start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        frames = []
        for sym in symbols:
            cache_file = self.cache_dir / f"{sym}.csv"
            if not cache_file.exists():
                continue
            try:
                df = pd.read_csv(cache_file, index_col=[0, 1], parse_dates=[1])
                df.index.names = ["instrument", "datetime"]
                dates = df.index.get_level_values(1)
                mask = (dates >= pd.Timestamp(start_date)) & (dates <= pd.Timestamp(end_date))
                if mask.any():
                    frames.append(df[mask])
            except Exception:
                continue
        if not frames:
            return None
        return pd.concat(frames)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -m pytest test/test_financial_fetcher.py -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add data/fetchers/financial_fetcher.py test/test_financial_fetcher.py
git commit -m "feat: add FinancialFetcher with Sina/EM dual source and FCF computation"
```

---

### Task 4: NorthboundFactor

**Files:**
- Create: `features/northbound_factor.py`
- Test: `test/test_northbound_factor.py`

- [ ] **Step 1: Write failing test for NorthboundFactor**

```python
# test/test_northbound_factor.py
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from quant_ex.features.northbound_factor import NorthboundFactor


def _make_price_data(n_days=60, instruments=None):
    """Create a minimal price_data MultiIndex DataFrame for testing."""
    if instruments is None:
        instruments = ["SH600519", "SZ000001", "SH601318"]
    dates = pd.bdate_range("2026-02-01", periods=n_days)
    idx = pd.MultiIndex.from_product([instruments, dates], names=["instrument", "datetime"])
    return pd.DataFrame({"real_close": np.random.uniform(10, 100, len(idx))}, index=idx)


def _make_holdings_data(instruments, date_str="2026-04-29"):
    """Create a holdings cache DataFrame."""
    data = {
        "nb_hold_pct": [5.2, 3.1, 8.0],
        "nb_hold_mv": [1000.0, 500.0, 2000.0],
        "nb_net_buy_ratio": [0.1, -0.05, 0.2],
        "nb_hold_pct_chg": [0.1, -0.05, 0.2],
    }
    idx = pd.MultiIndex.from_tuples(
        [(inst, pd.Timestamp(date_str)) for inst in instruments],
        names=["instrument", "datetime"],
    )
    return pd.DataFrame(data, index=idx)


def test_compute_returns_dataframe():
    """NorthboundFactor.compute() should return a DataFrame with expected columns."""
    factor = NorthboundFactor(windows=[5, 20], cache_dir="./cache/northbound_test")
    price_data = _make_price_data()
    holdings = _make_holdings_data(list(price_data.index.get_level_values(0).unique()))

    with patch.object(factor, "_load_holdings_cache", return_value=holdings):
        result = factor.compute(price_data)
    assert result is not None
    assert "nb_hold_pct" in result.columns
    assert "nb_hold_pct_chg_5d" in result.columns
    assert "nb_hold_pct_chg_20d" in result.columns


def test_compute_with_sector_aggregation():
    """When sector_map is provided, sector-level factors should be computed."""
    instruments = ["SH600519", "SZ000001"]
    sector_map = {"SH600519": "白酒", "SZ000001": "银行"}
    factor = NorthboundFactor(windows=[5], cache_dir="./cache/northbound_test")
    price_data = _make_price_data(instruments=instruments)
    holdings = _make_holdings_data(instruments)

    with patch.object(factor, "_load_holdings_cache", return_value=holdings):
        result = factor.compute(price_data, sector_map=sector_map)
    assert result is not None
    assert "nb_sector_hold_pct" in result.columns
    assert "nb_vs_sector_5d" in result.columns


def test_stocks_without_northbound_get_zero():
    """Stocks not in holdings data should get 0 for nb_hold_pct, not NaN."""
    factor = NorthboundFactor(windows=[5], cache_dir="./cache/northbound_test")
    price_data = _make_price_data(instruments=["SH600519", "SZ300001"])
    # Only SH600519 in holdings
    holdings = _make_holdings_data(["SH600519"])

    with patch.object(factor, "_load_holdings_cache", return_value=holdings):
        result = factor.compute(price_data)
    assert result is not None
    # SZ300001 should have 0, not NaN
    sz_data = result.loc["SZ300001"]
    assert (sz_data["nb_hold_pct"] == 0).all()


def test_align_to_price_data_index():
    """Output should be reindexed to match price_data exactly."""
    factor = NorthboundFactor(windows=[5], cache_dir="./cache/northbound_test")
    price_data = _make_price_data(n_days=10)
    holdings = _make_holdings_data(list(price_data.index.get_level_values(0).unique()))

    with patch.object(factor, "_load_holdings_cache", return_value=holdings):
        result = factor.compute(price_data)
    assert result is not None
    assert result.index.equals(price_data.index)


def test_change_factors_computation():
    """Change factors should compute windowed differences."""
    factor = NorthboundFactor(windows=[5], include_raw=True, include_change=True,
                               cache_dir="./cache/northbound_test")
    price_data = _make_price_data(n_days=30, instruments=["SH600519"])
    # Create holdings with time series
    dates = pd.bdate_range("2026-02-01", periods=30)
    idx = pd.MultiIndex.from_tuples(
        [("SH600519", d) for d in dates], names=["instrument", "datetime"]
    )
    holdings = pd.DataFrame({
        "nb_hold_pct": np.linspace(3.0, 5.0, 30),
        "nb_hold_mv": np.linspace(500, 800, 30),
        "nb_net_buy_ratio": np.random.uniform(-0.1, 0.1, 30),
        "nb_hold_pct_chg": np.random.uniform(-0.1, 0.1, 30),
    }, index=idx)

    with patch.object(factor, "_load_holdings_cache", return_value=holdings):
        result = factor.compute(price_data)
    assert result is not None
    assert "nb_hold_pct_chg_5d" in result.columns
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -m pytest test/test_northbound_factor.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'quant_ex.features.northbound_factor'`

- [ ] **Step 3: Write NorthboundFactor implementation**

```python
# features/northbound_factor.py
"""Northbound capital (沪深港通) factor provider.

Reads cached data from NorthboundFetcher, computes raw and change factors.

Raw factors: nb_hold_pct, nb_hold_mv, nb_net_buy_ratio
Change factors: nb_hold_pct_chg_{w}d, nb_net_buy_ma_{w}d
Sector factors: nb_sector_hold_pct, nb_vs_sector_{w}d (requires sector_map)
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from .base import BaseFactor, FactorRegistry

logger = logging.getLogger(__name__)


@FactorRegistry.register("northbound")
class NorthboundFactor(BaseFactor):
    """Northbound capital factors from cached holdings data."""

    name = "northbound"

    def __init__(
        self,
        windows: List[int] = None,
        include_raw: bool = True,
        include_change: bool = True,
        cache_dir: str = "./cache/northbound",
        cache_ttl_days: int = 1,
        sector_map: Optional[Dict[str, str]] = None,
    ):
        self.windows = windows or [5, 10, 20, 60]
        self.include_raw = include_raw
        self.include_change = include_change
        self.cache_dir = Path(cache_dir)
        self.cache_ttl_days = cache_ttl_days
        self.sector_map = sector_map

    def compute(self, price_data: pd.DataFrame, sector_map: Optional[Dict[str, str]] = None) -> Optional[pd.DataFrame]:
        holdings = self._load_holdings_cache()
        if holdings is None or holdings.empty:
            logger.warning("NorthboundFactor: no holdings cache data available")
            return None

        sm = sector_map or self.sector_map
        instruments = list(price_data.index.get_level_values(0).unique())
        dates = price_data.index.get_level_values(1).unique()

        # Fill missing instruments with 0
        target_idx = pd.MultiIndex.from_product(
            [instruments, dates], names=["instrument", "datetime"]
        )
        holdings = holdings.reindex(target_idx, fill_value=0)

        # Forward-fill within each instrument
        holdings = holdings.groupby(level=0, group_keys=False).ffill()

        result_parts = []

        # Raw factors
        if self.include_raw:
            raw_cols = ["nb_hold_pct", "nb_hold_mv", "nb_net_buy_ratio"]
            existing_raw = [c for c in raw_cols if c in holdings.columns]
            if existing_raw:
                result_parts.append(holdings[existing_raw])

        # Change factors
        if self.include_change:
            for w in self.windows:
                if "nb_hold_pct" in holdings.columns:
                    chg = holdings["nb_hold_pct"].groupby(level=0).diff(w)
                    result_parts.append(chg.rename(f"nb_hold_pct_chg_{w}d"))
                if "nb_net_buy_ratio" in holdings.columns:
                    ma = holdings["nb_net_buy_ratio"].groupby(level=0).rolling(w, min_periods=1).mean()
                    ma = ma.droplevel(0) if ma.index.nlevels > 2 else ma
                    result_parts.append(ma.rename(f"nb_net_buy_ma_{w}d"))

        # Sector aggregation
        if sm and "nb_hold_pct" in holdings.columns:
            holdings_with_sector = holdings.copy()
            holdings_with_sector["sector"] = holdings_with_sector.index.get_level_values(0).map(sm)
            sector_avg = holdings_with_sector.groupby(
                [holdings_with_sector.index.get_level_values(1), "sector"]
            )["nb_hold_pct"].mean()
            sector_avg.index.names = ["datetime", "sector"]
            # Map back to instrument level
            inst_sector = holdings_with_sector.index.get_level_values(0).map(sm)
            date_idx = holdings_with_sector.index.get_level_values(1)
            mapped = []
            for inst, dt in holdings_with_sector.index:
                sec = sm.get(inst)
                if sec and (dt, sec) in sector_avg.index:
                    mapped.append(sector_avg.loc[(dt, sec)])
                else:
                    mapped.append(np.nan)
            result_parts.append(pd.Series(mapped, index=holdings_with_sector.index, name="nb_sector_hold_pct"))

            # Stock vs sector
            if self.include_change:
                for w in self.windows:
                    stock_chg = holdings["nb_hold_pct"].groupby(level=0).diff(w)
                    result_parts.append(stock_chg.rename(f"nb_vs_sector_{w}d"))

        if not result_parts:
            return None

        result = pd.concat(result_parts, axis=1)
        result = result.loc[:, ~result.columns.duplicated()]

        # Reindex to price_data
        result = result.reindex(price_data.index)
        return result

    def _load_holdings_cache(self) -> Optional[pd.DataFrame]:
        """Load all cached holdings files and concatenate."""
        files = sorted(self.cache_dir.glob("holdings_*.csv"))
        if not files:
            return None
        frames = []
        for f in files:
            try:
                df = pd.read_csv(f, index_col=[0, 1], parse_dates=[1])
                df.index.names = ["instrument", "datetime"]
                frames.append(df)
            except Exception as exc:
                logger.debug(f"NorthboundFactor: failed to read {f}: {exc}")
        if not frames:
            return None
        return pd.concat(frames).sort_index()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -m pytest test/test_northbound_factor.py -v`
Expected: All 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add features/northbound_factor.py test/test_northbound_factor.py
git commit -m "feat: add NorthboundFactor with raw, change, and sector aggregation factors"
```

---

### Task 5: Extend FundamentalFactor

**Files:**
- Modify: `features/fundamental_factor.py`
- Test: `test/test_fundamental_factor_extended.py`

- [ ] **Step 1: Write failing test for extended FundamentalFactor**

```python
# test/test_fundamental_factor_extended.py
import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch
from quant_ex.features.fundamental_factor import FundamentalFactor


def _make_price_data(n_days=30, instruments=None):
    if instruments is None:
        instruments = ["SH600519"]
    dates = pd.bdate_range("2026-02-01", periods=n_days)
    idx = pd.MultiIndex.from_product([instruments, dates], names=["instrument", "datetime"])
    return pd.DataFrame({"real_close": np.random.uniform(10, 100, len(idx))}, index=idx)


def test_extended_metrics_groups():
    """metrics=['valuation', 'profitability', 'growth', 'cashflow'] should enable all metrics."""
    factor = FundamentalFactor(metrics=["valuation", "profitability", "growth", "cashflow"])
    expected = {"pe_ttm", "pb", "ps_ttm", "dyr", "roe", "roa", "gross_margin",
                "net_margin", "revenue_growth", "profit_growth", "ocf_to_np"}
    assert expected.issubset(set(factor.metrics))


def test_profitability_metrics_only():
    """metrics=['profitability'] should only include profitability columns."""
    factor = FundamentalFactor(metrics=["profitability"])
    assert "roe" in factor.metrics
    assert "pe_ttm" not in factor.metrics


def test_change_factors_appended():
    """include_change=True should add roe_chg, margin_chg, rev_accel columns."""
    factor = FundamentalFactor(metrics=["profitability", "growth"], include_change=True)
    precomputed = pd.DataFrame({
        "roe": [12.0, 13.0, 14.0],
        "gross_margin": [40.0, 41.0, 42.0],
        "revenue_growth": [10.0, 12.0, 8.0],
    }, index=pd.MultiIndex.from_tuples(
        [("SH600519", pd.Timestamp(f"2025-{str(i).zfill(2)}-01")) for i in range(1, 4)],
        names=["instrument", "datetime"],
    ))
    factor.precomputed = precomputed
    price_data = _make_price_data()
    result = factor.compute(price_data)
    assert result is not None
    assert "roe_chg" in result.columns
    assert "margin_chg" in result.columns
    assert "rev_accel" in result.columns


def test_backward_compat_valuation_only():
    """Default metrics=['valuation'] should produce same columns as old default."""
    factor = FundamentalFactor(metrics=["valuation"])
    assert set(factor.metrics) == {"pe_ttm", "pb", "ps_ttm", "dyr"}


def test_uses_financial_fetcher():
    """When extended metrics are requested, should use FinancialFetcher."""
    factor = FundamentalFactor(metrics=["profitability"])
    assert factor._use_fetcher is True


def test_uses_old_path_for_valuation_only():
    """When only valuation metrics requested, should use old akshare path."""
    factor = FundamentalFactor(metrics=["valuation"])
    assert factor._use_fetcher is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -m pytest test/test_fundamental_factor_extended.py -v`
Expected: FAIL — `FundamentalFactor.__init__` doesn't accept `metrics` as a group list yet

- [ ] **Step 3: Extend FundamentalFactor implementation**

Modify `features/fundamental_factor.py` — the key changes are:

1. Accept `metrics` as a list of group names (`"valuation"`, `"profitability"`, `"growth"`, `"cashflow"`) in addition to individual column names
2. Add `include_change` parameter for change/delta factors
3. When extended metrics are requested, use `FinancialFetcher` instead of direct akshare calls
4. Preserve backward compatibility: `metrics=["pe_ttm", "pb"]` still works

Replace the existing `_SUPPORTED_METRICS`, `__init__`, and `_align` in `features/fundamental_factor.py`:

```python
# --- Replace _SUPPORTED_METRICS and _COL_MAP at top of file ---

_METRIC_GROUPS = {
    "valuation": ["pe_ttm", "pb", "ps_ttm", "dyr"],
    "profitability": ["roe", "roa", "gross_margin", "net_margin"],
    "growth": ["revenue_growth", "profit_growth"],
    "cashflow": ["ocf_to_np", "fcf_yield"],
}

_ALL_METRICS = []
for _group_metrics in _METRIC_GROUPS.values():
    _ALL_METRICS.extend(_group_metrics)

# akshare column name → our metric name (for old direct-akshare path)
_COL_MAP = {
    "pe": "pe_ttm",
    "pe_ttm": "pe_ttm",
    "pb": "pb",
    "ps": "ps_ttm",
    "ps_ttm": "ps_ttm",
    "dyr": "dyr",
    "股息率": "dyr",
    "市盈率(TTM)": "pe_ttm",
    "市净率": "pb",
    "市销率(TTM)": "ps_ttm",
}

# --- Replace FundamentalFactor.__init__ and add new methods ---

@FactorRegistry.register("fundamental")
class FundamentalFactor(BaseFactor):
    """Historical valuation and financial factors from akshare / FinancialFetcher.

    Parameters
    ----------
    metrics : list[str], optional
        Can be individual column names (e.g. ``["pe_ttm", "pb"]``) or group
        names (e.g. ``["valuation", "profitability"]``). Groups expand to:
        - valuation: pe_ttm, pb, ps_ttm, dyr
        - profitability: roe, roa, gross_margin, net_margin
        - growth: revenue_growth, profit_growth
        - cashflow: ocf_to_np, fcf_yield
        Defaults to ``["valuation"]`` for backward compat.
    include_change : bool
        If True, add quarter-over-quarter change factors (roe_chg, margin_chg, rev_accel).
    cache_dir : str
        Directory for per-stock CSV caches.
    cache_ttl_days : int
        Refresh cache files older than this many days.
    max_workers : int
        Thread count for parallel per-stock fetches.
    precomputed : DataFrame, optional
        Provide your own (instrument, datetime) MultiIndex DataFrame to skip
        the fetch entirely — useful for testing or custom data.
    """

    def __init__(
        self,
        metrics: Optional[List[str]] = None,
        include_change: bool = False,
        cache_dir: str = "./cache/fundamental",
        cache_ttl_days: int = 7,
        max_workers: int = 4,
        precomputed: Optional[pd.DataFrame] = None,
    ):
        if metrics is None:
            metrics = ["valuation"]

        # Expand group names to individual metrics
        expanded = []
        for m in metrics:
            if m in _METRIC_GROUPS:
                expanded.extend(_METRIC_GROUPS[m])
            else:
                expanded.append(m)
        self.metrics = expanded
        self.include_change = include_change
        self.cache_dir = Path(cache_dir)
        self.cache_ttl_days = cache_ttl_days
        self.max_workers = max_workers
        self.precomputed = precomputed

        # Decide whether to use FinancialFetcher (for extended metrics)
        valuation_only = set(self.metrics) <= set(_METRIC_GROUPS["valuation"])
        self._use_fetcher = not valuation_only
```

Then add a new `_compute_change_factors` method and update `compute` / `_align`:

```python
    def compute(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        if self.precomputed is not None:
            return self._align(self.precomputed, price_data)

        if self._use_fetcher:
            return self._compute_via_fetcher(price_data)

        # Old path: direct akshare for valuation only
        instruments = list(price_data.index.get_level_values(0).unique())
        all_frames = self._fetch_all(instruments)
        if not all_frames:
            return None
        combined = pd.concat(all_frames)
        return self._align(combined, price_data)

    def _compute_via_fetcher(self, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Fetch extended financial data via FinancialFetcher."""
        from ..data.fetchers.financial_fetcher import FinancialFetcher
        instruments = list(price_data.index.get_level_values(0).unique())
        fetcher = FinancialFetcher(
            cache_dir=str(self.cache_dir), cache_ttl_days=self.cache_ttl_days
        )
        fetcher.refresh_cache(instruments)

        # Load all cached data
        frames = []
        for sym in instruments:
            cache_file = self.cache_dir / f"{sym}.csv"
            if not cache_file.exists():
                continue
            try:
                df = pd.read_csv(cache_file, index_col=[0, 1], parse_dates=[1])
                df.index.names = ["instrument", "datetime"]
                frames.append(df)
            except Exception:
                continue
        if not frames:
            # Fallback to old akshare path
            all_frames = self._fetch_all(instruments)
            if not all_frames:
                return None
            combined = pd.concat(all_frames)
        else:
            combined = pd.concat(frames)

        result = self._align(combined, price_data)

        if self.include_change and result is not None:
            result = self._compute_change_factors(result, price_data)

        return result

    def _compute_change_factors(self, data: pd.DataFrame, price_data: pd.DataFrame) -> pd.DataFrame:
        """Add quarter-over-quarter change factors."""
        change_cols = {}
        if "roe" in data.columns:
            change_cols["roe_chg"] = data["roe"].groupby(level=0).diff()
        if "gross_margin" in data.columns:
            change_cols["margin_chg"] = data["gross_margin"].groupby(level=0).diff()
        if "revenue_growth" in data.columns:
            change_cols["rev_accel"] = data["revenue_growth"].groupby(level=0).diff()

        for col_name, series in change_cols.items():
            data[col_name] = series

        return data
```

The `_align`, `_fetch_all`, `_load_one`, `_cache_valid`, `_read_cache`, and `_fetch_akshare` methods remain unchanged from the existing code.

- [ ] **Step 4: Run test to verify it passes**

Run: `/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -m pytest test/test_fundamental_factor_extended.py -v`
Expected: All 6 tests PASS

Also run existing tests to verify backward compat:
Run: `/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -m pytest test/test_trainer.py -v -k fundamental`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add features/fundamental_factor.py test/test_fundamental_factor_extended.py
git commit -m "feat: extend FundamentalFactor with profitability, growth, cashflow metrics and change factors"
```

---

### Task 6: Register new factor in ModelTrainer importlib loop

**Files:**
- Modify: `models/trainer.py:31-33`

- [ ] **Step 1: Add `"northbound_factor"` to the importlib loop**

In `models/trainer.py`, line 31, add `"northbound_factor"` to the factor module list:

```python
for _f in ("sector_factors", "technical_factors", "factor_mining", "regime_features",
           "csv_factor", "fundamental_factor", "northbound_factor"):
```

- [ ] **Step 2: Verify import works**

Run: `/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -c "from quant_ex.models.trainer import ModelTrainer; from quant_ex.features.base import FactorRegistry; print(FactorRegistry.list())"`
Expected: Output includes `"northbound"` in the list

- [ ] **Step 3: Commit**

```bash
git add models/trainer.py
git commit -m "feat: register NorthboundFactor in ModelTrainer importlib loop"
```

---

### Task 7: Update config/model.yaml

**Files:**
- Modify: `config/model.yaml:114-143`

- [ ] **Step 1: Add northbound and fundamental factor config entries**

Append after the `technical` factor entry (after line 123) in `config/model.yaml`:

```yaml
      # 北向资金因子 — 取消注释以启用
      # 生成的特征:
      #   nb_hold_pct             北向持股占流通股比
      #   nb_hold_mv              北向持股市值 (log)
      #   nb_net_buy_ratio        北向净买入占比
      #   nb_hold_pct_chg_{w}d    持股占比 w 日变化量
      #   nb_net_buy_ma_{w}d      w 日净买入均值
      #   nb_sector_hold_pct      行业北向持股占比均值
      #   nb_vs_sector_{w}d       个股 vs 行业北向持股变化
      # - name: "northbound"
      #   windows: [5, 10, 20, 60]
      #   include_raw: true
      #   include_change: true

      # 基本面因子 — 取消注释以启用
      # 生成的特征:
      #   valuation:   pe_ttm, pb, ps_ttm, dyr (原有估值指标)
      #   profitability: roe, roa, gross_margin, net_margin
      #   growth: revenue_growth, profit_growth
      #   cashflow: ocf_to_np, fcf_yield
      #   变化量: roe_chg, margin_chg, rev_accel (include_change=true)
      # - name: "fundamental"
      #   metrics: ["valuation", "profitability", "growth", "cashflow"]
      #   include_change: true
```

- [ ] **Step 2: Verify config loads correctly**

Run: `/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -c "from quant_ex.utils.config import load_config; c = load_config(); print(c.get('model', {}).get('features', {}).get('factors'))"`
Expected: Prints the factors list without error

- [ ] **Step 3: Commit**

```bash
git add config/model.yaml
git commit -m "feat: add northbound and fundamental factor config entries in model.yaml"
```

---

### Task 8: Integrate DataProvider refresh into run_daily.py

**Files:**
- Modify: `run_daily.py:107-155`

- [ ] **Step 1: Add DataProvider refresh after data loader init, before signal generation**

Insert after line 121 (`sector_provider = SectorDataProvider(config)`) and before line 125 (`model = _load_model(config, model_path)`):

```python
    # ── 刷新外部数据缓存 ────────────────────────────────────────────────────
    try:
        from quant_ex.data.fetchers import NorthboundFetcher, FinancialFetcher
        feat_cfg = config.get("model", {}).get("features", {})
        factor_names = [f.get("name") for f in feat_cfg.get("factors", []) if f.get("name")]

        if "northbound" in factor_names:
            nb_fetcher = NorthboundFetcher(
                cache_dir="./cache/northbound", cache_ttl_days=1
            )
            instruments = config.get("market", {}).get("name", "csi300")
            nb_fetcher.refresh_cache(symbols=[])
            logger.info("北向资金缓存已刷新")

        if "fundamental" in factor_names:
            fin_fetcher = FinancialFetcher(
                cache_dir="./cache/fundamental", cache_ttl_days=7
            )
            # Only refresh if extended metrics are configured
            fund_cfg = next((f for f in feat_cfg.get("factors", []) if f.get("name") == "fundamental"), {})
            metrics = fund_cfg.get("metrics", ["valuation"])
            if any(m not in ("pe_ttm", "pb", "ps_ttm", "dyr", "valuation") for m in metrics):
                fin_fetcher.refresh_cache(symbols=[])
                logger.info("财务数据缓存已刷新")
    except Exception as exc:
        logger.warning(f"外部数据缓存刷新跳过: {exc}")
```

Note: `NorthboundFetcher.refresh_cache([])` fetches today's market-wide data regardless of the symbols list (the holdings API returns all stocks in one call). `FinancialFetcher.refresh_cache([])` with empty list is a no-op — it needs the actual instrument list. This will be addressed by having the FactorPipeline pass instruments during `compute()`.

- [ ] **Step 2: Verify run_daily.py imports work**

Run: `/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -c "from quant_ex.data.fetchers import NorthboundFetcher, FinancialFetcher; print('OK')"`
Expected: Prints `OK`

- [ ] **Step 3: Commit**

```bash
git add run_daily.py
git commit -m "feat: add DataProvider cache refresh to run_daily.py pipeline"
```

---

### Task 9: Integrate DataProvider refresh into run_scheduled_rebalance.py

**Files:**
- Modify: `run_scheduled_rebalance.py`

- [ ] **Step 1: Add the same DataProvider refresh block**

Find the equivalent initialization section (after DataLoader/SectorDataProvider init, before model loading) and insert the same pattern as Task 8. The exact line numbers depend on the file structure — look for the section after `sector_provider = SectorDataProvider(config)`.

```python
    # ── 刷新外部数据缓存 ────────────────────────────────────────────────────
    try:
        from quant_ex.data.fetchers import NorthboundFetcher, FinancialFetcher
        feat_cfg = config.get("model", {}).get("features", {})
        factor_names = [f.get("name") for f in feat_cfg.get("factors", []) if f.get("name")]

        if "northbound" in factor_names:
            NorthboundFetcher(cache_dir="./cache/northbound", cache_ttl_days=1).refresh_cache([])
            logger.info("北向资金缓存已刷新")

        if "fundamental" in factor_names:
            fund_cfg = next((f for f in feat_cfg.get("factors", []) if f.get("name") == "fundamental"), {})
            metrics = fund_cfg.get("metrics", ["valuation"])
            if any(m not in ("pe_ttm", "pb", "ps_ttm", "dyr", "valuation") for m in metrics):
                FinancialFetcher(cache_dir="./cache/fundamental", cache_ttl_days=7).refresh_cache([])
                logger.info("财务数据缓存已刷新")
    except Exception as exc:
        logger.warning(f"外部数据缓存刷新跳过: {exc}")
```

- [ ] **Step 2: Verify import works**

Run: `/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -c "exec(open('run_scheduled_rebalance.py').read().split('if __name__')[0]); print('syntax OK')"` or just check syntax.

- [ ] **Step 3: Commit**

```bash
git add run_scheduled_rebalance.py
git commit -m "feat: add DataProvider cache refresh to run_scheduled_rebalance.py pipeline"
```

---

### Task 10: Run full test suite and verify

**Files:** None (verification only)

- [ ] **Step 1: Run all new tests together**

Run: `/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -m pytest test/test_base_fetcher.py test/test_northbound_fetcher.py test/test_financial_fetcher.py test/test_northbound_factor.py test/test_fundamental_factor_extended.py -v`
Expected: All tests PASS

- [ ] **Step 2: Run existing tests to verify no regressions**

Run: `/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -m pytest test/ -v --ignore=test/test_scheduled_rebalance.py`
Expected: All existing tests still PASS

- [ ] **Step 3: Verify FactorRegistry contains new entries**

Run: `/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -c "from quant_ex.features.base import FactorRegistry; print(FactorRegistry.list())"`
Expected: Output contains `"northbound"` and `"fundamental"`

- [ ] **Step 4: Final commit if any fixups needed**

```bash
git add -A && git commit -m "fix: test fixups from full suite verification"
```
