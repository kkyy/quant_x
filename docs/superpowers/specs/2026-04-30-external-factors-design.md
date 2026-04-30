# External Factors Design: Northbound Capital & Financial Fundamentals

## Summary

Add northbound capital (沪深港通) and extended financial fundamental factors to the quant_ex framework, using a layered DataProvider + Factor architecture that decouples data fetching from factor computation.

## Motivation

Current factor system covers technical, sector, regime, and basic valuation (PE/PB/PS/dyr) signals. Two high-value external data categories are missing:

1. **Northbound capital**: smart-money signal from HK-Connect flows, with both stock-level and industry-level granularity
2. **Financial fundamentals**: profitability, growth, and cash flow quality beyond simple valuation ratios

## Architecture: DataProvider + Factor Separation

```
DataProvider Layer (data/fetchers/)     Factor Layer (features/)
─────────────────────────────────      ────────────────────────
BaseDataFetcher (ABC)                  BaseFactor (existing ABC)
  ├── NorthboundFetcher                  ├── NorthboundFactor
  │     akshare → eastmoney fallback     │     reads cache/northbound/
  │     cache/northbound/                │
  └── FinancialFetcher                   └── FundamentalFactor (extended)
        akshare Sina → EM fallback            reads cache/financial/
        cache/financial/
```

Data fetchers handle API calls, caching, and fallback logic. Factors handle computation logic only, reading from cached data. Multiple factors can share one provider's cache.

## DataProvider Layer

### Directory Structure

```
data/fetchers/
  ├── __init__.py
  ├── base.py               — BaseDataFetcher (ABC)
  ├── northbound_fetcher.py  — NorthboundFetcher
  └── financial_fetcher.py   — FinancialFetcher
```

### BaseDataFetcher

```python
class BaseDataFetcher(ABC):
    cache_dir: str
    cache_ttl_days: int

    @abstractmethod
    def fetch(self, symbols: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """Fetch data, return (instrument, datetime) MultiIndex DataFrame"""

    @abstractmethod
    def refresh_cache(self, symbols: List[str]) -> None:
        """Refresh cache, called by daily pipeline before model inference"""

    def _is_cache_fresh(self, path: str) -> bool:
        """Check if cache file is within TTL"""
```

### NorthboundFetcher

**Data source priority**: akshare → East Money fallback

| Method | Source | Cache File | Frequency |
|---|---|---|---|
| `_fetch_holdings(date)` | `akshare.stock_hsgt_hold_stock_em(market='北向')` | `cache/northbound/holdings_{date}.csv` | Daily |
| `_fetch_hist_flow()` | `akshare.stock_hsgt_hist_em(symbol='北向资金')` | `cache/northbound/hist_flow.csv` | Daily append |
| `_fetch_individual(symbol)` | `akshare.stock_hsgt_individual_em(symbol=code)` | `cache/northbound/{symbol}_individual.csv` | On-demand |

- Full-market holdings snapshot covers all stocks in one API call (no per-stock loop needed)
- Individual stock detail fetched on-demand only (for institution-level data)
- Industry-level data aggregated from stock-level snapshot, no separate API
- Cache TTL: 1 day

### FinancialFetcher

**Data source priority**: akshare Sina version → akshare EM version fallback

| Method | Source | Cache File | Frequency |
|---|---|---|---|
| `_fetch_indicators(symbol)` | `akshare.stock_financial_analysis_indicator(symbol=code)` | `cache/financial/{symbol}.csv` | 7 days |
| `_fetch_cash_flow(symbol)` | `akshare.stock_cash_flow_sheet_by_report_em(symbol=qlib_code)` | `cache/financial/{symbol}_cf.csv` | 7 days |

- Sina version preferred: 6-digit code, 86 pre-computed ratio columns, simplest interface
- Free cash flow derived from cash flow statement: `operating CF - capex`
- EM version as fallback: requires `SH600519` format code
- Cache TTL: 7 days (quarterly report cycle)

### Cache Refresh Integration

Called in `run_daily.py` and `run_scheduled_rebalance.py` before model prediction:

```python
NorthboundFetcher(cache_dir="cache/northbound", cache_ttl_days=1).refresh_cache(symbols)
FinancialFetcher(cache_dir="cache/financial", cache_ttl_days=7).refresh_cache(symbols)
```

## Factor Layer

### NorthboundFactor (`features/northbound_factor.py`)

```python
@FactorRegistry.register("northbound")
class NorthboundFactor(BaseFactor):
    def __init__(self,
                 windows: List[int] = [5, 10, 20, 60],
                 include_raw: bool = True,
                 include_change: bool = True,
                 cache_dir: str = "cache/northbound",
                 cache_ttl_days: int = 1):
```

**Output factor columns**:

| Factor Name | Computation | Type |
|---|---|---|
| `nb_hold_pct` | Northbound holding % of free float | Raw |
| `nb_hold_mv` | Northbound holding market value (log) | Raw |
| `nb_net_buy_ratio` | Northbound net buy / daily turnover | Raw |
| `nb_hold_pct_chg_{w}d` | Holding % change over w days | Change |
| `nb_net_buy_ma_{w}d` | w-day average net buy | Change |
| `nb_sector_hold_pct` | Industry avg northbound holding % (requires sector_map) | Aggregate |
| `nb_vs_sector_{w}d` | Stock holding % - industry avg, w-day change | Cross |

- Reads from `NorthboundFetcher` cache, never calls API directly
- Industry aggregation computed internally via `sector_map` groupby
- Missing data forward-filled (`ffill`) for HK market closure days
- Stocks with no northbound holding: `nb_hold_pct = 0`, `nb_net_buy_ratio = 0` (not NaN)

### FundamentalFactor Extension (`features/fundamental_factor.py`)

Current 4 columns (`pe_ttm`, `pb`, `ps_ttm`, `dyr`) preserved. New columns added:

| Factor Name | Source | Type |
|---|---|---|
| `pe_ttm`, `pb`, `ps_ttm`, `dyr` | Existing (preserved) | Raw |
| `roe` | Sina: 净资产收益率 | Raw |
| `roa` | Sina: 总资产利润率 | Raw |
| `gross_margin` | Sina: 销售毛利率 | Raw |
| `net_margin` | Sina: 销售净利率 | Raw |
| `revenue_growth` | Sina: 主营业务收入增长率 | Raw |
| `profit_growth` | Sina: 净利润增长率 | Raw |
| `ocf_to_np` | Sina: 经营现金净流量与净利润比率 | Raw |
| `fcf_yield` | Free CF / market cap (from cash flow statement) | Raw |
| `roe_chg` | ROE quarter-over-quarter change | Change |
| `margin_chg` | Gross margin quarter-over-quarter change | Change |
| `rev_accel` | Revenue growth rate change (current - prior period) | Change |

**Extension approach**:

- New `metrics` parameter controls which indicator groups are enabled, default all
- Switches data fetching to `FinancialFetcher` while preserving per-stock CSV cache format (backward compatible with old caches)
- Existing 4-column logic unchanged, new metrics appended after

**Metrics groups**:
- `valuation`: pe_ttm, pb, ps_ttm, dyr (original)
- `profitability`: roe, roa, gross_margin, net_margin
- `growth`: revenue_growth, profit_growth
- `cashflow`: ocf_to_np, fcf_yield

## Configuration (`config/model.yaml`)

```yaml
model:
  features:
    factors:
      - name: "technical"
        ma_pairs: [[5, 20], [10, 60]]
        rsi_windows: [14, 20]
        bb_windows: [20]
        atr_windows: [14]
        obv_windows: [10, 20]
        vwap_windows: [5, 10, 20]

      - name: "northbound"
        windows: [5, 10, 20, 60]
        include_raw: true
        include_change: true

      - name: "fundamental"
        metrics: ["valuation", "profitability", "growth", "cashflow"]
        include_change: true
```

Factors are toggleable by commenting out entries. Metric groups allow partial enablement.

## Data Flow

```
Daily run (run_daily.py / run_scheduled_rebalance.py)
    │
    ├── 1. DataProvider refresh (before model prediction)
    │       NorthboundFetcher.refresh_cache()  → cache/northbound/
    │       FinancialFetcher.refresh_cache()   → cache/financial/
    │
    ├── 2. FactorPipeline.compute(price_data)
    │       ├── TechnicalFactorEngine  → pure OHLCV computation
    │       ├── NorthboundFactor       → reads cache/northbound/, computes factors
    │       ├── FundamentalFactor      → reads cache/financial/, computes factors
    │       └── (other existing factors...)
    │
    ├── 3. Align to price_data MultiIndex
    │       reindex + groupby.ffill()
    │
    └── 4. Model predict → signal postprocess → trade signals
```

Training flow is identical: `ModelTrainer.train()` builds `FactorPipeline.from_config()` which includes the new factors automatically.

## Error Handling & Degradation

| Scenario | Handling |
|---|---|
| akshare API error / rate limit | Auto fallback to East Money API (within same Fetcher) |
| Both sources fail | Skip today's update, use cached data + warning log |
| No cache and fetch fails | Factor columns all NaN; LightGBM handles NaN natively |
| Northbound data missing (HK holiday) | `ffill` from last available day, no interpolation |
| Financial data in quarterly gap | `ffill` most recent quarter (same as existing FundamentalFactor) |
| Stock has no northbound holding | `nb_hold_pct = 0`, `nb_net_buy_ratio = 0` (not NaN) |
| Code format differences | Fetcher internally converts: `SH600519` ↔ `600519` ↔ `600519.SH` |

## Testing

| Test File | Coverage |
|---|---|
| `test/test_northbound_fetcher.py` | API call mock + cache read/write + TTL check + fallback degradation |
| `test/test_financial_fetcher.py` | Sina/EM switch + free cash flow calculation + code format conversion |
| `test/test_northbound_factor.py` | Factor computation correctness + missing value fill + industry aggregation + MultiIndex alignment |
| `test/test_fundamental_factor_extended.py` | New metric computation + change factors + backward compat with old cache |
| Integration | `FactorPipeline.from_config()` with northbound + fundamental, end-to-end `compute()` |

## Implementation Order

1. `data/fetchers/base.py` — BaseDataFetcher abstract base class
2. `data/fetchers/northbound_fetcher.py` — Northbound data fetching + caching
3. `data/fetchers/financial_fetcher.py` — Financial data fetching + caching
4. `features/northbound_factor.py` — Northbound factor computation
5. Extend `features/fundamental_factor.py` — New financial metrics
6. Config update + `run_daily.py` / `run_scheduled_rebalance.py` integration
7. Tests
