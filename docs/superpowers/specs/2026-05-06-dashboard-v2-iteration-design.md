# Dashboard v2 Iteration Design

Date: 2026-05-06
Status: Draft

## Problem

The current web dashboard is a demo: 8 pages of tables and cards with zero charts, stub tabs (rebalance, notification), ~15 missing CLI parameters, and no raw data access. It cannot serve as a usable quantitative research frontend.

## Goals

1. **Backtest chart comparison** — equity curves, drawdown, metrics side-by-side for multi-model/multi-strategy runs
2. **Full parameter coverage** — every CLI parameter exposed in the web UI
3. **Raw data access** — stock OHLCV, sectors, computed factors, alternative data
4. **Research workflow** — pages organized by natural research flow, not by operation type

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Page structure | Research-centric reorg (6 core + 2 admin) | Workflow: data → research → models → backtest → signals |
| Charting library | ECharts (echarts + echarts-for-react) | Candlestick, heatmap, data zoom, standard in Chinese quant |
| Component approach | Custom Tailwind component library | Full control, consistent with current codebase, no extra deps |
| Backend data access | Hybrid: qlib for price, cache CSVs for alt data, on-demand factor computation with TTL cache | Single source of truth for price; fast reads for cached data; on-demand for expensive computation |
| Build phasing | 3 phases: Foundation+Data → Backtest+Charts → Research+Signals | Highest-impact first; each phase builds on the previous |

## Page Map

### Core Research Pages

**1. Overview** — System health at a glance
- Health cards: Python version, model count, latest signal date, regime status
- Quick-start actions: "Train model", "Run backtest", "Generate signals" (link to respective pages)
- Cache summary table (existing, unchanged)
- Recent task list with status badges

**2. Data Explorer** — Raw data access (absorbs old Data page)
- **Stock Quotes** tab: Symbol search (fuzzy) → ECharts candlestick chart with MA/BOLL/VWAP overlays, volume sub-chart, date range picker, data zoom. Left sidebar: quick info panel (OHLCV, change%), overlay toggles.
- **Sectors** tab: Sector list with constituent count. Rotation heatmap (1d/5d/20d returns, green=up, red=down). Click sector → constituent table with individual returns.
- **Alt Data** tab: Data type selector (northbound, margin, pledge, insider, etc.) → time series bar/line chart per stock. Symbol search. Date range.
- **Factor Values** tab: Factor multi-select + symbol input + date range → computed factor values table (date × factor). Supports viewing per-stock factor history.
- **Cache** tab: Cache management (moved from System page). Type, file count, size, latest, TTL, delete-expired action.

**3. Research** — Factor analysis (absorbs old Factors page)
- **Library** tab: Factor table (name, class, enabled badge, IC, ICIR). IC/ICIR values loaded from latest mining run or computed on demand.
- **IC Analysis** tab: Factor selector → IC decay curve (ECharts line chart, horizons 1-20d), rolling IC chart (window=20d). Metric cards: mean IC, ICIR, |IC|>0.03 ratio.
- **Heatmap** tab: Factor-returns correlation heatmap (ECharts). Select factors, date range. Color scale: red=negative, blue=positive.
- **Mining** tab: Launch factor mining (existing, full params: min_ic, min_icir, top_n). Task status with SSE streaming.

**4. Backtest** — Run and compare (major redesign)
- **Launch** tab: Full parameter form (see Parameter Coverage section). Grid search or WFV toggle. Task status with SSE streaming.
- **Compare** tab (NEW): Run selector (checkboxes from results list, max 8). Selected runs shown as color-coded chips. Charts: equity curve overlay (portfolio + excess toggle), drawdown overlay, monthly returns heatmap, metrics comparison table. Data zoom for all time-series charts. Per-run drill-down (click chip → full detail view).
- **Results** tab: Results file list (existing). Click → structured detail view with metrics cards + equity curve + drawdown chart (not raw CSV table).
- **Walk-Forward** tab: Full WFV form (see Parameter Coverage). Fold results table per configuration. Summary: mean Sharpe, Sharpe std, positive-fold ratio.

**5. Signals** — Generate and deploy (fixes stubs)
- **Generate** tab: Full parameter form (see Parameter Coverage). Regime status banner. Task status with SSE streaming.
- **Daily** tab: Signal history list + content viewer (existing, enhanced with syntax highlighting and table format for signal data).
- **Rebalance** tab (FIXED): Mock mode checkbox, dry-run checkbox, config override selector. Actually calls `POST /api/signals/rebalance`. Shows result with SSE streaming.
- **Regime** tab (NEW): Current regime status card. Regime history chart (ECharts, time series of regime labels). Regime rules table (read-only, from config).
- **Notification** tab (FIXED): Title input, content textarea, channel selector. Actually calls `POST /api/signals/notify-test`. Shows send result.

**6. Models** — Train and manage (enhanced)
- **Train** tab: Full parameter form (see Parameter Coverage). Factor multi-select from registry. Training progress with SSE streaming (log output).
- **Browser** tab: Model file list (existing). Click → detail view: meta info card, feature importance bar chart (ECharts, top 30), training config summary.
- **Registry** tab: Registered models + factors tables (existing, unchanged).

### Admin Pages

**7. Config** — YAML editor (existing, minor updates)
- Editor tab: Same as current (textarea, save/reload). Add validation feedback.
- Strategy Candidates tab: Read-only (existing).
- Daily Presets tab: List daily_*.yaml files, click to view (NEW endpoint).
- Regime Rules tab: Parsed rules table (existing).

**8. System** — Runtime and logs (existing, minor updates)
- Runtime tab: Same as current.
- Logs tab: Same as current.
- Tasks tab (NEW): Unified task monitor. All running/recent tasks with type, status, progress. Cancel button. SSE stream per task.

## Component Library

Shared components in `src/components/ui/`:

| Component | Purpose |
|-----------|---------|
| `Table` | Sortable, paginated data table. Props: columns, data, sortable, pageSize |
| `Card` | Content container with title + optional actions. Props: title, children, actions |
| `Badge` | Status/color indicator. Props: variant (success/warning/error/info), children |
| `Modal` | Overlay dialog. Props: open, onClose, title, children |
| `Tabs` | Tab navigation. Props: tabs[], activeKey, onChange |
| `Select` | Dropdown selector. Props: options, value, onChange, searchable |
| `MultiSelect` | Multi-value dropdown with chips. Props: options, values, onChange |
| `NumberInput` | Numeric input with step/min/max. Props: value, onChange, step, min, max |
| `DatePicker` | Date input. Props: value, onChange |
| `Form` / `FormField` | Form wrapper with validation. Props: onSubmit, schema (Zod). Field: label, error, children |
| `Toast` | Notification toast. Props: variant, message, duration |
| `TaskStatus` | Task progress indicator with SSE integration. Props: taskId, onComplete |
| `EChartsWrapper` | ECharts React wrapper. Props: option, height, loading, onEvents |
| `SearchInput` | Debounced search input. Props: value, onChange, placeholder, debounceMs |

## API Additions

### New Endpoints

**Data Router** (`/api/data/`):

```
GET  /stock/{symbol}/quotes   — OHLCV time series
     params: start, end, fields (default: $open,$high,$low,$close,$volume,$change)
     source: qlib DataLoader, cached in-memory with 1-day TTL
     returns: { symbol, name, data: [{date, open, high, low, close, volume, change}] }

GET  /stock/search             — Fuzzy symbol/name search
     params: q (query string), limit (default 10)
     source: load_stock_names() + fuzzy match
     returns: [{ symbol, name, exchange }]

GET  /sectors                  — Sector list
     source: cache/sector_map.json + sector_stocks.json
     returns: [{ sector_id, sector_name, stock_count }]

GET  /sectors/{sector_id}/stocks — Sector constituents
     source: cache/sector_stocks.json
     returns: { sector_id, sector_name, stocks: [{symbol, name}] }

GET  /sectors/rotation         — Sector rotation data
     params: windows (default: 1,5,20), date (default: latest)
     source: compute from qlib price data, cache 1-day TTL
     returns: [{ sector_id, sector_name, returns: {1d: x, 5d: y, 20d: z} }]

GET  /alt-data/{type}          — Browse cached alternative data
     params: symbol (optional), start, end, limit (default 100)
     source: read cache/<type>/*.csv
     returns: { type, columns: [...], rows: [...], total, has_more }
```

**Factor Router** (`/api/factors/`):

```
GET  /values                   — Computed factor values per stock/date
     params: factors (comma-separated), symbols (comma-separated), start, end
     source: FactorPipeline.compute() on-demand, cache with 1-day TTL
     returns: { factors: [...], data: [{symbol, date, factor1, factor2, ...}] }

GET  /ic-analysis              — IC analysis for a factor
     params: factor (required), horizon (default 5), window (default 20)
     source: compute from predictions + forward returns, cache 1-day TTL
     returns: { factor, ic_mean, icir, decay: [{horizon, ic}], rolling: [{date, ic}] }

GET  /heatmap                  — Factor correlation heatmap
     params: factors (comma-separated), start, end
     source: compute from factor values, cache 1-day TTL
     returns: { factors: [...], matrix: [[corr_ij]] }
```

**Backtest Router** (`/api/backtest/`):

```
GET  /results/{filename}/equity-curve — Parsed equity curve
     source: parse result CSV
     returns: { dates: [...], portfolio: [...], benchmark: [...], excess: [...] }

GET  /results/{filename}/metrics      — Structured metrics dict
     source: parse result CSV summary rows
     returns: { annual_return, sharpe, max_drawdown, calmar, ic, icir, rank_ic, rank_icir, win_rate, turnover }

GET  /results/{filename}/drawdown     — Drawdown series
     source: compute from equity curve
     returns: { dates: [...], drawdown: [...] }

POST /compare                         — Multi-run comparison
     body: { filenames: [str] }
     returns: { runs: [{ filename, label, color, equity_curve, drawdown, metrics }], dates: [...] }
```

**Signals Router** (`/api/signals/`):

```
POST /rebalance               — Run scheduled rebalance
     body: { mock: bool, dry_run: bool, config: optional_str }
     returns: { task_id }

POST /notify-test             — Send test notification
     body: { title, content, channel: optional_str }
     returns: { task_id, success: bool }
```

### Extended Endpoints (missing CLI params)

**`POST /api/models/train`** — add:
- `with_sector: bool = false`
- `no_extra_factors: bool = false`
- `skip_factor_pipeline: bool = false`
- `bagging_fraction: Optional[float]`
- `ensemble_seeds: Optional[list[int]]`

**`POST /api/backtest/grid`** — add:
- `optimize: bool = false`
- `n_iters: int = 3`
- `grid_workers: int = 1`
- `output_csv: Optional[str]`
- `slippage_multipliers: Optional[list[float]]`
- `markets: Optional[list[str]]` (for multi-market exploration)

**`POST /api/backtest/walk-forward`** — add:
- `seeds: bool = false`
- `run_id: Optional[str]`
- `grid_workers: int = 1`
- `robust_weights: Optional[dict]` (JSON object)
- `folds_config: Optional[str]` (path to YAML)
- `train_config: Optional[str]` (path to YAML)

**`POST /api/signals/generate`** — add:
- `universe: Optional[str]`
- `refresh_cache: bool = false`
- `config: Optional[str]` (path to daily config YAML)
- `position_date: Optional[str]`
- `min_action_value: Optional[float]`

## Data Flow

```
Browser
  │
  ├─ Stock Quotes ──→ GET /data/stock/{symbol}/quotes
  │                    └─ DataLoader (qlib bin) ──→ in-memory TTL cache (1d)
  │
  ├─ Sectors ───────→ GET /data/sectors, /sectors/rotation
  │                    └─ sector_map.json + qlib price ──→ TTL cache
  │
  ├─ Alt Data ──────→ GET /data/alt-data/{type}
  │                    └─ cache/<type>/*.csv ──→ direct read
  │
  ├─ Factor Values ─→ GET /factors/values
  │                    └─ FactorPipeline.compute() ──→ TTL cache (1d)
  │
  ├─ Backtest ──────→ GET /backtest/results/{f}/equity-curve, metrics, drawdown
  │                    └─ Parse result CSV ──→ on-demand (small files)
  │
  └─ Compare ───────→ POST /backtest/compare
                       └─ Parse multiple CSVs ──→ combined response
```

## Phased Implementation

### Phase 1: Foundation + Data Explorer

**Frontend:**
- Component library: Table, Card, Badge, Tabs, Select, MultiSelect, NumberInput, DatePicker, Form/FormField, SearchInput, EChartsWrapper, Toast, TaskStatus
- Page reorg: rename/reorder routes to match new structure
- Data Explorer page: all 5 sub-tabs (Stock Quotes, Sectors, Alt Data, Factor Values, Cache)
- Sidebar: updated navigation with new page names and icons (lucide-react)

**Backend:**
- 6 new data endpoints (stock quotes, search, sectors, sector stocks, rotation, alt-data)
- 3 new factor endpoints (values, ic-analysis, heatmap)
- qlib initialization helper for FastAPI (lazy init on first request)
- Factor computation cache (TTL-based, in-process dict)

**Estimated scope:** ~1500 lines frontend, ~800 lines backend

### Phase 2: Backtest + Comparison

**Frontend:**
- Backtest Compare tab: run selector, equity curve overlay, drawdown overlay, metrics table, monthly returns heatmap
- Backtest Launch tab: full parameter form (all missing params)
- Backtest Results tab: structured detail view with ECharts (replace raw CSV viewer)
- Walk-Forward tab: full param form, fold results visualization
- TaskStatus component: replace polling with SSE streaming

**Backend:**
- 4 new backtest endpoints (equity-curve, metrics, drawdown, compare)
- Extend grid/WFV request schemas with missing params
- Wire rebalance + notification endpoints (fix stubs)

**Estimated scope:** ~1200 lines frontend, ~500 lines backend

### Phase 3: Research + Signals + Polish

**Frontend:**
- Research page: Library, IC Analysis (decay + rolling charts), Heatmap, Mining
- Signals page: full param form, fixed Rebalance tab, Regime tab (history chart), fixed Notification tab
- Models Train tab: full param form (ensemble, sector toggles, etc.)
- Overview page: quick-start actions, recent tasks
- System Tasks tab: unified task monitor
- Config page: daily presets tab, validation feedback
- useSSE hook: wire into all async operations (replace setInterval polling)

**Backend:**
- Extend models/train + signals/generate schemas with missing params
- Regime history endpoint (time series of regime labels)
- Daily presets list endpoint (already exists, may need enhancement)

**Estimated scope:** ~1000 lines frontend, ~300 lines backend

## Dependencies

New npm packages:
- `echarts` (^5.5) — charting engine
- `echarts-for-react` (^3.0) — React wrapper

New Python packages: none (all backend functionality uses existing quant_ex modules)

Remove (currently unused): `recharts` (replaced by ECharts)

## File Structure

```
web/frontend/src/
  components/
    ui/                          # Shared component library
      Table.tsx
      Card.tsx
      Badge.tsx
      Modal.tsx
      Tabs.tsx
      Select.tsx
      MultiSelect.tsx
      NumberInput.tsx
      DatePicker.tsx
      Form.tsx
      FormField.tsx
      Toast.tsx
      TaskStatus.tsx
      EChartsWrapper.tsx
      SearchInput.tsx
    Layout.tsx                   # Existing (updated)
    Sidebar.tsx                  # Existing (updated nav items + lucide icons)
    LanguageToggle.tsx           # Existing (unchanged)
  pages/
    OverviewPage.tsx             # Renamed from DashboardPage
    DataExplorerPage.tsx         # NEW (replaces DataPage)
    ResearchPage.tsx             # NEW (replaces FactorsPage)
    BacktestPage.tsx             # Major redesign
    SignalsPage.tsx              # Major redesign
    ModelsPage.tsx               # Enhanced
    ConfigPage.tsx               # Minor updates
    SystemPage.tsx               # Minor updates
  hooks/
    useSSE.ts                    # Existing (wire into pages)
    useDebounce.ts               # NEW
    useFactorCache.ts            # NEW (factor value cache)
  api/
    client.ts                    # Existing (add new endpoint methods)
    types.ts                     # NEW (TypeScript interfaces for API responses)
  i18n/
    en.json                      # Updated with new keys
    zh.json                      # Updated with new keys

web/api/
  routers/
    data.py                      # Extended (6 new endpoints)
    factors.py                   # Extended (3 new endpoints)
    backtest.py                  # Extended (4 new endpoints + param extensions)
    signals.py                   # Extended (2 new endpoints + param extensions)
    models.py                    # Extended (param extensions)
    config.py                    # Minor updates
    system.py                    # Minor updates
  services/
    data_service.py              # NEW (qlib data access, TTL cache)
    factor_service.py            # NEW (factor computation, TTL cache)
    chart_service.py             # NEW (parse backtest CSVs into chart data)
    task_manager.py              # Existing (unchanged)
    stream.py                    # Existing (unchanged)
```

## Out of Scope

- Real-time streaming quotes (would require WebSocket, qlib is daily-only)
- Portfolio management / P&L tracking
- User authentication / multi-user
- Mobile-responsive layout (desktop-first)
- Export to PDF/Excel
- Custom chart annotations / drawing tools
- Strategy iteration log management (already in CSV, accessible via Config page)
