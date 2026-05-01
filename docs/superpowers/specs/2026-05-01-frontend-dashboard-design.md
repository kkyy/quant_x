# quant_ex Frontend Dashboard - Design Specification

**Date**: 2026-05-01
**Status**: Approved
**Stack**: React (Vite + TypeScript + Tailwind + shadcn/ui) + FastAPI backend
**Deployment**: Local only (localhost, no auth)

## 1. Overview

A single-page application (SPA) dashboard that provides interactive access to all quant_ex features: data management, model training, backtesting, signal generation, factor analysis, and system configuration. The frontend communicates with a FastAPI backend that wraps existing Python modules without duplicating logic.

## 2. Project Structure

```
quant_ex/
├── web/
│   ├── api/                          # FastAPI backend
│   │   ├── __init__.py
│   │   ├── app.py                    # FastAPI app factory + CORS + mount static
│   │   ├── deps.py                   # Shared deps (config loader, task manager singleton)
│   │   ├── routers/
│   │   │   ├── __init__.py
│   │   │   ├── data.py              # Data fetching & cache endpoints
│   │   │   ├── models.py            # Model training & management
│   │   │   ├── backtest.py          # Grid search, WFV, slippage, charts
│   │   │   ├── signals.py           # Signal generation & history
│   │   │   ├── factors.py           # Factor library, evaluation, mining
│   │   │   ├── config.py            # Config file CRUD
│   │   │   └── system.py            # Health, logs, cache management
│   │   └── services/
│   │       ├── __init__.py
│   │       ├── task_manager.py      # Background task orchestration + SSE
│   │       └── stream.py            # Log capture → SSE event helpers
│   ├── frontend/                    # React app
│   │   ├── src/
│   │   │   ├── App.tsx              # Root layout + router
│   │   │   ├── main.tsx
│   │   │   ├── api/                 # Typed API client functions
│   │   │   ├── components/          # Shared UI (tables, charts, forms)
│   │   │   ├── pages/               # Page components (one per section)
│   │   │   ├── hooks/               # useSSE, useTask, useConfig
│   │   │   ├── lib/                 # Formatters, constants
│   │   │   └── types/               # TypeScript interfaces
│   │   ├── index.html
│   │   ├── package.json
│   │   ├── vite.config.ts
│   │   ├── tsconfig.json
│   │   ├── tailwind.config.js
│   │   └── postcss.config.js
│   └── run_web.py                   # Entry: uvicorn web.api.app:app
```

## 3. Navigation & Pages

Sidebar-based layout with 8 top-level sections. Each section has horizontal tabs.

### 3.1 Dashboard (Overview)

The landing page showing system status at a glance:

- **Current regime badge**: From `RegimeStrategySwitch`, shows `calm_bull` / `calm_bear` / `volatile_bull` / `volatile_bear` with color coding
- **Latest signal card**: Date, top 5 picks, trade actions summary
- **Model status**: Last trained model (name, date, tag), last backtest run
- **Cache freshness grid**: 14 data types with file count, total size, TTL status (green=fresh, yellow=expiring, red=stale)
- **Quick actions**: "Generate Daily Signal" button, "Refresh All Data" button

### 3.2 Data (Data Management)

**Fetch tab**:
- Dropdown: data type (14 options, from `_FETCHER_REGISTRY`)
- Radio: scope (all A-shares / index universe / custom symbols)
- Fields: TTL override, force refresh toggle
- "Fetch" button starts background task
- SSE stream shows: symbols processed, new files cached, errors, progress bar
- Running task indicator with cancel button

**Cache Status tab**:
- Table: one row per data type, columns: type name, file count, total size (MB), latest file date, TTL remaining, status badge
- Click a row to expand: list of cached files with dates and sizes
- "Delete Expired" button per type

**Stock Lookup tab**:
- Search by symbol or name (uses `load_stock_names()`)
- Shows: which cache types have data for this stock, latest date per type

### 3.3 Models (Model Management)

**Train tab**:
- Form fields:
  - Model type: dropdown (from `ModelRegistry`, populated via API)
  - Tag: text input
  - Factors: checkbox group (from `FactorRegistry`, populated via API)
  - Training dates: date pickers (fit_start, fit_end, valid_start, valid_end, test_start)
  - Advanced: qlib-native toggle, multi-seed ensemble toggle
- "Train" button starts task
- SSE stream: training log lines, early stopping info, final metrics
- Completed training auto-refreshes model list

**Model Browser tab**:
- Table of `models/*.pkl` files: name, model type, tag, timestamp, file size
- Click row to expand:
  - Metadata card (from `_meta.json`): training config, factor list, runtime flags
  - Feature importance bar chart (top 30, from `_feature_importance.json`)
  - "Use for Backtest" / "Use for Signal" action buttons

**Registry tab**:
- Two sections: Registered Models (name, class, description) and Registered Factors (name, class, features produced)
- Read-only, informational

### 3.4 Backtest

**Grid Search tab**:
- Model selector: dropdown of saved models
- Parameter grid inputs: topk (comma-separated), n_drop, hold_thresh
- Date range pickers
- Market selector: csi300 / csi500 / csi800 / csi1000 / all
- Options: multi-seed evaluation, grid-workers
- "Run Grid Search" starts task
- SSE stream: per-combination result as it completes, progress counter
- Results table: sortable by any metric (sharpe, return, drawdown, calmar, etc.)
- Click row: expanded view with NAV chart, monthly heatmap, metrics detail
- "Export CSV" button

**Walk-Forward tab**:
- Train universes: multi-select checkboxes
- Eval market: dropdown
- Parameter grid (same as grid search)
- Options: seeds, workers, robust-weights JSON editor
- "Run WFV" starts task
- SSE stream: fold-by-fold progress
- Results: fold-level metrics table + aggregate summary (mean, median, min, std)
- Pareto frontier scatter chart (mean_sharpe vs min_sharpe)
- Statistical significance column (p-values)

**Slippage Analysis tab**:
- Select model + best params (or use from grid search result)
- Cost multipliers input (comma-separated)
- Results: table of multiplier vs sharpe/return, line chart, break-even point

**AI Optimizer tab**:
- Grid search results as input
- Number of iterations slider
- "Start Optimization" triggers Claude API loop
- Per-iteration panel: analysis text, suggested next grid, reasoning
- Read-only replay of past optimization runs

**Charts tab**:
- Gallery of chart images from `backtest_results/`
- Grid heatmap, monthly heatmap, NAV comparison, rolling Sharpe, scatter
- Each chart served as PNG via API endpoint

### 3.5 Signals

**Generate tab**:
- Model selector: dropdown of saved models
- Account value: number input
- Current positions: textarea (format: `SH600000:500,SZ000001:300`)
- Dry-run toggle
- "Generate Signal" starts task
- SSE stream: log output
- Result display: formatted signal card with positions table and trade actions

**History tab**:
- Table of `signals/signal_*.txt` files: date, file size
- Click to view full signal report text
- Filter by date range

**Rebalance tab**:
- Config form: model path, market, topk/n_drop/hold_thresh, account, start_date
- Mock mode toggle
- "Run Rebalance" starts task
- Result: position diff view (buy/sell/hold with quantities)

**Notification tab**:
- Channel list with status (configured/not configured)
- Test form: select channel, custom title/content
- "Send Test" button with success/failure feedback

### 3.6 Factors

**Factor Library tab**:
- Table: all 19+ registered factors with name, description, feature count, status (enabled in config / disabled)
- Enable/disable toggle per factor (writes to `config/model.yaml`)
- Click row: shows feature list, config kwargs

**Evaluation tab**:
- Factor selector dropdown
- Date range
- "Evaluate" computes rank-IC, ICIR, coverage
- Results: IC time series chart, IC decay curve (multi-horizon), summary stats

**Mining tab**:
- Threshold inputs: min-IC, min-ICIR, top-N
- "Start Mining" runs `run_factor_mining.py` logic
- Results: discovered factors table with IC/ICIR, expression

**Screening tab**:
- IC/ICIR threshold inputs
- Max correlation input
- "Run Screening" applies `FactorScreener`
- Results: passed factors, removed factors (with reason), correlation matrix heatmap

### 3.7 Config

**Config Editor tab**:
- Tab bar: base.yaml | model.yaml | notify.yaml
- Monaco-like code editor (or textarea with YAML syntax highlighting)
- "Save" button writes back to disk
- "Reload" button re-reads from disk

**Strategy Candidates tab**:
- Table from `config/strategy_candidates.yaml`
- Compare mode: select 2+ strategies, side-by-side metrics

**Regime Rules tab**:
- Visual editor for regime strategy overrides
- 4 regime cards, each with topk/n_drop/hold_thresh inputs
- "Save" writes to config

### 3.8 System

**Logs tab**:
- Log file viewer (tails `logs/quant_ex_*.log`)
- Auto-scroll toggle
- Filter: level (INFO/WARNING/ERROR), keyword search

**Cache Management tab**:
- Per-type cache browser: list files, sizes, dates
- "Delete Expired" bulk action
- Total disk usage summary

**Runtime tab**:
- Python version, installed packages list
- qlib data path and status
- Disk space
- Environment variables (non-sensitive)

## 4. API Endpoints

### 4.1 Data (`data.py`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/data/cache-status` | Per-type: file count, size, latest date, TTL status |
| POST | `/api/data/fetch` | Start fetch task. Body: `{type, scope, symbols?, ttl?, force?}` |
| GET | `/api/data/fetch/{task_id}/stream` | SSE: progress events |
| DELETE | `/api/data/cache/{type}/expired` | Delete expired cache files |
| GET | `/api/data/stock-lookup/{symbol}` | Cache availability for a stock |

### 4.2 Models (`models.py`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/models` | List saved models with metadata |
| GET | `/api/models/{filename}/meta` | Get meta.json sidecar |
| GET | `/api/models/{filename}/importance` | Get feature importance data |
| POST | `/api/models/train` | Start training. Body: `{model, tag, factors[], dates, options}` |
| GET | `/api/models/train/{task_id}/stream` | SSE: training log |
| GET | `/api/models/registry` | List registered models + factors |

### 4.3 Backtest (`backtest.py`)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/backtest/grid` | Start grid search. Body: params |
| GET | `/api/backtest/grid/{task_id}/stream` | SSE: per-combo results |
| GET | `/api/backtest/results` | List result CSVs |
| GET | `/api/backtest/results/{filename}` | Get result data |
| POST | `/api/backtest/walk-forward` | Start WFV. Body: params |
| GET | `/api/backtest/walk-forward/{task_id}/stream` | SSE: fold progress |
| GET | `/api/backtest/wfv/results` | List WFV runs |
| GET | `/api/backtest/wfv/results/{run_id}` | Get WFV summary |
| POST | `/api/backtest/slippage` | Run slippage analysis |
| GET | `/api/backtest/charts/{filename}` | Serve PNG chart |

### 4.4 Signals (`signals.py`)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/signals/generate` | Generate daily signal. Body: `{model_path, account, positions?, dry_run?}` |
| GET | `/api/signals/generate/{task_id}/stream` | SSE: generation log |
| GET | `/api/signals/history` | List signal files |
| GET | `/api/signals/history/{date}` | Get signal content |
| GET | `/api/signals/regime` | Current regime detection |
| POST | `/api/signals/rebalance` | Run rebalance. Body: params |
| POST | `/api/signals/notify/test` | Test notification channel |

### 4.5 Factors (`factors.py`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/factors` | List all registered factors |
| POST | `/api/factors/{name}/evaluate` | Compute IC/ICIR metrics |
| GET | `/api/factors/{name}/evaluate/{task_id}/stream` | SSE: evaluation progress |
| POST | `/api/factors/mine` | Start factor mining |
| GET | `/api/factors/library` | Factor library catalog |
| POST | `/api/factors/screen` | Run factor screening |

### 4.6 Config (`config.py`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/config/{name}` | Read config YAML (base/model/notify) |
| PUT | `/api/config/{name}` | Write config YAML |
| GET | `/api/config/strategy-candidates` | Strategy candidates data |
| GET | `/api/config/daily-presets` | List daily config overrides |

### 4.7 System (`system.py`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/system/health` | Health check |
| GET | `/api/system/logs` | Tail log file. Query: `?lines=N&level=ERROR` |
| GET | `/api/system/cache/{type}` | Browse cache directory |
| DELETE | `/api/system/cache/{type}/expired` | Delete expired files |
| GET | `/api/system/runtime` | Runtime info |

## 5. Task Manager & SSE Streaming

### TaskManager (`web/api/services/task_manager.py`)

Central orchestrator for all background tasks:

```python
class TaskManager:
    def __init__(self):
        self._tasks: dict[str, TaskState] = {}
        self._queues: dict[str, asyncio.Queue] = {}

    async def start_task(self, task_type: str, coro_or_fn) -> str:
        """Start background task, return task_id."""

    async def stream_events(self, task_id: str) -> AsyncGenerator[dict, None]:
        """Yield SSE events from the task's queue."""

    def emit(self, task_id: str, event: str, data: dict):
        """Push event to task's queue (called from background worker)."""
```

### Event types

```json
{"type": "log", "data": {"level": "info", "message": "..."}}
{"type": "progress", "data": {"current": 500, "total": 5000, "unit": "symbols"}}
{"type": "result", "data": {"topk": 10, "n_drop": 3, "sharpe": 1.42}}
{"type": "error", "data": {"message": "..."}}
{"type": "done", "data": {"summary": "..."}}
```

### Log capture (`web/api/services/stream.py`)

Wraps Python `logging` handlers to intercept log records and forward them as SSE events. Background functions (existing CLI logic) emit logs as usual — the stream layer captures them.

## 6. Frontend Architecture

### 6.1 Key libraries

- **shadcn/ui** (Radix primitives + Tailwind): Tables, forms, dialogs, tabs, cards, badges
- **TanStack Table**: Sortable/filterable data tables for backtest results, model lists
- **Recharts** or **ECharts**: Interactive charts (NAV curves, heatmaps, scatter plots, IC decay)
- **React Hook Form + Zod**: Form validation for training, backtest, config forms
- **react-router**: SPA routing (sidebar navigation)

### 6.2 Custom hooks

```typescript
useSSE(taskId: string): { events, status, error }
useConfig(name: string): { config, save, loading }
usePolling(fetchFn, intervalMs): { data, refresh }
```

### 6.3 Component patterns

- **ModelSelector**: Reusable dropdown populated from `GET /api/models`
- **FactorCheckboxes**: Dynamic checkbox group from `GET /api/factors`
- **TaskRunner**: Generic wrapper that starts a task, shows SSE stream, displays result
- **MetricsTable**: Sortable table with metric columns (sharpe, return, drawdown, etc.)
- **ChartViewer**: Renders PNG from API or interactive chart from JSON data

### 6.4 Extensibility patterns

1. **Registry-driven UI**: All model/factor dropdowns populated from API, not hardcoded
2. **Config-driven toggles**: Factor enable/disable writes to YAML, no frontend code changes
3. **Generic task streaming**: `TaskRunner` component works with any task type
4. **Tab-based pages**: New features added as new tabs, one line in page definition
5. **Data-type plugins**: New fetcher types appear automatically in data management

## 7. Startup

```bash
# Backend
cd quant_ex
python web/run_web.py            # Starts FastAPI on :8000

# Frontend (dev mode)
cd quant_ex/web/frontend
npm install
npm run dev                      # Vite dev server on :5173, proxies /api to :8000

# Production build (served by FastAPI)
cd quant_ex/web/frontend
npm run build                    # Output to web/frontend/dist/
# FastAPI serves static files from dist/ at /
```

In production mode, a single `python web/run_web.py` serves both API and static frontend.

## 8. Implementation Phases

### Phase 1: Foundation
- FastAPI app skeleton with CORS and static file serving
- TaskManager with SSE streaming
- React app with Vite + Tailwind + shadcn/ui setup
- Sidebar layout and routing skeleton
- System health endpoint + Dashboard overview page

### Phase 2: Data & Models
- Data cache status API + Data page
- Fetch with SSE progress streaming
- Model registry API + Model Browser page
- Training form + SSE log streaming

### Phase 3: Backtest & Signals
- Grid search API + results table with charts
- Signal generation API + signal history
- Regime detection endpoint
- Rebalance simulation

### Phase 4: Factors & Config
- Factor library API + Factor page
- Factor evaluation with IC charts
- Config editor (YAML read/write)
- Strategy candidates viewer

### Phase 5: Advanced Features
- Walk-forward validation UI
- Slippage sensitivity analysis
- AI optimizer replay
- Factor mining + screening
- Log viewer
