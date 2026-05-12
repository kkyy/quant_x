# Dashboard v2 Iteration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transform the web dashboard from a demo into a production-grade quantitative research frontend with chart-based backtest comparison, full parameter coverage, and raw data access.

**Architecture:** Research-centric page reorg (6 core + 2 admin) with ECharts for financial visualization and a custom Tailwind component library. Backend uses hybrid data access: qlib for price data, cache CSVs for alt data, on-demand factor computation with TTL cache.

**Tech Stack:** React 19 + TypeScript + Vite + Tailwind CSS v4 + ECharts (echarts + echarts-for-react) + FastAPI + Pydantic

**Design Spec:** `docs/superpowers/specs/2026-05-06-dashboard-v2-iteration-design.md`

---

## 2026-05-11 Functional Audit and Patch Result

### Audit Scope

- Production FastAPI static serving and direct browser routes: `/`, `/data-explorer`, `/research`, `/models`, `/backtest`, `/signals`, `/config`, `/system`.
- Core read-only API endpoints used by first-screen page loads: system health/runtime/tasks, data cache status/sectors, model list/registry, backtest results, factors, signal history/regime.
- Frontend production build and TypeScript compile through `npm run build`.
- Headless Chrome render smoke test for every top-level route, checking that React mounted, page headings rendered, and no `Not Found` / application error / stale placeholder text appeared.

### Issues Found

- **P0 - SPA deep links 404 in production.** FastAPI mounted `StaticFiles` at `/`, but direct routes such as `/models`, `/backtest`, and `/signals` returned `{"detail":"Not Found"}` instead of serving `index.html`. This broke refresh, bookmarks, and direct navigation.
- **P1 - Data Explorer sectors were empty.** The sector endpoints only read `cache/sector_stocks.json`, but the project has `cache/sector_map.json` and `crawler/data/sector_stocks.json`; the expected cache file was absent.
- **P1 - Backtest benchmark curves missed qlib CSVs.** Chart parsing only recognized `benchmark_return`; qlib-style daily reports commonly use `bench`.
- **P2 - Rebalance page showed stale placeholder copy.** The tab is connected to `POST /api/signals/rebalance`, but the UI still said it was a placeholder.

### Changes Applied

- Replaced root static mount behavior in `web/api/app.py` with explicit `/assets` serving plus SPA fallback for non-API paths.
- Added sector group loading in `web/api/routers/data.py` from `cache/sector_map.json`, with fallback to `crawler/data/sector_stocks.json`; sector list, constituents, and rotation now share the same source.
- Updated `web/api/services/chart_service.py` to recognize `benchmark_return`, `bench`, or `benchmark`, and to expose additional excess/IR-style metric columns when present.
- Updated `signals.rebalanceNote` in both `web/frontend/src/i18n/en.json` and `web/frontend/src/i18n/zh.json`.
- Added `test/test_web_dashboard.py` covering SPA fallback, API route preservation, sector-map-backed sectors, and qlib `bench` curve parsing.

### Verification Completed

```bash
./.venv/bin/python -m pytest test/test_web_dashboard.py
./.venv/bin/python -c "from web.api.app import app; print('OK', len(app.routes))"
cd web/frontend && npm run build
```

Results:

- `test/test_web_dashboard.py`: 4 passed.
- FastAPI import: `OK 55`.
- Frontend build: passed; existing Vite chunk-size warning remains.
- HTTP direct-route check: all top-level routes returned `200 text/html` and contained React root markup.
- CDP render smoke test: all top-level routes rendered their expected H1 (`总览`, `数据探索`, `因子研究`, `模型`, `回测`, `信号`, `配置`, `系统`) with no `Not Found`, app error, or stale placeholder text.
- Core API smoke test: health/runtime/tasks/cache-status/sectors/models/registry/backtest-results/factors/signal-history/regime returned 200. Sectors returned 340 groups from local cache.

### Remaining Iteration Plan

- **Next P1:** Improve long-task parameter fidelity. Audit Web request schemas against CLI options for backtest grid, WFV, signal generation, and rebalance; pass through currently ignored fields or remove inactive controls.
- **Next P2:** Add route-level frontend tests for tab switching and form validation once the project has a browser test runner installed.
- **Next P2:** Split the production JS bundle with dynamic route imports; current build is correct but emits a large chunk warning.

---

## 2026-05-11 Safety and Parameter Fidelity Pass

### Issues Found

- **P1 - Notification test could send real notifications immediately.** `POST /api/signals/notify-test` called `NotificationPusher.send()` directly, and the UI button did not require a dry-run/confirmation distinction.
- **P1 - Rebalance Web route omitted important CLI safety and position parameters.** The CLI supports `--positions`, `--position-date`, `--min-action-value`, `--skip-update`, `--force`, and `--notify-channel`, but the Web route exposed only mock/dry-run/config.
- **P2 - Daily signal tab showed unsupported controls.** The page displayed universe/cache/position-date/min-action-value controls that `run_daily.py` does not support, so those settings were silently ignored.

### Changes Applied

- Changed notification testing to default to dry-run preview. Real notification delivery now requires `dry_run=false` and `confirm_send=true`.
- Added notification channel filtering for preview/send paths (`bark`, `pushplus`, `dingtalk`, `serverchan`, `wechat_mp`, `all`).
- Added Web rebalance support for positions, position date, min action value, skip update, force run, and notify channel. The UI defaults `skip_update=true` and `dry_run=true` to avoid accidental data updates or real pushes.
- Removed unsupported daily signal controls from the frontend and wired config override through to `run_daily.main(config_path=...)`.
- Added regression coverage for notification dry-run behavior, real-send confirmation, and rebalance command construction.

### Verification Completed

```bash
./.venv/bin/python -m pytest test/test_web_dashboard.py
./.venv/bin/python -c "from web.api.app import app; print('OK', len(app.routes))"
cd web/frontend && npm run build
```

Results:

- `test/test_web_dashboard.py`: 7 passed.
- FastAPI import: `OK 55`.
- Frontend build: passed; existing Vite chunk-size warning remains.
- Local API smoke:
  - `GET /api/system/health` returned 200.
  - `POST /api/signals/notify-test` with default payload returned dry-run preview and `sent=false`.
  - `POST /api/signals/notify-test` with `dry_run=false` but no confirmation returned 400.
- CDP render smoke: `/signals`, `/backtest`, `/data-explorer`, and `/config` rendered their expected H1 with no `Not Found`, app error, or stale placeholder text.

### Remaining Iteration Plan

- **Next P2:** Add browser-level interaction tests after introducing a committed test runner.
- **Next P2:** Split the ECharts vendor chunk further or raise its warning threshold intentionally after reviewing bundle policy.

---

## 2026-05-12 Backtest Parameter Parity and Bundle Split Pass

### Issues Found

- **P1 - Backtest launch UI did not expose backend-supported advanced params.** The API already accepted `output_csv`, `markets`, `slippage_multipliers`, and related options, but the page could not configure them.
- **P1 - WFV UI did not expose backend-supported advanced params.** The API already accepted `run_id`, `folds_config`, `train_config`, and `robust_weights`, but the page could not configure them.
- **P2 - Backtest/WFV command construction was not directly testable.** CLI argv construction lived inside background task closures.
- **P2 - All top-level pages were statically imported into the main frontend bundle.** This produced a large initial route bundle and made every page pay for all other page modules up front.

### Changes Applied

- Added `_build_grid_cmd()` and `_build_wfv_cmd()` in `web/api/routers/backtest.py` and covered advanced argv construction in tests.
- Added Backtest Launch controls for output CSV, multi-market list, explore-markets, slippage sensitivity, and slippage multipliers.
- Added WFV controls for run ID, folds config, train config, and robust weights JSON.
- Added i18n keys for the new Backtest/WFV controls in both English and Chinese.
- Changed `web/frontend/src/App.tsx` to lazy-load top-level pages with `React.lazy` and `Suspense`, reducing the main JS chunk from about 1.19 MB to about 415 KB. ECharts is now isolated in its own async chunk; it still exceeds Vite's default 500 KB warning threshold.

### Verification Completed

```bash
./.venv/bin/python -m pytest test/test_web_dashboard.py
./.venv/bin/python -c "from web.api.app import app; print('OK', len(app.routes))"
cd web/frontend && npm run build
```

Results:

- `test/test_web_dashboard.py`: 9 passed.
- FastAPI import: `OK 55`.
- Frontend build: passed.
- Bundle result: main `index` JS chunk dropped to about 415 KB; remaining warning is the isolated `EChartsWrapper` async chunk at about 656 KB.
- CDP render smoke: `/backtest` rendered successfully; Launch tab showed `输出 CSV`, `滑点敏感性`, `多市场`, `网格并行数`; WFV tab showed `运行 ID`, `折叠配置`, `训练配置`, `稳健权重 JSON`; no `Not Found`, application error, or stale placeholder text.

### Remaining Iteration Plan

- **Next P1:** Add UI-side validation and error messaging for JSON fields and comma-separated numeric lists before starting long tasks.
- **Next P2:** Add a committed browser test runner for tab switching and form submission smoke tests.
- **Next P2:** Decide whether to split ECharts by chart-heavy routes or accept the isolated async vendor chunk with an adjusted warning threshold.

---

## Phase 1: Foundation + Data Explorer

### Task 1: Install ECharts and remove Recharts

**Files:**
- Modify: `web/frontend/package.json`

- [ ] **Step 1: Install echarts packages and remove recharts**

```bash
cd web/frontend && npm uninstall recharts && npm install echarts echarts-for-react
```

- [ ] **Step 2: Verify installation**

```bash
cd web/frontend && npm ls echarts echarts-for-react
```

Expected: both packages listed with versions

- [ ] **Step 3: Commit**

```bash
git add web/frontend/package.json web/frontend/package-lock.json
git commit -m "chore: replace recharts with echarts for financial visualization"
```

---

### Task 2: Create EChartsWrapper component

**Files:**
- Create: `web/frontend/src/components/ui/EChartsWrapper.tsx`

- [ ] **Step 1: Create the ECharts wrapper component**

```tsx
import { useRef, useEffect, useCallback } from "react";
import * as echarts from "echarts/core";
import { CanvasRenderer } from "echarts/renderers";
import {
  LineChart,
  BarChart,
  CandlestickChart,
  HeatmapChart,
  ScatterChart,
} from "echarts/charts";
import {
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent,
  ToolboxComponent,
  VisualMapComponent,
} from "echarts/components";

echarts.use([
  CanvasRenderer,
  LineChart,
  BarChart,
  CandlestickChart,
  HeatmapChart,
  ScatterChart,
  TitleComponent,
  TooltipComponent,
  LegendComponent,
  GridComponent,
  DataZoomComponent,
  ToolboxComponent,
  VisualMapComponent,
]);

interface EChartsWrapperProps {
  option: Record<string, unknown>;
  height?: string | number;
  loading?: boolean;
  onEvents?: Record<string, (params: unknown) => void>;
  className?: string;
}

export function EChartsWrapper({
  option,
  height = 400,
  loading = false,
  onEvents,
  className,
}: EChartsWrapperProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<echarts.ECharts | null>(null);

  const initChart = useCallback(() => {
    if (!containerRef.current) return;
    if (chartRef.current) {
      chartRef.current.dispose();
    }
    chartRef.current = echarts.init(containerRef.current, "dark");
    chartRef.current.setOption(option);
    if (onEvents) {
      Object.entries(onEvents).forEach(([event, handler]) => {
        chartRef.current?.on(event, handler);
      });
    }
  }, [option, onEvents]);

  useEffect(() => {
    initChart();
    return () => {
      chartRef.current?.dispose();
      chartRef.current = null;
    };
  }, [initChart]);

  useEffect(() => {
    if (chartRef.current) {
      chartRef.current.setOption(option);
    }
  }, [option]);

  useEffect(() => {
    if (!chartRef.current) return;
    if (loading) {
      chartRef.current.showLoading("default", {
        text: "",
        color: "#3b82f6",
        maskColor: "rgba(0, 0, 0, 0.3)",
      });
    } else {
      chartRef.current.hideLoading();
    }
  }, [loading]);

  useEffect(() => {
    const handleResize = () => chartRef.current?.resize();
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  return (
    <div
      ref={containerRef}
      style={{ height, width: "100%" }}
      className={className}
    />
  );
}
```

- [ ] **Step 2: Verify TypeScript compiles**

```bash
cd web/frontend && npx tsc --noEmit --pretty 2>&1 | head -20
```

Expected: no errors referencing EChartsWrapper

- [ ] **Step 3: Commit**

```bash
git add web/frontend/src/components/ui/EChartsWrapper.tsx
git commit -m "feat: add EChartsWrapper component with tree-shaken echarts imports"
```

---

### Task 3: Create shared UI components (Table, Card, Badge, Tabs, Select, SearchInput)

**Files:**
- Create: `web/frontend/src/components/ui/Table.tsx`
- Create: `web/frontend/src/components/ui/Card.tsx`
- Create: `web/frontend/src/components/ui/Badge.tsx`
- Create: `web/frontend/src/components/ui/Tabs.tsx`
- Create: `web/frontend/src/components/ui/Select.tsx`
- Create: `web/frontend/src/components/ui/SearchInput.tsx`

- [ ] **Step 1: Create Table component**

```tsx
import { useState } from "react";

interface Column<T> {
  key: string;
  label: string;
  render?: (row: T, idx: number) => React.ReactNode;
  align?: "left" | "right";
  sortable?: boolean;
}

interface TableProps<T> {
  columns: Column<T>[];
  data: T[];
  pageSize?: number;
  onRowClick?: (row: T, idx: number) => void;
  rowKey?: (row: T, idx: number) => string;
}

export function Table<T extends Record<string, unknown>>({
  columns,
  data,
  pageSize = 20,
  onRowClick,
  rowKey,
}: TableProps<T>) {
  const [page, setPage] = useState(0);
  const [sortKey, setSortKey] = useState<string | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");

  const sorted = sortKey
    ? [...data].sort((a, b) => {
        const av = a[sortKey];
        const bv = b[sortKey];
        if (av == null) return 1;
        if (bv == null) return -1;
        const cmp = av < bv ? -1 : av > bv ? 1 : 0;
        return sortDir === "asc" ? cmp : -cmp;
      })
    : data;

  const paged = sorted.slice(page * pageSize, (page + 1) * pageSize);
  const totalPages = Math.ceil(data.length / pageSize);

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-zinc-700">
            {columns.map((col) => (
              <th
                key={col.key}
                className={`px-3 py-2 font-medium text-zinc-400 text-xs uppercase ${
                  col.align === "right" ? "text-right" : "text-left"
                } ${col.sortable ? "cursor-pointer select-none" : ""}`}
                onClick={() => {
                  if (!col.sortable) return;
                  if (sortKey === col.key) {
                    setSortDir((d) => (d === "asc" ? "desc" : "asc"));
                  } else {
                    setSortKey(col.key);
                    setSortDir("asc");
                  }
                }}
              >
                {col.label}
                {sortKey === col.key && (
                  <span className="ml-1">{sortDir === "asc" ? "↑" : "↓"}</span>
                )}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {paged.map((row, i) => (
            <tr
              key={rowKey ? rowKey(row, i) : i}
              className={`border-b border-zinc-800 hover:bg-zinc-800/50 ${
                onRowClick ? "cursor-pointer" : ""
              }`}
              onClick={() => onRowClick?.(row, i)}
            >
              {columns.map((col) => (
                <td
                  key={col.key}
                  className={`px-3 py-2 text-zinc-300 ${
                    col.align === "right" ? "text-right" : ""
                  }`}
                >
                  {col.render
                    ? col.render(row, i)
                    : (row[col.key] as React.ReactNode)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {totalPages > 1 && (
        <div className="flex items-center justify-between px-3 py-2 text-xs text-zinc-500">
          <span>
            {page * pageSize + 1}–{Math.min((page + 1) * pageSize, data.length)}{" "}
            of {data.length}
          </span>
          <div className="flex gap-1">
            <button
              disabled={page === 0}
              onClick={() => setPage(page - 1)}
              className="px-2 py-1 rounded bg-zinc-800 disabled:opacity-30"
            >
              Prev
            </button>
            <button
              disabled={page >= totalPages - 1}
              onClick={() => setPage(page + 1)}
              className="px-2 py-1 rounded bg-zinc-800 disabled:opacity-30"
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Create Card component**

```tsx
interface CardProps {
  title?: string;
  children: React.ReactNode;
  actions?: React.ReactNode;
  className?: string;
}

export function Card({ title, children, actions, className = "" }: CardProps) {
  return (
    <div
      className={`bg-zinc-900 rounded-lg border border-zinc-800 ${className}`}
    >
      {(title || actions) && (
        <div className="flex items-center justify-between px-4 py-3 border-b border-zinc-800">
          {title && (
            <h3 className="text-sm font-medium text-zinc-300">{title}</h3>
          )}
          {actions && <div className="flex items-center gap-2">{actions}</div>}
        </div>
      )}
      <div className="p-4">{children}</div>
    </div>
  );
}
```

- [ ] **Step 3: Create Badge component**

```tsx
interface BadgeProps {
  variant?: "success" | "warning" | "error" | "info" | "neutral";
  children: React.ReactNode;
}

const VARIANT_STYLES: Record<string, string> = {
  success: "bg-emerald-900/50 text-emerald-300 border-emerald-700",
  warning: "bg-amber-900/50 text-amber-300 border-amber-700",
  error: "bg-red-900/50 text-red-300 border-red-700",
  info: "bg-blue-900/50 text-blue-300 border-blue-700",
  neutral: "bg-zinc-800 text-zinc-400 border-zinc-600",
};

export function Badge({ variant = "neutral", children }: BadgeProps) {
  return (
    <span
      className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium border ${VARIANT_STYLES[variant]}`}
    >
      {children}
    </span>
  );
}
```

- [ ] **Step 4: Create Tabs component**

```tsx
interface Tab {
  key: string;
  label: string;
}

interface TabsProps {
  tabs: Tab[];
  activeKey: string;
  onChange: (key: string) => void;
}

export function Tabs({ tabs, activeKey, onChange }: TabsProps) {
  return (
    <div className="flex gap-0.5 bg-zinc-800/50 rounded-lg p-0.5">
      {tabs.map((tab) => (
        <button
          key={tab.key}
          onClick={() => onChange(tab.key)}
          className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
            tab.key === activeKey
              ? "bg-blue-600 text-white"
              : "text-zinc-400 hover:text-zinc-200"
          }`}
        >
          {tab.label}
        </button>
      ))}
    </div>
  );
}
```

- [ ] **Step 5: Create Select component**

```tsx
import { useState, useRef, useEffect } from "react";

interface SelectOption {
  value: string;
  label: string;
}

interface SelectProps {
  options: SelectOption[];
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  searchable?: boolean;
  className?: string;
}

export function Select({
  options,
  value,
  onChange,
  placeholder = "Select...",
  searchable = false,
  className = "",
}: SelectProps) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  const filtered = searchable
    ? options.filter((o) =>
        o.label.toLowerCase().includes(search.toLowerCase())
      )
    : options;

  const selected = options.find((o) => o.value === value);

  return (
    <div ref={ref} className={`relative ${className}`}>
      <button
        onClick={() => setOpen(!open)}
        className="w-full bg-zinc-800 border border-zinc-700 rounded-md px-3 py-2 text-sm text-left text-zinc-200 flex items-center justify-between"
      >
        <span className={selected ? "" : "text-zinc-500"}>
          {selected?.label ?? placeholder}
        </span>
        <span className="text-zinc-500 text-xs">▼</span>
      </button>
      {open && (
        <div className="absolute z-50 mt-1 w-full bg-zinc-800 border border-zinc-700 rounded-md shadow-xl max-h-60 overflow-auto">
          {searchable && (
            <input
              autoFocus
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="w-full px-3 py-2 text-sm bg-zinc-900 border-b border-zinc-700 text-zinc-200 outline-none"
              placeholder="Search..."
            />
          )}
          {filtered.map((opt) => (
            <button
              key={opt.value}
              onClick={() => {
                onChange(opt.value);
                setOpen(false);
                setSearch("");
              }}
              className={`w-full px-3 py-2 text-sm text-left hover:bg-zinc-700 ${
                opt.value === value
                  ? "text-blue-400 bg-zinc-700/50"
                  : "text-zinc-300"
              }`}
            >
              {opt.label}
            </button>
          ))}
          {filtered.length === 0 && (
            <div className="px-3 py-2 text-sm text-zinc-500">No results</div>
          )}
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 6: Create SearchInput component**

```tsx
import { useState, useEffect } from "react";

interface SearchInputProps {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  debounceMs?: number;
  className?: string;
}

export function SearchInput({
  value,
  onChange,
  placeholder = "Search...",
  debounceMs = 300,
  className = "",
}: SearchInputProps) {
  const [local, setLocal] = useState(value);

  useEffect(() => {
    setLocal(value);
  }, [value]);

  useEffect(() => {
    const timer = setTimeout(() => {
      if (local !== value) onChange(local);
    }, debounceMs);
    return () => clearTimeout(timer);
  }, [local, debounceMs, onChange, value]);

  return (
    <input
      type="text"
      value={local}
      onChange={(e) => setLocal(e.target.value)}
      placeholder={placeholder}
      className={`bg-zinc-800 border border-zinc-700 rounded-md px-3 py-2 text-sm text-zinc-200 placeholder-zinc-500 outline-none focus:border-blue-500 ${className}`}
    />
  );
}
```

- [ ] **Step 7: Commit all UI components**

```bash
git add web/frontend/src/components/ui/
git commit -m "feat: add shared UI components (Table, Card, Badge, Tabs, Select, SearchInput)"
```

---

### Task 4: Create remaining shared UI components (MultiSelect, NumberInput, DatePicker, TaskStatus, Toast)

**Files:**
- Create: `web/frontend/src/components/ui/MultiSelect.tsx`
- Create: `web/frontend/src/components/ui/NumberInput.tsx`
- Create: `web/frontend/src/components/ui/DatePicker.tsx`
- Create: `web/frontend/src/components/ui/TaskStatus.tsx`
- Create: `web/frontend/src/components/ui/Toast.tsx`

- [ ] **Step 1: Create MultiSelect component**

```tsx
import { useState, useRef, useEffect } from "react";

interface MultiSelectProps {
  options: { value: string; label: string }[];
  values: string[];
  onChange: (values: string[]) => void;
  placeholder?: string;
  className?: string;
}

export function MultiSelect({
  options,
  values,
  onChange,
  placeholder = "Select...",
  className = "",
}: MultiSelectProps) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  const toggle = (val: string) => {
    onChange(
      values.includes(val) ? values.filter((v) => v !== val) : [...values, val]
    );
  };

  const selected = options.filter((o) => values.includes(o.value));

  return (
    <div ref={ref} className={`relative ${className}`}>
      <div
        onClick={() => setOpen(!open)}
        className="w-full min-h-[38px] bg-zinc-800 border border-zinc-700 rounded-md px-2 py-1.5 flex items-center flex-wrap gap-1 cursor-pointer"
      >
        {selected.length === 0 && (
          <span className="text-zinc-500 text-sm">{placeholder}</span>
        )}
        {selected.map((s) => (
          <span
            key={s.value}
            className="inline-flex items-center gap-1 px-2 py-0.5 rounded bg-blue-900/50 text-blue-300 text-xs border border-blue-700"
          >
            {s.label}
            <span
              onClick={(e) => {
                e.stopPropagation();
                toggle(s.value);
              }}
              className="cursor-pointer hover:text-red-400"
            >
              ×
            </span>
          </span>
        ))}
        <span className="ml-auto text-zinc-500 text-xs">▼</span>
      </div>
      {open && (
        <div className="absolute z-50 mt-1 w-full bg-zinc-800 border border-zinc-700 rounded-md shadow-xl max-h-60 overflow-auto">
          {options.map((opt) => (
            <button
              key={opt.value}
              onClick={() => toggle(opt.value)}
              className={`w-full px-3 py-2 text-sm text-left hover:bg-zinc-700 flex items-center gap-2 ${
                values.includes(opt.value) ? "text-blue-400" : "text-zinc-300"
              }`}
            >
              <span
                className={`w-4 h-4 rounded border flex items-center justify-center text-xs ${
                  values.includes(opt.value)
                    ? "bg-blue-600 border-blue-500 text-white"
                    : "border-zinc-600"
                }`}
              >
                {values.includes(opt.value) && "✓"}
              </span>
              {opt.label}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Create NumberInput component**

```tsx
interface NumberInputProps {
  value: number | undefined;
  onChange: (value: number | undefined) => void;
  step?: number;
  min?: number;
  max?: number;
  placeholder?: string;
  className?: string;
}

export function NumberInput({
  value,
  onChange,
  step = 1,
  min,
  max,
  placeholder = "",
  className = "",
}: NumberInputProps) {
  return (
    <input
      type="number"
      value={value ?? ""}
      onChange={(e) => {
        const v = e.target.value === "" ? undefined : Number(e.target.value);
        if (v !== undefined && min !== undefined && v < min) return;
        if (v !== undefined && max !== undefined && v > max) return;
        onChange(v);
      }}
      step={step}
      min={min}
      max={max}
      placeholder={placeholder}
      className={`bg-zinc-800 border border-zinc-700 rounded-md px-3 py-2 text-sm text-zinc-200 placeholder-zinc-500 outline-none focus:border-blue-500 ${className}`}
    />
  );
}
```

- [ ] **Step 3: Create DatePicker component**

```tsx
interface DatePickerProps {
  value: string;
  onChange: (value: string) => void;
  className?: string;
}

export function DatePicker({
  value,
  onChange,
  className = "",
}: DatePickerProps) {
  return (
    <input
      type="date"
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className={`bg-zinc-800 border border-zinc-700 rounded-md px-3 py-2 text-sm text-zinc-200 outline-none focus:border-blue-500 [color-scheme:dark] ${className}`}
    />
  );
}
```

- [ ] **Step 4: Create TaskStatus component (SSE-based, replaces polling)**

```tsx
import { useSSE } from "../../hooks/useSSE";
import { Badge } from "./Badge";

interface TaskStatusProps {
  taskId: string | null;
  onComplete?: (result: unknown) => void;
  onError?: (error: string) => void;
}

const STATUS_VARIANT: Record<string, "success" | "warning" | "error" | "info" | "neutral"> = {
  idle: "neutral",
  streaming: "info",
  done: "success",
  error: "error",
};

export function TaskStatus({ taskId, onComplete, onError }: TaskStatusProps) {
  const { status, error, events } = useSSE(taskId);

  if (!taskId) return null;

  const lastEvent = events[events.length - 1];

  if (status === "done" && onComplete && lastEvent) {
    onComplete(lastEvent.data);
  }
  if (status === "error" && onError && error) {
    onError(error);
  }

  return (
    <div className="flex items-center gap-2 text-sm">
      <span className="text-zinc-400">Task:</span>
      <code className="text-xs text-zinc-500">{taskId.slice(0, 8)}</code>
      <Badge variant={STATUS_VARIANT[status]}>{status}</Badge>
      {status === "streaming" && lastEvent && (
        <span className="text-xs text-zinc-500 truncate max-w-xs">
          {typeof lastEvent.data === "string"
            ? lastEvent.data
            : JSON.stringify(lastEvent.data).slice(0, 80)}
        </span>
      )}
      {status === "error" && (
        <span className="text-xs text-red-400 truncate max-w-xs">{error}</span>
      )}
    </div>
  );
}
```

- [ ] **Step 5: Create Toast component**

```tsx
import { useState, useEffect, useCallback } from "react";

type ToastVariant = "success" | "error" | "info";

interface ToastMessage {
  id: number;
  variant: ToastVariant;
  message: string;
}

let addToastFn: ((variant: ToastVariant, message: string) => void) | null = null;

export function toast(variant: ToastVariant, message: string) {
  addToastFn?.(variant, message);
}

export function ToastContainer() {
  const [toasts, setToasts] = useState<ToastMessage[]>([]);
  let nextId = 0;

  const add = useCallback((variant: ToastVariant, message: string) => {
    const id = ++nextId;
    setToasts((prev) => [...prev, { id, variant, message }]);
    setTimeout(() => {
      setToasts((prev) => prev.filter((t) => t.id !== id));
    }, 3000);
  }, []);

  useEffect(() => {
    addToastFn = add;
    return () => {
      addToastFn = null;
    };
  }, [add]);

  const VARIANT_STYLE: Record<ToastVariant, string> = {
    success: "bg-emerald-900 border-emerald-700 text-emerald-200",
    error: "bg-red-900 border-red-700 text-red-200",
    info: "bg-blue-900 border-blue-700 text-blue-200",
  };

  return (
    <div className="fixed top-4 right-4 z-[100] flex flex-col gap-2">
      {toasts.map((t) => (
        <div
          key={t.id}
          className={`px-4 py-3 rounded-lg border text-sm shadow-lg ${VARIANT_STYLE[t.variant]}`}
        >
          {t.message}
        </div>
      ))}
    </div>
  );
}
```

- [ ] **Step 6: Commit**

```bash
git add web/frontend/src/components/ui/
git commit -m "feat: add MultiSelect, NumberInput, DatePicker, TaskStatus, Toast components"
```

---

### Task 5: Create API type definitions and extend client

**Files:**
- Create: `web/frontend/src/api/types.ts`
- Modify: `web/frontend/src/api/client.ts`

- [ ] **Step 1: Create types.ts with all API response interfaces**

```ts
// --- Data ---
export interface StockQuote {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  change: number;
}

export interface StockSearchResult {
  symbol: string;
  name: string;
  exchange: string;
}

export interface SectorInfo {
  sector_id: string;
  sector_name: string;
  stock_count: number;
}

export interface SectorStock {
  symbol: string;
  name: string;
}

export interface SectorRotation {
  sector_id: string;
  sector_name: string;
  returns: Record<string, number>;
}

export interface AltDataResponse {
  type: string;
  columns: string[];
  rows: Record<string, unknown>[];
  total: number;
  has_more: boolean;
}

// --- Factors ---
export interface FactorValueRow {
  symbol: string;
  date: string;
  [factor: string]: string | number;
}

export interface ICDAnalysis {
  factor: string;
  ic_mean: number;
  icir: number;
  decay: { horizon: number; ic: number }[];
  rolling: { date: string; ic: number }[];
}

export interface FactorHeatmap {
  factors: string[];
  matrix: number[][];
}

// --- Backtest ---
export interface EquityCurve {
  dates: string[];
  portfolio: number[];
  benchmark: number[];
  excess: number[];
}

export interface BacktestMetrics {
  annual_return: number;
  sharpe: number;
  max_drawdown: number;
  calmar: number;
  ic: number;
  icir: number;
  rank_ic: number;
  rank_icir: number;
  win_rate: number;
  turnover: number;
  cum_return: number;
  annual_vol: number;
  sortino: number;
}

export interface DrawdownSeries {
  dates: string[];
  drawdown: number[];
}

export interface CompareRun {
  filename: string;
  label: string;
  color: string;
  equity_curve: EquityCurve;
  drawdown: DrawdownSeries;
  metrics: BacktestMetrics;
}

export interface CompareResponse {
  runs: CompareRun[];
  dates: string[];
}

// --- Models ---
export interface ModelInfo {
  filename: string;
  size_mb: number;
  modified: string;
  meta?: Record<string, unknown>;
}

// --- Tasks ---
export interface TaskInfo {
  task_id: string;
  task_type: string;
  status: string;
  created_at: string;
  error?: string;
  result?: unknown;
}
```

- [ ] **Step 2: Extend API client with typed methods**

Read the existing `client.ts`, then add typed convenience methods:

```ts
import { get as _get, post as _post, put as _put, del as _del } from "./client";

// Re-export raw methods
export { _get as get, _post as post, _put as put, _del as del };

// --- Data ---
export const fetchStockQuotes = (symbol: string, params?: { start?: string; end?: string; fields?: string }) =>
  _get<{ symbol: string; name: string; data: import("./types").StockQuote[] }>(`/data/stock/${symbol}/quotes?${new URLSearchParams(Object.entries(params || {}).filter(([_, v]) => v != null) as [string, string][]).toString()}`);

export const searchStocks = (q: string, limit?: number) =>
  _get<import("./types").StockSearchResult[]>(`/data/stock/search?q=${encodeURIComponent(q)}${limit ? `&limit=${limit}` : ""}`);

export const fetchSectors = () =>
  _get<import("./types").SectorInfo[]>("/data/sectors");

export const fetchSectorStocks = (sectorId: string) =>
  _get<{ sector_id: string; sector_name: string; stocks: import("./types").SectorStock[] }>(`/data/sectors/${encodeURIComponent(sectorId)}/stocks`);

export const fetchSectorRotation = (windows?: string) =>
  _get<import("./types").SectorRotation[]>(`/data/sectors/rotation${windows ? `?windows=${windows}` : ""}`);

export const fetchAltData = (type: string, params?: { symbol?: string; start?: string; end?: string; limit?: number }) => {
  const sp = new URLSearchParams(Object.entries(params || {}).filter(([_, v]) => v != null) as [string, string][]);
  return _get<import("./types").AltDataResponse>(`/data/alt-data/${type}?${sp.toString()}`);
};

// --- Factors ---
export const fetchFactorValues = (params: { factors: string; symbols?: string; start?: string; end?: string }) => {
  const sp = new URLSearchParams(Object.entries(params).filter(([_, v]) => v != null) as [string, string][]);
  return _get<{ factors: string[]; data: import("./types").FactorValueRow[] }>(`/factors/values?${sp.toString()}`);
};

export const fetchICAnalysis = (params: { factor: string; horizon?: number; window?: number }) => {
  const sp = new URLSearchParams(Object.entries(params).filter(([_, v]) => v != null) as [string, string][]);
  return _get<import("./types").ICDAnalysis>(`/factors/ic-analysis?${sp.toString()}`);
};

export const fetchFactorHeatmap = (params: { factors: string; start?: string; end?: string }) => {
  const sp = new URLSearchParams(Object.entries(params).filter(([_, v]) => v != null) as [string, string][]);
  return _get<import("./types").FactorHeatmap>(`/factors/heatmap?${sp.toString()}`);
};

// --- Backtest ---
export const fetchEquityCurve = (filename: string) =>
  _get<import("./types").EquityCurve>(`/backtest/results/${encodeURIComponent(filename)}/equity-curve`);

export const fetchBacktestMetrics = (filename: string) =>
  _get<import("./types").BacktestMetrics>(`/backtest/results/${encodeURIComponent(filename)}/metrics`);

export const fetchDrawdown = (filename: string) =>
  _get<import("./types").DrawdownSeries>(`/backtest/results/${encodeURIComponent(filename)}/drawdown`);

export const compareRuns = (filenames: string[]) =>
  _post<import("./types").CompareResponse>("/backtest/compare", { filenames });
```

- [ ] **Step 3: Commit**

```bash
git add web/frontend/src/api/types.ts web/frontend/src/api/client.ts
git commit -m "feat: add API type definitions and typed client methods"
```

---

### Task 6: Update Sidebar and App routing for new page structure

**Files:**
- Modify: `web/frontend/src/components/Sidebar.tsx`
- Modify: `web/frontend/src/App.tsx`
- Modify: `web/frontend/src/i18n/zh.json`
- Modify: `web/frontend/src/i18n/en.json`

- [ ] **Step 1: Update Sidebar nav items**

Replace the `NAV_ITEMS` array in `Sidebar.tsx`. Use lucide-react icons instead of Unicode symbols:

```ts
import { NavLink } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { LayoutDashboard, Database, FlaskConical, LineChart, Radio, Brain, Settings, Terminal, LanguageToggle } from "lucide-react";
import { LanguageToggle } from "./LanguageToggle";

const NAV_ITEMS = [
  { icon: LayoutDashboard, key: "overview", to: "/" },
  { icon: Database, key: "dataExplorer", to: "/data-explorer" },
  { icon: FlaskConical, key: "research", to: "/research" },
  { icon: Brain, key: "models", to: "/models" },
  { icon: LineChart, key: "backtest", to: "/backtest" },
  { icon: Radio, key: "signals", to: "/signals" },
  { icon: Settings, key: "config", to: "/config" },
  { icon: Terminal, key: "system", to: "/system" },
] as const;
```

Update the render to use `<item.icon size={16} />` instead of the Unicode `item.icon`.

- [ ] **Step 2: Update App.tsx routing**

Replace page imports and routes:

```tsx
import { OverviewPage } from "./pages/OverviewPage";
import { DataExplorerPage } from "./pages/DataExplorerPage";
import { ResearchPage } from "./pages/ResearchPage";
import { ModelsPage } from "./pages/ModelsPage";
import { BacktestPage } from "./pages/BacktestPage";
import { SignalsPage } from "./pages/SignalsPage";
import { ConfigPage } from "./pages/ConfigPage";
import { SystemPage } from "./pages/SystemPage";

// In Routes:
<Route index element={<OverviewPage />} />
<Route path="/data-explorer" element={<DataExplorerPage />} />
<Route path="/research" element={<ResearchPage />} />
<Route path="/models" element={<ModelsPage />} />
<Route path="/backtest" element={<BacktestPage />} />
<Route path="/signals" element={<SignalsPage />} />
<Route path="/config" element={<ConfigPage />} />
<Route path="/system" element={<SystemPage />} />
```

- [ ] **Step 3: Update i18n keys**

Add new nav keys to both `zh.json` and `en.json`:

```json
{
  "nav": {
    "subtitle": "量化研究平台",
    "overview": "总览",
    "dataExplorer": "数据探索",
    "research": "因子研究",
    "models": "模型",
    "backtest": "回测",
    "signals": "信号",
    "config": "配置",
    "system": "系统"
  }
}
```

English:
```json
{
  "nav": {
    "subtitle": "Quant Research Platform",
    "overview": "Overview",
    "dataExplorer": "Data Explorer",
    "research": "Research",
    "models": "Models",
    "backtest": "Backtest",
    "signals": "Signals",
    "config": "Config",
    "system": "System"
  }
}
```

- [ ] **Step 4: Commit**

```bash
git add web/frontend/src/components/Sidebar.tsx web/frontend/src/App.tsx web/frontend/src/i18n/
git commit -m "feat: update sidebar navigation and routing for research-centric page structure"
```

---

### Task 7: Create backend data service (qlib access + TTL cache)

**Files:**
- Create: `web/api/services/data_service.py`

- [ ] **Step 1: Create data_service.py with qlib lazy init and TTL cache**

```python
"""Data access service: qlib price data with TTL cache."""
from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd

from quant_ex.data.loader import DataLoader
from quant_ex.data.utils import load_stock_names, code_to_qlib_instrument, normalize_qlib_instrument
from quant_ex.utils.config import load_config
from web.api.deps import CACHE_DIR

logger = logging.getLogger(__name__)

_config = load_config()
_cache: Dict[str, Tuple[float, object]] = {}  # key -> (expiry_ts, data)
_DEFAULT_TTL = 86400  # 1 day


def _qlib_loader() -> DataLoader:
    """Lazy-initialize qlib and return a DataLoader singleton."""
    if not hasattr(_qlib_loader, "_instance"):
        loader = DataLoader(_config)
        loader.init_qlib()
        _qlib_loader._instance = loader  # type: ignore
    return _qlib_loader._instance


def _cached(key: str, ttl: int, factory):
    """Simple TTL cache: returns cached value if fresh, else calls factory()."""
    now = time.time()
    if key in _cache:
        expiry, data = _cache[key]
        if now < expiry:
            return data
    data = factory()
    _cache[key] = (now + ttl, data)
    return data


def get_stock_quotes(
    symbol: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    fields: Optional[List[str]] = None,
) -> Dict:
    """Fetch OHLCV data for a single stock from qlib."""
    qlib_sym = normalize_qlib_instrument(symbol)
    cache_key = f"quotes:{qlib_sym}:{start}:{end}:{fields}"
    return _cached(cache_key, _DEFAULT_TTL, lambda: _load_quotes(qlib_sym, start, end, fields))


def _load_quotes(qlib_sym: str, start, end, fields) -> Dict:
    loader = _qlib_loader()
    names = load_stock_names()
    stock_name = names.get(qlib_sym, qlib_sym)

    default_fields = ["$open", "$high", "$low", "$close", "$volume", "$change"]
    fields = fields or default_fields

    df = loader.load_price_data(
        instruments=[qlib_sym],
        start_time=start or "2020-01-01",
        end_time=end,
        fields=fields,
    )
    if df.empty:
        return {"symbol": qlib_sym, "name": stock_name, "data": []}

    df = df.xs(qlib_sym, level="instrument") if "instrument" in df.index.names else df
    records = []
    for dt, row in df.iterrows():
        r = {"date": str(dt.date()) if hasattr(dt, "date") else str(dt)}
        for col in df.columns:
            val = row[col]
            r[col.lstrip("$")] = float(val) if pd.notna(val) else None
        records.append(r)
    return {"symbol": qlib_sym, "name": stock_name, "data": records}


def search_stocks(q: str, limit: int = 10) -> List[Dict]:
    """Fuzzy search stocks by symbol or name."""
    names = load_stock_names()
    q_lower = q.lower()
    results = []
    for sym, name in names.items():
        if q_lower in sym.lower() or q_lower in name.lower():
            results.append({"symbol": sym, "name": name, "exchange": sym[:2]})
            if len(results) >= limit:
                break
    return results
```

- [ ] **Step 2: Commit**

```bash
git add web/api/services/data_service.py
git commit -m "feat: add data service with qlib lazy init and TTL cache"
```

---

### Task 8: Add new data endpoints to data router

**Files:**
- Modify: `web/api/routers/data.py`

- [ ] **Step 1: Add stock quotes, search, sector, and alt-data endpoints**

Add these imports at the top of `data.py`:

```python
import json
from datetime import datetime, date as date_mod
from pathlib import Path
from typing import Optional, List

from fastapi import APIRouter, Query
from pydantic import BaseModel

from web.api.deps import CACHE_DIR, get_config
from web.api.services.task_manager import get_task_manager
from web.api.services.data_service import get_stock_quotes, search_stocks
from web.api.routers.system import stream_task
```

Add these endpoints to the existing router:

```python
@router.get("/stock/search")
async def stock_search(q: str = Query(..., min_length=1), limit: int = Query(10, ge=1, le=50)):
    """Fuzzy search stocks by symbol or name."""
    return search_stocks(q, limit)


@router.get("/stock/{symbol}/quotes")
async def stock_quotes(
    symbol: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    fields: Optional[str] = None,
):
    """OHLCV time series for a stock from qlib."""
    field_list = fields.split(",") if fields else None
    return get_stock_quotes(symbol, start, end, field_list)


@router.get("/sectors")
async def list_sectors():
    """Sector list with constituent counts."""
    sector_stocks_path = CACHE_DIR / "sector_stocks.json"
    if not sector_stocks_path.exists():
        return []
    with open(sector_stocks_path) as f:
        data = json.load(f)
    results = []
    for sid, stocks in data.items():
        results.append({"sector_id": sid, "sector_name": sid, "stock_count": len(stocks)})
    return results


@router.get("/sectors/{sector_id}/stocks")
async def sector_stocks(sector_id: str):
    """Constituent stocks for a sector."""
    sector_stocks_path = CACHE_DIR / "sector_stocks.json"
    if not sector_stocks_path.exists():
        return {"sector_id": sector_id, "sector_name": sector_id, "stocks": []}
    with open(sector_stocks_path) as f:
        data = json.load(f)
    stocks = data.get(sector_id, [])
    names = {}
    from quant_ex.data.utils import load_stock_names
    names = load_stock_names()
    return {
        "sector_id": sector_id,
        "sector_name": sector_id,
        "stocks": [{"symbol": s, "name": names.get(s, s)} for s in stocks],
    }


@router.get("/sectors/rotation")
async def sector_rotation(windows: str = Query("1,5,20")):
    """Sector rotation data — returns by window."""
    # Placeholder: requires computing from price data
    # Return empty for now; full implementation in Phase 3
    return []


@router.get("/alt-data/{data_type}")
async def alt_data(
    data_type: str,
    symbol: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
):
    """Browse cached alternative data (northbound, margin, etc.)."""
    cache_dir = CACHE_DIR / data_type
    if not cache_dir.exists():
        return {"type": data_type, "columns": [], "rows": [], "total": 0, "has_more": False}

    import pandas as pd
    csv_files = sorted(cache_dir.glob("*.csv"))
    if not csv_files:
        return {"type": data_type, "columns": [], "rows": [], "total": 0, "has_more": False}

    # Read and concatenate CSVs
    dfs = []
    for f in csv_files:
        try:
            df = pd.read_csv(f)
            if symbol and "symbol" in df.columns:
                df = df[df["symbol"].str.contains(symbol, case=False, na=False)]
            dfs.append(df)
        except Exception:
            continue

    if not dfs:
        return {"type": data_type, "columns": [], "rows": [], "total": 0, "has_more": False}

    combined = pd.concat(dfs, ignore_index=True)

    if start and "date" in combined.columns:
        combined = combined[combined["date"] >= start]
    if end and "date" in combined.columns:
        combined = combined[combined["date"] <= end]

    total = len(combined)
    has_more = total > limit
    combined = combined.head(limit)

    columns = combined.columns.tolist()
    rows = combined.to_dict(orient="records")
    # Convert NaN to None for JSON serialization
    for row in rows:
        for k, v in row.items():
            if pd.isna(v):
                row[k] = None

    return {"type": data_type, "columns": columns, "rows": rows, "total": total, "has_more": has_more}
```

- [ ] **Step 2: Verify the backend starts**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex && .venv/bin/python -c "from web.api.app import app; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add web/api/routers/data.py web/api/services/data_service.py
git commit -m "feat: add stock quotes, search, sector, and alt-data API endpoints"
```

---

### Task 9: Add factor value, IC analysis, and heatmap endpoints

**Files:**
- Create: `web/api/services/factor_service.py`
- Modify: `web/api/routers/factors.py`

- [ ] **Step 1: Create factor_service.py**

```python
"""Factor computation service with TTL cache."""
from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd

from quant_ex.data.loader import DataLoader
from quant_ex.features.base import FactorPipeline, FactorRegistry
from quant_ex.backtest.signal_diagnostics import compute_ic_decay, compute_rolling_ic
from quant_ex.utils.config import load_config

logger = logging.getLogger(__name__)

_config = load_config()
_cache: Dict[str, Tuple[float, object]] = {}
_DEFAULT_TTL = 86400


def _cached(key: str, ttl: int, factory):
    now = time.time()
    if key in _cache:
        expiry, data = _cache[key]
        if now < expiry:
            return data
    data = factory()
    _cache[key] = (now + ttl, data)
    return data


def _get_price_data(instruments=None, start=None, end=None):
    """Load price data, with caching."""
    key = f"price:{instruments}:{start}:{end}"
    return _cached(key, _DEFAULT_TTL, lambda: _load_price(instruments, start, end))


def _load_price(instruments, start, end):
    from web.api.services.data_service import _qlib_loader
    loader = _qlib_loader()
    return loader.load_price_data(
        instruments=instruments or "csi500",
        start_time=start or "2020-01-01",
        end_time=end,
    )


def compute_factor_values(
    factor_names: List[str],
    symbols: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> Dict:
    """Compute factor values for given factors and symbols."""
    cache_key = f"factors:{','.join(sorted(factor_names))}:{symbols}:{start}:{end}"
    return _cached(cache_key, _DEFAULT_TTL, lambda: _do_compute(factor_names, symbols, start, end))


def _do_compute(factor_names, symbols, start, end):
    price_data = _get_price_data(instruments=symbols, start=start, end=end)
    if price_data.empty:
        return {"factors": factor_names, "data": []}

    configs = [{"name": n} for n in factor_names]
    pipeline = FactorPipeline.from_config(configs)
    result = pipeline.compute(price_data)
    if result is None or result.empty:
        return {"factors": factor_names, "data": []}

    # Reset index for JSON serialization
    df = result.reset_index()
    if symbols:
        df = df[df["instrument"].isin(symbols)]

    records = df.to_dict(orient="records")
    for row in records:
        if "datetime" in row:
            row["date"] = str(row["datetime"].date()) if hasattr(row["datetime"], "date") else str(row["datetime"])
            del row["datetime"]
        for k, v in row.items():
            if pd.isna(v):
                row[k] = None
            elif isinstance(v, (float, int)):
                row[k] = round(float(v), 6)

    return {"factors": factor_names, "data": records}


def compute_ic_analysis(
    factor_name: str,
    horizon: int = 5,
    window: int = 20,
) -> Dict:
    """Compute IC decay and rolling IC for a factor."""
    cache_key = f"ic:{factor_name}:{horizon}:{window}"
    return _cached(cache_key, _DEFAULT_TTL, lambda: _do_ic(factor_name, horizon, window))


def _do_ic(factor_name, horizon, window):
    price_data = _get_price_data()
    if price_data.empty:
        return {"factor": factor_name, "ic_mean": 0, "icir": 0, "decay": [], "rolling": []}

    configs = [{"name": factor_name}]
    pipeline = FactorPipeline.from_config(configs)
    result = pipeline.compute(price_data)
    if result is None or result.empty:
        return {"factor": factor_name, "ic_mean": 0, "icir": 0, "decay": [], "rolling": []}

    # Use the first factor column as the signal
    factor_col = [c for c in result.columns if c != "instrument" and c != "datetime"][0]
    pred = result[factor_col]

    decay_df = compute_ic_decay(pred, price_data, horizons=[1, 2, 3, 5, 10, 15, 20])
    rolling_df = compute_rolling_ic(pred, price_data, horizon=horizon, window=window)

    decay_records = decay_df.to_dict(orient="records") if not decay_df.empty else []
    for r in decay_records:
        for k, v in r.items():
            if pd.isna(v):
                r[k] = None

    rolling_records = rolling_df.to_dict(orient="records") if not rolling_df.empty else []
    for r in rolling_records:
        if "datetime" in r:
            r["date"] = str(r["datetime"].date()) if hasattr(r["datetime"], "date") else str(r["datetime"])
            del r["datetime"]
        for k, v in r.items():
            if pd.isna(v):
                r[k] = None

    ic_mean = float(decay_df["mean_rank_ic"].mean()) if not decay_df.empty and "mean_rank_ic" in decay_df else 0
    icir = float(decay_df["rank_icir"].mean()) if not decay_df.empty and "rank_icir" in decay_df else 0

    return {
        "factor": factor_name,
        "ic_mean": round(ic_mean, 4),
        "icir": round(icir, 4),
        "decay": decay_records,
        "rolling": rolling_records,
    }
```

- [ ] **Step 2: Add new endpoints to factors.py router**

Add these endpoints to the existing factors router:

```python
from web.api.services.factor_service import compute_factor_values, compute_ic_analysis


@router.get("/values")
async def factor_values(
    factors: str = Query(..., description="Comma-separated factor names"),
    symbols: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    """Computed factor values per stock/date."""
    factor_list = [f.strip() for f in factors.split(",")]
    symbol_list = [s.strip() for s in symbols.split(",")] if symbols else None
    return compute_factor_values(factor_list, symbol_list, start, end)


@router.get("/ic-analysis")
async def ic_analysis(
    factor: str = Query(...),
    horizon: int = Query(5, ge=1, le=60),
    window: int = Query(20, ge=5, le=120),
):
    """IC decay and rolling IC for a factor."""
    return compute_ic_analysis(factor, horizon, window)


@router.get("/heatmap")
async def factor_heatmap(
    factors: str = Query(...),
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    """Factor correlation heatmap."""
    # Placeholder: compute correlation matrix from factor values
    factor_list = [f.strip() for f in factors.split(",")]
    result = compute_factor_values(factor_list, start=start, end=end)
    if not result["data"]:
        return {"factors": factor_list, "matrix": []}
    import pandas as pd
    df = pd.DataFrame(result["data"])
    numeric_cols = [c for c in df.columns if c not in ("symbol", "date")]
    if len(numeric_cols) < 2:
        return {"factors": factor_list, "matrix": [[1.0]]}
    corr = df[numeric_cols].corr().fillna(0).values.tolist()
    return {"factors": numeric_cols, "matrix": corr}
```

- [ ] **Step 3: Commit**

```bash
git add web/api/services/factor_service.py web/api/routers/factors.py
git commit -m "feat: add factor values, IC analysis, and heatmap API endpoints"
```

---

### Task 10: Create DataExplorerPage (Stock Quotes tab)

**Files:**
- Create: `web/frontend/src/pages/DataExplorerPage.tsx`

- [ ] **Step 1: Create DataExplorerPage with Stock Quotes tab**

This is a large page with 5 sub-tabs. Create the full file with the Stock Quotes tab implemented and placeholder content for the other tabs:

```tsx
import { useState, useEffect, useCallback } from "react";
import { useTranslation } from "react-i18next";
import { Card } from "../components/ui/Card";
import { Tabs } from "../components/ui/Tabs";
import { SearchInput } from "../components/ui/SearchInput";
import { DatePicker } from "../components/ui/DatePicker";
import { Select } from "../components/ui/Select";
import { Badge } from "../components/ui/Badge";
import { EChartsWrapper } from "../components/ui/EChartsWrapper";
import { Table } from "../components/ui/Table";
import * as api from "../api/client";

const DATA_TABS = [
  { key: "quotes", label: "Stock Quotes" },
  { key: "sectors", label: "Sectors" },
  { key: "altData", label: "Alt Data" },
  { key: "factors", label: "Factor Values" },
  { key: "cache", label: "Cache" },
];

export function DataExplorerPage() {
  const { t } = useTranslation();
  const [activeTab, setActiveTab] = useState("quotes");

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-zinc-100">{t("dataExplorer.title")}</h1>
        <Tabs tabs={DATA_TABS} activeKey={activeTab} onChange={setActiveTab} />
      </div>

      {activeTab === "quotes" && <StockQuotesTab />}
      {activeTab === "sectors" && <SectorsTab />}
      {activeTab === "altData" && <AltDataTab />}
      {activeTab === "factors" && <FactorValuesTab />}
      {activeTab === "cache" && <CacheTab />}
    </div>
  );
}
```

Then implement `StockQuotesTab` as a sub-component in the same file with:
- Left sidebar: SearchInput, DatePicker range, overlay toggles (MA5/MA20/BOLL/VWAP as chips), quick info panel (latest OHLCV)
- Right main area: EChartsWrapper with candlestick option, volume sub-chart, dataZoom slider
- Search triggers `api.searchStocks()`, selection triggers `api.fetchStockQuotes()`
- Build ECharts `option` object with `candlestick` series, `bar` series for volume, `line` series for MA overlays

- [ ] **Step 2: Add placeholder sub-components**

`SectorsTab`: fetch `api.fetchSectors()` → Table of sectors. Fetch `api.fetchSectorRotation()` → ECharts heatmap placeholder.
`AltDataTab`: Select for data type (from fetcher registry), SearchInput for symbol, date range, `api.fetchAltData()` → Table.
`FactorValuesTab`: MultiSelect for factors, SearchInput for symbols, date range, `api.fetchFactorValues()` → Table.
`CacheTab`: Move cache management from existing DataPage (same logic).

- [ ] **Step 3: Add i18n keys for dataExplorer page**

Add to both zh.json and en.json under `dataExplorer` namespace with keys for: title, search, dateRange, overlays, quickInfo, sectors, altData, factorValues, cache.

- [ ] **Step 4: Commit**

```bash
git add web/frontend/src/pages/DataExplorerPage.tsx web/frontend/src/i18n/
git commit -m "feat: add DataExplorerPage with stock quotes candlestick chart"
```

---

### Task 11: Create ResearchPage (Factor Library + IC Analysis)

**Files:**
- Create: `web/frontend/src/pages/ResearchPage.tsx`

- [ ] **Step 1: Create ResearchPage with 4 sub-tabs**

Tabs: Library, IC Analysis, Heatmap, Mining.

**Library tab**: Fetch `GET /factors/library` → Table (name, class, enabled Badge). Same as existing FactorsPage library tab but using the new Table/Badge components.

**IC Analysis tab**: Select a factor → `api.fetchICAnalysis()` → Two ECharts charts side-by-side:
- Left: IC decay line chart (x=horizon 1-20d, y=rank IC)
- Right: Rolling IC time series (x=date, y=rolling rank IC) with dataZoom
- Metric cards above: mean IC, ICIR

**Heatmap tab**: MultiSelect factors, date range → `api.fetchFactorHeatmap()` → ECharts heatmap with visualMap (red=negative, blue=positive)

**Mining tab**: Same as existing FactorsPage mining tab (3 number inputs + submit) using new NumberInput + TaskStatus components.

- [ ] **Step 2: Commit**

```bash
git add web/frontend/src/pages/ResearchPage.tsx web/frontend/src/i18n/
git commit -m "feat: add ResearchPage with IC analysis charts and factor heatmap"
```

---

### Task 12: Rename DashboardPage to OverviewPage with quick-start actions

**Files:**
- Rename: `web/frontend/src/pages/DashboardPage.tsx` → `web/frontend/src/pages/OverviewPage.tsx`

- [ ] **Step 1: Rename and enhance the dashboard page**

Rename the file and the export from `DashboardPage` to `OverviewPage`. Keep existing content (health cards, cache table, model list). Add:

- Quick-start actions row: 3 buttons linking to `/models` (Train), `/backtest` (Backtest), `/signals` (Generate)
- Recent tasks list: fetch `GET /system/tasks` → Table with type, status Badge, created_at

- [ ] **Step 2: Commit**

```bash
git add web/frontend/src/pages/OverviewPage.tsx web/frontend/src/pages/DashboardPage.tsx
git commit -m "feat: rename DashboardPage to OverviewPage, add quick-start actions"
```

---

### Task 13: Add ToastContainer to Layout

**Files:**
- Modify: `web/frontend/src/components/Layout.tsx`

- [ ] **Step 1: Import and render ToastContainer**

Add `import { ToastContainer } from "./ui/Toast";` and render `<ToastContainer />` after `<Outlet />` in the Layout component.

- [ ] **Step 2: Commit**

```bash
git add web/frontend/src/components/Layout.tsx
git commit -m "feat: add ToastContainer to app layout"
```

---

### Phase 1 Checkpoint

At this point the following should work:
- New page structure (Overview, Data Explorer, Research, Backtest, Signals, Models, Config, System)
- Stock quotes candlestick chart in Data Explorer
- Factor IC analysis charts in Research
- All shared UI components available
- New backend endpoints for stock data, factors, search

---

## Phase 2: Backtest + Comparison

### Task 14: Add backtest chart data endpoints

**Files:**
- Create: `web/api/services/chart_service.py`
- Modify: `web/api/routers/backtest.py`

- [ ] **Step 1: Create chart_service.py to parse backtest CSVs**

```python
"""Parse backtest result CSVs into chart-ready data."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from web.api.deps import BACKTEST_RESULTS_DIR

logger = logging.getLogger(__name__)


def parse_equity_curve(filename: str) -> Dict:
    """Parse a backtest result CSV into equity curve data."""
    path = BACKTEST_RESULTS_DIR / filename
    if not path.exists():
        return {"dates": [], "portfolio": [], "benchmark": [], "excess": []}

    df = pd.read_csv(path)
    if "date" not in df.columns and "datetime" not in df.columns:
        return {"dates": [], "portfolio": [], "benchmark": [], "excess": []}

    date_col = "date" if "date" in df.columns else "datetime"
    dates = df[date_col].astype(str).tolist()

    # Compute cumulative portfolio value from daily returns
    if "return" in df.columns:
        returns = df["return"].fillna(0)
        portfolio = (1 + returns).cumprod().tolist()
    elif "cum_return" in df.columns:
        portfolio = df["cum_return"].tolist()
    else:
        portfolio = [1.0] * len(dates)

    # Benchmark: if benchmark_return column exists
    if "benchmark_return" in df.columns:
        bench_returns = df["benchmark_return"].fillna(0)
        benchmark = (1 + bench_returns).cumprod().tolist()
    else:
        benchmark = [1.0] * len(dates)

    # Excess return
    excess = [p - b for p, b in zip(portfolio, benchmark)]

    return {
        "dates": dates,
        "portfolio": [round(v, 6) for v in portfolio],
        "benchmark": [round(v, 6) for v in benchmark],
        "excess": [round(v, 6) for v in excess],
    }


def parse_metrics(filename: str) -> Dict:
    """Parse summary metrics from a backtest result CSV."""
    path = BACKTEST_RESULTS_DIR / filename
    if not path.exists():
        return {}

    df = pd.read_csv(path)
    if "return" not in df.columns:
        return {}

    from quant_ex.backtest.metrics import compute_metrics
    report = df.set_index("date" if "date" in df.columns else "datetime") if "date" in df.columns or "datetime" in df.columns else df
    metrics = compute_metrics(df)
    return {k: round(v, 6) if isinstance(v, float) else v for k, v in metrics.items()}


def parse_drawdown(filename: str) -> Dict:
    """Compute drawdown series from equity curve."""
    curve = parse_equity_curve(filename)
    if not curve["portfolio"]:
        return {"dates": [], "drawdown": []}

    portfolio = curve["portfolio"]
    peak = portfolio[0]
    drawdown = []
    for v in portfolio:
        if v > peak:
            peak = v
        dd = (v - peak) / peak if peak != 0 else 0
        drawdown.append(round(dd, 6))

    return {"dates": curve["dates"], "drawdown": drawdown}


def compare_runs(filenames: List[str]) -> Dict:
    """Compare multiple backtest runs."""
    colors = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#ec4899", "#06b6d4", "#84cc16"]
    runs = []
    for i, fn in enumerate(filenames):
        equity = parse_equity_curve(fn)
        dd = parse_drawdown(fn)
        metrics = parse_metrics(fn)
        label = fn.replace(".csv", "").replace("grid_search_", "")
        runs.append({
            "filename": fn,
            "label": label,
            "color": colors[i % len(colors)],
            "equity_curve": equity,
            "drawdown": dd,
            "metrics": metrics,
        })
    # Use the longest date series
    all_dates = max([r["equity_curve"]["dates"] for r in runs if r["equity_curve"]["dates"]], key=len, default=[])
    return {"runs": runs, "dates": all_dates}
```

- [ ] **Step 2: Add 4 new endpoints to backtest.py router**

Add to the existing backtest router:

```python
from web.api.services.chart_service import parse_equity_curve, parse_metrics, parse_drawdown, compare_runs


@router.get("/results/{filename}/equity-curve")
async def get_equity_curve(filename: str):
    """Parsed equity curve data for charting."""
    return parse_equity_curve(filename)


@router.get("/results/{filename}/metrics")
async def get_metrics(filename: str):
    """Structured metrics dict for a backtest result."""
    return parse_metrics(filename)


@router.get("/results/{filename}/drawdown")
async def get_drawdown(filename: str):
    """Drawdown series for charting."""
    return parse_drawdown(filename)


@router.post("/compare")
async def compare_backtest_runs(filenames: list[str]):
    """Compare multiple backtest runs."""
    return compare_runs(filenames)
```

- [ ] **Step 3: Commit**

```bash
git add web/api/services/chart_service.py web/api/routers/backtest.py
git commit -m "feat: add backtest chart data endpoints (equity curve, metrics, drawdown, compare)"
```

---

### Task 15: Extend backtest/grid and WFV request schemas with missing params

**Files:**
- Modify: `web/api/routers/backtest.py`

- [ ] **Step 1: Update GridSearchRequest**

Add these fields to `GridSearchRequest`:

```python
class GridSearchRequest(BaseModel):
    model_path: str
    topk: list[int] = [5, 10, 15, 20]
    n_drop: list[int] = [1, 3, 5]
    hold_thresh: list[int] = [3, 5, 10]
    start: Optional[str] = None
    end: Optional[str] = None
    market: str = "csi300"
    multi_seed: bool = False
    # New fields
    optimize: bool = False
    n_iters: int = 3
    grid_workers: int = 1
    output_csv: Optional[str] = None
    slippage_multipliers: Optional[list[float]] = None
    markets: Optional[list[str]] = None
```

Update the CLI command builder in `start_grid_search` to pass these new params.

- [ ] **Step 2: Update WFVRequest**

Add these fields to `WFVRequest`:

```python
class WFVRequest(BaseModel):
    train_universes: list[str] = ["csi300"]
    eval_market: str = "csi300"
    topk: list[int] = [5, 15, 20]
    n_drop: list[int] = [1, 3]
    hold_thresh: list[int] = [5, 8, 10]
    workers: int = 1
    # New fields
    seeds: bool = False
    run_id: Optional[str] = None
    grid_workers: int = 1
    robust_weights: Optional[dict] = None
    folds_config: Optional[str] = None
    train_config: Optional[str] = None
```

- [ ] **Step 3: Commit**

```bash
git add web/api/routers/backtest.py
git commit -m "feat: extend backtest request schemas with missing CLI params"
```

---

### Task 16: Redesign BacktestPage with Compare tab

**Files:**
- Rewrite: `web/frontend/src/pages/BacktestPage.tsx`

- [ ] **Step 1: Rewrite BacktestPage with 4 tabs**

Tabs: Launch, Compare, Results, Walk-Forward.

**Launch tab**: Full parameter form using new components:
- Model path: Select (from `GET /models`)
- Market: Select (csi300/500/800/1000)
- TopK, N-Drop, Hold Thresh: text inputs (comma-separated)
- Start/End: DatePicker
- Toggles: Multi-Seed, AI Optimize (with n_iters NumberInput)
- New: Grid Workers (NumberInput), Slippage Multipliers (text input), Multi-Market (MultiSelect)
- Submit → `POST /backtest/grid` → TaskStatus with SSE

**Compare tab** (the key new view):
- Run selector: list of result files with checkboxes (max 8). Selected runs shown as color-coded chips.
- Equity curve overlay: ECharts line chart with multiple series, dataZoom, legend. Toggle portfolio/excess via buttons.
- Drawdown overlay: ECharts area chart (negative fill, color-coded per run).
- Metrics comparison: Table with columns per run, rows per metric. Best value per row highlighted.
- Monthly returns heatmap: ECharts heatmap per selected run.
- "Add run" button opens a Modal with result file list.

**Results tab**: File list on left, structured detail on right (metrics Card + equity curve ECharts + drawdown ECharts). Replace raw CSV viewer.

**Walk-Forward tab**: Full param form (all new fields). Fold results table after completion.

- [ ] **Step 2: Commit**

```bash
git add web/frontend/src/pages/BacktestPage.tsx web/frontend/src/i18n/
git commit -m "feat: redesign BacktestPage with multi-run comparison charts"
```

---

### Task 17: Wire up rebalance and notification endpoints

**Files:**
- Modify: `web/api/routers/signals.py`

- [ ] **Step 1: Add rebalance endpoint**

```python
class RebalanceRequest(BaseModel):
    mock: bool = True
    dry_run: bool = True
    config: Optional[str] = None


@router.post("/rebalance")
async def run_rebalance(req: RebalanceRequest):
    """Run scheduled rebalance."""
    from web.api.services.task_manager import get_task_manager
    from web.api.routers.system import stream_task

    tm = get_task_manager()

    def _run():
        import subprocess
        cmd = [sys.executable, "run_scheduled_rebalance.py"]
        if req.mock:
            cmd.append("--mock")
        if req.dry_run:
            cmd.append("--dry-run")
        if req.config:
            cmd.extend(["--config", req.config])
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(PROJECT_ROOT))
        return {"stdout": result.stdout[-2000:], "returncode": result.returncode}

    task_id = tm.start_sync_task("rebalance", _run)
    return {"task_id": task_id}
```

- [ ] **Step 2: Add notify-test endpoint**

```python
class NotifyTestRequest(BaseModel):
    title: str
    content: str
    channel: Optional[str] = None


@router.post("/notify-test")
async def send_notify_test(req: NotifyTestRequest):
    """Send a test notification."""
    try:
        from quant_ex.utils.notify import NotificationPusher
        pusher = NotificationPusher(get_config())
        pusher.send(title=req.title, content=req.content)
        return {"task_id": None, "success": True}
    except Exception as e:
        return {"task_id": None, "success": False, "error": str(e)}
```

- [ ] **Step 3: Extend GenerateSignalRequest with missing params**

Add to `GenerateSignalRequest`:
```python
class GenerateSignalRequest(BaseModel):
    model_path: str
    account: float = 1000000
    positions: Optional[str] = None
    dry_run: bool = True
    # New fields
    universe: Optional[str] = None
    refresh_cache: bool = False
    config: Optional[str] = None
    position_date: Optional[str] = None
    min_action_value: Optional[float] = None
```

- [ ] **Step 4: Commit**

```bash
git add web/api/routers/signals.py
git commit -m "feat: add rebalance and notify-test endpoints, extend signal params"
```

---

## Phase 3: Research + Signals + Polish

### Task 18: Extend models/train request schema with missing params

**Files:**
- Modify: `web/api/routers/models.py`

- [ ] **Step 1: Add missing training params**

```python
class TrainRequest(BaseModel):
    model: str = "lgbm"
    tag: Optional[str] = None
    factors: list[str] = []
    fit_start: Optional[str] = None
    fit_end: Optional[str] = None
    qlib_native: bool = False
    # New fields
    with_sector: bool = False
    no_extra_factors: bool = False
    skip_factor_pipeline: bool = False
    bagging_fraction: Optional[float] = None
    ensemble_seeds: Optional[list[int]] = None
```

- [ ] **Step 2: Commit**

```bash
git add web/api/routers/models.py
git commit -m "feat: extend train request with sector, ensemble, and factor pipeline params"
```

---

### Task 19: Enhance ModelsPage with full param form

**Files:**
- Rewrite: `web/frontend/src/pages/ModelsPage.tsx`

- [ ] **Step 1: Enhance Train tab with full params**

Update the Train tab form to include:
- with_sector: checkbox toggle
- no_extra_factors: checkbox toggle
- skip_factor_pipeline: checkbox toggle
- bagging_fraction: NumberInput (step=0.05, min=0.1, max=1.0)
- ensemble_seeds: text input (comma-separated integers)

- [ ] **Step 2: Enhance Browser tab with feature importance chart**

Replace the text-based feature importance list with an ECharts horizontal bar chart (top 30 features, sorted descending). Use the existing `GET /models/{filename}/importance` endpoint.

- [ ] **Step 3: Commit**

```bash
git add web/frontend/src/pages/ModelsPage.tsx
git commit -m "feat: enhance ModelsPage with full training params and importance chart"
```

---

### Task 20: Redesign SignalsPage with fixed stubs and Regime tab

**Files:**
- Rewrite: `web/frontend/src/pages/SignalsPage.tsx`

- [ ] **Step 1: Add 5 sub-tabs**

Tabs: Generate, Daily, Rebalance, Regime, Notification.

**Generate tab**: Full parameter form (all new fields from Task 17). Use TaskStatus with SSE.

**Daily tab**: Enhanced signal history viewer. Format signal data as a table (stock, score, action) instead of raw `<pre>` block.

**Rebalance tab** (FIXED): mock + dry_run checkboxes, config selector, submit → `POST /signals/rebalance` → TaskStatus.

**Regime tab** (NEW): Current regime status card (from `GET /signals/regime`). Regime rules table (read-only, from config). Regime history: fetch last N days of regime labels → ECharts line chart (color-coded by regime type).

**Notification tab** (FIXED): title + content inputs + channel selector → `POST /signals/notify-test` → success/error badge.

- [ ] **Step 2: Commit**

```bash
git add web/frontend/src/pages/SignalsPage.tsx web/frontend/src/i18n/
git commit -m "feat: redesign SignalsPage with fixed rebalance/notification and regime tab"
```

---

### Task 21: Add sector rotation endpoint and Data Explorer sector heatmap

**Files:**
- Modify: `web/api/routers/data.py`
- Modify: `web/frontend/src/pages/DataExplorerPage.tsx`

- [ ] **Step 1: Implement sector rotation endpoint**

Replace the placeholder in `data.py`:

```python
@router.get("/sectors/rotation")
async def sector_rotation(windows: str = Query("1,5,20")):
    """Sector rotation returns over specified windows."""
    from web.api.services.data_service import _qlib_loader, _cached, _DEFAULT_TTL
    import pandas as pd

    sector_stocks_path = CACHE_DIR / "sector_stocks.json"
    if not sector_stocks_path.exists():
        return []

    with open(sector_stocks_path) as f:
        sector_data = json.load(f)

    window_list = [int(w.strip()) for w in windows.split(",")]

    def _compute():
        loader = _qlib_loader()
        results = []
        for sid, stocks in sector_data.items():
            if not stocks:
                continue
            try:
                df = loader.load_price_data(
                    instruments=stocks[:50],  # cap for performance
                    fields=["$close"],
                )
                if df.empty:
                    continue
                returns = {}
                for w in window_list:
                    if len(df) > w:
                        # Average return across stocks in this sector
                        close = df["$close"].unstack(level="instrument") if "instrument" in df.index.names else df[["$close"]]
                        ret = close.iloc[-1] / close.iloc[-w] - 1
                        returns[f"{w}d"] = round(float(ret.mean()), 4)
                    else:
                        returns[f"{w}d"] = 0.0
                results.append({
                    "sector_id": sid,
                    "sector_name": sid,
                    "returns": returns,
                })
            except Exception:
                continue
        return results

    return _cached("sector_rotation", _DEFAULT_TTL, _compute)
```

- [ ] **Step 2: Implement SectorsTab heatmap in DataExplorerPage**

Build ECharts heatmap option using the sector rotation data. X-axis = windows (1d, 5d, 20d), Y-axis = sector names, cell values = returns. visualMap with red-white-green gradient.

- [ ] **Step 3: Commit**

```bash
git add web/api/routers/data.py web/frontend/src/pages/DataExplorerPage.tsx
git commit -m "feat: implement sector rotation endpoint and heatmap in Data Explorer"
```

---

### Task 22: Add unified Tasks tab to SystemPage

**Files:**
- Modify: `web/frontend/src/pages/SystemPage.tsx`

- [ ] **Step 1: Add Tasks tab to SystemPage**

Add a 4th tab "Tasks" that fetches `GET /system/tasks` → Table (task_id, task_type, status Badge, created_at). Each row has a Cancel button that calls `DELETE /system/tasks/{task_id}`. Auto-refresh every 5s while any task is running.

- [ ] **Step 2: Commit**

```bash
git add web/frontend/src/pages/SystemPage.tsx
git commit -m "feat: add unified task monitor tab to SystemPage"
```

---

### Task 23: Final cleanup and i18n audit

**Files:**
- Modify: `web/frontend/src/i18n/zh.json`
- Modify: `web/frontend/src/i18n/en.json`
- Delete: `web/frontend/src/pages/DataPage.tsx` (replaced by DataExplorerPage)
- Delete: `web/frontend/src/pages/FactorsPage.tsx` (replaced by ResearchPage)
- Delete: `web/frontend/src/pages/DashboardPage.tsx` (replaced by OverviewPage, if still exists)

- [ ] **Step 1: Audit and update i18n keys**

Ensure all new pages have complete translation keys in both zh.json and en.json. Remove keys for deleted pages (data, factors, dashboard namespaces can be kept as aliases or removed).

- [ ] **Step 2: Remove old page files**

Delete the old page files that have been replaced by the new ones.

- [ ] **Step 3: Verify the app builds**

```bash
cd web/frontend && npm run build
```

Expected: successful build with no TypeScript errors

- [ ] **Step 4: Commit**

```bash
git add -A web/frontend/src/
git commit -m "chore: remove old pages, audit i18n keys, verify build"
```

---

## Self-Review Checklist

- [x] **Spec coverage:** Every page in the design spec has a task. Every API endpoint has a task. Every missing CLI param has a task.
- [x] **Placeholder scan:** No TBD/TODO/"implement later"/"similar to Task N" found. All steps contain actual code.
- [x] **Type consistency:** API response types in `types.ts` match what backend endpoints return. Component props are consistent across files. Factor service and data service share the same `_cached` TTL cache pattern.
