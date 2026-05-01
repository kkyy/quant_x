# Frontend Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a React + FastAPI single-page dashboard for quant_ex, covering data management, model training, backtesting, signal generation, factor analysis, and system configuration.

**Spec:** `docs/superpowers/specs/2026-05-01-frontend-dashboard-design.md` (approved)

**Stack:** FastAPI 0.135 + Pydantic v2 backend, React 18 + Vite + TypeScript + Tailwind + shadcn/ui frontend. Local-only deployment (localhost, no auth).

**Environment:**
- Python: `.venv/bin/python` (FastAPI 0.135, uvicorn 0.41, Pydantic 2.12 already installed)
- Node: v25.6.0, npm 11.8.0 (system-wide)
- qlib data: `/Users/weidian/code/algorithms/investment_data/qlib_data/qlib_bin`

---

## Phase 1: Foundation

### Task 1: FastAPI app skeleton

**Files:**
- Create: `web/__init__.py`
- Create: `web/api/__init__.py`
- Create: `web/api/app.py`
- Create: `web/api/deps.py`
- Create: `web/run_web.py`

- [ ] **Step 1: Create FastAPI app factory**

```python
# web/__init__.py
```

```python
# web/api/__init__.py
```

```python
# web/api/deps.py
"""Shared FastAPI dependencies."""
from pathlib import Path
from functools import lru_cache

from quant_ex.utils.config import load_config


@lru_cache(maxsize=1)
def get_config() -> dict:
    return load_config()


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
MODELS_DIR = PROJECT_ROOT / "models"
CACHE_DIR = PROJECT_ROOT / "cache"
SIGNALS_DIR = PROJECT_ROOT / "signals"
BACKTEST_RESULTS_DIR = PROJECT_ROOT / "backtest_results"
LOGS_DIR = PROJECT_ROOT / "logs"
CONFIG_DIR = PROJECT_ROOT / "config"
```

```python
# web/api/app.py
"""FastAPI application factory."""
from __future__ import annotations

import os
import sys
from pathlib import Path
from contextlib import asynccontextmanager

# Ensure quant_ex package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


def create_app() -> FastAPI:
    app = FastAPI(
        title="quant_ex Dashboard",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    from web.api.routers import system, data, models, backtest, signals, factors, config as config_router

    app.include_router(system.router, prefix="/api/system", tags=["system"])
    app.include_router(data.router, prefix="/api/data", tags=["data"])
    app.include_router(models.router, prefix="/api/models", tags=["models"])
    app.include_router(backtest.router, prefix="/api/backtest", tags=["backtest"])
    app.include_router(signals.router, prefix="/api/signals", tags=["signals"])
    app.include_router(factors.router, prefix="/api/factors", tags=["factors"])
    app.include_router(config_router.router, prefix="/api/config", tags=["config"])

    static_dir = Path(__file__).resolve().parent.parent / "frontend" / "dist"
    if static_dir.is_dir():
        app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")

    return app


app = create_app()
```

```python
# web/run_web.py
"""Entry point: uvicorn web.api.app:app"""
import sys
from pathlib import Path

# Ensure quant_ex package is importable (same pattern as run_daily.py)
sys.path.insert(0, str(Path(__file__).parent.parent))

import uvicorn

if __name__ == "__main__":
    uvicorn.run("web.api.app:app", host="0.0.0.0", port=8000, reload=True)
```

- [ ] **Step 2: Create empty router stubs**

```python
# web/api/routers/__init__.py
```

```python
# web/api/routers/system.py
from fastapi import APIRouter

router = APIRouter()


@router.get("/health")
async def health():
    return {"status": "ok"}
```

```python
# web/api/routers/data.py
from fastapi import APIRouter

router = APIRouter()
```

```python
# web/api/routers/models.py
from fastapi import APIRouter

router = APIRouter()
```

```python
# web/api/routers/backtest.py
from fastapi import APIRouter

router = APIRouter()
```

```python
# web/api/routers/signals.py
from fastapi import APIRouter

router = APIRouter()
```

```python
# web/api/routers/factors.py
from fastapi import APIRouter

router = APIRouter()
```

```python
# web/api/routers/config.py
from fastapi import APIRouter

router = APIRouter()
```

- [ ] **Step 3: Verify backend starts**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex
.venv/bin/python -c "from web.api.app import app; print('FastAPI app created:', app.title)"
.venv/bin/python web/run_web.py &  # should start on :8000
sleep 2 && curl -s http://localhost:8000/api/system/health
# Expected: {"status":"ok"}
kill %1
```

- [ ] **Step 4: Commit**

```bash
git add web/__init__.py web/api/ web/run_web.py
git commit -m "feat: add FastAPI app skeleton with CORS, routers, and health endpoint"
```

---

### Task 2: TaskManager with SSE streaming

**Files:**
- Create: `web/api/services/__init__.py`
- Create: `web/api/services/task_manager.py`
- Create: `web/api/services/stream.py`

- [ ] **Step 1: Implement TaskManager**

```python
# web/api/services/__init__.py
from .task_manager import TaskManager, get_task_manager

__all__ = ["TaskManager", "get_task_manager"]
```

```python
# web/api/services/task_manager.py
"""Background task orchestrator with SSE event streaming."""
from __future__ import annotations

import asyncio
import logging
import uuid
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class TaskState:
    task_id: str
    task_type: str
    status: TaskStatus = TaskStatus.PENDING
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    result: Optional[Any] = None
    error: Optional[str] = None


class TaskManager:
    def __init__(self):
        self._tasks: dict[str, TaskState] = {}
        self._queues: dict[str, asyncio.Queue] = {}
        self._bg_tasks: dict[str, asyncio.Task] = {}

    async def start_task(
        self,
        task_type: str,
        coro: Awaitable,
    ) -> str:
        task_id = uuid.uuid4().hex[:12]
        state = TaskState(task_id=task_id, task_type=task_type)
        self._tasks[task_id] = state
        self._queues[task_id] = asyncio.Queue()

        bg = asyncio.create_task(self._run(task_id, coro))
        self._bg_tasks[task_id] = bg
        return task_id

    async def start_sync_task(
        self,
        task_type: str,
        fn: Callable,
        *args,
        **kwargs,
    ) -> str:
        task_id = uuid.uuid4().hex[:12]
        state = TaskState(task_id=task_id, task_type=task_type)
        self._tasks[task_id] = state
        self._queues[task_id] = asyncio.Queue()

        async def _wrapper():
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, fn, *args, **kwargs)

        bg = asyncio.create_task(self._run(task_id, _wrapper()))
        self._bg_tasks[task_id] = bg
        return task_id

    async def _run(self, task_id: str, coro: Awaitable):
        state = self._tasks[task_id]
        state.status = TaskStatus.RUNNING
        try:
            result = await coro
            state.status = TaskStatus.DONE
            state.result = result
            await self._queues[task_id].put({"type": "done", "data": {"result": str(result)[:500]}})
        except asyncio.CancelledError:
            state.status = TaskStatus.CANCELLED
            await self._queues[task_id].put({"type": "done", "data": {"status": "cancelled"}})
        except Exception as exc:
            state.status = TaskStatus.FAILED
            state.error = str(exc)
            logger.exception(f"Task {task_id} failed")
            await self._queues[task_id].put({"type": "error", "data": {"message": str(exc), "traceback": traceback.format_exc()}})
        finally:
            await self._queues[task_id].put(None)  # sentinel

    def emit(self, task_id: str, event_type: str, data: dict):
        if task_id in self._queues:
            self._queues[task_id].put_nowait({"type": event_type, "data": data})

    async def stream_events(self, task_id: str):
        if task_id not in self._queues:
            yield {"type": "error", "data": {"message": f"Task {task_id} not found"}}
            return
        q = self._queues[task_id]
        while True:
            event = await q.get()
            if event is None:
                break
            yield event

    def get_state(self, task_id: str) -> Optional[TaskState]:
        return self._tasks.get(task_id)

    def list_tasks(self) -> list[TaskState]:
        return list(self._tasks.values())

    async def cancel(self, task_id: str) -> bool:
        bg = self._bg_tasks.get(task_id)
        if bg and not bg.done():
            bg.cancel()
            return True
        return False


_manager: Optional[TaskManager] = None


def get_task_manager() -> TaskManager:
    global _manager
    if _manager is None:
        _manager = TaskManager()
    return _manager
```

```python
# web/api/services/stream.py
"""Log capture → SSE event helpers.

Intercepts Python logging records and forwards them as TaskManager events.
"""
from __future__ import annotations

import logging
import asyncio
from typing import Optional


class SSELogHandler(logging.Handler):
    """Captures log records and emits them as SSE events via TaskManager."""

    def __init__(self, task_manager, task_id: str, loop: Optional[asyncio.AbstractEventLoop] = None):
        super().__init__()
        self.task_manager = task_manager
        self.task_id = task_id
        self._loop = loop

    def emit(self, record: logging.LogRecord):
        msg = self.format(record)
        data = {"level": record.levelname.lower(), "message": msg}
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(
                self.task_manager.emit, self.task_id, "log", data
            )
        else:
            self.task_manager.emit(self.task_id, "log", data)
```

- [ ] **Step 2: Add SSE streaming endpoint to system router**

Add to `web/api/routers/system.py`:

```python
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from web.api.services.task_manager import get_task_manager
import json

router = APIRouter()


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/tasks")
async def list_tasks():
    tm = get_task_manager()
    return [
        {
            "task_id": t.task_id,
            "task_type": t.task_type,
            "status": t.status.value,
            "created_at": t.created_at,
            "error": t.error,
        }
        for t in tm.list_tasks()
    ]


@router.get("/tasks/{task_id}/stream")
async def stream_task(task_id: str):
    tm = get_task_manager()
    state = tm.get_state(task_id)
    if not state:
        return StreamingResponse(
            iter([f"data: {json.dumps({'type': 'error', 'data': {'message': 'Task not found'}})}\n\n"]),
            media_type="text/event-stream",
        )

    async def event_generator():
        async for event in tm.stream_events(task_id):
            yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.delete("/tasks/{task_id}")
async def cancel_task(task_id: str):
    tm = get_task_manager()
    cancelled = await tm.cancel(task_id)
    return {"cancelled": cancelled}
```

- [ ] **Step 3: Verify SSE streaming works**

```bash
.venv/bin/python -c "
import asyncio
from web.api.services.task_manager import TaskManager

async def test():
    tm = TaskManager()
    async def my_task():
        tm.emit('test', 'log', {'message': 'hello'})
        return 'done'

    tid = await tm.start_task('test', my_task())
    print('task_id:', tid)
    async for ev in tm.stream_events(tid):
        print('event:', ev)

asyncio.run(test())
"
# Expected: task_id: <hex>, event: {type: log, data: {message: hello}}
```

- [ ] **Step 4: Commit**

```bash
git add web/api/services/ web/api/routers/
git commit -m "feat: add TaskManager with SSE streaming and log capture"
```

---

### Task 3: React app scaffolding

**Files:**
- Create: `web/frontend/` (via `npm create vite`)

- [ ] **Step 1: Scaffold Vite + React + TypeScript project**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex
npm create vite@latest web/frontend -- --template react-ts
cd web/frontend
npm install
```

- [ ] **Step 2: Install UI dependencies**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex/web/frontend
npm install tailwindcss @tailwindcss/vite
npm install react-router-dom
npm install @tanstack/react-table
npm install recharts
npm install react-hook-form @hookform/resolvers zod
npm install lucide-react clsx tailwind-merge
```

- [ ] **Step 3: Configure Tailwind**

Replace `web/frontend/vite.config.ts`:

```typescript
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import path from "path";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
});
```

Replace `web/frontend/src/index.css`:

```css
@import "tailwindcss";

:root {
  --background: #ffffff;
  --foreground: #0a0a0a;
}

@media (prefers-color-scheme: dark) {
  :root {
    --background: #0a0a0a;
    --foreground: #ededed;
  }
}

body {
  color: var(--foreground);
  background: var(--background);
  font-family: system-ui, -apple-system, sans-serif;
}
```

- [ ] **Step 4: Set up shadcn/ui manually (no CLI)**

Create `web/frontend/src/lib/utils.ts`:

```typescript
import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}
```

We'll add individual shadcn/ui components as needed (copy-paste pattern from https://ui.shadcn.com/docs). No shadcn CLI needed.

- [ ] **Step 5: Create directory structure**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex/web/frontend
mkdir -p src/{api,components/ui,pages,hooks,lib,types}
```

- [ ] **Step 6: Create API client**

```typescript
// web/frontend/src/api/client.ts
const BASE = "/api";

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...options?.headers },
    ...options,
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status}: ${body}`);
  }
  return res.json();
}

export function get<T>(path: string): Promise<T> {
  return request<T>(path);
}

export function post<T>(path: string, body?: unknown): Promise<T> {
  return request<T>(path, { method: "POST", body: JSON.stringify(body) });
}

export function put<T>(path: string, body?: unknown): Promise<T> {
  return request<T>(path, { method: "PUT", body: JSON.stringify(body) });
}

export function del<T>(path: string): Promise<T> {
  return request<T>(path, { method: "DELETE" });
}
```

- [ ] **Step 7: Create SSE hook**

```typescript
// web/frontend/src/hooks/useSSE.ts
import { useState, useEffect, useRef, useCallback } from "react";

export interface SSEEvent {
  type: string;
  data: Record<string, unknown>;
}

export function useSSE(taskId: string | null) {
  const [events, setEvents] = useState<SSEEvent[]>([]);
  const [status, setStatus] = useState<"idle" | "streaming" | "done" | "error">("idle");
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const stop = useCallback(() => {
    abortRef.current?.abort();
    setStatus("done");
  }, []);

  useEffect(() => {
    if (!taskId) return;

    setEvents([]);
    setStatus("streaming");
    setError(null);

    const controller = new AbortController();
    abortRef.current = controller;

    fetch(`/api/system/tasks/${taskId}/stream`, { signal: controller.signal })
      .then((res) => {
        const reader = res.body?.getReader();
        if (!reader) throw new Error("No readable stream");
        const decoder = new TextDecoder();
        let buffer = "";

        function read(): Promise<void> {
          return reader.read().then(({ done, value }) => {
            if (done) {
              setStatus("done");
              return;
            }
            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split("\n");
            buffer = lines.pop() || "";

            for (const line of lines) {
              if (line.startsWith("data: ")) {
                try {
                  const event: SSEEvent = JSON.parse(line.slice(6));
                  setEvents((prev) => [...prev, event]);
                  if (event.type === "error") {
                    setError(event.data.message as string);
                  }
                  if (event.type === "done") {
                    setStatus("done");
                  }
                } catch {}
              }
            }
            return read();
          });
        }
        return read();
      })
      .catch((err) => {
        if (err.name !== "AbortError") {
          setError(err.message);
          setStatus("error");
        }
      });

    return () => controller.abort();
  }, [taskId]);

  return { events, status, error, stop };
}
```

- [ ] **Step 8: Verify frontend starts**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex/web/frontend
npm run dev  # should start on :5173
```

Open http://localhost:5173 — should see the default Vite + React page.

- [ ] **Step 9: Commit**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex
git add web/frontend/
git commit -m "feat: scaffold React frontend with Vite, Tailwind, and API client"
```

---

### Task 4: Sidebar layout and routing

**Files:**
- Modify: `web/frontend/src/App.tsx`
- Create: `web/frontend/src/components/Sidebar.tsx`
- Create: `web/frontend/src/components/Layout.tsx`
- Create: `web/frontend/src/pages/DashboardPage.tsx`
- Create: `web/frontend/src/pages/DataPage.tsx`
- Create: `web/frontend/src/pages/ModelsPage.tsx`
- Create: `web/frontend/src/pages/BacktestPage.tsx`
- Create: `web/frontend/src/pages/SignalsPage.tsx`
- Create: `web/frontend/src/pages/FactorsPage.tsx`
- Create: `web/frontend/src/pages/ConfigPage.tsx`
- Create: `web/frontend/src/pages/SystemPage.tsx`

- [ ] **Step 1: Create Sidebar component**

```tsx
// web/frontend/src/components/Sidebar.tsx
import { NavLink } from "react-router-dom";

const NAV_ITEMS = [
  { to: "/", label: "Dashboard", icon: "◉" },
  { to: "/data", label: "Data", icon: "◈" },
  { to: "/models", label: "Models", icon: "◆" },
  { to: "/backtest", label: "Backtest", icon: "◇" },
  { to: "/signals", label: "Signals", icon: "▸" },
  { to: "/factors", label: "Factors", icon: "⋄" },
  { to: "/config", label: "Config", icon: "⚙" },
  { to: "/system", label: "System", icon: "⊙" },
];

export function Sidebar() {
  return (
    <aside className="w-56 border-r border-gray-200 bg-gray-50 h-screen sticky top-0 flex flex-col">
      <div className="p-4 border-b border-gray-200">
        <h1 className="text-lg font-bold">quant_ex</h1>
        <p className="text-xs text-gray-500">Dashboard</p>
      </div>
      <nav className="flex-1 p-2">
        {NAV_ITEMS.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.to === "/"}
            className={({ isActive }) =>
              `flex items-center gap-2 px-3 py-2 rounded-md text-sm transition-colors ${
                isActive
                  ? "bg-gray-900 text-white"
                  : "text-gray-700 hover:bg-gray-200"
              }`
            }
          >
            <span>{item.icon}</span>
            <span>{item.label}</span>
          </NavLink>
        ))}
      </nav>
    </aside>
  );
}
```

- [ ] **Step 2: Create Layout component**

```tsx
// web/frontend/src/components/Layout.tsx
import { Outlet } from "react-router-dom";
import { Sidebar } from "./Sidebar";

export function Layout() {
  return (
    <div className="flex min-h-screen">
      <Sidebar />
      <main className="flex-1 p-6 overflow-auto">
        <Outlet />
      </main>
    </div>
  );
}
```

- [ ] **Step 3: Create page placeholders**

Each page is a minimal placeholder that will be filled in later tasks:

```tsx
// web/frontend/src/pages/DashboardPage.tsx
export function DashboardPage() {
  return <div><h2 className="text-2xl font-bold mb-4">Dashboard</h2><p className="text-gray-500">System overview — coming in Phase 1 Task 5.</p></div>;
}
```

```tsx
// web/frontend/src/pages/DataPage.tsx
export function DataPage() {
  return <div><h2 className="text-2xl font-bold mb-4">Data Management</h2><p className="text-gray-500">Coming in Phase 2.</p></div>;
}
```

```tsx
// web/frontend/src/pages/ModelsPage.tsx
export function ModelsPage() {
  return <div><h2 className="text-2xl font-bold mb-4">Models</h2><p className="text-gray-500">Coming in Phase 2.</p></div>;
}
```

```tsx
// web/frontend/src/pages/BacktestPage.tsx
export function BacktestPage() {
  return <div><h2 className="text-2xl font-bold mb-4">Backtest</h2><p className="text-gray-500">Coming in Phase 3.</p></div>;
}
```

```tsx
// web/frontend/src/pages/SignalsPage.tsx
export function SignalsPage() {
  return <div><h2 className="text-2xl font-bold mb-4">Signals</h2><p className="text-gray-500">Coming in Phase 3.</p></div>;
}
```

```tsx
// web/frontend/src/pages/FactorsPage.tsx
export function FactorsPage() {
  return <div><h2 className="text-2xl font-bold mb-4">Factors</h2><p className="text-gray-500">Coming in Phase 4.</p></div>;
}
```

```tsx
// web/frontend/src/pages/ConfigPage.tsx
export function ConfigPage() {
  return <div><h2 className="text-2xl font-bold mb-4">Config</h2><p className="text-gray-500">Coming in Phase 4.</p></div>;
}
```

```tsx
// web/frontend/src/pages/SystemPage.tsx
export function SystemPage() {
  return <div><h2 className="text-2xl font-bold mb-4">System</h2><p className="text-gray-500">Coming in Phase 5.</p></div>;
}
```

- [ ] **Step 4: Wire up App.tsx with router**

Replace `web/frontend/src/App.tsx`:

```tsx
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { Layout } from "./components/Layout";
import { DashboardPage } from "./pages/DashboardPage";
import { DataPage } from "./pages/DataPage";
import { ModelsPage } from "./pages/ModelsPage";
import { BacktestPage } from "./pages/BacktestPage";
import { SignalsPage } from "./pages/SignalsPage";
import { FactorsPage } from "./pages/FactorsPage";
import { ConfigPage } from "./pages/ConfigPage";
import { SystemPage } from "./pages/SystemPage";

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<DashboardPage />} />
          <Route path="data" element={<DataPage />} />
          <Route path="models" element={<ModelsPage />} />
          <Route path="backtest" element={<BacktestPage />} />
          <Route path="signals" element={<SignalsPage />} />
          <Route path="factors" element={<FactorsPage />} />
          <Route path="config" element={<ConfigPage />} />
          <Route path="system" element={<SystemPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
```

- [ ] **Step 5: Verify navigation works**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex/web/frontend
npm run dev
```

Click through all 8 sidebar items — each should show its placeholder page.

- [ ] **Step 6: Commit**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex
git add web/frontend/src/
git commit -m "feat: add sidebar layout with 8-page routing skeleton"
```

---

### Task 5: Dashboard overview page

**Files:**
- Expand: `web/api/routers/system.py` (add runtime, cache-status endpoints)
- Expand: `web/api/routers/signals.py` (add regime endpoint)
- Expand: `web/api/routers/models.py` (add model list)
- Replace: `web/frontend/src/pages/DashboardPage.tsx`

- [ ] **Step 1: Add system runtime endpoint**

Add to `web/api/routers/system.py`:

```python
import sys
import os

from web.api.deps import CACHE_DIR, MODELS_DIR, LOGS_DIR

@router.get("/runtime")
async def runtime_info():
    config = get_config()
    cache_types = {}
    if CACHE_DIR.exists():
        for d in sorted(CACHE_DIR.iterdir()):
            if d.is_dir():
                files = list(d.glob("*.csv"))
                total_size = sum(f.stat().st_size for f in files) if files else 0
                latest = max((f.stat().st_mtime for f in files), default=0)
                from datetime import datetime
                cache_types[d.name] = {
                    "file_count": len(files),
                    "total_size_mb": round(total_size / 1024 / 1024, 2),
                    "latest": datetime.fromtimestamp(latest).isoformat() if latest else None,
                }

    return {
        "python_version": sys.version,
        "qlib_data_path": config.get("qlib", {}).get("provider_uri", ""),
        "models_count": len(list(MODELS_DIR.glob("*.pkl"))) if MODELS_DIR.exists() else 0,
        "cache_types": cache_types,
        "disk_usage": {},  # placeholder
    }
```

- [ ] **Step 2: Add minimal models list endpoint**

Add to `web/api/routers/models.py`:

```python
import json
from datetime import datetime
from fastapi import APIRouter
from web.api.deps import MODELS_DIR

router = APIRouter()


@router.get("")
async def list_models():
    if not MODELS_DIR.exists():
        return []
    models = []
    for pkl in sorted(MODELS_DIR.glob("*.pkl")):
        meta_path = MODELS_DIR / f"{pkl.stem}_meta.json"
        meta = {}
        if meta_path.exists():
            with open(meta_path) as f:
                meta = json.load(f)
        models.append({
            "filename": pkl.name,
            "size_mb": round(pkl.stat().st_size / 1024 / 1024, 2),
            "modified": datetime.fromtimestamp(pkl.stat().st_mtime).isoformat(),
            "meta": meta,
        })
    return models


@router.get("/registry")
async def model_registry():
    from quant_ex.models.base import ModelRegistry
    from quant_ex.features.base import FactorRegistry

    # Trigger auto-imports
    try:
        from quant_ex.models import trainer
    except Exception:
        pass

    return {
        "models": [{"name": n} for n in ModelRegistry.list()],
        "factors": [{"name": n} for n in FactorRegistry.list()],
    }
```

- [ ] **Step 3: Add regime detection endpoint**

Add to `web/api/routers/signals.py`:

```python
from fastapi import APIRouter
from fastapi import Depends
from web.api.deps import get_config

router = APIRouter()


@router.get("/regime")
async def get_regime():
    config = get_config()
    try:
        from quant_ex.strategy.regime_switch import RegimeStrategySwitch
        rss = RegimeStrategySwitch.from_config(config)
        if rss is None:
            return {"enabled": False, "regime": None, "label": None}

        # Regime detection requires price data — return last cached result
        # For now, return config info
        return {"enabled": True, "regime": None, "label": "requires_price_data"}
    except Exception as exc:
        return {"enabled": False, "error": str(exc)}
```

- [ ] **Step 4: Build Dashboard frontend page**

Replace `web/frontend/src/pages/DashboardPage.tsx`:

```tsx
import { useEffect, useState } from "react";
import { get } from "../api/client";

interface RuntimeInfo {
  python_version: string;
  qlib_data_path: string;
  models_count: number;
  cache_types: Record<string, { file_count: number; total_size_mb: number; latest: string | null }>;
}

interface ModelInfo {
  filename: string;
  size_mb: number;
  modified: string;
  meta: Record<string, unknown>;
}

export function DashboardPage() {
  const [runtime, setRuntime] = useState<RuntimeInfo | null>(null);
  const [models, setModels] = useState<ModelInfo[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([get<RuntimeInfo>("/system/runtime"), get<ModelInfo[]>("/models")])
      .then(([rt, ms]) => {
        setRuntime(rt);
        setModels(ms);
      })
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <p>Loading...</p>;

  const lastModel = models.length > 0 ? models[models.length - 1] : null;

  return (
    <div className="space-y-6">
      <h2 className="text-2xl font-bold">Dashboard</h2>

      <div className="grid grid-cols-3 gap-4">
        <div className="border rounded-lg p-4">
          <h3 className="text-sm text-gray-500 mb-1">Python</h3>
          <p className="text-sm font-mono">{runtime?.python_version?.split(" ")[0]}</p>
        </div>
        <div className="border rounded-lg p-4">
          <h3 className="text-sm text-gray-500 mb-1">Models</h3>
          <p className="text-2xl font-bold">{runtime?.models_count ?? 0}</p>
          {lastModel && (
            <p className="text-xs text-gray-500 mt-1">
              Latest: {lastModel.filename} ({new Date(lastModel.modified).toLocaleDateString()})
            </p>
          )}
        </div>
        <div className="border rounded-lg p-4">
          <h3 className="text-sm text-gray-500 mb-1">qlib Data</h3>
          <p className="text-xs font-mono break-all">{runtime?.qlib_data_path}</p>
        </div>
      </div>

      <div>
        <h3 className="text-lg font-semibold mb-3">Cache Status</h3>
        <div className="border rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50">
              <tr>
                <th className="text-left px-4 py-2">Type</th>
                <th className="text-right px-4 py-2">Files</th>
                <th className="text-right px-4 py-2">Size (MB)</th>
                <th className="text-left px-4 py-2">Latest</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(runtime?.cache_types ?? {}).map(([type, info]) => (
                <tr key={type} className="border-t">
                  <td className="px-4 py-2">{type}</td>
                  <td className="text-right px-4 py-2">{info.file_count}</td>
                  <td className="text-right px-4 py-2">{info.total_size_mb}</td>
                  <td className="px-4 py-2">{info.latest ? new Date(info.latest).toLocaleDateString() : "-"}</td>
                </tr>
              ))}
              {Object.keys(runtime?.cache_types ?? {}).length === 0 && (
                <tr><td colSpan={4} className="px-4 py-4 text-center text-gray-400">No cache data</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
```

- [ ] **Step 5: Verify dashboard shows live data**

Start both servers:
```bash
# Terminal 1: backend
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex
.venv/bin/python web/run_web.py

# Terminal 2: frontend
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex/web/frontend
npm run dev
```

Open http://localhost:5173 — Dashboard should show model count, cache table.

- [ ] **Step 6: Commit**

```bash
git add web/
git commit -m "feat: add Dashboard overview page with runtime info, cache status, and model summary"
```

---

## Phase 2: Data & Models

### Task 6: Data cache status API + Data page

**Files:**
- Expand: `web/api/routers/data.py`
- Replace: `web/frontend/src/pages/DataPage.tsx`

- [ ] **Step 1: Implement data router**

Replace `web/api/routers/data.py`:

```python
import os
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel
from web.api.deps import CACHE_DIR, get_config
from web.api.services.task_manager import get_task_manager

router = APIRouter()


class FetchRequest(BaseModel):
    type: str
    scope: str = "all"  # all | universe | custom
    symbols: Optional[list[str]] = None
    universe: Optional[str] = None
    ttl: Optional[int] = None
    force: bool = False


class CacheStatus(BaseModel):
    type: str
    file_count: int
    total_size_mb: float
    latest: Optional[str]
    ttl_days: int


def _get_fetcher_registry():
    """Import _FETCHER_REGISTRY from run_fetch_data.py."""
    from quant_ex.run_fetch_data import _FETCHER_REGISTRY
    return _FETCHER_REGISTRY


@router.get("/cache-status")
async def cache_status():
    registry = _get_fetcher_registry()
    results = []
    for name, (cls_name, cache_dir, ttl) in registry.items():
        d = Path(cache_dir)
        if not d.exists():
            results.append(CacheStatus(type=name, file_count=0, total_size_mb=0.0, latest=None, ttl_days=ttl))
            continue
        files = list(d.glob("*.csv"))
        total_size = sum(f.stat().st_size for f in files)
        latest = max((f.stat().st_mtime for f in files), default=0)
        results.append(CacheStatus(
            type=name,
            file_count=len(files),
            total_size_mb=round(total_size / 1024 / 1024, 2),
            latest=datetime.fromtimestamp(latest).isoformat() if latest else None,
            ttl_days=ttl,
        ))
    return results


@router.post("/fetch")
async def start_fetch(req: FetchRequest):
    tm = get_task_manager()

    def _fetch():
        from quant_ex.run_fetch_data import _get_fetcher_cls, fetch_generic, _FETCHER_REGISTRY
        registry = _FETCHER_REGISTRY
        if req.type == "all":
            types_to_fetch = list(registry.keys())
        else:
            types_to_fetch = [req.type]

        results = {}
        for t in types_to_fetch:
            cls_name, cache_dir, ttl = registry[t]
            ttl = req.ttl or ttl
            fetch_generic(t, symbols=[], cache_dir=cache_dir, ttl_days=ttl)
            results[t] = "done"
        return results

    task_id = await tm.start_sync_task("data_fetch", _fetch)
    return {"task_id": task_id}


@router.get("/fetch/{task_id}/stream")
async def stream_fetch(task_id: str):
    from web.api.routers.system import stream_task
    return await stream_task(task_id)


@router.delete("/cache/{data_type}/expired")
async def delete_expired(data_type: str):
    registry = _get_fetcher_registry()
    if data_type not in registry:
        return {"error": f"Unknown type: {data_type}"}
    _, cache_dir, ttl = registry[data_type]
    d = Path(cache_dir)
    if not d.exists():
        return {"deleted": 0}
    from datetime import date as date_mod
    deleted = 0
    for f in d.glob("*.csv"):
        mtime = date_mod.fromtimestamp(f.stat().st_mtime)
        if (date_mod.today() - mtime).days >= ttl:
            f.unlink()
            deleted += 1
    return {"deleted": deleted}


@router.get("/stock-lookup/{symbol}")
async def stock_lookup(symbol: str):
    from quant_ex.data.utils import load_stock_names
    names = load_stock_names()
    matched = {k: v for k, v in names.items() if symbol.upper() in k or symbol.lower() in v.lower()}
    if not matched:
        return {"symbol": symbol, "name": None, "cache_files": []}

    result = {}
    for sym, name in matched.items():
        cache_files = []
        registry = _get_fetcher_registry()
        for dtype, (_, cache_dir, _) in registry.items():
            d = Path(cache_dir)
            if d.exists():
                files = list(d.glob(f"*{sym}*")) + list(d.glob(f"*{sym[2:]}*"))
                for f in files:
                    cache_files.append({
                        "type": dtype,
                        "file": f.name,
                        "modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
                    })
        result[sym] = {"name": name, "cache_files": cache_files}
    return result
```

- [ ] **Step 2: Build Data page frontend**

Replace `web/frontend/src/pages/DataPage.tsx` with a tabbed layout (Fetch / Cache Status / Stock Lookup). Use a simple tab state. Each tab shows the relevant form or table from the spec section 3.2.

- [ ] **Step 3: Verify data page works**

```bash
curl -s http://localhost:8000/api/data/cache-status | python -m json.tool
```

- [ ] **Step 4: Commit**

```bash
git add web/
git commit -m "feat: add Data Management page with cache status, fetch, and stock lookup"
```

---

### Task 7: Model registry API + Model Browser page

**Files:**
- Expand: `web/api/routers/models.py`
- Replace: `web/frontend/src/pages/ModelsPage.tsx`

- [ ] **Step 1: Add training endpoint to models router**

```python
@router.get("/{filename}/meta")
async def get_meta(filename: str):
    meta_path = MODELS_DIR / f"{Path(filename).stem}_meta.json"
    if not meta_path.exists():
        return {}
    with open(meta_path) as f:
        return json.load(f)


@router.get("/{filename}/importance")
async def get_importance(filename: str):
    imp_path = MODELS_DIR / f"{Path(filename).stem}_feature_importance.json"
    if not imp_path.exists():
        return {}
    with open(imp_path) as f:
        return json.load(f)


class TrainRequest(BaseModel):
    model: str = "lgbm"
    tag: Optional[str] = None
    factors: list[str] = []
    fit_start: Optional[str] = None
    fit_end: Optional[str] = None
    valid_start: Optional[str] = None
    valid_end: Optional[str] = None
    test_start: Optional[str] = None
    qlib_native: bool = False
    ensemble: bool = False


@router.post("/train")
async def start_training(req: TrainRequest):
    tm = get_task_manager()
    config = get_config()

    def _train():
        import logging
        from quant_ex.utils.config import load_config
        from quant_ex.data.loader import DataLoader
        from quant_ex.models.trainer import ModelTrainer
        from quant_ex.models.base import ModelRegistry
        from quant_ex.features.base import FactorRegistry, FactorPipeline

        cfg = load_config()
        loader = DataLoader(cfg)
        trainer = ModelTrainer(cfg, loader)

        factor_pipeline = None
        if req.factors:
            factor_configs = [{"name": f} for f in req.factors]
            factor_pipeline = FactorPipeline.from_config(factor_configs)

        dates = {}
        if req.fit_start: dates["fit_start"] = req.fit_start
        if req.fit_end: dates["fit_end"] = req.fit_end

        model, dataset, recorder_id = trainer.train(
            model_name=req.model,
            tag=req.tag,
            factor_pipeline=factor_pipeline,
            qlib_native=req.qlib_native,
            **dates,
        )
        return {"model_path": str(getattr(model, '_save_path', 'unknown')), "recorder_id": recorder_id}

    task_id = await tm.start_sync_task("model_train", _train)
    return {"task_id": task_id}
```

- [ ] **Step 2: Build Models page with tabs (Train / Model Browser / Registry)**

The Train tab has form fields per spec section 3.3. Model Browser lists `*.pkl` files with expandable meta/importance. Registry tab shows registered models and factors from `GET /api/models/registry`.

- [ ] **Step 3: Commit**

```bash
git add web/
git commit -m "feat: add Models page with training form, browser, and registry tabs"
```

---

## Phase 3: Backtest & Signals

### Task 8: Grid search API + Backtest page

**Files:**
- Expand: `web/api/routers/backtest.py`
- Replace: `web/frontend/src/pages/BacktestPage.tsx`

- [ ] **Step 1: Implement backtest router**

```python
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from web.api.deps import get_config, BACKTEST_RESULTS_DIR
from web.api.services.task_manager import get_task_manager

router = APIRouter()


class GridSearchRequest(BaseModel):
    model_path: str
    topk: list[int] = [5, 10, 15, 20]
    n_drop: list[int] = [1, 3, 5]
    hold_thresh: list[int] = [3, 5, 10]
    start: Optional[str] = None
    end: Optional[str] = None
    market: str = "csi300"
    multi_seed: bool = False
    grid_workers: int = -1


@router.post("/grid")
async def start_grid_search(req: GridSearchRequest):
    tm = get_task_manager()

    def _grid():
        from quant_ex.run_backtest import main as backtest_main
        import sys

        argv = [
            "--model-path", req.model_path,
            "--topk", ",".join(str(x) for x in req.topk),
            "--n-drop", ",".join(str(x) for x in req.n_drop),
            "--hold-thresh", ",".join(str(x) for x in req.hold_thresh),
            "--market", req.market,
        ]
        if req.start:
            argv.extend(["--start", req.start])
        if req.end:
            argv.extend(["--end", req.end])
        if req.multi_seed:
            argv.append("--seeds")
        if req.grid_workers != -1:
            argv.extend(["--grid-workers", str(req.grid_workers)])

        backtest_main(argv=sys.argv[:1] + argv)
        return {"status": "completed"}

    task_id = await tm.start_sync_task("grid_search", _grid)
    return {"task_id": task_id}


@router.get("/results")
async def list_results():
    if not BACKTEST_RESULTS_DIR.exists():
        return []
    import json
    from datetime import datetime
    results = []
    for f in sorted(BACKTEST_RESULTS_DIR.glob("*.csv"), reverse=True):
        results.append({
            "filename": f.name,
            "size_kb": round(f.stat().st_size / 1024, 1),
            "modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
        })
    return results


@router.get("/results/{filename}")
async def get_result(filename: str):
    import pandas as pd
    path = BACKTEST_RESULTS_DIR / filename
    if not path.exists():
        return {"error": "Not found"}
    df = pd.read_csv(path)
    return {"columns": list(df.columns), "rows": df.to_dict(orient="records")[:200]}


@router.get("/charts/{filename}")
async def get_chart(filename: str):
    from fastapi.responses import FileResponse
    path = BACKTEST_RESULTS_DIR / filename
    if not path.exists():
        return {"error": "Not found"}
    return FileResponse(str(path), media_type="image/png")
```

- [ ] **Step 2: Build Backtest page with tabs (Grid Search / Walk-Forward / Slippage / AI Optimizer / Charts)**

Grid Search tab has model selector, param grid inputs, and results table per spec section 3.4.

- [ ] **Step 3: Commit**

```bash
git add web/
git commit -m "feat: add Backtest page with grid search, results table, and chart viewer"
```

---

### Task 9: Signal generation API + Signals page

**Files:**
- Expand: `web/api/routers/signals.py`
- Replace: `web/frontend/src/pages/SignalsPage.tsx`

- [ ] **Step 1: Implement signals router**

```python
from pydantic import BaseModel
from web.api.deps import SIGNALS_DIR

router = APIRouter()


class GenerateSignalRequest(BaseModel):
    model_path: str
    account: float = 1000000
    positions: Optional[str] = None  # "SH600000:500,SZ000001:300"
    dry_run: bool = True


class RebalanceRequest(BaseModel):
    model_path: str
    market: str = "csi300"
    topk: int = 10
    n_drop: int = 3
    hold_thresh: int = 5
    account: float = 500000
    start_date: Optional[str] = None
    mock: bool = False
    dry_run: bool = True


class NotifyTestRequest(BaseModel):
    title: str = "Test Notification"
    content: str = "This is a test from quant_ex dashboard."


@router.post("/generate")
async def generate_signal(req: GenerateSignalRequest):
    tm = get_task_manager()

    def _generate():
        from quant_ex.run_daily import main as daily_main
        positions = {}
        if req.positions:
            for pair in req.positions.split(","):
                sym, qty = pair.strip().split(":")
                positions[sym] = float(qty)

        daily_main(
            model_path=req.model_path,
            account=req.account,
            current_positions=positions if positions else None,
            dry_run=req.dry_run,
        )
        return {"status": "completed"}

    task_id = await tm.start_sync_task("signal_generate", _generate)
    return {"task_id": task_id}


@router.get("/history")
async def signal_history():
    if not SIGNALS_DIR.exists():
        return []
    from datetime import datetime
    results = []
    for f in sorted(SIGNALS_DIR.glob("signal_*.txt"), reverse=True):
        results.append({
            "filename": f.name,
            "size_kb": round(f.stat().st_size / 1024, 1),
            "modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
        })
    return results


@router.get("/history/{filename}")
async def get_signal(filename: str):
    path = SIGNALS_DIR / filename
    if not path.exists():
        return {"error": "Not found"}
    return {"content": path.read_text(encoding="utf-8")}


@router.post("/rebalance")
async def run_rebalance(req: RebalanceRequest):
    tm = get_task_manager()

    def _rebalance():
        from quant_ex.run_scheduled_rebalance import main
        # Run via CLI args
        import subprocess, sys
        cmd = [sys.executable, "run_scheduled_rebalance.py"]
        if req.mock:
            cmd.append("--mock")
        if req.dry_run:
            cmd.append("--dry-run")
        subprocess.run(cmd, check=False)
        return {"status": "completed"}

    task_id = await tm.start_sync_task("rebalance", _rebalance)
    return {"task_id": task_id}


@router.post("/notify/test")
async def test_notification(req: NotifyTestRequest):
    from quant_ex.notify.pusher import NotificationPusher
    config = get_config()
    pusher = NotificationPusher(config)
    results = pusher.send(req.title, req.content)
    return results
```

- [ ] **Step 2: Build Signals page with tabs (Generate / History / Rebalance / Notification)**

- [ ] **Step 3: Commit**

```bash
git add web/
git commit -m "feat: add Signals page with generation, history, rebalance, and notification tabs"
```

---

## Phase 4: Factors & Config

### Task 10: Factor library API + Factors page

**Files:**
- Expand: `web/api/routers/factors.py`
- Replace: `web/frontend/src/pages/FactorsPage.tsx`

- [ ] **Step 1: Implement factors router**

```python
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from web.api.services.task_manager import get_task_manager

router = APIRouter()


class EvaluateRequest(BaseModel):
    name: str
    start: Optional[str] = None
    end: Optional[str] = None


class MineRequest(BaseModel):
    min_ic: float = 0.03
    min_icir: float = 0.4
    top_n: int = 30


class ScreenRequest(BaseModel):
    min_ic: float = 0.02
    min_icir: float = 0.3
    max_corr: float = 0.7


@router.get("")
async def list_factors():
    from quant_ex.features.base import FactorRegistry
    try:
        from quant_ex.models import trainer
    except Exception:
        pass
    factors = []
    for name in FactorRegistry.list():
        cls = FactorRegistry.get(name)
        factors.append({
            "name": name,
            "class": cls.__name__,
            "description": cls.__doc__ or "",
        })
    return factors


@router.get("/library")
async def factor_library():
    from quant_ex.features.base import FactorRegistry
    from web.api.deps import get_config
    try:
        from quant_ex.models import trainer
    except Exception:
        pass

    config = get_config()
    enabled = set()
    for fc in config.get("model", {}).get("features", {}).get("factors", []):
        enabled.add(fc.get("name"))

    result = []
    for name in FactorRegistry.list():
        cls = FactorRegistry.get(name)
        result.append({
            "name": name,
            "class": cls.__name__,
            "enabled": name in enabled,
        })
    return result


@router.post("/evaluate")
async def evaluate_factor(req: EvaluateRequest):
    tm = get_task_manager()

    def _eval():
        from quant_ex.utils.config import load_config
        from quant_ex.data.loader import DataLoader
        from quant_ex.features.base import FactorRegistry
        from quant_ex.backtest.signal_diagnostics import compute_signal_ic

        cfg = load_config()
        loader = DataLoader(cfg)
        price_data = loader.load_features(["$close"])

        factor_cls = FactorRegistry.get(req.name)
        factor = factor_cls()
        factor_data = factor.compute(price_data)
        if factor_data is None:
            return {"error": "Factor returned no data"}

        # Compute IC on first available column
        col = factor_data.columns[0]
        pred = factor_data[col].droplevel(0) if factor_data.index.nlevels > 1 else factor_data[col]
        ic = compute_signal_ic(pred, price_data)
        return {"factor": req.name, "column": col, "metrics": ic}

    task_id = await tm.start_sync_task("factor_eval", _eval)
    return {"task_id": task_id}


@router.post("/mine")
async def mine_factors(req: MineRequest):
    tm = get_task_manager()

    def _mine():
        from quant_ex.run_factor_mining import main as mine_main
        import sys
        mine_main(sys.argv[:1] + [
            "--min-ic", str(req.min_ic),
            "--min-icir", str(req.min_icir),
            "--top-n", str(req.top_n),
        ])
        return {"status": "completed"}

    task_id = await tm.start_sync_task("factor_mine", _mine)
    return {"task_id": task_id}


@router.post("/screen")
async def screen_factors(req: ScreenRequest):
    # Factor screening requires forward returns and computed factor data
    return {"message": "Screening requires full pipeline context — use run_factor_mining.py CLI for now"}
```

- [ ] **Step 2: Build Factors page with tabs (Library / Evaluation / Mining / Screening)**

- [ ] **Step 3: Commit**

```bash
git add web/
git commit -m "feat: add Factors page with library, evaluation, mining, and screening tabs"
```

---

### Task 11: Config editor API + Config page

**Files:**
- Expand: `web/api/routers/config.py`
- Replace: `web/frontend/src/pages/ConfigPage.tsx`

- [ ] **Step 1: Implement config router**

```python
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from web.api.deps import CONFIG_DIR

router = APIRouter()

_VALID_CONFIGS = {"base", "model", "notify", "strategy_candidates"}


@router.get("/{name}")
async def read_config(name: str):
    if name not in _VALID_CONFIGS:
        raise HTTPException(404, f"Unknown config: {name}")
    path = CONFIG_DIR / f"{name}.yaml"
    if name == "strategy_candidates":
        path = CONFIG_DIR / "strategy_candidates.yaml"
    if not path.exists():
        return {"content": "", "exists": False}
    return {"content": path.read_text(encoding="utf-8"), "exists": True}


class ConfigUpdate(BaseModel):
    content: str


@router.put("/{name}")
async def write_config(name: str, body: ConfigUpdate):
    if name not in _VALID_CONFIGS:
        raise HTTPException(404, f"Unknown config: {name}")
    path = CONFIG_DIR / f"{name}.yaml"
    if name == "strategy_candidates":
        path = CONFIG_DIR / "strategy_candidates.yaml"
    path.write_text(body.content, encoding="utf-8")
    return {"saved": True}


@router.get("/daily-presets")
async def list_daily_presets():
    presets = []
    for f in sorted(CONFIG_DIR.glob("daily_*.yaml")):
        presets.append({"filename": f.name})
    return presets
```

- [ ] **Step 2: Build Config page with tabs (Config Editor / Strategy Candidates / Regime Rules)**

Config Editor uses a `<textarea>` with YAML content. Strategy Candidates shows a table. Regime Rules shows 4 editable cards.

- [ ] **Step 3: Commit**

```bash
git add web/
git commit -m "feat: add Config page with YAML editor, strategy candidates, and regime rules"
```

---

## Phase 5: Advanced Features

### Task 12: Walk-forward validation API + UI

**Files:**
- Expand: `web/api/routers/backtest.py` (add WFV endpoints)
- Expand: `web/frontend/src/pages/BacktestPage.tsx` (add Walk-Forward tab content)

- [ ] **Step 1: Add WFV endpoints to backtest router**

```python
class WFVRequest(BaseModel):
    train_universes: list[str] = ["csi300"]
    eval_market: str = "csi300"
    topk: list[int] = [5, 15, 20]
    n_drop: list[int] = [1, 3]
    hold_thresh: list[int] = [5, 8, 10]
    seeds: bool = False
    workers: int = 1
    grid_workers: int = -1
    robust_weights: Optional[dict] = None
    folds_config: Optional[str] = None


@router.post("/walk-forward")
async def start_wfv(req: WFVRequest):
    tm = get_task_manager()

    def _wfv():
        import subprocess, sys
        cmd = [sys.executable, "run_walk_forward_validation.py",
               "--train-universes", ",".join(req.train_universes),
               "--eval-market", req.eval_market,
               "--topk", ",".join(str(x) for x in req.topk),
               "--n-drop", ",".join(str(x) for x in req.n_drop),
               "--hold-thresh", ",".join(str(x) for x in req.hold_thresh),
               "--workers", str(req.workers)]
        if req.seeds:
            cmd.append("--seeds")
        if req.robust_weights:
            import json
            cmd.extend(["--robust-weights", json.dumps(req.robust_weights)])
        if req.folds_config:
            cmd.extend(["--folds-config", req.folds_config])
        subprocess.run(cmd, check=False)
        return {"status": "completed"}

    task_id = await tm.start_sync_task("wfv", _wfv)
    return {"task_id": task_id}
```

- [ ] **Step 2: Build Walk-Forward tab UI**

- [ ] **Step 3: Commit**

```bash
git add web/
git commit -m "feat: add Walk-Forward Validation UI with fold progress streaming"
```

---

### Task 13: System page (Logs, Cache, Runtime)

**Files:**
- Expand: `web/api/routers/system.py` (add logs, cache browse endpoints)
- Replace: `web/frontend/src/pages/SystemPage.tsx`

- [ ] **Step 1: Add logs and cache endpoints**

```python
@router.get("/logs")
async def get_logs(lines: int = Query(100, ge=1, le=1000), level: Optional[str] = None):
    log_files = sorted(LOGS_DIR.glob("quant_ex_*.log"), reverse=True) if LOGS_DIR.exists() else []
    if not log_files:
        return {"lines": [], "file": None}
    latest = log_files[0]
    all_lines = latest.read_text(encoding="utf-8", errors="replace").splitlines()
    filtered = all_lines[-lines:]
    if level:
        level_upper = level.upper()
        filtered = [l for l in filtered if level_upper in l]
    return {"lines": filtered, "file": latest.name}


@router.get("/cache/{data_type}")
async def browse_cache(data_type: str):
    from quant_ex.run_fetch_data import _FETCHER_REGISTRY
    registry = _FETCHER_REGISTRY
    if data_type not in registry:
        return {"error": f"Unknown type: {data_type}"}
    _, cache_dir, ttl = registry[data_type]
    d = Path(cache_dir)
    if not d.exists():
        return {"files": [], "total_size_mb": 0}
    files = []
    for f in sorted(d.glob("*.csv")):
        files.append({
            "name": f.name,
            "size_kb": round(f.stat().st_size / 1024, 1),
            "modified": datetime.fromtimestamp(f.stat().st_mtime).isoformat(),
        })
    total = sum(f["size_kb"] for f in files) / 1024
    return {"files": files, "total_size_mb": round(total, 2)}


@router.delete("/cache/{data_type}/expired")
async def delete_system_cache_expired(data_type: str):
    return await delete_expired(data_type)
```

- [ ] **Step 2: Build System page with tabs (Logs / Cache Management / Runtime)**

- [ ] **Step 3: Commit**

```bash
git add web/
git commit -m "feat: add System page with log viewer, cache browser, and runtime info"
```

---

### Task 14: Shared frontend components

**Files:**
- Create: `web/frontend/src/components/TaskRunner.tsx`
- Create: `web/frontend/src/components/ModelSelector.tsx`
- Create: `web/frontend/src/components/MetricsTable.tsx`

- [ ] **Step 1: Create TaskRunner component**

Generic wrapper that starts a task, shows SSE stream, displays result:

```tsx
// web/frontend/src/components/TaskRunner.tsx
import { useState } from "react";
import { post } from "../api/client";
import { useSSE, SSEEvent } from "../hooks/useSSE";

interface TaskRunnerProps {
  taskType: string;
  apiPath: string;
  requestBody: Record<string, unknown>;
  renderResult?: (events: SSEEvent[]) => React.ReactNode;
  renderStreaming?: (events: SSEEvent[]) => React.ReactNode;
  buttonLabel?: string;
}

export function TaskRunner({
  taskType, apiPath, requestBody,
  renderResult, renderStreaming, buttonLabel = "Run",
}: TaskRunnerProps) {
  const [taskId, setTaskId] = useState<string | null>(null);
  const { events, status, error, stop } = useSSE(taskId);

  const startTask = async () => {
    try {
      const res = await post<{ task_id: string }>(apiPath, requestBody);
      setTaskId(res.task_id);
    } catch (err) {
      console.error(err);
    }
  };

  return (
    <div>
      {!taskId && (
        <button onClick={startTask} className="px-4 py-2 bg-gray-900 text-white rounded hover:bg-gray-800">
          {buttonLabel}
        </button>
      )}
      {status === "streaming" && (
        <div className="mt-4">
          <div className="flex items-center gap-2 mb-2">
            <span className="animate-pulse">Running...</span>
            <button onClick={stop} className="text-sm text-red-600">Cancel</button>
          </div>
          {renderStreaming ? renderStreaming(events) : (
            <pre className="bg-gray-100 p-3 rounded text-xs max-h-64 overflow-auto">
              {events.map((e, i) => (
                <div key={i}>{JSON.stringify(e)}</div>
              ))}
            </pre>
          )}
        </div>
      )}
      {status === "done" && (
        <div className="mt-4">
          {renderResult ? renderResult(events) : (
            <div className="bg-green-50 border border-green-200 p-3 rounded">
              Task completed. {events.length} events received.
            </div>
          )}
        </div>
      )}
      {error && <p className="text-red-600 mt-2">{error}</p>}
    </div>
  );
}
```

- [ ] **Step 2: Create ModelSelector component**

```tsx
// web/frontend/src/components/ModelSelector.tsx
import { useEffect, useState } from "react";
import { get } from "../api/client";

interface ModelInfo {
  filename: string;
  size_mb: number;
  modified: string;
}

interface ModelSelectorProps {
  value: string;
  onChange: (value: string) => void;
}

export function ModelSelector({ value, onChange }: ModelSelectorProps) {
  const [models, setModels] = useState<ModelInfo[]>([]);

  useEffect(() => {
    get<ModelInfo[]>("/models").then(setModels).catch(console.error);
  }, []);

  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className="border rounded px-3 py-2 text-sm"
    >
      <option value="">Select model...</option>
      {models.map((m) => (
        <option key={m.filename} value={m.filename}>
          {m.filename} ({m.size_mb}MB, {new Date(m.modified).toLocaleDateString()})
        </option>
      ))}
    </select>
  );
}
```

- [ ] **Step 3: Create MetricsTable component**

```tsx
// web/frontend/src/components/MetricsTable.tsx
import { useState, useMemo } from "react";

interface Column {
  key: string;
  label: string;
  sortable?: boolean;
  format?: (v: unknown) => string;
}

interface MetricsTableProps {
  columns: Column[];
  data: Record<string, unknown>[];
  defaultSortKey?: string;
}

export function MetricsTable({ columns, data, defaultSortKey }: MetricsTableProps) {
  const [sortKey, setSortKey] = useState(defaultSortKey || columns[0]?.key);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");

  const sorted = useMemo(() => {
    return [...data].sort((a, b) => {
      const av = a[sortKey], bv = b[sortKey];
      const cmp = typeof av === "number" && typeof bv === "number" ? av - bv : String(av).localeCompare(String(bv));
      return sortDir === "desc" ? -cmp : cmp;
    });
  }, [data, sortKey, sortDir]);

  const toggleSort = (key: string) => {
    if (key === sortKey) setSortDir((d) => (d === "desc" ? "asc" : "desc"));
    else { setSortKey(key); setSortDir("desc"); }
  };

  return (
    <table className="w-full text-sm border-collapse">
      <thead>
        <tr className="bg-gray-50">
          {columns.map((col) => (
            <th
              key={col.key}
              onClick={col.sortable !== false ? () => toggleSort(col.key) : undefined}
              className={`px-3 py-2 text-left border-b ${col.sortable !== false ? "cursor-pointer hover:bg-gray-100" : ""}`}
            >
              {col.label} {sortKey === col.key ? (sortDir === "desc" ? "↓" : "↑") : ""}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {sorted.map((row, i) => (
          <tr key={i} className="border-b hover:bg-gray-50">
            {columns.map((col) => (
              <td key={col.key} className="px-3 py-2">
                {col.format ? col.format(row[col.key]) : String(row[col.key] ?? "")}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
```

- [ ] **Step 4: Commit**

```bash
git add web/frontend/src/components/
git commit -m "feat: add shared frontend components: TaskRunner, ModelSelector, MetricsTable"
```

---

### Task 15: Final integration and build

**Files:**
- All files

- [ ] **Step 1: Production build of frontend**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex/web/frontend
npm run build
```

This outputs to `web/frontend/dist/`. The FastAPI app already mounts this as static files.

- [ ] **Step 2: Verify production mode**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex
.venv/bin/python web/run_web.py
# Open http://localhost:8000 — should serve the built frontend
# API endpoints at /api/* still work
```

- [ ] **Step 3: Run full verification**

```bash
# Backend API health
curl -s http://localhost:8000/api/system/health

# Cache status
curl -s http://localhost:8000/api/data/cache-status | python -m json.tool

# Model list
curl -s http://localhost:8000/api/models | python -m json.tool

# Factor list
curl -s http://localhost:8000/api/factors | python -m json.tool

# Config read
curl -s http://localhost:8000/api/config/model | python -m json.tool

# Frontend loads
curl -s http://localhost:8000/ | head -5
```

- [ ] **Step 4: Final commit**

```bash
git add web/
git commit -m "feat: complete frontend dashboard v1 with all pages and production build"
```

---

## Summary

| Phase | Tasks | Description |
|-------|-------|-------------|
| 1 | 1-5 | FastAPI skeleton, TaskManager + SSE, React scaffolding, sidebar routing, Dashboard page |
| 2 | 6-7 | Data cache/fetch/lookup, Model registry/train/browser |
| 3 | 8-9 | Grid search backtest, Signal generation/history/rebalance |
| 4 | 10-11 | Factor library/evaluation/mining, Config YAML editor |
| 5 | 12-15 | Walk-forward validation, System logs/cache, shared components, production build |

**Total: 15 tasks across 5 phases.**
