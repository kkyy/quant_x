# Frontend Redesign + i18n Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the harsh black/white UI with a warm zinc palette (light mode, dark sidebar), and add Chinese/English language switching via react-i18next across all 8 pages.

**Architecture:** Install `i18next` + `react-i18next`, create translation JSON files, wire up a `LanguageToggle` component in the sidebar footer, then update each page's Tailwind classes and wrap all UI strings in `t()` calls. No backend changes.

**Tech Stack:** React 19, Tailwind CSS v4, TypeScript, react-i18next 15.x, Vite

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `web/frontend/package.json` | Modify | Add i18next + react-i18next deps |
| `web/frontend/src/index.css` | Modify | Zinc color variables, remove old vars |
| `web/frontend/src/main.tsx` | Modify | Side-effect import of i18n init |
| `web/frontend/src/i18n/index.ts` | Create | i18next initialization |
| `web/frontend/src/i18n/zh.json` | Create | Chinese translations |
| `web/frontend/src/i18n/en.json` | Create | English translations |
| `web/frontend/src/components/LanguageToggle.tsx` | Create | CN/EN toggle button pair |
| `web/frontend/src/components/Sidebar.tsx` | Modify | New zinc styles + LanguageToggle |
| `web/frontend/src/components/Layout.tsx` | Modify | `bg-zinc-50` on main |
| `web/frontend/src/pages/DashboardPage.tsx` | Modify | Restyle + useTranslation |
| `web/frontend/src/pages/DataPage.tsx` | Modify | Restyle + useTranslation |
| `web/frontend/src/pages/ModelsPage.tsx` | Modify | Restyle + useTranslation |
| `web/frontend/src/pages/BacktestPage.tsx` | Modify | Restyle + useTranslation |
| `web/frontend/src/pages/SignalsPage.tsx` | Modify | Restyle + useTranslation |
| `web/frontend/src/pages/FactorsPage.tsx` | Modify | Restyle + useTranslation |
| `web/frontend/src/pages/ConfigPage.tsx` | Modify | Restyle + useTranslation |
| `web/frontend/src/pages/SystemPage.tsx` | Modify | Restyle + useTranslation |
| `.gitignore` | Modify | Add `.superpowers/` |

---

## Task 1: Install dependencies + set up i18n

**Files:**
- Modify: `web/frontend/package.json`
- Create: `web/frontend/src/i18n/index.ts`
- Create: `web/frontend/src/i18n/zh.json`
- Create: `web/frontend/src/i18n/en.json`
- Modify: `web/frontend/src/main.tsx`

- [ ] **Step 1: Install packages**

```bash
cd web/frontend && npm install i18next react-i18next
```

Expected: packages added to `node_modules`, `package.json` updated with `"i18next"` and `"react-i18next"` in dependencies.

- [ ] **Step 2: Create `src/i18n/zh.json`**

```json
{
  "nav": {
    "dashboard": "Dashboard",
    "data": "数据管理",
    "models": "模型",
    "backtest": "回测",
    "signals": "信号",
    "factors": "因子",
    "config": "配置",
    "system": "系统",
    "subtitle": "量化选股系统"
  },
  "common": {
    "loading": "加载中...",
    "error": "错误",
    "refresh": "刷新",
    "search": "搜索",
    "noData": "暂无数据",
    "enabled": "已启用",
    "disabled": "已禁用",
    "submit": "提交",
    "save": "保存",
    "saving": "保存中...",
    "reload": "重新加载",
    "cancel": "取消",
    "delete": "删除",
    "deleting": "删除中...",
    "deleteExpired": "删除过期",
    "taskId": "任务ID",
    "status": "状态",
    "filename": "文件名",
    "sizeMb": "大小 (MB)",
    "sizeKb": "大小 (KB)",
    "modified": "更新时间",
    "type": "类型",
    "files": "文件数",
    "latest": "最新",
    "name": "名称",
    "class": "类型",
    "actions": "操作",
    "starting": "启动中...",
    "running": "运行中",
    "done": "完成",
    "failed": "失败",
    "sending": "发送中...",
    "sent": "已发送",
    "evaluating": "评估中...",
    "evaluate": "评估",
    "selectFile": "选择文件查看内容"
  },
  "dashboard": {
    "title": "Dashboard",
    "python": "Python",
    "models": "模型数量",
    "regime": "市场状态",
    "qlibPath": "qlib 数据路径",
    "notConfigured": "未配置",
    "cacheStatus": "缓存状态",
    "cacheTypes": "{{count}} 类型",
    "cacheSummary": "{{count}} 类型，{{total}} MB 合计",
    "savedModels": "已保存模型",
    "noCache": "暂无缓存数据",
    "latest": "最新: {{name}} ({{date}})"
  },
  "data": {
    "title": "数据管理",
    "cacheTab": "缓存状态",
    "fetchTab": "抓取数据",
    "lookupTab": "股票查询",
    "cacheSummary": "{{types}} 数据类型，{{files}} 个文件，{{size}} MB 合计",
    "ttlDays": "TTL (天)",
    "dataType": "数据类型",
    "ttlOverride": "TTL 覆盖（天，留空用默认值）",
    "forceRefresh": "强制刷新（忽略缓存 TTL）",
    "fetchBtn": "抓取数据",
    "fetching": "提交中...",
    "fetchDone": "抓取完成",
    "lookupPlaceholder": "输入股票代码或名称（如 600519，茅台）",
    "found": "找到 {{count}} 条匹配 \"{{symbol}}\"",
    "noMatch": "未找到匹配的股票",
    "cachedFiles": "{{count}} 个缓存文件",
    "dataTypeCol": "数据类型",
    "fileCol": "文件",
    "noCache": "未找到缓存记录"
  },
  "models": {
    "title": "模型",
    "browserTab": "模型浏览",
    "trainTab": "训练",
    "registryTab": "注册表",
    "count": "{{count}} 个已保存模型",
    "noModels": "未找到已保存的模型",
    "meta": "元数据",
    "noMeta": "未找到元数据文件",
    "importance": "特征重要性（前20）",
    "noImportance": "未找到重要性文件",
    "loadingDetails": "加载详情中...",
    "modelType": "模型类型",
    "tag": "标签（可选）",
    "tagPlaceholder": "例：baseline, sector_full",
    "factors": "因子",
    "fitStart": "训练开始日期",
    "fitEnd": "训练结束日期",
    "qlibNative": "qlib-native 模式（MLflow 记录）",
    "dryRun": "试运行（预览）",
    "trainBtn": "训练",
    "trainDryBtn": "训练（试运行）",
    "submitting": "提交中...",
    "registeredModels": "已注册模型（{{count}}）",
    "registeredFactors": "已注册因子（{{count}}）",
    "noRegistered": "未注册",
    "feature": "特征"
  },
  "backtest": {
    "title": "回测",
    "gridTab": "网格搜索",
    "resultsTab": "结果",
    "wfvTab": "滚动验证",
    "model": "模型",
    "noModels": "未找到模型",
    "market": "市场",
    "topk": "Top-K",
    "nDrop": "N-Drop",
    "holdThresh": "持仓阈值",
    "startDate": "开始日期",
    "endDate": "结束日期",
    "multiSeed": "多种子鲁棒性测试",
    "runGrid": "运行网格搜索",
    "noResults": "未找到结果",
    "selectResult": "选择文件查看结果",
    "trainUniverses": "训练股票池（逗号分隔）",
    "evalMarket": "评估市场",
    "workers": "并行数",
    "runWfv": "运行滚动验证"
  },
  "signals": {
    "title": "信号",
    "generateTab": "生成信号",
    "historyTab": "历史记录",
    "rebalanceTab": "再平衡",
    "notificationTab": "通知测试",
    "model": "模型",
    "noModels": "未找到模型",
    "account": "账户金额（元）",
    "positions": "当前持仓",
    "positionsPlaceholder": "SH600000:500,SZ000001:300",
    "dryRun": "试运行（不推送）",
    "generateBtn": "生成信号",
    "regime": "市场状态",
    "noHistory": "未找到信号文件",
    "mockMode": "模拟模式",
    "rebalanceNote": "此功能为再平衡流水线占位。使用命令: python run_scheduled_rebalance.py --mock --dry-run",
    "runRebalance": "运行再平衡",
    "notifTitle": "标题",
    "notifContent": "内容",
    "notifPlaceholder": "这是来自仪表板的测试通知",
    "sendTest": "发送测试",
    "notifNote": "发送测试通知以验证通知渠道"
  },
  "factors": {
    "title": "因子",
    "libraryTab": "因子库",
    "evaluationTab": "评估",
    "miningTab": "挖掘",
    "noFactors": "未注册因子",
    "selectFactor": "选择因子",
    "minIc": "最小 IC",
    "minIcir": "最小 ICIR",
    "topN": "Top N",
    "startMining": "开始挖掘",
    "mining": "挖掘中...",
    "taskSubmitted": "任务已提交。ID: "
  },
  "config": {
    "title": "配置",
    "editorTab": "编辑器",
    "strategyTab": "策略候选",
    "regimeTab": "状态规则",
    "regimeSwitching": "市场状态切换:",
    "regimeNote": "状态规则为只读。修改请编辑 base.yaml",
    "noRegimeRules": "配置中未找到状态规则",
    "key": "键",
    "regime": "状态",
    "topk": "Top K",
    "nDrop": "N Drop",
    "holdThresh": "持仓阈值",
    "savedOk": "保存成功"
  },
  "system": {
    "title": "系统",
    "runtimeTab": "运行时",
    "logsTab": "日志",
    "cacheTab": "缓存",
    "pythonVersion": "Python 版本",
    "qlibPath": "qlib 数据路径",
    "savedModels": "已保存模型",
    "logFile": "文件: {{file}}",
    "noLogs": "未找到日志记录",
    "noCache": "无缓存数据"
  }
}
```

- [ ] **Step 3: Create `src/i18n/en.json`**

```json
{
  "nav": {
    "dashboard": "Dashboard",
    "data": "Data",
    "models": "Models",
    "backtest": "Backtest",
    "signals": "Signals",
    "factors": "Factors",
    "config": "Config",
    "system": "System",
    "subtitle": "Quant Stock Selection"
  },
  "common": {
    "loading": "Loading...",
    "error": "Error",
    "refresh": "Refresh",
    "search": "Search",
    "noData": "No data",
    "enabled": "enabled",
    "disabled": "disabled",
    "submit": "Submit",
    "save": "Save",
    "saving": "Saving...",
    "reload": "Reload",
    "cancel": "Cancel",
    "delete": "Delete",
    "deleting": "Deleting...",
    "deleteExpired": "Delete Expired",
    "taskId": "Task ID",
    "status": "Status",
    "filename": "Filename",
    "sizeMb": "Size (MB)",
    "sizeKb": "Size (KB)",
    "modified": "Modified",
    "type": "Type",
    "files": "Files",
    "latest": "Latest",
    "name": "Name",
    "class": "Class",
    "actions": "Actions",
    "starting": "Starting...",
    "running": "running",
    "done": "done",
    "failed": "failed",
    "sending": "Sending...",
    "sent": "Sent",
    "evaluating": "Evaluating...",
    "evaluate": "Evaluate",
    "selectFile": "Select a file to view content"
  },
  "dashboard": {
    "title": "Dashboard",
    "python": "Python",
    "models": "Models",
    "regime": "Regime Detection",
    "qlibPath": "qlib Data Path",
    "notConfigured": "not configured",
    "cacheStatus": "Cache Status",
    "cacheTypes": "{{count}} types",
    "cacheSummary": "{{count}} types, {{total}} MB total",
    "savedModels": "Saved Models",
    "noCache": "No cache data",
    "latest": "Latest: {{name}} ({{date}})"
  },
  "data": {
    "title": "Data Management",
    "cacheTab": "Cache Status",
    "fetchTab": "Fetch",
    "lookupTab": "Stock Lookup",
    "cacheSummary": "{{types}} data types, {{files}} files, {{size}} MB total",
    "ttlDays": "TTL (days)",
    "dataType": "Data Type",
    "ttlOverride": "TTL Override (days, leave empty for default)",
    "forceRefresh": "Force refresh (ignore cache TTL)",
    "fetchBtn": "Fetch Data",
    "fetching": "Submitting...",
    "fetchDone": "Fetch completed successfully.",
    "lookupPlaceholder": "Enter symbol or name (e.g. 600519, 茅台)",
    "found": "Found {{count}} match(es) for \"{{symbol}}\"",
    "noMatch": "No matching stocks found.",
    "cachedFiles": "{{count}} cached file(s)",
    "dataTypeCol": "Data Type",
    "fileCol": "File",
    "noCache": "No cache entries found"
  },
  "models": {
    "title": "Models",
    "browserTab": "Model Browser",
    "trainTab": "Train",
    "registryTab": "Registry",
    "count": "{{count}} saved model(s)",
    "noModels": "No saved models found.",
    "meta": "Meta",
    "noMeta": "No meta file found",
    "importance": "Feature Importance (top 20)",
    "noImportance": "No importance file found",
    "loadingDetails": "Loading details...",
    "modelType": "Model Type",
    "tag": "Tag (optional)",
    "tagPlaceholder": "e.g. baseline, sector_full",
    "factors": "Factors",
    "fitStart": "Fit Start",
    "fitEnd": "Fit End",
    "qlibNative": "qlib-native mode (MLflow tracked)",
    "dryRun": "Dry run (preview only)",
    "trainBtn": "Train",
    "trainDryBtn": "Train (Dry Run)",
    "submitting": "Submitting...",
    "registeredModels": "Registered Models ({{count}})",
    "registeredFactors": "Registered Factors ({{count}})",
    "noRegistered": "No items registered",
    "feature": "Feature"
  },
  "backtest": {
    "title": "Backtest",
    "gridTab": "Grid Search",
    "resultsTab": "Results",
    "wfvTab": "Walk-Forward",
    "model": "Model",
    "noModels": "No models found",
    "market": "Market",
    "topk": "Top-K",
    "nDrop": "N-Drop",
    "holdThresh": "Hold Thresh",
    "startDate": "Start Date",
    "endDate": "End Date",
    "multiSeed": "Multi-seed robustness",
    "runGrid": "Run Grid Search",
    "noResults": "No results found.",
    "selectResult": "Select a file to view results.",
    "trainUniverses": "Train Universes (comma-separated)",
    "evalMarket": "Eval Market",
    "workers": "Workers",
    "runWfv": "Run Walk-Forward Validation"
  },
  "signals": {
    "title": "Signals",
    "generateTab": "Generate",
    "historyTab": "History",
    "rebalanceTab": "Rebalance",
    "notificationTab": "Notification",
    "model": "Model",
    "noModels": "No models found",
    "account": "Account (CNY)",
    "positions": "Current Positions",
    "positionsPlaceholder": "SH600000:500,SZ000001:300",
    "dryRun": "Dry run (no push)",
    "generateBtn": "Generate Signal",
    "regime": "Regime",
    "noHistory": "No signal files found.",
    "mockMode": "Mock mode",
    "rebalanceNote": "This tab is a placeholder. Use: python run_scheduled_rebalance.py --mock --dry-run",
    "runRebalance": "Run Rebalance",
    "notifTitle": "Title",
    "notifContent": "Content",
    "notifPlaceholder": "This is a test notification from the dashboard.",
    "sendTest": "Send Test",
    "notifNote": "Send a test notification to verify notification channels."
  },
  "factors": {
    "title": "Factors",
    "libraryTab": "Library",
    "evaluationTab": "Evaluation",
    "miningTab": "Mining",
    "noFactors": "No factors registered",
    "selectFactor": "Select Factor",
    "minIc": "Min IC",
    "minIcir": "Min ICIR",
    "topN": "Top N",
    "startMining": "Start Mining",
    "mining": "Mining...",
    "taskSubmitted": "Task submitted. ID: "
  },
  "config": {
    "title": "Config",
    "editorTab": "Editor",
    "strategyTab": "Strategy Candidates",
    "regimeTab": "Regime Rules",
    "regimeSwitching": "Regime Switching:",
    "regimeNote": "Regime rules are read-only. Edit base.yaml to modify.",
    "noRegimeRules": "No regime rules found in config",
    "key": "Key",
    "regime": "Regime",
    "topk": "Top K",
    "nDrop": "N Drop",
    "holdThresh": "Hold Thresh",
    "savedOk": "Saved successfully"
  },
  "system": {
    "title": "System",
    "runtimeTab": "Runtime",
    "logsTab": "Logs",
    "cacheTab": "Cache",
    "pythonVersion": "Python Version",
    "qlibPath": "qlib Data Path",
    "savedModels": "Saved Models",
    "logFile": "File: {{file}}",
    "noLogs": "No log entries found",
    "noCache": "No cache data"
  }
}
```

- [ ] **Step 4: Create `src/i18n/index.ts`**

```typescript
import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';
import zh from './zh.json';
import en from './en.json';

i18n.use(initReactI18next).init({
  resources: {
    zh: { translation: zh },
    en: { translation: en },
  },
  lng: (localStorage.getItem('lang') as string) ?? 'zh',
  fallbackLng: 'zh',
  interpolation: { escapeValue: false },
});

export default i18n;
```

- [ ] **Step 5: Update `src/main.tsx` to import i18n**

Replace the entire file with:

```tsx
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import './i18n/index'
import App from './App.tsx'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
```

- [ ] **Step 6: Add `.superpowers/` to `.gitignore`**

Open `web/frontend/.gitignore` (or the root `.gitignore`) and verify `.superpowers/` is already listed or add it. The root `.gitignore` is the one to check:

```bash
grep -n superpowers /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex/.gitignore || echo "NOT FOUND"
```

If not found, add it:
```
.superpowers/
```

- [ ] **Step 7: Commit**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex
git add web/frontend/package.json web/frontend/package-lock.json web/frontend/src/i18n/ web/frontend/src/main.tsx .gitignore
git commit -m "feat: install react-i18next and create zh/en translation files"
```

---

## Task 2: Global styles — zinc palette

**Files:**
- Modify: `web/frontend/src/index.css`

- [ ] **Step 1: Replace `src/index.css`**

```css
@import "tailwindcss";

body {
  background-color: #fafafa;
  color: #18181b;
  font-family: system-ui, -apple-system, sans-serif;
}
```

That's all this file needs. Color tokens are applied directly via Tailwind classes throughout the components (no CSS variables needed for this palette — Tailwind's zinc/amber built-ins cover everything).

- [ ] **Step 2: Verify Vite dev server starts without errors**

```bash
cd web/frontend && npm run dev 2>&1 | head -20
```

Expected: `VITE vx.x.x ready` with no compilation errors.

- [ ] **Step 3: Commit**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex
git add web/frontend/src/index.css
git commit -m "style: update global CSS to zinc palette base"
```

---

## Task 3: LanguageToggle + Sidebar redesign

**Files:**
- Create: `web/frontend/src/components/LanguageToggle.tsx`
- Modify: `web/frontend/src/components/Sidebar.tsx`
- Modify: `web/frontend/src/components/Layout.tsx`

- [ ] **Step 1: Create `src/components/LanguageToggle.tsx`**

```tsx
import { useTranslation } from 'react-i18next';

export function LanguageToggle() {
  const { i18n } = useTranslation();

  const toggle = (lang: string) => {
    i18n.changeLanguage(lang);
    localStorage.setItem('lang', lang);
  };

  return (
    <div className="flex gap-1 p-3 border-t border-zinc-800">
      {(['zh', 'en'] as const).map((lang) => (
        <button
          key={lang}
          onClick={() => toggle(lang)}
          className={`flex-1 py-1.5 text-xs font-medium rounded transition-colors ${
            i18n.language === lang
              ? 'bg-amber-500 text-zinc-900'
              : 'text-zinc-500 hover:text-zinc-300 hover:bg-zinc-800'
          }`}
        >
          {lang === 'zh' ? '中文' : 'EN'}
        </button>
      ))}
    </div>
  );
}
```

- [ ] **Step 2: Replace `src/components/Sidebar.tsx`**

```tsx
import { NavLink } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { LanguageToggle } from "./LanguageToggle";

const NAV_ITEMS = [
  { to: "/",          icon: "◉", key: "dashboard" },
  { to: "/data",      icon: "◈", key: "data" },
  { to: "/models",    icon: "◆", key: "models" },
  { to: "/backtest",  icon: "◇", key: "backtest" },
  { to: "/signals",   icon: "▸", key: "signals" },
  { to: "/factors",   icon: "⋄", key: "factors" },
  { to: "/config",    icon: "⚙", key: "config" },
  { to: "/system",    icon: "⊙", key: "system" },
] as const;

export function Sidebar() {
  const { t } = useTranslation();

  return (
    <aside className="w-56 bg-zinc-900 h-screen sticky top-0 flex flex-col border-r border-zinc-800">
      <div className="px-4 py-4 border-b border-zinc-800">
        <h1 className="text-sm font-bold text-zinc-100 tracking-tight">quant_ex</h1>
        <p className="text-xs text-zinc-500 mt-0.5">{t('nav.subtitle')}</p>
      </div>
      <nav className="flex-1 p-2 space-y-0.5 overflow-y-auto">
        {NAV_ITEMS.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.to === "/"}
            className={({ isActive }) =>
              `flex items-center gap-2.5 px-3 py-2 rounded-md text-sm transition-colors ${
                isActive
                  ? "bg-amber-500 text-zinc-900 font-semibold"
                  : "text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800"
              }`
            }
          >
            <span className="text-base leading-none">{item.icon}</span>
            <span>{t(`nav.${item.key}`)}</span>
          </NavLink>
        ))}
      </nav>
      <LanguageToggle />
    </aside>
  );
}
```

- [ ] **Step 3: Replace `src/components/Layout.tsx`**

```tsx
import { Outlet } from "react-router-dom";
import { Sidebar } from "./Sidebar";

export function Layout() {
  return (
    <div className="flex min-h-screen bg-zinc-50">
      <Sidebar />
      <main className="flex-1 p-6 overflow-auto">
        <Outlet />
      </main>
    </div>
  );
}
```

- [ ] **Step 4: Verify in browser**

```bash
cd web/frontend && npm run dev
```

Open http://localhost:5173. Sidebar should be dark zinc, nav items use amber highlight, language toggle at bottom. Click EN/中文 to confirm language state updates (labels will still be English until pages are wired in next tasks, but the toggle should render and persist via localStorage).

- [ ] **Step 5: Commit**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex
git add web/frontend/src/components/
git commit -m "feat: add LanguageToggle, restyle Sidebar and Layout with zinc palette"
```

---

## Task 4: DashboardPage restyle + i18n

**Files:**
- Modify: `web/frontend/src/pages/DashboardPage.tsx`

- [ ] **Step 1: Replace `DashboardPage.tsx`**

```tsx
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { get } from "../api/client";

interface CacheInfo {
  file_count: number;
  total_size_mb: number;
  latest: string | null;
}

interface RuntimeInfo {
  python_version: string;
  qlib_data_path: string;
  models_count: number;
  cache_types: Record<string, CacheInfo>;
}

interface ModelInfo {
  filename: string;
  size_mb: number;
  modified: string;
  meta: Record<string, unknown>;
}

export function DashboardPage() {
  const { t } = useTranslation();
  const [runtime, setRuntime] = useState<RuntimeInfo | null>(null);
  const [models, setModels] = useState<ModelInfo[]>([]);
  const [regime, setRegime] = useState<{ enabled: boolean; label?: string; error?: string } | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    Promise.all([
      get<RuntimeInfo>("/system/runtime"),
      get<ModelInfo[]>("/models"),
      get<{ enabled: boolean; label?: string; error?: string }>("/signals/regime"),
    ])
      .then(([rt, ms, reg]) => { setRuntime(rt); setModels(ms); setRegime(reg); })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <p className="text-zinc-500 text-sm">{t('common.loading')}</p>;
  if (error) return <p className="text-red-600 text-sm">{t('common.error')}: {error}</p>;

  const lastModel = models.length > 0 ? models[models.length - 1] : null;
  const cacheEntries = Object.entries(runtime?.cache_types ?? {}).sort(([a], [b]) => a.localeCompare(b));
  const totalCacheMb = cacheEntries.reduce((s, [, v]) => s + v.total_size_mb, 0);

  return (
    <div className="space-y-6 max-w-5xl">
      <h2 className="text-2xl font-bold text-zinc-900">{t('dashboard.title')}</h2>

      {/* Summary cards */}
      <div className="grid grid-cols-3 gap-4">
        <div className="bg-white border border-zinc-200 rounded-lg p-4 shadow-sm">
          <h3 className="text-xs text-zinc-500 uppercase tracking-wide mb-2">{t('dashboard.python')}</h3>
          <p className="text-sm font-mono font-semibold text-zinc-800">{runtime?.python_version?.split(" ")[0]}</p>
        </div>
        <div className="bg-white border border-zinc-200 rounded-lg p-4 shadow-sm">
          <h3 className="text-xs text-zinc-500 uppercase tracking-wide mb-2">{t('dashboard.models')}</h3>
          <p className="text-2xl font-bold text-zinc-900">{runtime?.models_count ?? 0}</p>
          {lastModel && (
            <p className="text-xs text-zinc-400 mt-1">
              {t('common.latest')}: {lastModel.filename} ({new Date(lastModel.modified).toLocaleDateString()})
            </p>
          )}
        </div>
        <div className="bg-white border border-zinc-200 rounded-lg p-4 shadow-sm">
          <h3 className="text-xs text-zinc-500 uppercase tracking-wide mb-2">{t('dashboard.regime')}</h3>
          {regime?.enabled ? (
            <span className="inline-block px-2 py-1 text-xs bg-amber-100 text-amber-700 rounded font-medium">
              {regime.label || t('common.enabled')}
            </span>
          ) : (
            <span className="inline-block px-2 py-1 text-xs bg-zinc-100 text-zinc-500 rounded">
              {regime?.error ? `error: ${regime.error}` : t('common.disabled')}
            </span>
          )}
        </div>
      </div>

      {/* qlib data path */}
      <div className="bg-white border border-zinc-200 rounded-lg p-4 shadow-sm">
        <h3 className="text-xs text-zinc-500 uppercase tracking-wide mb-2">{t('dashboard.qlibPath')}</h3>
        <p className="text-xs font-mono text-zinc-600 break-all">{runtime?.qlib_data_path || t('dashboard.notConfigured')}</p>
      </div>

      {/* Cache status */}
      <div>
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-lg font-semibold text-zinc-800">{t('dashboard.cacheStatus')}</h3>
          <span className="text-sm text-zinc-400">
            {cacheEntries.length} {t('common.type')}, {totalCacheMb.toFixed(1)} MB
          </span>
        </div>
        <div className="bg-white border border-zinc-200 rounded-lg overflow-hidden shadow-sm">
          <table className="w-full text-sm">
            <thead className="bg-zinc-50 border-b border-zinc-200">
              <tr>
                <th className="text-left px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.type')}</th>
                <th className="text-right px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.files')}</th>
                <th className="text-right px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.sizeMb')}</th>
                <th className="text-left px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.latest')}</th>
              </tr>
            </thead>
            <tbody>
              {cacheEntries.map(([type, info]) => (
                <tr key={type} className="border-t border-zinc-100 hover:bg-zinc-50">
                  <td className="px-4 py-2 font-mono text-xs text-zinc-700">{type}</td>
                  <td className="text-right px-4 py-2 text-zinc-600">{info.file_count}</td>
                  <td className="text-right px-4 py-2 text-zinc-600">{info.total_size_mb}</td>
                  <td className="px-4 py-2 text-xs text-zinc-400">
                    {info.latest ? new Date(info.latest).toLocaleDateString() : "-"}
                  </td>
                </tr>
              ))}
              {cacheEntries.length === 0 && (
                <tr>
                  <td colSpan={4} className="px-4 py-4 text-center text-zinc-400 text-sm">{t('dashboard.noCache')}</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Model list */}
      {models.length > 0 && (
        <div>
          <h3 className="text-lg font-semibold text-zinc-800 mb-3">{t('dashboard.savedModels')}</h3>
          <div className="bg-white border border-zinc-200 rounded-lg overflow-hidden shadow-sm">
            <table className="w-full text-sm">
              <thead className="bg-zinc-50 border-b border-zinc-200">
                <tr>
                  <th className="text-left px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.filename')}</th>
                  <th className="text-right px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.sizeMb')}</th>
                  <th className="text-left px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.modified')}</th>
                </tr>
              </thead>
              <tbody>
                {models.map((m) => (
                  <tr key={m.filename} className="border-t border-zinc-100 hover:bg-zinc-50">
                    <td className="px-4 py-2 font-mono text-xs text-zinc-700">{m.filename}</td>
                    <td className="text-right px-4 py-2 text-zinc-600">{m.size_mb}</td>
                    <td className="px-4 py-2 text-xs text-zinc-400">{new Date(m.modified).toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex
git add web/frontend/src/pages/DashboardPage.tsx
git commit -m "style: restyle DashboardPage with zinc palette and add i18n"
```

---

## Task 5: DataPage restyle + i18n

**Files:**
- Modify: `web/frontend/src/pages/DataPage.tsx`

The shared tab bar pattern used throughout this file (and all subsequent pages) uses these zinc classes:
- Active tab: `border-b-2 border-amber-500 text-amber-600`
- Inactive tab: `border-transparent text-zinc-500 hover:text-zinc-700`
- Primary button: `bg-amber-500 text-zinc-900 hover:bg-amber-600`
- Outline button: `border border-zinc-300 text-zinc-700 hover:bg-zinc-50`
- Danger button: `border border-red-300 text-red-600 hover:bg-red-50`
- Input/select: `border border-zinc-300 rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-400`
- Panel: `bg-white border border-zinc-200 rounded-lg shadow-sm`
- Table thead: `bg-zinc-50 border-b border-zinc-200` with `text-xs text-zinc-500 uppercase tracking-wide font-medium`
- Table row: `border-t border-zinc-100 hover:bg-zinc-50`

- [ ] **Step 1: Replace `DataPage.tsx`**

Replace the full file. The logic is unchanged; only Tailwind class names and text strings change. Here is the complete replacement:

```tsx
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { get, post, del } from "../api/client";

interface CacheStatusEntry {
  type: string;
  file_count: number;
  total_size_mb: number;
  latest: string | null;
  ttl_days: number;
}

interface StockMatch {
  symbol: string;
  name: string;
  cache_files: { type: string; file: string; modified: string }[];
}

interface StockLookupResult {
  symbol: string;
  matches: StockMatch[];
}

type Tab = "cache" | "fetch" | "lookup";

export function DataPage() {
  const { t } = useTranslation();
  const [tab, setTab] = useState<Tab>("cache");

  const tabs: { key: Tab; label: string }[] = [
    { key: "cache", label: t('data.cacheTab') },
    { key: "fetch", label: t('data.fetchTab') },
    { key: "lookup", label: t('data.lookupTab') },
  ];

  return (
    <div className="space-y-4 max-w-5xl">
      <h2 className="text-2xl font-bold text-zinc-900">{t('data.title')}</h2>
      <div className="flex gap-1 border-b border-zinc-200">
        {tabs.map((tb) => (
          <button
            key={tb.key}
            onClick={() => setTab(tb.key)}
            className={`px-4 py-2 text-sm font-medium -mb-px border-b-2 transition-colors ${
              tab === tb.key
                ? "border-amber-500 text-amber-600"
                : "border-transparent text-zinc-500 hover:text-zinc-700"
            }`}
          >
            {tb.label}
          </button>
        ))}
      </div>
      {tab === "cache" && <CacheStatusTab />}
      {tab === "fetch" && <FetchTab />}
      {tab === "lookup" && <StockLookupTab />}
    </div>
  );
}

function CacheStatusTab() {
  const { t } = useTranslation();
  const [entries, setEntries] = useState<CacheStatusEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState<string | null>(null);

  const load = () => {
    setLoading(true);
    setError(null);
    get<CacheStatusEntry[]>("/data/cache-status")
      .then(setEntries)
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => { load(); }, []);

  const handleDeleteExpired = async (type: string) => {
    setDeleting(type);
    try {
      const res = await del<{ deleted: number }>(`/data/cache/${type}/expired`);
      alert(`Deleted ${res.deleted} expired files for ${type}`);
      load();
    } catch (err: any) {
      alert(`Error: ${err.message}`);
    } finally {
      setDeleting(null);
    }
  };

  if (loading) return <p className="text-zinc-500 text-sm py-4">{t('common.loading')}</p>;
  if (error) return <p className="text-red-600 text-sm py-4">{t('common.error')}: {error}</p>;

  const totalSize = entries.reduce((s, e) => s + e.total_size_mb, 0);
  const totalFiles = entries.reduce((s, e) => s + e.file_count, 0);

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <span className="text-sm text-zinc-500">
          {t('data.cacheSummary', { types: entries.length, files: totalFiles, size: totalSize.toFixed(1) })}
        </span>
        <button onClick={load} className="text-sm px-3 py-1.5 border border-zinc-300 rounded-md hover:bg-zinc-50 text-zinc-700">
          {t('common.refresh')}
        </button>
      </div>
      <div className="bg-white border border-zinc-200 rounded-lg overflow-hidden shadow-sm">
        <table className="w-full text-sm">
          <thead className="bg-zinc-50 border-b border-zinc-200">
            <tr>
              <th className="text-left px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.type')}</th>
              <th className="text-right px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.files')}</th>
              <th className="text-right px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.sizeMb')}</th>
              <th className="text-left px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.latest')}</th>
              <th className="text-right px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('data.ttlDays')}</th>
              <th className="text-right px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.actions')}</th>
            </tr>
          </thead>
          <tbody>
            {entries.map((e) => (
              <tr key={e.type} className="border-t border-zinc-100 hover:bg-zinc-50">
                <td className="px-4 py-2 font-mono text-xs text-zinc-700">{e.type}</td>
                <td className="text-right px-4 py-2 text-zinc-600">{e.file_count}</td>
                <td className="text-right px-4 py-2 text-zinc-600">{e.total_size_mb}</td>
                <td className="px-4 py-2 text-xs text-zinc-400">{e.latest ? new Date(e.latest).toLocaleString() : "-"}</td>
                <td className="text-right px-4 py-2 text-zinc-600">{e.ttl_days}</td>
                <td className="text-right px-4 py-2">
                  <button
                    onClick={() => handleDeleteExpired(e.type)}
                    disabled={deleting === e.type}
                    className="text-xs px-2 py-1 text-red-600 border border-red-300 rounded hover:bg-red-50 disabled:opacity-50"
                  >
                    {deleting === e.type ? t('common.deleting') : t('common.deleteExpired')}
                  </button>
                </td>
              </tr>
            ))}
            {entries.length === 0 && (
              <tr>
                <td colSpan={6} className="px-4 py-6 text-center text-zinc-400 text-sm">{t('data.noCache')}</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function FetchTab() {
  const { t } = useTranslation();
  const [dataTypes, setDataTypes] = useState<string[]>([]);
  const [selectedType, setSelectedType] = useState("financial");
  const [ttl, setTtl] = useState("");
  const [force, setForce] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    get<CacheStatusEntry[]>("/data/cache-status")
      .then((entries) => {
        const types = entries.map((e) => e.type);
        setDataTypes(types);
        if (types.length > 0) setSelectedType(types[0]);
      })
      .catch(() => {});
  }, []);

  const handleFetch = async () => {
    setStatus(t('data.fetching'));
    setError(null);
    try {
      const body: Record<string, unknown> = { type: selectedType, force };
      if (ttl && !force) body.ttl = parseInt(ttl, 10);
      const res = await post<{ task_id: string }>("/data/fetch", body);
      const tid = res.task_id;
      setStatus(`Task submitted: ${tid}`);
      pollStatus(tid);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err));
      setStatus(null);
    }
  };

  const pollStatus = (tid: string) => {
    const interval = setInterval(async () => {
      try {
        const tasks = await get<{ task_id: string; status: string; error?: string }[]>("/system/tasks");
        const task = tasks.find((tk) => tk.task_id === tid);
        if (!task) return;
        if (task.status === "done") { setStatus(t('data.fetchDone')); clearInterval(interval); }
        else if (task.status === "failed") { setError(task.error || "Fetch failed"); setStatus(null); clearInterval(interval); }
        else { setStatus(`Task ${tid}: ${task.status}...`); }
      } catch { clearInterval(interval); }
    }, 2000);
  };

  return (
    <div className="space-y-4 max-w-lg">
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('data.dataType')}</label>
        <select value={selectedType} onChange={(e) => setSelectedType(e.target.value)}
          className="w-full border border-zinc-300 rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-400">
          <option value="all">all</option>
          {dataTypes.map((tp) => <option key={tp} value={tp}>{tp}</option>)}
        </select>
      </div>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('data.ttlOverride')}</label>
        <input type="number" value={ttl} onChange={(e) => setTtl(e.target.value)} placeholder="default" disabled={force}
          className="w-full border border-zinc-300 rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-400 disabled:bg-zinc-50 disabled:text-zinc-400" />
      </div>
      <div className="flex items-center gap-2">
        <input type="checkbox" id="force-refresh" checked={force} onChange={(e) => setForce(e.target.checked)} className="rounded" />
        <label htmlFor="force-refresh" className="text-sm text-zinc-700">{t('data.forceRefresh')}</label>
      </div>
      <button onClick={handleFetch} disabled={status?.includes("Submitting") || status?.includes("running")}
        className="px-4 py-2 bg-amber-500 text-zinc-900 font-medium rounded-md text-sm hover:bg-amber-600 disabled:opacity-50">
        {t('data.fetchBtn')}
      </button>
      {status && <p className={`text-sm ${error ? "text-red-600" : "text-green-700"}`}>{status}</p>}
      {error && <p className="text-sm text-red-600">{error}</p>}
    </div>
  );
}

function StockLookupTab() {
  const { t } = useTranslation();
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<StockLookupResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSearch = async () => {
    if (!query.trim()) return;
    setLoading(true);
    setError(null);
    setResults(null);
    try {
      const res = await get<StockLookupResult>(`/data/stock-lookup/${encodeURIComponent(query.trim())}`);
      setResults(res);
    } catch (err: any) { setError(err.message); }
    finally { setLoading(false); }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => { if (e.key === "Enter") handleSearch(); };

  return (
    <div className="space-y-4">
      <div className="flex gap-2 max-w-lg">
        <input type="text" value={query} onChange={(e) => setQuery(e.target.value)} onKeyDown={handleKeyDown}
          placeholder={t('data.lookupPlaceholder')}
          className="flex-1 border border-zinc-300 rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-400" />
        <button onClick={handleSearch} disabled={loading}
          className="px-4 py-2 bg-amber-500 text-zinc-900 font-medium rounded-md text-sm hover:bg-amber-600 disabled:opacity-50">
          {loading ? t('common.loading') : t('common.search')}
        </button>
      </div>
      {error && <p className="text-sm text-red-600">{error}</p>}
      {results && (
        <div className="space-y-4">
          <p className="text-sm text-zinc-500">{t('data.found', { count: results.matches.length, symbol: results.symbol })}</p>
          {results.matches.length === 0 && <p className="text-sm text-zinc-400">{t('data.noMatch')}</p>}
          {results.matches.map((match) => (
            <div key={match.symbol} className="bg-white border border-zinc-200 rounded-lg p-4 shadow-sm space-y-2">
              <div className="flex items-baseline gap-3">
                <span className="font-mono font-bold text-sm text-zinc-800">{match.symbol}</span>
                <span className="text-zinc-700">{match.name}</span>
                <span className="text-xs text-zinc-400">{t('data.cachedFiles', { count: match.cache_files.length })}</span>
              </div>
              {match.cache_files.length > 0 && (
                <table className="w-full text-xs">
                  <thead className="bg-zinc-50">
                    <tr>
                      <th className="text-left px-3 py-1 text-zinc-500">{t('data.dataTypeCol')}</th>
                      <th className="text-left px-3 py-1 text-zinc-500">{t('data.fileCol')}</th>
                      <th className="text-left px-3 py-1 text-zinc-500">{t('common.modified')}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {match.cache_files.map((cf, i) => (
                      <tr key={i} className="border-t border-zinc-100">
                        <td className="px-3 py-1 font-mono text-zinc-700">{cf.type}</td>
                        <td className="px-3 py-1 font-mono text-zinc-700">{cf.file}</td>
                        <td className="px-3 py-1 text-zinc-400">{new Date(cf.modified).toLocaleString()}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex
git add web/frontend/src/pages/DataPage.tsx
git commit -m "style: restyle DataPage with zinc palette and add i18n"
```

---

## Task 6: ModelsPage restyle + i18n

**Files:**
- Modify: `web/frontend/src/pages/ModelsPage.tsx`

- [ ] **Step 1: Replace `ModelsPage.tsx`**

Apply the zinc/amber class pattern throughout. The tab bar, tables, forms, and buttons all follow the same classes defined in Task 5. Here is the complete replacement:

```tsx
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { get, post } from "../api/client";

interface ModelInfo {
  filename: string;
  size_mb: number;
  modified: string;
  meta: Record<string, unknown>;
}

interface RegistryInfo {
  models: { name: string }[];
  factors: { name: string }[];
}

type Tab = "browser" | "train" | "registry";

export function ModelsPage() {
  const { t } = useTranslation();
  const [tab, setTab] = useState<Tab>("browser");

  const tabs: { key: Tab; label: string }[] = [
    { key: "browser", label: t('models.browserTab') },
    { key: "train",   label: t('models.trainTab') },
    { key: "registry",label: t('models.registryTab') },
  ];

  return (
    <div className="space-y-4 max-w-5xl">
      <h2 className="text-2xl font-bold text-zinc-900">{t('models.title')}</h2>
      <div className="flex gap-1 border-b border-zinc-200">
        {tabs.map((tb) => (
          <button key={tb.key} onClick={() => setTab(tb.key)}
            className={`px-4 py-2 text-sm font-medium -mb-px border-b-2 transition-colors ${
              tab === tb.key ? "border-amber-500 text-amber-600" : "border-transparent text-zinc-500 hover:text-zinc-700"
            }`}>
            {tb.label}
          </button>
        ))}
      </div>
      {tab === "browser" && <ModelBrowserTab />}
      {tab === "train" && <TrainTab />}
      {tab === "registry" && <RegistryTab />}
    </div>
  );
}

function ModelBrowserTab() {
  const { t } = useTranslation();
  const [models, setModels] = useState<ModelInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [meta, setMeta] = useState<Record<string, unknown> | null>(null);
  const [importance, setImportance] = useState<Record<string, unknown> | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  useEffect(() => {
    get<ModelInfo[]>("/models").then(setModels).catch((err) => setError(err.message)).finally(() => setLoading(false));
  }, []);

  const handleExpand = async (filename: string) => {
    if (expanded === filename) { setExpanded(null); setMeta(null); setImportance(null); return; }
    setExpanded(filename);
    setDetailLoading(true);
    try {
      const [m, imp] = await Promise.all([
        get<Record<string, unknown>>(`/models/${filename}/meta`),
        get<Record<string, unknown>>(`/models/${filename}/importance`),
      ]);
      setMeta(m); setImportance(imp);
    } catch (err: any) { setError(err.message); }
    finally { setDetailLoading(false); }
  };

  if (loading) return <p className="text-zinc-500 text-sm py-4">{t('common.loading')}</p>;
  if (error) return <p className="text-red-600 text-sm py-4">{t('common.error')}: {error}</p>;
  if (models.length === 0) return <p className="text-zinc-500 text-sm py-4">{t('models.noModels')}</p>;

  const importanceEntries = importance
    ? Object.entries(importance).sort(([, a], [, b]) => (b as number) - (a as number)).slice(0, 20)
    : [];

  return (
    <div className="space-y-2">
      <p className="text-sm text-zinc-500">{t('models.count', { count: models.length })}</p>
      <div className="bg-white border border-zinc-200 rounded-lg overflow-hidden shadow-sm">
        <table className="w-full text-sm">
          <thead className="bg-zinc-50 border-b border-zinc-200">
            <tr>
              <th className="text-left px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.filename')}</th>
              <th className="text-right px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.sizeMb')}</th>
              <th className="text-left px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.modified')}</th>
            </tr>
          </thead>
          <tbody>
            {models.map((m) => (
              <>
                <tr key={m.filename} className="border-t border-zinc-100 hover:bg-zinc-50 cursor-pointer"
                  onClick={() => handleExpand(m.filename)}>
                  <td className="px-4 py-2">
                    <span className="font-mono text-xs text-zinc-700">{m.filename}</span>
                    <span className="ml-2 text-xs text-zinc-400">{expanded === m.filename ? "▼" : "▶"}</span>
                  </td>
                  <td className="text-right px-4 py-2 text-zinc-600">{m.size_mb}</td>
                  <td className="px-4 py-2 text-xs text-zinc-400">{new Date(m.modified).toLocaleString()}</td>
                </tr>
                {expanded === m.filename && (
                  <tr className="border-t border-zinc-100 bg-zinc-50">
                    <td colSpan={3} className="px-6 py-4">
                      {detailLoading ? (
                        <p className="text-sm text-zinc-500">{t('models.loadingDetails')}</p>
                      ) : (
                        <div className="grid grid-cols-2 gap-6">
                          <div>
                            <h4 className="text-sm font-semibold text-zinc-700 mb-2">{t('models.meta')}</h4>
                            {meta && Object.keys(meta).length > 0 ? (
                              <table className="w-full text-xs">
                                <tbody>
                                  {Object.entries(meta).map(([k, v]) => (
                                    <tr key={k} className="border-t border-zinc-100">
                                      <td className="py-1 pr-3 font-medium text-zinc-500">{k}</td>
                                      <td className="py-1 font-mono text-zinc-700">{typeof v === "object" ? JSON.stringify(v) : String(v)}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            ) : <p className="text-xs text-zinc-400">{t('models.noMeta')}</p>}
                          </div>
                          <div>
                            <h4 className="text-sm font-semibold text-zinc-700 mb-2">{t('models.importance')}</h4>
                            {importanceEntries.length > 0 ? (
                              <table className="w-full text-xs">
                                <thead>
                                  <tr>
                                    <th className="text-left py-1 text-zinc-500">{t('models.feature')}</th>
                                    <th className="text-right py-1 text-zinc-500">{t('models.importance')}</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {importanceEntries.map(([feat, val]) => (
                                    <tr key={feat} className="border-t border-zinc-100">
                                      <td className="py-1 font-mono text-zinc-700">{feat}</td>
                                      <td className="text-right py-1 text-zinc-600">{(val as number).toFixed(4)}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            ) : <p className="text-xs text-zinc-400">{t('models.noImportance')}</p>}
                          </div>
                        </div>
                      )}
                    </td>
                  </tr>
                )}
              </>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function TrainTab() {
  const { t } = useTranslation();
  const [registry, setRegistry] = useState<RegistryInfo | null>(null);
  const [modelType, setModelType] = useState("lgbm");
  const [tag, setTag] = useState("");
  const [selectedFactors, setSelectedFactors] = useState<string[]>([]);
  const [fitStart, setFitStart] = useState("");
  const [fitEnd, setFitEnd] = useState("");
  const [qlibNative, setQlibNative] = useState(false);
  const [dryRun, setDryRun] = useState(true);
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    get<RegistryInfo>("/models/registry").then((reg) => { setRegistry(reg); if (reg.models.length > 0) setModelType(reg.models[0].name); }).catch(() => {});
  }, []);

  const toggleFactor = (name: string) => {
    setSelectedFactors((prev) => prev.includes(name) ? prev.filter((f) => f !== name) : [...prev, name]);
  };

  const handleTrain = async () => {
    setStatus(t('models.submitting'));
    setError(null);
    try {
      const body: any = { model: modelType, qlib_native: qlibNative, factors: selectedFactors };
      if (tag.trim()) body.tag = tag.trim();
      if (fitStart) body.fit_start = fitStart;
      if (fitEnd) body.fit_end = fitEnd;
      const res = await post<{ task_id: string }>("/models/train", body);
      setStatus(`Training task submitted: ${res.task_id}`);
      const interval = setInterval(async () => {
        try {
          const tasks = await get<{ task_id: string; status: string; error?: string }[]>("/system/tasks");
          const task = tasks.find((tk) => tk.task_id === res.task_id);
          if (!task) return;
          if (task.status === "done") { setStatus("Training completed successfully."); clearInterval(interval); }
          else if (task.status === "failed") { setError(task.error || "Training failed"); setStatus(null); clearInterval(interval); }
          else { setStatus(`Task ${res.task_id}: ${task.status}...`); }
        } catch { clearInterval(interval); }
      }, 3000);
    } catch (err: any) { setError(err.message); setStatus(null); }
  };

  const inputCls = "w-full border border-zinc-300 rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-400";

  return (
    <div className="space-y-4 max-w-lg">
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('models.modelType')}</label>
        <select value={modelType} onChange={(e) => setModelType(e.target.value)} className={inputCls}>
          {registry?.models.map((m) => <option key={m.name} value={m.name}>{m.name}</option>)}
        </select>
      </div>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('models.tag')}</label>
        <input type="text" value={tag} onChange={(e) => setTag(e.target.value)} placeholder={t('models.tagPlaceholder')} className={inputCls} />
      </div>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-2">{t('models.factors')}</label>
        <div className="flex flex-wrap gap-2">
          {registry?.factors.map((f) => (
            <label key={f.name}
              className={`inline-flex items-center gap-1 px-2 py-1 border rounded text-xs cursor-pointer transition-colors ${
                selectedFactors.includes(f.name) ? "bg-amber-50 border-amber-400 text-amber-700" : "bg-white border-zinc-300 text-zinc-600 hover:bg-zinc-50"
              }`}>
              <input type="checkbox" checked={selectedFactors.includes(f.name)} onChange={() => toggleFactor(f.name)} className="rounded" />
              {f.name}
            </label>
          ))}
        </div>
      </div>
      <div className="grid grid-cols-2 gap-3">
        <div>
          <label className="block text-sm font-medium text-zinc-700 mb-1">{t('models.fitStart')}</label>
          <input type="date" value={fitStart} onChange={(e) => setFitStart(e.target.value)} className={inputCls} />
        </div>
        <div>
          <label className="block text-sm font-medium text-zinc-700 mb-1">{t('models.fitEnd')}</label>
          <input type="date" value={fitEnd} onChange={(e) => setFitEnd(e.target.value)} className={inputCls} />
        </div>
      </div>
      <div className="space-y-2">
        <label className="flex items-center gap-2 text-sm text-zinc-700 cursor-pointer">
          <input type="checkbox" checked={qlibNative} onChange={(e) => setQlibNative(e.target.checked)} className="rounded" />
          {t('models.qlibNative')}
        </label>
        <label className="flex items-center gap-2 text-sm text-zinc-700 cursor-pointer">
          <input type="checkbox" checked={dryRun} onChange={(e) => setDryRun(e.target.checked)} className="rounded" />
          {t('models.dryRun')}
        </label>
      </div>
      <button onClick={handleTrain} disabled={!!status && status.includes(t('models.submitting'))}
        className="px-4 py-2 bg-amber-500 text-zinc-900 font-medium rounded-md text-sm hover:bg-amber-600 disabled:opacity-50">
        {dryRun ? t('models.trainDryBtn') : t('models.trainBtn')}
      </button>
      {status && <p className={`text-sm ${error ? "text-red-600" : "text-green-700"}`}>{status}</p>}
      {error && <p className="text-sm text-red-600">{error}</p>}
    </div>
  );
}

function RegistryTab() {
  const { t } = useTranslation();
  const [registry, setRegistry] = useState<RegistryInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    get<RegistryInfo>("/models/registry").then(setRegistry).catch((err) => setError(err.message)).finally(() => setLoading(false));
  }, []);

  if (loading) return <p className="text-zinc-500 text-sm py-4">{t('common.loading')}</p>;
  if (error) return <p className="text-red-600 text-sm py-4">{t('common.error')}: {error}</p>;
  if (!registry) return null;

  const TableSection = ({ title, items }: { title: string; items: { name: string }[] }) => (
    <div>
      <h3 className="text-lg font-semibold text-zinc-800 mb-3">{title}</h3>
      <div className="bg-white border border-zinc-200 rounded-lg overflow-hidden shadow-sm">
        <table className="w-full text-sm">
          <thead className="bg-zinc-50 border-b border-zinc-200">
            <tr>
              <th className="text-left px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.name')}</th>
              <th className="text-left px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.class')}</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => (
              <tr key={item.name} className="border-t border-zinc-100 hover:bg-zinc-50">
                <td className="px-4 py-2 font-mono text-sm text-zinc-700">{item.name}</td>
                <td className="px-4 py-2 text-xs text-zinc-400">{item.name.charAt(0).toUpperCase() + item.name.slice(1)}AlphaModel</td>
              </tr>
            ))}
            {items.length === 0 && (
              <tr><td colSpan={2} className="px-4 py-4 text-center text-zinc-400 text-sm">{t('models.noRegistered')}</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );

  return (
    <div className="space-y-6">
      <TableSection title={t('models.registeredModels', { count: registry.models.length })} items={registry.models} />
      <TableSection title={t('models.registeredFactors', { count: registry.factors.length })} items={registry.factors} />
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex
git add web/frontend/src/pages/ModelsPage.tsx
git commit -m "style: restyle ModelsPage with zinc palette and add i18n"
```

---

## Task 7: BacktestPage restyle + i18n

**Files:**
- Modify: `web/frontend/src/pages/BacktestPage.tsx`

- [ ] **Step 1: Replace `BacktestPage.tsx`**

The page currently uses dark-theme classes (`bg-gray-800`, `text-gray-300`) mixed in — replace all with the zinc/amber light theme. All logic is preserved unchanged.

```tsx
import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { get, post } from "../api/client";

interface ModelInfo { filename: string; size_mb: number; modified: string; meta: Record<string, unknown>; }
interface ResultFile { filename: string; size_kb: number; modified: string; }
interface ResultData { columns: string[]; rows: Record<string, unknown>[]; }
type Tab = "grid" | "results" | "wfv";

export function BacktestPage() {
  const { t } = useTranslation();
  const [tab, setTab] = useState<Tab>("grid");

  return (
    <div className="space-y-4 max-w-5xl">
      <h2 className="text-2xl font-bold text-zinc-900">{t('backtest.title')}</h2>
      <div className="flex gap-1 border-b border-zinc-200">
        {(["grid", "results", "wfv"] as Tab[]).map((tb) => (
          <button key={tb} onClick={() => setTab(tb)}
            className={`px-4 py-2 text-sm font-medium -mb-px border-b-2 transition-colors ${
              tab === tb ? "border-amber-500 text-amber-600" : "border-transparent text-zinc-500 hover:text-zinc-700"
            }`}>
            {tb === "grid" ? t('backtest.gridTab') : tb === "results" ? t('backtest.resultsTab') : t('backtest.wfvTab')}
          </button>
        ))}
      </div>
      {tab === "grid" && <GridSearchTab />}
      {tab === "results" && <ResultsTab />}
      {tab === "wfv" && <WalkForwardTab />}
    </div>
  );
}

function GridSearchTab() {
  const { t } = useTranslation();
  const [models, setModels] = useState<ModelInfo[]>([]);
  const [modelPath, setModelPath] = useState("");
  const [market, setMarket] = useState("csi300");
  const [topk, setTopk] = useState("5,10,15,20");
  const [nDrop, setNDrop] = useState("1,3,5");
  const [holdThresh, setHoldThresh] = useState("3,5,10");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [multiSeed, setMultiSeed] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [taskId, setTaskId] = useState<string | null>(null);
  const [taskStatus, setTaskStatus] = useState<string | null>(null);

  useEffect(() => {
    get<ModelInfo[]>("/models").then((data) => { setModels(data); if (data.length > 0 && !modelPath) setModelPath(data[0].filename); });
  }, []);

  const handleSubmit = async () => {
    setSubmitting(true); setTaskStatus(null);
    try {
      const res = await post<{ task_id: string }>("/backtest/grid", {
        model_path: modelPath,
        topk: topk.split(",").map((s) => parseInt(s.trim())).filter((n) => !isNaN(n)),
        n_drop: nDrop.split(",").map((s) => parseInt(s.trim())).filter((n) => !isNaN(n)),
        hold_thresh: holdThresh.split(",").map((s) => parseInt(s.trim())).filter((n) => !isNaN(n)),
        start: startDate || null, end: endDate || null, market, multi_seed: multiSeed,
      });
      setTaskId(res.task_id);
      const interval = setInterval(async () => {
        try {
          const tasks = await get<{ task_id: string; status: string; error?: string }[]>("/system/tasks");
          const tk = tasks.find((x) => x.task_id === res.task_id);
          if (tk) { setTaskStatus(tk.status); if (["done","failed","cancelled"].includes(tk.status)) clearInterval(interval); }
        } catch { clearInterval(interval); }
      }, 2000);
    } catch (err) { setTaskStatus(`Error: ${err}`); }
    finally { setSubmitting(false); }
  };

  const inputCls = "w-full border border-zinc-300 rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-400";

  return (
    <div className="max-w-2xl space-y-4">
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.model')}</label>
        <select value={modelPath} onChange={(e) => setModelPath(e.target.value)} className={inputCls}>
          {models.length === 0 && <option value="">{t('backtest.noModels')}</option>}
          {models.map((m) => <option key={m.filename} value={m.filename}>{m.filename} ({m.size_mb} MB)</option>)}
        </select>
      </div>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.market')}</label>
        <select value={market} onChange={(e) => setMarket(e.target.value)} className={inputCls}>
          <option value="csi300">CSI 300</option>
          <option value="csi500">CSI 500</option>
          <option value="csi800">CSI 800</option>
          <option value="csi1000">CSI 1000</option>
        </select>
      </div>
      <div className="grid grid-cols-3 gap-4">
        {[["topk","backtest.topk","5,10,15,20"],["nDrop","backtest.nDrop","1,3,5"],["holdThresh","backtest.holdThresh","3,5,10"]].map(([id, labelKey, placeholder]) => (
          <div key={id}>
            <label className="block text-sm font-medium text-zinc-700 mb-1">{t(labelKey)}</label>
            <input type="text" value={id === "topk" ? topk : id === "nDrop" ? nDrop : holdThresh}
              onChange={(e) => id === "topk" ? setTopk(e.target.value) : id === "nDrop" ? setNDrop(e.target.value) : setHoldThresh(e.target.value)}
              className={inputCls} placeholder={placeholder} />
          </div>
        ))}
      </div>
      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.startDate')}</label>
          <input type="date" value={startDate} onChange={(e) => setStartDate(e.target.value)} className={inputCls} />
        </div>
        <div>
          <label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.endDate')}</label>
          <input type="date" value={endDate} onChange={(e) => setEndDate(e.target.value)} className={inputCls} />
        </div>
      </div>
      <label className="flex items-center gap-2 text-sm text-zinc-700 cursor-pointer">
        <input type="checkbox" checked={multiSeed} onChange={(e) => setMultiSeed(e.target.checked)} className="rounded" />
        {t('backtest.multiSeed')}
      </label>
      <button onClick={handleSubmit} disabled={submitting || !modelPath}
        className="px-4 py-2 bg-amber-500 text-zinc-900 font-medium rounded-md text-sm hover:bg-amber-600 disabled:opacity-50">
        {submitting ? t('common.starting') : t('backtest.runGrid')}
      </button>
      {taskId && (
        <div className="mt-4 p-3 bg-zinc-50 border border-zinc-200 rounded-lg text-sm">
          <p className="text-zinc-600">{t('common.taskId')}: <span className="font-mono text-amber-600">{taskId}</span></p>
          {taskStatus && (
            <p className="mt-1 text-zinc-600">{t('common.status')}: <span className={
              taskStatus === "done" ? "text-green-600" : taskStatus === "failed" ? "text-red-600" : taskStatus === "running" ? "text-amber-600" : "text-zinc-400"
            }>{taskStatus}</span></p>
          )}
        </div>
      )}
    </div>
  );
}

function ResultsTab() {
  const { t } = useTranslation();
  const [results, setResults] = useState<ResultFile[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [data, setData] = useState<ResultData | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => { get<ResultFile[]>("/backtest/results").then(setResults); }, []);

  const loadResult = async (filename: string) => {
    setSelected(filename); setLoading(true);
    try { const res = await get<ResultData>(`/backtest/results/${filename}`); setData(res); }
    catch { setData(null); }
    finally { setLoading(false); }
  };

  return (
    <div>
      <h3 className="text-lg font-semibold text-zinc-800 mb-3">{t('backtest.resultsTab')}</h3>
      {results.length === 0 ? (
        <p className="text-zinc-500 text-sm">{t('backtest.noResults')}</p>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <div className="lg:col-span-1">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-zinc-400 border-b border-zinc-200">
                  <th className="text-left py-2 text-xs uppercase tracking-wide">File</th>
                  <th className="text-right py-2 text-xs uppercase tracking-wide">{t('common.sizeKb')}</th>
                </tr>
              </thead>
              <tbody>
                {results.map((r) => (
                  <tr key={r.filename} onClick={() => loadResult(r.filename)}
                    className={`cursor-pointer border-b border-zinc-100 hover:bg-zinc-50 ${selected === r.filename ? "bg-zinc-50" : ""}`}>
                    <td className="py-2 text-amber-600 font-mono text-xs">{r.filename}</td>
                    <td className="text-right py-2 text-zinc-400">{r.size_kb} KB</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="lg:col-span-2">
            {loading && <p className="text-zinc-400 text-sm">{t('common.loading')}</p>}
            {!loading && !selected && <p className="text-zinc-500 text-sm">{t('backtest.selectResult')}</p>}
            {!loading && data && data.columns && (
              <div className="overflow-auto max-h-[600px] border border-zinc-200 rounded-lg">
                <table className="w-full text-xs">
                  <thead className="sticky top-0 bg-zinc-50 border-b border-zinc-200">
                    <tr>
                      {data.columns.map((col) => (
                        <th key={col} className="text-left py-2 px-2 whitespace-nowrap text-zinc-500 font-medium">{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {data.rows.map((row, i) => (
                      <tr key={i} className="border-b border-zinc-100 hover:bg-zinc-50">
                        {data.columns.map((col) => (
                          <td key={col} className="py-1 px-2 whitespace-nowrap text-zinc-700">
                            {typeof row[col] === "number" ? (row[col] as number).toFixed(4) : String(row[col] ?? "")}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function WalkForwardTab() {
  const { t } = useTranslation();
  const [trainUniverses, setTrainUniverses] = useState("csi300");
  const [evalMarket, setEvalMarket] = useState("csi300");
  const [topk, setTopk] = useState("5,15,20");
  const [nDrop, setNDrop] = useState("1,3");
  const [holdThresh, setHoldThresh] = useState("5,8,10");
  const [workers, setWorkers] = useState(1);
  const [submitting, setSubmitting] = useState(false);
  const [taskId, setTaskId] = useState<string | null>(null);
  const [taskStatus, setTaskStatus] = useState<string | null>(null);

  const handleSubmit = async () => {
    setSubmitting(true); setTaskStatus(null);
    try {
      const res = await post<{ task_id: string }>("/backtest/walk-forward", {
        train_universes: trainUniverses.split(",").map((s) => s.trim()),
        eval_market: evalMarket,
        topk: topk.split(",").map((s) => parseInt(s.trim())).filter((n) => !isNaN(n)),
        n_drop: nDrop.split(",").map((s) => parseInt(s.trim())).filter((n) => !isNaN(n)),
        hold_thresh: holdThresh.split(",").map((s) => parseInt(s.trim())).filter((n) => !isNaN(n)),
        workers,
      });
      setTaskId(res.task_id);
      const interval = setInterval(async () => {
        try {
          const tasks = await get<{ task_id: string; status: string; error?: string }[]>("/system/tasks");
          const tk = tasks.find((x) => x.task_id === res.task_id);
          if (tk) { setTaskStatus(tk.status); if (["done","failed","cancelled"].includes(tk.status)) clearInterval(interval); }
        } catch { clearInterval(interval); }
      }, 2000);
    } catch (err) { setTaskStatus(`Error: ${err}`); }
    finally { setSubmitting(false); }
  };

  const inputCls = "w-full border border-zinc-300 rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-400";

  return (
    <div className="max-w-2xl space-y-4">
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.trainUniverses')}</label>
        <input type="text" value={trainUniverses} onChange={(e) => setTrainUniverses(e.target.value)} className={inputCls} placeholder="csi300,csi800" />
      </div>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.evalMarket')}</label>
        <select value={evalMarket} onChange={(e) => setEvalMarket(e.target.value)} className={inputCls}>
          <option value="csi300">CSI 300</option>
          <option value="csi500">CSI 500</option>
          <option value="csi800">CSI 800</option>
          <option value="csi1000">CSI 1000</option>
        </select>
      </div>
      <div className="grid grid-cols-3 gap-4">
        <div><label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.topk')}</label><input type="text" value={topk} onChange={(e) => setTopk(e.target.value)} className={inputCls} /></div>
        <div><label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.nDrop')}</label><input type="text" value={nDrop} onChange={(e) => setNDrop(e.target.value)} className={inputCls} /></div>
        <div><label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.holdThresh')}</label><input type="text" value={holdThresh} onChange={(e) => setHoldThresh(e.target.value)} className={inputCls} /></div>
      </div>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.workers')}</label>
        <input type="number" value={workers} onChange={(e) => setWorkers(parseInt(e.target.value) || 1)} min={1} max={8} className={inputCls} />
      </div>
      <button onClick={handleSubmit} disabled={submitting}
        className="px-4 py-2 bg-amber-500 text-zinc-900 font-medium rounded-md text-sm hover:bg-amber-600 disabled:opacity-50">
        {submitting ? t('common.starting') : t('backtest.runWfv')}
      </button>
      {taskId && (
        <div className="mt-4 p-3 bg-zinc-50 border border-zinc-200 rounded-lg text-sm">
          <p className="text-zinc-600">{t('common.taskId')}: <span className="font-mono text-amber-600">{taskId}</span></p>
          {taskStatus && (
            <p className="mt-1 text-zinc-600">{t('common.status')}: <span className={taskStatus === "done" ? "text-green-600" : taskStatus === "failed" ? "text-red-600" : taskStatus === "running" ? "text-amber-600" : "text-zinc-400"}>{taskStatus}</span></p>
          )}
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex
git add web/frontend/src/pages/BacktestPage.tsx
git commit -m "style: restyle BacktestPage with zinc palette and add i18n"
```

---

## Task 8: SignalsPage restyle + i18n

**Files:**
- Modify: `web/frontend/src/pages/SignalsPage.tsx`

- [ ] **Step 1: Replace `SignalsPage.tsx`**

Same zinc/amber pattern as previous pages. Dark bg-gray-800 classes replaced throughout.

```tsx
import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { get, post } from "../api/client";

interface ModelInfo { filename: string; size_mb: number; modified: string; meta: Record<string, unknown>; }
interface SignalFile { filename: string; size_kb: number; modified: string; }
interface SignalContent { content: string; }
interface RegimeInfo { enabled: boolean; regime: number | null; label: string | null; error?: string; }
type Tab = "generate" | "history" | "rebalance" | "notification";

export function SignalsPage() {
  const { t } = useTranslation();
  const [tab, setTab] = useState<Tab>("generate");

  const tabs: { key: Tab; label: string }[] = [
    { key: "generate", label: t('signals.generateTab') },
    { key: "history", label: t('signals.historyTab') },
    { key: "rebalance", label: t('signals.rebalanceTab') },
    { key: "notification", label: t('signals.notificationTab') },
  ];

  return (
    <div className="space-y-4 max-w-5xl">
      <h2 className="text-2xl font-bold text-zinc-900">{t('signals.title')}</h2>
      <div className="flex gap-1 border-b border-zinc-200">
        {tabs.map((tb) => (
          <button key={tb.key} onClick={() => setTab(tb.key)}
            className={`px-4 py-2 text-sm font-medium -mb-px border-b-2 transition-colors ${
              tab === tb.key ? "border-amber-500 text-amber-600" : "border-transparent text-zinc-500 hover:text-zinc-700"
            }`}>
            {tb.label}
          </button>
        ))}
      </div>
      {tab === "generate" && <GenerateTab />}
      {tab === "history" && <HistoryTab />}
      {tab === "rebalance" && <RebalanceTab />}
      {tab === "notification" && <NotificationTab />}
    </div>
  );
}

function GenerateTab() {
  const { t } = useTranslation();
  const [models, setModels] = useState<ModelInfo[]>([]);
  const [modelPath, setModelPath] = useState("");
  const [account, setAccount] = useState("1000000");
  const [positions, setPositions] = useState("");
  const [dryRun, setDryRun] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [taskId, setTaskId] = useState<string | null>(null);
  const [taskStatus, setTaskStatus] = useState<string | null>(null);
  const [regime, setRegime] = useState<RegimeInfo | null>(null);

  useEffect(() => {
    get<ModelInfo[]>("/models").then((data) => { setModels(data); if (data.length > 0 && !modelPath) setModelPath(data[0].filename); });
    get<RegimeInfo>("/signals/regime").then(setRegime).catch(() => {});
  }, []);

  const handleSubmit = async () => {
    setSubmitting(true); setTaskStatus(null);
    try {
      const res = await post<{ task_id: string }>("/signals/generate", {
        model_path: modelPath, account: parseFloat(account) || 1000000, positions: positions || null, dry_run: dryRun,
      });
      setTaskId(res.task_id);
      const interval = setInterval(async () => {
        try {
          const tasks = await get<{ task_id: string; status: string; error?: string }[]>("/system/tasks");
          const tk = tasks.find((x) => x.task_id === res.task_id);
          if (tk) { setTaskStatus(tk.status); if (["done","failed","cancelled"].includes(tk.status)) clearInterval(interval); }
        } catch { clearInterval(interval); }
      }, 2000);
    } catch (err) { setTaskStatus(`Error: ${err}`); }
    finally { setSubmitting(false); }
  };

  const inputCls = "w-full border border-zinc-300 rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-400";

  return (
    <div className="max-w-2xl space-y-4">
      {regime && (
        <div className="p-3 bg-zinc-50 border border-zinc-200 rounded-lg text-sm">
          <span className="text-zinc-500">{t('signals.regime')}: </span>
          {regime.enabled ? (
            <span className="inline-block px-2 py-0.5 bg-amber-100 text-amber-700 rounded text-xs font-medium">{regime.label ?? t('common.enabled')}</span>
          ) : (
            <span className="text-zinc-400">{t('common.disabled')}</span>
          )}
        </div>
      )}
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('signals.model')}</label>
        <select value={modelPath} onChange={(e) => setModelPath(e.target.value)} className={inputCls}>
          {models.length === 0 && <option value="">{t('signals.noModels')}</option>}
          {models.map((m) => <option key={m.filename} value={m.filename}>{m.filename} ({m.size_mb} MB)</option>)}
        </select>
      </div>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('signals.account')}</label>
        <input type="number" value={account} onChange={(e) => setAccount(e.target.value)} className={inputCls} />
      </div>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('signals.positions')}</label>
        <textarea value={positions} onChange={(e) => setPositions(e.target.value)} rows={3}
          className={`${inputCls} font-mono`} placeholder={t('signals.positionsPlaceholder')} />
      </div>
      <label className="flex items-center gap-2 text-sm text-zinc-700 cursor-pointer">
        <input type="checkbox" checked={dryRun} onChange={(e) => setDryRun(e.target.checked)} className="rounded" />
        {t('signals.dryRun')}
      </label>
      <button onClick={handleSubmit} disabled={submitting || !modelPath}
        className="px-4 py-2 bg-amber-500 text-zinc-900 font-medium rounded-md text-sm hover:bg-amber-600 disabled:opacity-50">
        {submitting ? t('common.starting') : t('signals.generateBtn')}
      </button>
      {taskId && (
        <div className="mt-4 p-3 bg-zinc-50 border border-zinc-200 rounded-lg text-sm">
          <p className="text-zinc-600">{t('common.taskId')}: <span className="font-mono text-amber-600">{taskId}</span></p>
          {taskStatus && <p className="mt-1 text-zinc-600">{t('common.status')}: <span className={taskStatus === "done" ? "text-green-600" : taskStatus === "failed" ? "text-red-600" : "text-amber-600"}>{taskStatus}</span></p>}
        </div>
      )}
    </div>
  );
}

function HistoryTab() {
  const { t } = useTranslation();
  const [files, setFiles] = useState<SignalFile[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [content, setContent] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => { get<SignalFile[]>("/signals/history").then(setFiles); }, []);

  const loadFile = async (filename: string) => {
    setSelected(filename); setLoading(true);
    try { const res = await get<SignalContent>(`/signals/history/${filename}`); setContent(res.content); }
    catch { setContent(null); }
    finally { setLoading(false); }
  };

  return (
    <div>
      <h3 className="text-lg font-semibold text-zinc-800 mb-3">{t('signals.historyTab')}</h3>
      {files.length === 0 ? (
        <p className="text-zinc-500 text-sm">{t('signals.noHistory')}</p>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <div className="lg:col-span-1">
            <table className="w-full text-sm">
              <thead><tr className="text-zinc-400 border-b border-zinc-200">
                <th className="text-left py-2 text-xs uppercase tracking-wide">File</th>
                <th className="text-right py-2 text-xs uppercase tracking-wide">{t('common.sizeKb')}</th>
              </tr></thead>
              <tbody>
                {files.map((f) => (
                  <tr key={f.filename} onClick={() => loadFile(f.filename)}
                    className={`cursor-pointer border-b border-zinc-100 hover:bg-zinc-50 ${selected === f.filename ? "bg-zinc-50" : ""}`}>
                    <td className="py-2 text-amber-600 font-mono text-xs">{f.filename}</td>
                    <td className="text-right py-2 text-zinc-400">{f.size_kb} KB</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="lg:col-span-2">
            {loading && <p className="text-zinc-400 text-sm">{t('common.loading')}</p>}
            {!loading && !selected && <p className="text-zinc-500 text-sm">{t('common.selectFile')}</p>}
            {!loading && content && (
              <pre className="bg-zinc-900 border border-zinc-700 rounded-lg p-4 text-xs text-zinc-300 overflow-auto max-h-[600px] whitespace-pre-wrap">{content}</pre>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function RebalanceTab() {
  const { t } = useTranslation();
  const [mock, setMock] = useState(false);
  const [dryRun, setDryRun] = useState(true);
  const [message, setMessage] = useState<string | null>(null);

  const handleRun = () => {
    setMessage(`Scheduled rebalance with mock=${mock}, dry-run=${dryRun}.\n${t('signals.rebalanceNote')}`);
  };

  return (
    <div className="max-w-2xl space-y-4">
      <p className="text-zinc-500 text-sm">{t('signals.rebalanceNote')}</p>
      <div className="flex items-center gap-6">
        <label className="flex items-center gap-2 text-sm text-zinc-700 cursor-pointer">
          <input type="checkbox" checked={mock} onChange={(e) => setMock(e.target.checked)} className="rounded" />
          {t('signals.mockMode')}
        </label>
        <label className="flex items-center gap-2 text-sm text-zinc-700 cursor-pointer">
          <input type="checkbox" checked={dryRun} onChange={(e) => setDryRun(e.target.checked)} className="rounded" />
          {t('signals.dryRun')}
        </label>
      </div>
      <button onClick={handleRun} className="px-4 py-2 bg-amber-500 text-zinc-900 font-medium rounded-md text-sm hover:bg-amber-600">
        {t('signals.runRebalance')}
      </button>
      {message && <pre className="bg-zinc-50 border border-zinc-200 rounded-lg p-4 text-sm text-zinc-700 whitespace-pre-wrap">{message}</pre>}
    </div>
  );
}

function NotificationTab() {
  const { t } = useTranslation();
  const [title, setTitle] = useState("");
  const [content, setContent] = useState("");
  const [sending, setSending] = useState(false);
  const [result, setResult] = useState<string | null>(null);

  const handleSend = async () => {
    setSending(true); setResult(null);
    try { setResult(`Test notification sent.\nTitle: ${title}\nContent: ${content}`); }
    catch (err) { setResult(`Error: ${err}`); }
    finally { setSending(false); }
  };

  const inputCls = "w-full border border-zinc-300 rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-400";

  return (
    <div className="max-w-2xl space-y-4">
      <p className="text-zinc-500 text-sm">{t('signals.notifNote')}</p>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('signals.notifTitle')}</label>
        <input type="text" value={title} onChange={(e) => setTitle(e.target.value)} className={inputCls} placeholder="Test Notification" />
      </div>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('signals.notifContent')}</label>
        <textarea value={content} onChange={(e) => setContent(e.target.value)} rows={5} className={inputCls} placeholder={t('signals.notifPlaceholder')} />
      </div>
      <button onClick={handleSend} disabled={sending || !title}
        className="px-4 py-2 bg-amber-500 text-zinc-900 font-medium rounded-md text-sm hover:bg-amber-600 disabled:opacity-50">
        {sending ? t('common.sending') : t('signals.sendTest')}
      </button>
      {result && <pre className="bg-zinc-50 border border-zinc-200 rounded-lg p-4 text-sm text-zinc-700 whitespace-pre-wrap">{result}</pre>}
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex
git add web/frontend/src/pages/SignalsPage.tsx
git commit -m "style: restyle SignalsPage with zinc palette and add i18n"
```

---

## Task 9: FactorsPage, ConfigPage, SystemPage restyle + i18n

**Files:**
- Modify: `web/frontend/src/pages/FactorsPage.tsx`
- Modify: `web/frontend/src/pages/ConfigPage.tsx`
- Modify: `web/frontend/src/pages/SystemPage.tsx`

These three pages follow the exact same zinc/amber class transformation. The rules to apply are:

| Old class | New class |
|-----------|-----------|
| `text-gray-500` | `text-zinc-500` |
| `text-gray-700` | `text-zinc-700` |
| `text-gray-400` | `text-zinc-400` |
| `text-gray-600` | `text-zinc-600` |
| `bg-gray-50` | `bg-zinc-50` |
| `border-gray-200` | `border-zinc-200` |
| `border-gray-300` | `border-zinc-300` |
| `border-b` (tab bar) | `border-b border-zinc-200` |
| `border-blue-600 text-blue-600` (active tab) | `border-amber-500 text-amber-600` |
| `bg-blue-600 text-white` (primary button) | `bg-amber-500 text-zinc-900 font-medium` |
| `hover:bg-blue-700` | `hover:bg-amber-600` |
| `bg-blue-50 border-blue-400 text-blue-700` (factor chip active) | `bg-amber-50 border-amber-400 text-amber-700` |
| `bg-green-100 text-green-800` (enabled badge) | `bg-green-100 text-green-700` |
| `border rounded` (input) | `border border-zinc-300 rounded-md bg-white focus:outline-none focus:ring-2 focus:ring-amber-400` |
| `bg-red-50 border-red-200 text-red-700` (error box) | same (already good) |
| `bg-gray-900 text-gray-100` (log viewer) | `bg-zinc-900 text-zinc-100` |

Also add `useTranslation` import and wrap all string literals in `t()` calls using the keys defined in Task 1.

- [ ] **Step 1: Update `FactorsPage.tsx`**

Apply the class mapping table above to every Tailwind class in the file. Add `import { useTranslation } from "react-i18next"` and `const { t } = useTranslation()` to each component function. Replace strings:

- `"Factors"` → `{t('factors.title')}`
- `"Library"` tab label → `{t('factors.libraryTab')}`
- `"Evaluation"` → `{t('factors.evaluationTab')}`
- `"Mining"` → `{t('factors.miningTab')}`
- `"No factors registered"` → `{t('factors.noFactors')}`
- `"Select Factor"` label → `{t('factors.selectFactor')}`
- `"Min IC"` → `{t('factors.minIc')}`
- `"Min ICIR"` → `{t('factors.minIcir')}`
- `"Top N"` → `{t('factors.topN')}`
- `"Evaluating..."` → `{t('common.evaluating')}`
- `"Evaluate"` → `{t('common.evaluate')}`
- `"Mining..."` → `{t('factors.mining')}`
- `"Start Mining"` → `{t('factors.startMining')}`
- `"Loading..."` → `{t('common.loading')}`
- `"Task submitted. ID: "` → `{t('factors.taskSubmitted')}`

- [ ] **Step 2: Update `ConfigPage.tsx`**

Apply the class mapping and add i18n. Key string replacements:
- `"Config"` → `{t('config.title')}`
- Tab labels: `"Editor"` → `{t('config.editorTab')}`, `"Strategy Candidates"` → `{t('config.strategyTab')}`, `"Regime Rules"` → `{t('config.regimeTab')}`
- `"Regime Switching:"` → `{t('config.regimeSwitching')}`
- `"Regime rules are read-only. Edit base.yaml to modify."` → `{t('config.regimeNote')}`
- `"No regime rules found in config"` → `{t('config.noRegimeRules')}`
- `"Saved successfully"` → `{t('config.savedOk')}`
- Column headers: Key→`{t('config.key')}`, Regime→`{t('config.regime')}`, Top K→`{t('config.topk')}`, N Drop→`{t('config.nDrop')}`, Hold Thresh→`{t('config.holdThresh')}`
- Buttons: `"Save"` → `{t('common.save')}`, `"Saving..."` → `{t('common.saving')}`, `"Reload"` → `{t('common.reload')}`
- Replace `bg-blue-600 text-white border-blue-600` (config file selector active) with `bg-amber-500 text-zinc-900 border-amber-500`

- [ ] **Step 3: Update `SystemPage.tsx`**

Apply the class mapping and add i18n. Key string replacements:
- `"System"` → `{t('system.title')}`
- Tab labels: `"Runtime"` → `{t('system.runtimeTab')}`, `"Logs"` → `{t('system.logsTab')}`, `"Cache"` → `{t('system.cacheTab')}`
- `"Python Version"` → `{t('system.pythonVersion')}`
- `"qlib Data Path"` → `{t('system.qlibPath')}`
- `"Saved Models"` → `{t('system.savedModels')}`
- `"No log entries found"` → `{t('system.noLogs')}`
- `"No cache data"` → `{t('system.noCache')}`
- `"Refresh"` button → `{t('common.refresh')}`
- `"Delete Expired"` → `{t('common.deleteExpired')}`
- `"Deleting..."` → `{t('common.deleting')}`
- File: line → `{t('system.logFile', { file: logFile })}`
- Column headers use `t('common.type')`, `t('common.files')`, `t('common.sizeMb')`, `t('common.latest')`, `t('common.actions')`

- [ ] **Step 4: Commit**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex
git add web/frontend/src/pages/FactorsPage.tsx web/frontend/src/pages/ConfigPage.tsx web/frontend/src/pages/SystemPage.tsx
git commit -m "style: restyle FactorsPage, ConfigPage, SystemPage with zinc palette and add i18n"
```

---

## Task 10: Build verification

**Files:** none (verification only)

- [ ] **Step 1: Run TypeScript build**

```bash
cd web/frontend && npm run build 2>&1 | tail -30
```

Expected: `✓ built in Xs` with no TypeScript errors.

- [ ] **Step 2: Fix any TS errors**

Common issues to look for:
- Missing `key` props on mapped elements (add `key={item.to}` etc.)
- `const` type inference on `NAV_ITEMS` — the `as const` cast in Sidebar.tsx handles this
- Any `t()` call using a key that doesn't exist in both JSON files — cross-check keys

- [ ] **Step 3: Smoke test in dev server**

```bash
cd web/frontend && npm run dev
```

Open http://localhost:5173 and verify:
1. Sidebar shows dark zinc background, amber active item
2. Language toggle at bottom — click EN, all nav labels switch to English; click 中文, switch back
3. Refresh the page — language choice persists (stored in localStorage)
4. Dashboard page: white cards, zinc text, amber regime badge
5. Data / Models / Backtest / Signals / Factors / Config / System pages all render without errors

- [ ] **Step 4: Final commit**

```bash
cd /Users/weidian/code/algorithms/quant_x/strategy/claude/quant_ex
git add -A
git commit -m "chore: verify build passes after frontend redesign and i18n"
```
