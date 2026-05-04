# Frontend Redesign + i18n Design Spec

**Date:** 2026-05-01  
**Scope:** `web/frontend/src/`

---

## Goals

1. Replace the current harsh black/white color scheme with a warm zinc palette that is easy on the eyes and professionally styled.
2. Add Chinese/English language switching across all 8 pages using `react-i18next`.

---

## 1. Color System

### Palette (Warm Zinc)

| Token | Value | Usage |
|-------|-------|-------|
| `bg-page` | `#fafafa` (zinc-50) | Main content background |
| `bg-surface` | `#ffffff` | Cards, panels |
| `bg-sidebar` | `#18181b` (zinc-900) | Sidebar |
| `bg-sidebar-hover` | `#27272a` (zinc-800) | Sidebar nav hover |
| `border` | `#e4e4e7` (zinc-200) | Panel borders, table dividers |
| `border-sidebar` | `#27272a` (zinc-800) | Sidebar internal borders |
| `text-primary` | `#18181b` (zinc-900) | Main content headings and body |
| `text-secondary` | `#71717a` (zinc-500) | Labels, captions, hints |
| `text-muted` | `#a1a1aa` (zinc-400) | Placeholder, disabled |
| `text-sidebar` | `#a1a1aa` (zinc-400) | Sidebar nav inactive |
| `text-sidebar-active` | `#111111` | Active nav item text (on amber bg) |
| `accent` | `#f59e0b` (amber-500) | Active nav highlight, primary buttons |
| `accent-hover` | `#d97706` (amber-600) | Button hover |
| `accent-muted` | `#fef3c7` (amber-100) | Badge backgrounds (e.g. regime label) |
| `accent-text` | `#b45309` (amber-700) | Badge text on amber-100 |
| `danger` | `#ef4444` | Destructive actions (delete buttons) |
| `danger-muted` | `#fee2e2` | Danger badge bg |
| `success` | `#22c55e` | Success states |
| `info-muted` | `#dbeafe` | Info badge bg |
| `info-text` | `#1d4ed8` | Info badge text |

### Application

- **Sidebar:** `bg-sidebar` background, `border-sidebar` right border. Active nav item: `accent` background, `text-sidebar-active` text. Inactive: `text-sidebar` with `bg-sidebar-hover` on hover.
- **Main area:** `bg-page` background, `p-6` padding.
- **Cards/panels:** `bg-surface` with `border` and `rounded-lg`. Subtle `shadow-sm` (not `shadow-md` — keep it flat).
- **Tables:** thead uses `bg-page` (not pure white), `border-b border` dividers, `hover:bg-zinc-50` on rows.
- **Buttons:** Primary: `bg-accent text-white hover:bg-accent-hover`. Secondary/outline: `border border-zinc-300 text-zinc-700 hover:bg-zinc-50`. Danger: `border border-red-300 text-red-600 hover:bg-red-50`.
- **Tabs (DataPage):** Active tab: `border-b-2 border-amber-500 text-amber-600`. Inactive: `text-zinc-500 hover:text-zinc-700`.
- **Form inputs/selects:** `border border-zinc-300 rounded-md px-3 py-2 bg-white focus:outline-none focus:ring-2 focus:ring-amber-400`.

### CSS Variables (index.css)

Replace the current variables with a Tailwind-compatible set of CSS custom properties. Since the project uses Tailwind v4, colors will be declared as `@theme` variables for use throughout.

---

## 2. Typography & Spacing

- Font: keep `system-ui, -apple-system, sans-serif`
- Page titles: `text-2xl font-bold text-zinc-900`
- Section headings: `text-lg font-semibold text-zinc-800`
- Body: `text-sm text-zinc-700`
- Captions/labels: `text-xs text-zinc-500 uppercase tracking-wide`
- Sidebar: `text-sm`

---

## 3. Sidebar Redesign

**Structure:**
```
┌─────────────────────┐
│ quant_ex            │  ← logo area, border-bottom
│ 量化选股系统         │
├─────────────────────┤
│ ◉ Dashboard         │  ← active (amber bg)
│ ◈ 数据管理          │
│ ◆ 模型              │
│ ◇ 回测              │
│ ▸ 信号              │
│ ⋄ 因子              │
│ ⚙ 配置              │
│ ⊙ 系统              │
├─────────────────────┤
│ [中文] [EN]         │  ← language toggle, bottom
└─────────────────────┘
```

The language toggle sits in a `border-t border-sidebar` footer section, fixed at the bottom of the sidebar. Two pill buttons: active language is `bg-amber-500 text-black`, inactive is `text-zinc-500 hover:text-zinc-300`.

---

## 4. i18n Architecture

### Library

`react-i18next` + `i18next`. No backend plugin needed — translations are bundled as static JSON.

### File Structure

```
web/frontend/src/
  i18n/
    index.ts          ← i18next init, exports `useT` re-export
    zh.json           ← Chinese translations (default)
    en.json           ← English translations
```

### Initialization (`i18n/index.ts`)

```typescript
import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';
import zh from './zh.json';
import en from './en.json';

i18n.use(initReactI18next).init({
  resources: { zh: { translation: zh }, en: { translation: en } },
  lng: localStorage.getItem('lang') ?? 'zh',
  fallbackLng: 'zh',
  interpolation: { escapeValue: false },
});

export default i18n;
```

Import `i18n/index.ts` once in `main.tsx` (side-effect import).

### Translation Key Structure

Keys use dot-notation namespaced by feature area:

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
    "system": "系统"
  },
  "dashboard": {
    "title": "Dashboard",
    "python": "Python",
    "models": "模型数量",
    "regime": "市场状态",
    "qlibPath": "qlib 数据路径",
    "cacheStatus": "缓存状态",
    "savedModels": "已保存模型",
    "noCache": "暂无缓存数据",
    "latest": "最新",
    "type": "类型",
    "files": "文件数",
    "sizeMb": "大小 MB",
    "filename": "文件名",
    "modified": "更新时间"
  },
  "data": { ... },
  "models": { ... },
  "backtest": { ... },
  "signals": { ... },
  "factors": { ... },
  "config": { ... },
  "system": { ... },
  "common": {
    "loading": "加载中...",
    "error": "错误",
    "refresh": "刷新",
    "search": "搜索",
    "submit": "提交",
    "cancel": "取消",
    "delete": "删除",
    "enabled": "已启用",
    "disabled": "已禁用",
    "noData": "暂无数据"
  }
}
```

### Language Persistence

`localStorage.setItem('lang', newLang)` on toggle. `i18n.changeLanguage(newLang)` triggers React re-render via `react-i18next` context.

### Usage in Components

```typescript
import { useTranslation } from 'react-i18next';

function MyComponent() {
  const { t } = useTranslation();
  return <h2>{t('dashboard.title')}</h2>;
}
```

---

## 5. Language Toggle Component

A `LanguageToggle` component in `components/LanguageToggle.tsx`:

```typescript
export function LanguageToggle() {
  const { i18n } = useTranslation();
  const langs = [
    { code: 'zh', label: '中文' },
    { code: 'en', label: 'EN' },
  ];
  return (
    <div className="flex gap-1 p-3 border-t border-zinc-800">
      {langs.map(({ code, label }) => (
        <button
          key={code}
          onClick={() => { i18n.changeLanguage(code); localStorage.setItem('lang', code); }}
          className={i18n.language === code ? 'active-style' : 'inactive-style'}
        >
          {label}
        </button>
      ))}
    </div>
  );
}
```

Mounted at the bottom of `Sidebar.tsx`.

---

## 6. Files Changed

| File | Change |
|------|--------|
| `web/frontend/package.json` | Add `i18next`, `react-i18next` |
| `web/frontend/src/index.css` | Replace CSS variables, add `@theme` tokens |
| `web/frontend/src/components/Sidebar.tsx` | New styles + `LanguageToggle` |
| `web/frontend/src/components/Layout.tsx` | Minor: `bg-page` on main |
| `web/frontend/src/components/LanguageToggle.tsx` | New file |
| `web/frontend/src/i18n/index.ts` | New file |
| `web/frontend/src/i18n/zh.json` | New file |
| `web/frontend/src/i18n/en.json` | New file |
| `web/frontend/src/main.tsx` | Import `i18n/index.ts` |
| `web/frontend/src/pages/DashboardPage.tsx` | Restyled + `useTranslation` |
| `web/frontend/src/pages/DataPage.tsx` | Restyled + `useTranslation` |
| `web/frontend/src/pages/ModelsPage.tsx` | Restyled + `useTranslation` |
| `web/frontend/src/pages/BacktestPage.tsx` | Restyled + `useTranslation` |
| `web/frontend/src/pages/SignalsPage.tsx` | Restyled + `useTranslation` |
| `web/frontend/src/pages/FactorsPage.tsx` | Restyled + `useTranslation` |
| `web/frontend/src/pages/ConfigPage.tsx` | Restyled + `useTranslation` |
| `web/frontend/src/pages/SystemPage.tsx` | Restyled + `useTranslation` |
| `.gitignore` | Add `.superpowers/` |

---

## 7. Out of Scope

- Dark mode toggle (user chose light mode only)
- Backend/API changes
- Recharts chart theming (charts keep default colors)
- New pages or features
