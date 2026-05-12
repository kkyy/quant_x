import { lazy, Suspense } from "react";
import type { ReactNode } from "react";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { Layout } from "./components/Layout";
import { ErrorBoundary } from "./components/ui/ErrorBoundary";

const OverviewPage = lazy(() => import("./pages/OverviewPage").then((m) => ({ default: m.OverviewPage })));
const DataExplorerPage = lazy(() => import("./pages/DataExplorerPage").then((m) => ({ default: m.DataExplorerPage })));
const ResearchPage = lazy(() => import("./pages/ResearchPage").then((m) => ({ default: m.ResearchPage })));
const ModelsPage = lazy(() => import("./pages/ModelsPage").then((m) => ({ default: m.ModelsPage })));
const BacktestPage = lazy(() => import("./pages/BacktestPage").then((m) => ({ default: m.BacktestPage })));
const SignalsPage = lazy(() => import("./pages/SignalsPage").then((m) => ({ default: m.SignalsPage })));
const ConfigPage = lazy(() => import("./pages/ConfigPage").then((m) => ({ default: m.ConfigPage })));
const SystemPage = lazy(() => import("./pages/SystemPage").then((m) => ({ default: m.SystemPage })));

function PageBoundary({ children }: { children: ReactNode }) {
  return (
    <ErrorBoundary>
      <Suspense
        fallback={
          <div className="h-24 w-full animate-pulse rounded-sm border border-terminal-border bg-terminal-surface" />
        }
      >
        {children}
      </Suspense>
    </ErrorBoundary>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<PageBoundary><OverviewPage /></PageBoundary>} />
          <Route path="/data-explorer" element={<PageBoundary><DataExplorerPage /></PageBoundary>} />
          <Route path="/research" element={<PageBoundary><ResearchPage /></PageBoundary>} />
          <Route path="/models" element={<PageBoundary><ModelsPage /></PageBoundary>} />
          <Route path="/backtest" element={<PageBoundary><BacktestPage /></PageBoundary>} />
          <Route path="/signals" element={<PageBoundary><SignalsPage /></PageBoundary>} />
          <Route path="/config" element={<PageBoundary><ConfigPage /></PageBoundary>} />
          <Route path="/system" element={<PageBoundary><SystemPage /></PageBoundary>} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
