import { BrowserRouter, Routes, Route } from "react-router-dom";
import { Layout } from "./components/Layout";
import { ErrorBoundary } from "./components/ui/ErrorBoundary";
import { OverviewPage } from "./pages/OverviewPage";
import { DataExplorerPage } from "./pages/DataExplorerPage";
import { ResearchPage } from "./pages/ResearchPage";
import { ModelsPage } from "./pages/ModelsPage";
import { BacktestPage } from "./pages/BacktestPage";
import { SignalsPage } from "./pages/SignalsPage";
import { ConfigPage } from "./pages/ConfigPage";
import { SystemPage } from "./pages/SystemPage";

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<ErrorBoundary><OverviewPage /></ErrorBoundary>} />
          <Route path="/data-explorer" element={<ErrorBoundary><DataExplorerPage /></ErrorBoundary>} />
          <Route path="/research" element={<ErrorBoundary><ResearchPage /></ErrorBoundary>} />
          <Route path="/models" element={<ErrorBoundary><ModelsPage /></ErrorBoundary>} />
          <Route path="/backtest" element={<ErrorBoundary><BacktestPage /></ErrorBoundary>} />
          <Route path="/signals" element={<ErrorBoundary><SignalsPage /></ErrorBoundary>} />
          <Route path="/config" element={<ErrorBoundary><ConfigPage /></ErrorBoundary>} />
          <Route path="/system" element={<ErrorBoundary><SystemPage /></ErrorBoundary>} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
