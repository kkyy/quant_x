import { BrowserRouter, Routes, Route } from "react-router-dom";
import { Layout } from "./components/Layout";
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
          <Route index element={<OverviewPage />} />
          <Route path="/data-explorer" element={<DataExplorerPage />} />
          <Route path="/research" element={<ResearchPage />} />
          <Route path="/models" element={<ModelsPage />} />
          <Route path="/backtest" element={<BacktestPage />} />
          <Route path="/signals" element={<SignalsPage />} />
          <Route path="/config" element={<ConfigPage />} />
          <Route path="/system" element={<SystemPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
