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
