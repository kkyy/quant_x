const BASE = "/api";

import type {
  StockQuote,
  StockSearchResult,
  SectorInfo,
  SectorStock,
  SectorRotation,
  AltDataResponse,
  FactorValueRow,
  ICDAnalysis,
  FactorHeatmap,
  EquityCurve,
  BacktestMetrics,
  DrawdownSeries,
  CompareResponse,
} from "./types";

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

// --- Data ---

export function fetchStockQuotes(
  symbol: string,
  params?: { start?: string; end?: string; fields?: string }
) {
  const sp = new URLSearchParams(
    Object.entries(params || {}).filter(([, v]) => v != null) as [string, string][]
  ).toString();
  return get<{ symbol: string; name: string; data: StockQuote[] }>(
    `/data/stock/${symbol}/quotes${sp ? `?${sp}` : ""}`
  );
}

export function searchStocks(q: string, limit?: number) {
  return get<StockSearchResult[]>(
    `/data/stock/search?q=${encodeURIComponent(q)}${limit ? `&limit=${limit}` : ""}`
  );
}

export function fetchSectors() {
  return get<SectorInfo[]>("/data/sectors");
}

export function fetchSectorStocks(sectorId: string) {
  return get<{ sector_id: string; sector_name: string; stocks: SectorStock[] }>(
    `/data/sectors/${encodeURIComponent(sectorId)}/stocks`
  );
}

export function fetchSectorRotation(windows?: string) {
  return get<SectorRotation[]>(`/data/sectors/rotation${windows ? `?windows=${windows}` : ""}`);
}

export function fetchAltData(
  type: string,
  params?: { symbol?: string; start?: string; end?: string; limit?: number }
) {
  const sp = new URLSearchParams(
    Object.entries(params || {}).filter(([, v]) => v != null) as [string, string][]
  ).toString();
  return get<AltDataResponse>(`/data/alt-data/${type}?${sp}`);
}

// --- Factors ---

export function fetchFactorValues(params: {
  factors: string;
  symbols?: string;
  start?: string;
  end?: string;
}) {
  const sp = new URLSearchParams(
    Object.entries(params).filter(([, v]) => v != null) as [string, string][]
  ).toString();
  return get<{ factors: string[]; data: FactorValueRow[] }>(`/factors/values?${sp}`);
}

export function fetchICAnalysis(params: { factor: string; horizon?: number; window?: number }) {
  const sp = new URLSearchParams(
    Object.entries(params).filter(([, v]) => v != null) as [string, string][]
  ).toString();
  return get<ICDAnalysis>(`/factors/ic-analysis?${sp}`);
}

export function fetchFactorHeatmap(params: { factors: string; start?: string; end?: string }) {
  const sp = new URLSearchParams(
    Object.entries(params).filter(([, v]) => v != null) as [string, string][]
  ).toString();
  return get<FactorHeatmap>(`/factors/heatmap?${sp}`);
}

// --- Backtest ---

export function fetchEquityCurve(filename: string) {
  return get<EquityCurve>(`/backtest/results/${encodeURIComponent(filename)}/equity-curve`);
}

export function fetchBacktestMetrics(filename: string) {
  return get<BacktestMetrics>(`/backtest/results/${encodeURIComponent(filename)}/metrics`);
}

export function fetchDrawdown(filename: string) {
  return get<DrawdownSeries>(`/backtest/results/${encodeURIComponent(filename)}/drawdown`);
}

export function compareRuns(filenames: string[]) {
  return post<CompareResponse>("/backtest/compare", { filenames });
}
