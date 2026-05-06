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

export interface ICDecayPoint {
  horizon: number;
  ic: number;
}

export interface RollingICPoint {
  date: string;
  ic: number;
}

export interface ICDAnalysis {
  factor: string;
  ic_mean: number;
  icir: number;
  decay: ICDecayPoint[];
  rolling: RollingICPoint[];
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
