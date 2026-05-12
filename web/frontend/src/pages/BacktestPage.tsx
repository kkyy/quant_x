import { useState, useEffect, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Card } from "../components/ui/Card";
import { Tabs } from "../components/ui/Tabs";
import { Table } from "../components/ui/Table";
import { Select } from "../components/ui/Select";
import { NumberInput } from "../components/ui/NumberInput";
import { DatePicker } from "../components/ui/DatePicker";
import { TaskStatus } from "../components/ui/TaskStatus";
import { EChartsWrapper } from "../components/ui/EChartsWrapper";
import { Skeleton } from "../components/ui/Skeleton";
import {
  get,
  post,
  fetchEquityCurve,
  fetchBacktestMetrics,
  fetchDrawdown,
  compareRuns,
} from "../api/client";
import type {
  EquityCurve,
  BacktestMetrics,
  DrawdownSeries,
  CompareRun,
} from "../api/types";

const BACKTEST_TABS = [
  { key: "launch", label: "Launch" },
  { key: "compare", label: "Compare" },
  { key: "results", label: "Results" },
  { key: "wfv", label: "Walk-Forward" },
];

interface ModelInfo {
  filename: string;
  size_mb: number;
  modified: string;
  meta: Record<string, unknown>;
}
interface ResultFile {
  filename: string;
  size_kb: number;
  modified: string;
}

const COMPARE_COLORS = [
  "#22c55e",
  "#10b981",
  "#f59e0b",
  "#ef4444",
  "#8b5cf6",
  "#ec4899",
  "#06b6d4",
  "#f97316",
];

const CHART_DATA_ZOOM = [
  { type: "inside" as const },
  {
    type: "slider" as const,
    bottom: 10,
    height: 16,
    borderColor: "#27272a",
    fillerColor: "rgba(34,197,94,0.15)",
    handleStyle: { color: "#22c55e" },
    dataBackground: {
      lineStyle: { color: "#27272a" },
      areaStyle: { color: "#1e1e22" },
    },
    selectedDataBackground: {
      lineStyle: { color: "#22c55e" },
      areaStyle: { color: "rgba(34,197,94,0.1)" },
    },
  },
];

const parseIntList = (value: string) =>
  value
    .split(",")
    .map((s) => parseInt(s.trim(), 10))
    .filter((n) => !isNaN(n));

const parseFloatList = (value: string) =>
  value
    .split(",")
    .map((s) => parseFloat(s.trim()))
    .filter((n) => !isNaN(n));

const parseStringList = (value: string) =>
  value
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

const parseJsonObject = (value: string) => {
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  try {
    const parsed = JSON.parse(trimmed);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? parsed
      : undefined;
  } catch {
    return undefined;
  }
};

// ─── Launch Tab ────────────────────────────────────────────────────────

function LaunchTab() {
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
  const [optimize, setOptimize] = useState(false);
  const [nIters, setNIters] = useState(3);
  const [gridWorkers, setGridWorkers] = useState(1);
  const [outputCsv, setOutputCsv] = useState("");
  const [markets, setMarkets] = useState("");
  const [exploreMarkets, setExploreMarkets] = useState(false);
  const [slippageSensitivity, setSlippageSensitivity] = useState(false);
  const [slippageMultipliers, setSlippageMultipliers] = useState("0,0.25,0.5,1,1.5,2,3,5");
  const [taskId, setTaskId] = useState<string | null>(null);

  useEffect(() => {
    get<ModelInfo[]>("/models").then((data) => {
      setModels(data);
      if (data.length > 0 && !modelPath) setModelPath(data[0].filename);
    });
  }, []);

  const handleSubmit = async () => {
    const body: Record<string, unknown> = {
      model_path: modelPath,
      topk: parseIntList(topk),
      n_drop: parseIntList(nDrop),
      hold_thresh: parseIntList(holdThresh),
      start: startDate || null,
      end: endDate || null,
      market,
      multi_seed: multiSeed,
      optimize,
      n_iters: optimize ? nIters : undefined,
      grid_workers: gridWorkers > 1 ? gridWorkers : undefined,
      output_csv: outputCsv.trim() || undefined,
      markets: parseStringList(markets).length ? parseStringList(markets) : undefined,
      explore_markets: exploreMarkets || undefined,
      slippage_sensitivity: slippageSensitivity || undefined,
      slippage_multipliers: slippageSensitivity && parseFloatList(slippageMultipliers).length
        ? parseFloatList(slippageMultipliers)
        : undefined,
    };
    const res = await post<{ task_id: string }>("/backtest/grid", body);
    setTaskId(res.task_id);
  };

  const modelOptions = models.map((m) => ({
    value: m.filename,
    label: `${m.filename} (${m.size_mb} MB)`,
  }));

  const marketOptions = [
    { value: "csi300", label: "CSI 300" },
    { value: "csi500", label: "CSI 500" },
    { value: "csi800", label: "CSI 800" },
    { value: "csi1000", label: "CSI 1000" },
  ];

  return (
    <div className="space-y-4 max-w-2xl">
      <Card title={t("backtest.gridTab")}>
        <div className="space-y-4">
          <div>
            <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
              {t("backtest.model")}
            </p>
            <Select
              options={modelOptions}
              value={modelPath}
              onChange={setModelPath}
              searchable
            />
          </div>
          <div>
            <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
              {t("backtest.market")}
            </p>
            <Select
              options={marketOptions}
              value={market}
              onChange={setMarket}
            />
          </div>
          <div className="grid grid-cols-3 gap-4">
            <div>
              <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
                {t("backtest.topk")}
              </p>
              <input
                type="text"
                value={topk}
                onChange={(e) => setTopk(e.target.value)}
                className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
                placeholder="5,10,15,20"
              />
            </div>
            <div>
              <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
                {t("backtest.nDrop")}
              </p>
              <input
                type="text"
                value={nDrop}
                onChange={(e) => setNDrop(e.target.value)}
                className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
                placeholder="1,3,5"
              />
            </div>
            <div>
              <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
                {t("backtest.holdThresh")}
              </p>
              <input
                type="text"
                value={holdThresh}
                onChange={(e) => setHoldThresh(e.target.value)}
                className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
                placeholder="3,5,10"
              />
            </div>
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
                {t("backtest.startDate")}
              </p>
              <DatePicker value={startDate} onChange={setStartDate} />
            </div>
            <div>
              <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
                {t("backtest.endDate")}
              </p>
              <DatePicker value={endDate} onChange={setEndDate} />
            </div>
          </div>
          <div className="flex flex-wrap items-center gap-6">
            <label className="flex items-center gap-2 text-xs font-mono text-terminal-text cursor-pointer">
              <input
                type="checkbox"
                checked={multiSeed}
                onChange={(e) => setMultiSeed(e.target.checked)}
                className="accent-terminal-green"
              />
              {t("backtest.multiSeed")}
            </label>
            <label className="flex items-center gap-2 text-xs font-mono text-terminal-text cursor-pointer">
              <input
                type="checkbox"
                checked={optimize}
                onChange={(e) => setOptimize(e.target.checked)}
                className="accent-terminal-green"
              />
              AI Optimize
            </label>
            <label className="flex items-center gap-2 text-xs font-mono text-terminal-text cursor-pointer">
              <input
                type="checkbox"
                checked={exploreMarkets}
                onChange={(e) => setExploreMarkets(e.target.checked)}
                className="accent-terminal-green"
              />
              {t("backtest.exploreMarkets")}
            </label>
            <label className="flex items-center gap-2 text-xs font-mono text-terminal-text cursor-pointer">
              <input
                type="checkbox"
                checked={slippageSensitivity}
                onChange={(e) => setSlippageSensitivity(e.target.checked)}
                className="accent-terminal-green"
              />
              {t("backtest.slippageSensitivity")}
            </label>
            {optimize && (
              <div className="flex items-center gap-2">
                <p className="text-xs font-mono text-terminal-text-dim">{t("backtest.nIters")}</p>
                <NumberInput
                  value={nIters}
                  onChange={(v) => setNIters(v ?? 3)}
                  min={1}
                  max={20}
                />
              </div>
            )}
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">{t("backtest.gridWorkers")}</p>
              <NumberInput
                value={gridWorkers}
                onChange={(v) => setGridWorkers(v ?? 1)}
                min={1}
                max={8}
              />
            </div>
            <div>
              <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">{t("backtest.markets")}</p>
              <input
                type="text"
                value={markets}
                onChange={(e) => setMarkets(e.target.value)}
                className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
                placeholder="csi300,csi1000"
              />
            </div>
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">{t("backtest.outputCsv")}</p>
              <input
                type="text"
                value={outputCsv}
                onChange={(e) => setOutputCsv(e.target.value)}
                className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
                placeholder="backtest_results/my_run.csv"
              />
            </div>
            {slippageSensitivity && (
              <div>
                <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">{t("backtest.slippageMultipliers")}</p>
                <input
                  type="text"
                  value={slippageMultipliers}
                  onChange={(e) => setSlippageMultipliers(e.target.value)}
                  className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
                  placeholder="0,0.25,0.5,1,2"
                />
              </div>
            )}
          </div>
          <button
            onClick={handleSubmit}
            disabled={!modelPath}
            className="px-3 py-1.5 text-xs font-mono border border-terminal-green text-terminal-green hover:bg-terminal-green-glow transition-colors rounded-sm disabled:opacity-30"
          >
            {t("backtest.runGrid")}
          </button>
        </div>
      </Card>
      <TaskStatus taskId={taskId} />
    </div>
  );
}

// ─── Compare Tab ───────────────────────────────────────────────────────

function CompareTab() {
  const { t } = useTranslation();
  const [results, setResults] = useState<ResultFile[]>([]);
  const [selected, setSelected] = useState<string[]>([]);
  const [compareData, setCompareData] = useState<CompareRun[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    get<ResultFile[]>("/backtest/results").then(setResults);
  }, []);

  // Deterministic filename → color mapping (stable regardless of selection order)
  const filenameColorMap = useMemo(() => {
    const map = new Map<string, string>();
    results.forEach((r, i) => {
      map.set(r.filename, COMPARE_COLORS[i % COMPARE_COLORS.length]);
    });
    return map;
  }, [results]);

  const toggleFile = (filename: string) => {
    setSelected((prev) => {
      if (prev.includes(filename)) return prev.filter((f) => f !== filename);
      if (prev.length >= 8) return prev;
      return [...prev, filename];
    });
  };

  const runCompare = async () => {
    if (selected.length < 2) return;
    setLoading(true);
    try {
      const res = await compareRuns(selected);
      setCompareData(res.runs);
    } catch {
      setCompareData([]);
    } finally {
      setLoading(false);
    }
  };

  const hasEquityData = compareData.some((r) => r.equity_curve?.dates?.length > 0);
  const hasDrawdownData = compareData.some((r) => r.drawdown?.dates?.length > 0);

  const equityChartOption = useMemo(() => {
    if (compareData.length === 0 || !hasEquityData) return null;
    const dates = compareData.find((r) => r.equity_curve?.dates?.length)?.equity_curve?.dates ?? [];
    return {
      tooltip: { trigger: "axis" },
      legend: {
        data: compareData.map((r) => r.label),
        textStyle: { color: "#c8ccd0", fontSize: 11 },
        top: 0,
      },
      grid: { left: 60, right: 20, top: 40, bottom: 60 },
      xAxis: {
        type: "category",
        data: dates,
        axisLine: { lineStyle: { color: "#27272a" } },
        axisLabel: { color: "#71717a", fontSize: 10, rotate: 30 },
      },
      yAxis: {
        type: "value",
        splitLine: { lineStyle: { color: "#1e1e22" } },
        axisLabel: { color: "#71717a" },
      },
      dataZoom: CHART_DATA_ZOOM,
      series: compareData.map((run) => ({
        name: run.label,
        type: "line",
        data: run.equity_curve.portfolio,
        lineStyle: { color: run.color, width: 2 },
        symbol: "none",
      })),
      title: {
        text: "Equity Curve Comparison",
        textStyle: { color: "#c8ccd0", fontSize: 13 },
      },
    };
  }, [compareData, hasEquityData]);

  const drawdownChartOption = useMemo(() => {
    if (compareData.length === 0 || !hasDrawdownData) return null;
    const dates = compareData.find((r) => r.drawdown?.dates?.length)?.drawdown?.dates ?? [];
    return {
      tooltip: { trigger: "axis" },
      legend: {
        data: compareData.map((r) => r.label),
        textStyle: { color: "#c8ccd0", fontSize: 11 },
        top: 0,
      },
      grid: { left: 60, right: 20, top: 40, bottom: 60 },
      xAxis: {
        type: "category",
        data: dates,
        axisLine: { lineStyle: { color: "#27272a" } },
        axisLabel: { color: "#71717a", fontSize: 10, rotate: 30 },
      },
      yAxis: {
        type: "value",
        splitLine: { lineStyle: { color: "#1e1e22" } },
        axisLabel: { color: "#71717a", formatter: (v: number) => `${(v * 100).toFixed(1)}%` },
      },
      dataZoom: CHART_DATA_ZOOM,
      series: compareData.map((run) => ({
        name: run.label,
        type: "line",
        data: run.drawdown.drawdown,
        lineStyle: { color: run.color, width: 1.5 },
        areaStyle: { color: run.color, opacity: 0.08 },
        symbol: "none",
      })),
      title: {
        text: "Drawdown Comparison",
        textStyle: { color: "#c8ccd0", fontSize: 13 },
      },
    };
  }, [compareData, hasDrawdownData]);

  const mf = (m: BacktestMetrics | Record<string, never>, key: keyof BacktestMetrics, fmt: (v: number) => string) => {
    const v = (m as Record<string, unknown>)[key];
    return v != null ? fmt(v as number) : "—";
  };
  const mv = (m: BacktestMetrics | Record<string, never>, key: keyof BacktestMetrics) => (m as Record<string, unknown>)[key] as number | undefined;

  const metricsColumns = [
    { key: "label", label: "Run", render: (row: Record<string, unknown>) => {
      const run = row as unknown as CompareRun;
      return (
        <span className="flex items-center gap-2">
          <span
            className="w-3 h-3 rounded-full inline-block"
            style={{ backgroundColor: run.color }}
          />
          <span className="text-xs font-mono text-terminal-text truncate max-w-[200px]">
            {run.label}
          </span>
        </span>
      );
    }},
    { key: "annual_return", label: "Ann Ret", render: (row: Record<string, unknown>) => {
      const m = (row as unknown as CompareRun).metrics;
      const v = mv(m, "annual_return");
      return <span className={v != null && v >= 0 ? "text-terminal-green" : "text-terminal-red"}>{mf(m, "annual_return", v => `${(v * 100).toFixed(2)}%`)}</span>;
    }},
    { key: "sharpe", label: "Sharpe", render: (row: Record<string, unknown>) => {
      const m = (row as unknown as CompareRun).metrics;
      const v = mv(m, "sharpe");
      return <span className={v != null && v >= 1 ? "text-terminal-green" : v != null && v >= 0 ? "text-terminal-amber" : "text-terminal-red"}>{mf(m, "sharpe", v => v.toFixed(3))}</span>;
    }},
    { key: "max_drawdown", label: "Max DD", render: (row: Record<string, unknown>) => {
      const m = (row as unknown as CompareRun).metrics;
      return <span className="text-terminal-red">{mf(m, "max_drawdown", v => `${(v * 100).toFixed(2)}%`)}</span>;
    }},
    { key: "calmar", label: "Calmar", render: (row: Record<string, unknown>) => {
      const m = (row as unknown as CompareRun).metrics;
      return <span>{mf(m, "calmar", v => v.toFixed(3))}</span>;
    }},
    { key: "ic", label: "IC", render: (row: Record<string, unknown>) => {
      const m = (row as unknown as CompareRun).metrics;
      return <span>{mf(m, "ic", v => v.toFixed(4))}</span>;
    }},
    { key: "icir", label: "ICIR", render: (row: Record<string, unknown>) => {
      const m = (row as unknown as CompareRun).metrics;
      return <span>{mf(m, "icir", v => v.toFixed(3))}</span>;
    }},
    { key: "win_rate", label: "Win Rate", render: (row: Record<string, unknown>) => {
      const m = (row as unknown as CompareRun).metrics;
      return <span>{mf(m, "win_rate", v => `${(v * 100).toFixed(1)}%`)}</span>;
    }},
    { key: "turnover", label: "Turnover", render: (row: Record<string, unknown>) => {
      const m = (row as unknown as CompareRun).metrics;
      return <span>{mf(m, "turnover", v => `${(v * 100).toFixed(1)}%`)}</span>;
    }},
  ];

  return (
    <div className="space-y-4">
      <Card title="Select Runs to Compare (2-8)">
        <div className="space-y-3">
          {results.length === 0 ? (
            <p className="text-terminal-text-dim text-xs font-mono">{t("backtest.noResults")}</p>
          ) : (
            <div className="flex flex-wrap gap-2">
              {results.map((r) => {
                const isSelected = selected.includes(r.filename);
                const color = filenameColorMap.get(r.filename) ?? "#71717a";
                return (
                  <button
                    key={r.filename}
                    onClick={() => toggleFile(r.filename)}
                    className={`flex items-center gap-2 px-3 py-1.5 rounded-sm text-xs font-mono border transition-colors ${
                      isSelected
                        ? "border-terminal-green bg-terminal-green-glow text-terminal-green"
                        : "border-terminal-border bg-terminal-surface text-terminal-text-dim hover:border-terminal-text-dim"
                    }`}
                  >
                    {isSelected && (
                      <span
                        className="w-2.5 h-2.5 rounded-full"
                        style={{ backgroundColor: color }}
                      />
                    )}
                    {r.filename}
                    <span className="text-terminal-text-dim">({r.size_kb}KB)</span>
                  </button>
                );
              })}
            </div>
          )}
          <div className="flex items-center gap-3">
            <button
              onClick={runCompare}
              disabled={selected.length < 2 || loading}
              className="px-3 py-1.5 text-xs font-mono border border-terminal-green text-terminal-green hover:bg-terminal-green-glow transition-colors rounded-sm disabled:opacity-30"
            >
              {loading ? t("common.loading") : "Compare"}
            </button>
            <span className="text-xs font-mono text-terminal-text-dim">
              {selected.length} selected
            </span>
          </div>
        </div>
      </Card>

      {compareData.length > 0 && (
        <>
          <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
            {equityChartOption ? (
              <EChartsWrapper option={equityChartOption} height={360} />
            ) : (
              <Card title="Equity Curve Comparison">
                <p className="text-terminal-text-dim text-xs font-mono py-8 text-center">
                  {t("backtest.noCurveData")}
                </p>
              </Card>
            )}
            {drawdownChartOption ? (
              <EChartsWrapper option={drawdownChartOption} height={360} />
            ) : (
              <Card title="Drawdown Comparison">
                <p className="text-terminal-text-dim text-xs font-mono py-8 text-center">
                  {t("backtest.noCurveData")}
                </p>
              </Card>
            )}
          </div>
          <Card title="Metrics Comparison">
            <Table
              columns={metricsColumns}
              data={compareData as unknown as Record<string, unknown>[]}
              pageSize={10}
            />
          </Card>
        </>
      )}
    </div>
  );
}

// ─── Results Tab ───────────────────────────────────────────────────────

function ResultsTab() {
  const { t } = useTranslation();
  const [results, setResults] = useState<ResultFile[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [equity, setEquity] = useState<EquityCurve | null>(null);
  const [metrics, setMetrics] = useState<BacktestMetrics | null>(null);
  const [drawdownData, setDrawdownData] = useState<DrawdownSeries | null>(
    null
  );
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    get<ResultFile[]>("/backtest/results").then(setResults);
  }, []);

  const loadResult = async (filename: string) => {
    setSelected(filename);
    setLoading(true);
    setEquity(null);
    setMetrics(null);
    setDrawdownData(null);
    try {
      const [eq, mt, dd] = await Promise.all([
        fetchEquityCurve(filename),
        fetchBacktestMetrics(filename),
        fetchDrawdown(filename),
      ]);
      setEquity(eq);
      setMetrics(mt);
      setDrawdownData(dd);
    } catch {
      // ignore
    } finally {
      setLoading(false);
    }
  };

  const equityChartOption = useMemo(() => {
    if (!equity || !equity.dates?.length) return null;
    return {
      tooltip: { trigger: "axis" },
      legend: {
        data: ["Portfolio", "Benchmark", "Excess"],
        textStyle: { color: "#c8ccd0", fontSize: 11 },
        top: 0,
      },
      grid: { left: 60, right: 20, top: 40, bottom: 60 },
      xAxis: {
        type: "category",
        data: equity.dates,
        axisLine: { lineStyle: { color: "#27272a" } },
        axisLabel: { color: "#71717a", fontSize: 10, rotate: 30 },
      },
      yAxis: {
        type: "value",
        splitLine: { lineStyle: { color: "#1e1e22" } },
        axisLabel: { color: "#71717a" },
      },
      dataZoom: CHART_DATA_ZOOM,
      series: [
        {
          name: "Portfolio",
          type: "line",
          data: equity.portfolio,
          lineStyle: { color: "#22c55e", width: 2 },
          symbol: "none",
        },
        {
          name: "Benchmark",
          type: "line",
          data: equity.benchmark,
          lineStyle: { color: "#71717a", width: 1.5 },
          symbol: "none",
        },
        {
          name: "Excess",
          type: "line",
          data: equity.excess,
          lineStyle: { color: "#10b981", width: 1.5, type: "dashed" },
          symbol: "none",
        },
      ],
      title: {
        text: "Equity Curve",
        textStyle: { color: "#c8ccd0", fontSize: 13 },
      },
    };
  }, [equity]);

  const drawdownChartOption = useMemo(() => {
    if (!drawdownData || !drawdownData.dates?.length) return null;
    return {
      tooltip: { trigger: "axis" },
      grid: { left: 60, right: 20, top: 30, bottom: 60 },
      xAxis: {
        type: "category",
        data: drawdownData.dates,
        axisLine: { lineStyle: { color: "#27272a" } },
        axisLabel: { color: "#71717a", fontSize: 10, rotate: 30 },
      },
      yAxis: {
        type: "value",
        splitLine: { lineStyle: { color: "#1e1e22" } },
        axisLabel: {
          color: "#71717a",
          formatter: (v: number) => `${(v * 100).toFixed(1)}%`,
        },
      },
      dataZoom: CHART_DATA_ZOOM,
      series: [
        {
          type: "line",
          data: drawdownData.drawdown,
          lineStyle: { color: "#ef4444", width: 1.5 },
          areaStyle: { color: "rgba(239,68,68,0.1)" },
          symbol: "none",
        },
      ],
      title: {
        text: "Drawdown",
        textStyle: { color: "#c8ccd0", fontSize: 13 },
      },
    };
  }, [drawdownData]);

  const metricItems = metrics && Object.keys(metrics).length > 0
    ? [
        {
          label: "Ann Return",
          value: metrics.annual_return != null ? `${(metrics.annual_return * 100).toFixed(2)}%` : "—",
          color: metrics.annual_return != null && metrics.annual_return >= 0 ? "text-terminal-green" : "text-terminal-red",
        },
        {
          label: "Sharpe",
          value: metrics.sharpe != null ? metrics.sharpe.toFixed(3) : "—",
          color:
            metrics.sharpe == null ? "" :
            metrics.sharpe >= 1
              ? "text-terminal-green"
              : metrics.sharpe >= 0
              ? "text-terminal-amber"
              : "text-terminal-red",
        },
        {
          label: "Max DD",
          value: metrics.max_drawdown != null ? `${(metrics.max_drawdown * 100).toFixed(2)}%` : "—",
          color: "text-terminal-red",
        },
        { label: "Calmar", value: metrics.calmar != null ? metrics.calmar.toFixed(3) : "—", color: "" },
        { label: "IC", value: metrics.ic != null ? metrics.ic.toFixed(4) : "—", color: "" },
        { label: "ICIR", value: metrics.icir != null ? metrics.icir.toFixed(3) : "—", color: "" },
        {
          label: "Win Rate",
          value: metrics.win_rate != null ? `${(metrics.win_rate * 100).toFixed(1)}%` : "—",
          color: "",
        },
        {
          label: "Turnover",
          value: metrics.turnover != null ? `${(metrics.turnover * 100).toFixed(1)}%` : "—",
          color: "",
        },
        { label: "Rank IC", value: metrics.rank_ic != null ? metrics.rank_ic.toFixed(4) : "—", color: "" },
        {
          label: "Rank ICIR",
          value: metrics.rank_icir != null ? metrics.rank_icir.toFixed(3) : "—",
          color: "",
        },
        { label: "Sortino", value: metrics.sortino != null ? metrics.sortino.toFixed(3) : "—", color: "" },
        {
          label: "Ann Vol",
          value: metrics.annual_vol != null ? `${(metrics.annual_vol * 100).toFixed(2)}%` : "—",
          color: "",
        },
      ]
    : [];

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 lg:grid-cols-4 gap-4">
        <div className="lg:col-span-1">
          <Card title={t("backtest.resultsTab")}>
            <div className="space-y-1 max-h-[600px] overflow-y-auto">
              {results.length === 0 ? (
                <p className="text-terminal-text-dim text-xs font-mono">
                  {t("backtest.noResults")}
                </p>
              ) : (
                results.map((r) => (
                  <button
                    key={r.filename}
                    onClick={() => loadResult(r.filename)}
                    className={`w-full text-left px-3 py-2 rounded-sm text-xs font-mono transition-colors ${
                      selected === r.filename
                        ? "bg-terminal-green-glow text-terminal-green"
                        : "text-terminal-text-dim hover:bg-terminal-raised hover:text-terminal-text"
                    }`}
                  >
                    <span className="block truncate">{r.filename}</span>
                    <span className="text-terminal-text-dim">{r.size_kb} KB</span>
                  </button>
                ))
              )}
            </div>
          </Card>
        </div>
        <div className="lg:col-span-3 space-y-4">
          {loading && (
            <div className="space-y-4">
              <div className="grid grid-cols-4 xl:grid-cols-6 gap-3">
                {Array.from({ length: 6 }).map((_, i) => (
                  <Skeleton key={i} className="h-16 w-full" />
                ))}
              </div>
              <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
                <Skeleton className="h-80 w-full" />
                <Skeleton className="h-80 w-full" />
              </div>
            </div>
          )}
          {!loading && !selected && (
            <p className="text-terminal-text-dim text-xs font-mono">
              {t("backtest.selectResult")}
            </p>
          )}
          {!loading && metrics && (
            <>
              <div className="grid grid-cols-4 xl:grid-cols-6 gap-3">
                {metricItems.map((m) => (
                  <div
                    key={m.label}
                    className="bg-terminal-raised border border-terminal-border rounded-sm px-3 py-2"
                  >
                    <p className="text-xs font-mono text-terminal-text-dim uppercase">{m.label}</p>
                    <p className={`text-sm font-mono font-semibold ${m.color || "text-terminal-text-bright"}`}>
                      {m.value}
                    </p>
                  </div>
                ))}
              </div>
              <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
                {equityChartOption ? (
                  <EChartsWrapper option={equityChartOption} height={320} />
                ) : (
                  <Card title="Equity Curve">
                    <p className="text-terminal-text-dim text-xs font-mono py-8 text-center">
                      {t("backtest.noCurveData")}
                    </p>
                  </Card>
                )}
                {drawdownChartOption ? (
                  <EChartsWrapper option={drawdownChartOption} height={320} />
                ) : (
                  <Card title="Drawdown">
                    <p className="text-terminal-text-dim text-xs font-mono py-8 text-center">
                      {t("backtest.noCurveData")}
                    </p>
                  </Card>
                )}
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

// ─── Walk-Forward Tab ──────────────────────────────────────────────────

function WalkForwardTab() {
  const { t } = useTranslation();
  const [trainUniverses, setTrainUniverses] = useState("csi300");
  const [evalMarket, setEvalMarket] = useState("csi300");
  const [topk, setTopk] = useState("5,15,20");
  const [nDrop, setNDrop] = useState("1,3");
  const [holdThresh, setHoldThresh] = useState("5,8,10");
  const [workers, setWorkers] = useState(1);
  const [gridWorkers, setGridWorkers] = useState(1);
  const [seeds, setSeeds] = useState(false);
  const [runId, setRunId] = useState("");
  const [foldsConfig, setFoldsConfig] = useState("");
  const [trainConfig, setTrainConfig] = useState("");
  const [robustWeights, setRobustWeights] = useState("");
  const [taskId, setTaskId] = useState<string | null>(null);

  const marketOptions = [
    { value: "csi300", label: "CSI 300" },
    { value: "csi500", label: "CSI 500" },
    { value: "csi800", label: "CSI 800" },
    { value: "csi1000", label: "CSI 1000" },
  ];

  const handleSubmit = async () => {
    const body: Record<string, unknown> = {
      train_universes: trainUniverses.split(",").map((s) => s.trim()),
      eval_market: evalMarket,
      topk: parseIntList(topk),
      n_drop: parseIntList(nDrop),
      hold_thresh: parseIntList(holdThresh),
      workers,
      grid_workers: gridWorkers > 1 ? gridWorkers : undefined,
      seeds: seeds || undefined,
      run_id: runId.trim() || undefined,
      folds_config: foldsConfig.trim() || undefined,
      train_config: trainConfig.trim() || undefined,
      robust_weights: parseJsonObject(robustWeights),
    };
    const res = await post<{ task_id: string }>("/backtest/walk-forward", body);
    setTaskId(res.task_id);
  };

  return (
    <div className="space-y-4 max-w-2xl">
      <Card title={t("backtest.wfvTab")}>
        <div className="space-y-4">
          <div>
            <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
              {t("backtest.trainUniverses")}
            </p>
            <input
              type="text"
              value={trainUniverses}
              onChange={(e) => setTrainUniverses(e.target.value)}
              className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
              placeholder="csi300,csi800"
            />
          </div>
          <div>
            <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
              {t("backtest.evalMarket")}
            </p>
            <Select
              options={marketOptions}
              value={evalMarket}
              onChange={setEvalMarket}
            />
          </div>
          <div className="grid grid-cols-3 gap-4">
            <div>
              <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
                {t("backtest.topk")}
              </p>
              <input
                type="text"
                value={topk}
                onChange={(e) => setTopk(e.target.value)}
                className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
              />
            </div>
            <div>
              <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
                {t("backtest.nDrop")}
              </p>
              <input
                type="text"
                value={nDrop}
                onChange={(e) => setNDrop(e.target.value)}
                className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
              />
            </div>
            <div>
              <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
                {t("backtest.holdThresh")}
              </p>
              <input
                type="text"
                value={holdThresh}
                onChange={(e) => setHoldThresh(e.target.value)}
                className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
              />
            </div>
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
                {t("backtest.workers")}
              </p>
              <NumberInput
                value={workers}
                onChange={(v) => setWorkers(v ?? 1)}
                min={1}
                max={8}
              />
            </div>
            <div>
              <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
                {t("backtest.gridWorkers")}
              </p>
              <NumberInput
                value={gridWorkers}
                onChange={(v) => setGridWorkers(v ?? 1)}
                min={1}
                max={8}
              />
            </div>
          </div>
          <label className="flex items-center gap-2 text-xs font-mono text-terminal-text cursor-pointer">
            <input
              type="checkbox"
              checked={seeds}
              onChange={(e) => setSeeds(e.target.checked)}
              className="accent-terminal-green"
            />
            {t("backtest.multiSeed")}
          </label>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
                {t("backtest.runId")}
              </p>
              <input
                type="text"
                value={runId}
                onChange={(e) => setRunId(e.target.value)}
                className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
                placeholder="wfv_csi1000_20260512"
              />
            </div>
            <div>
              <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
                {t("backtest.foldsConfig")}
              </p>
              <input
                type="text"
                value={foldsConfig}
                onChange={(e) => setFoldsConfig(e.target.value)}
                className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
                placeholder="config/walk_forward_folds.yaml"
              />
            </div>
          </div>
          <div>
            <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
              {t("backtest.trainConfig")}
            </p>
            <input
              type="text"
              value={trainConfig}
              onChange={(e) => setTrainConfig(e.target.value)}
              className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
              placeholder="config/model.yaml"
            />
          </div>
          <div>
            <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
              {t("backtest.robustWeights")}
            </p>
            <textarea
              value={robustWeights}
              onChange={(e) => setRobustWeights(e.target.value)}
              rows={3}
              className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
              placeholder='{"mean_sharpe":1.0,"sharpe_std":-0.3}'
            />
          </div>
          <button
            onClick={handleSubmit}
            className="px-3 py-1.5 text-xs font-mono border border-terminal-green text-terminal-green hover:bg-terminal-green-glow transition-colors rounded-sm"
          >
            {t("backtest.runWfv")}
          </button>
        </div>
      </Card>
      <TaskStatus taskId={taskId} />
    </div>
  );
}

// ─── Main Page ─────────────────────────────────────────────────────────

export function BacktestPage() {
  const { t } = useTranslation();
  const [activeTab, setActiveTab] = useState("launch");

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-sm font-mono font-semibold text-terminal-text-bright uppercase tracking-wider">
          {t("backtest.title")}
        </h1>
        <Tabs
          tabs={BACKTEST_TABS}
          activeKey={activeTab}
          onChange={setActiveTab}
        />
      </div>

      {activeTab === "launch" && <LaunchTab />}
      {activeTab === "compare" && <CompareTab />}
      {activeTab === "results" && <ResultsTab />}
      {activeTab === "wfv" && <WalkForwardTab />}
    </div>
  );
}
