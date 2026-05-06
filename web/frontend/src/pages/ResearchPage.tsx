import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Card } from "../components/ui/Card";
import { Tabs } from "../components/ui/Tabs";
import { Badge } from "../components/ui/Badge";
import { Table } from "../components/ui/Table";
import { Select } from "../components/ui/Select";
import { MultiSelect } from "../components/ui/MultiSelect";
import { DatePicker } from "../components/ui/DatePicker";
import { NumberInput } from "../components/ui/NumberInput";
import { TaskStatus } from "../components/ui/TaskStatus";
import { EChartsWrapper } from "../components/ui/EChartsWrapper";
import { get, post, fetchICAnalysis, fetchFactorHeatmap } from "../api/client";
import type { ICDAnalysis, FactorHeatmap } from "../api/types";

const RESEARCH_TABS = [
  { key: "library", label: "Library" },
  { key: "icAnalysis", label: "IC Analysis" },
  { key: "heatmap", label: "Heatmap" },
  { key: "mining", label: "Mining" },
];

interface FactorLibEntry {
  name: string;
  class_name: string;
  enabled: boolean;
}

function LibraryTab() {
  const { t } = useTranslation();
  const [factors, setFactors] = useState<FactorLibEntry[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    get<FactorLibEntry[]>("/factors/library")
      .then(setFactors)
      .catch(() => setFactors([]))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <p className="text-zinc-500 text-sm">{t("common.loading")}</p>;

  return (
    <Table
      columns={[
        { key: "name", label: "Name", sortable: true },
        { key: "class_name", label: "Class", sortable: true },
        {
          key: "enabled",
          label: "Status",
          render: (row) => (
            <Badge variant={row.enabled ? "success" : "neutral"}>
              {row.enabled ? "Enabled" : "Disabled"}
            </Badge>
          ),
        },
      ]}
      data={factors as unknown as Record<string, unknown>[]}
      pageSize={20}
    />
  );
}

function ICAnalysisTab() {
  const { t } = useTranslation();
  const [factorList, setFactorList] = useState<{ value: string; label: string }[]>([]);
  const [selectedFactor, setSelectedFactor] = useState("");
  const [horizon, setHorizon] = useState(5);
  const [window, setWindow] = useState(20);
  const [result, setResult] = useState<ICDAnalysis | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    get<{ name: string }[]>("/factors")
      .then((factors) => setFactorList(factors.map(f => ({ value: f.name, label: f.name }))))
      .catch(() => setFactorList([]));
  }, []);

  const analyze = () => {
    if (!selectedFactor) return;
    setLoading(true);
    fetchICAnalysis({ factor: selectedFactor, horizon, window })
      .then(setResult)
      .catch(() => setResult(null))
      .finally(() => setLoading(false));
  };

  const decayChartOption = result && result.decay.length > 0 ? {
    tooltip: { trigger: "axis" },
    grid: { left: 50, right: 20, top: 30, bottom: 40 },
    xAxis: { type: "category", data: result.decay.map(d => `${d.horizon}d`), axisLine: { lineStyle: { color: "#374151" } }, axisLabel: { color: "#6b7280" } },
    yAxis: { type: "value", splitLine: { lineStyle: { color: "#1f2937" } }, axisLabel: { color: "#6b7280" } },
    series: [{
      type: "line",
      data: result.decay.map(d => d.ic),
      smooth: true,
      lineStyle: { color: "#3b82f6", width: 2 },
      areaStyle: { color: "rgba(59,130,246,0.1)" },
      itemStyle: { color: "#3b82f6" },
    }],
    title: { text: "IC Decay", textStyle: { color: "#9ca3af", fontSize: 13 } },
  } : undefined;

  const rollingChartOption = result && result.rolling.length > 0 ? {
    tooltip: { trigger: "axis" },
    grid: { left: 50, right: 20, top: 30, bottom: 50 },
    xAxis: { type: "category", data: result.rolling.map(r => r.date), axisLine: { lineStyle: { color: "#374151" } }, axisLabel: { color: "#6b7280", fontSize: 10, rotate: 30 } },
    yAxis: { type: "value", splitLine: { lineStyle: { color: "#1f2937" } }, axisLabel: { color: "#6b7280" } },
    dataZoom: [{ type: "inside" }, { type: "slider", bottom: 10, height: 16 }],
    series: [{
      type: "line",
      data: result.rolling.map(r => r.ic),
      lineStyle: { color: "#10b981", width: 1.5 },
      symbol: "none",
    }],
    title: { text: "Rolling IC", textStyle: { color: "#9ca3af", fontSize: 13 } },
  } : undefined;

  return (
    <div className="space-y-4">
      <div className="flex gap-3 items-end">
        <div className="w-48">
          <p className="text-xs text-zinc-500 uppercase mb-1">{t("research.selectFactor")}</p>
          <Select options={factorList} value={selectedFactor} onChange={setSelectedFactor} searchable />
        </div>
        <div>
          <p className="text-xs text-zinc-500 uppercase mb-1">Horizon</p>
          <NumberInput value={horizon} onChange={(v) => setHorizon(v ?? 5)} min={1} max={60} />
        </div>
        <div>
          <p className="text-xs text-zinc-500 uppercase mb-1">Window</p>
          <NumberInput value={window} onChange={(v) => setWindow(v ?? 20)} min={5} max={120} />
        </div>
        <button
          onClick={analyze}
          disabled={!selectedFactor}
          className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 disabled:opacity-30"
        >
          {t("research.analyze")}
        </button>
      </div>

      {loading && <p className="text-zinc-500 text-sm">{t("common.loading")}</p>}

      {result && (
        <>
          <div className="grid grid-cols-2 gap-4">
            <Card>
              <p className="text-xs text-zinc-500 uppercase mb-1">{t("research.meanIC")}</p>
              <p className="text-2xl font-bold text-zinc-100">{result.ic_mean}</p>
            </Card>
            <Card>
              <p className="text-xs text-zinc-500 uppercase mb-1">{t("research.icir")}</p>
              <p className="text-2xl font-bold text-zinc-100">{result.icir}</p>
            </Card>
          </div>
          <div className="grid grid-cols-2 gap-4">
            {decayChartOption && <EChartsWrapper option={decayChartOption} height={280} />}
            {rollingChartOption && <EChartsWrapper option={rollingChartOption} height={280} />}
          </div>
        </>
      )}
    </div>
  );
}

function HeatmapTab() {
  const { t } = useTranslation();
  const [factorList, setFactorList] = useState<{ value: string; label: string }[]>([]);
  const [selectedFactors, setSelectedFactors] = useState<string[]>([]);
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [result, setResult] = useState<FactorHeatmap | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    get<{ name: string }[]>("/factors")
      .then((factors) => setFactorList(factors.map(f => ({ value: f.name, label: f.name }))))
      .catch(() => setFactorList([]));
  }, []);

  const generate = () => {
    if (selectedFactors.length < 2) return;
    setLoading(true);
    fetchFactorHeatmap({ factors: selectedFactors.join(","), start: startDate || undefined, end: endDate || undefined })
      .then(setResult)
      .catch(() => setResult(null))
      .finally(() => setLoading(false));
  };

  const heatmapOption = result && result.factors.length > 0 ? {
    tooltip: { position: "top" },
    grid: { left: 80, right: 30, top: 10, bottom: 50 },
    xAxis: { type: "category", data: result.factors, axisLabel: { color: "#6b7280", fontSize: 10, rotate: 30 } },
    yAxis: { type: "category", data: result.factors, axisLabel: { color: "#6b7280", fontSize: 10 } },
    visualMap: {
      min: -1, max: 1,
      inRange: { color: ["#7f1d1d", "#1f2937", "#065f46"] },
      textStyle: { color: "#9ca3af" },
    },
    series: [{
      type: "heatmap",
      data: result.matrix.flatMap((row, i) => row.map((val, j) => [j, i, val])),
      label: { show: true, fontSize: 9, color: "#d1d5db", formatter: (p: any) => p.data[2].toFixed(2) },
    }],
  } : undefined;

  return (
    <div className="space-y-4">
      <div className="flex gap-3 items-end">
        <div className="w-72">
          <p className="text-xs text-zinc-500 uppercase mb-1">Factors (select 2+)</p>
          <MultiSelect options={factorList} values={selectedFactors} onChange={setSelectedFactors} placeholder="Select factors..." />
        </div>
        <DatePicker value={startDate} onChange={setStartDate} />
        <DatePicker value={endDate} onChange={setEndDate} />
        <button
          onClick={generate}
          disabled={selectedFactors.length < 2}
          className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 disabled:opacity-30"
        >
          Generate
        </button>
      </div>
      {loading && <p className="text-zinc-500 text-sm">{t("common.loading")}</p>}
      {heatmapOption && <EChartsWrapper option={heatmapOption} height={400} />}
    </div>
  );
}

function MiningTab() {
  const { t } = useTranslation();
  const [minIC, setMinIC] = useState(0.03);
  const [minICIR, setMinICIR] = useState(0.4);
  const [topN, setTopN] = useState(30);
  const [taskId, setTaskId] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  const startMining = () => {
    setSubmitting(true);
    post<{ task_id: string }>("/factors/mine", { min_ic: minIC, min_icir: minICIR, top_n: topN })
      .then((res) => setTaskId(res.task_id))
      .catch(() => {})
      .finally(() => setSubmitting(false));
  };

  return (
    <div className="space-y-4 max-w-lg">
      <div className="grid grid-cols-3 gap-4">
        <div>
          <p className="text-xs text-zinc-500 uppercase mb-1">{t("research.minIC")}</p>
          <NumberInput value={minIC} onChange={(v) => setMinIC(v ?? 0.03)} step={0.01} min={0} />
        </div>
        <div>
          <p className="text-xs text-zinc-500 uppercase mb-1">{t("research.minICIR")}</p>
          <NumberInput value={minICIR} onChange={(v) => setMinICIR(v ?? 0.4)} step={0.1} min={0} />
        </div>
        <div>
          <p className="text-xs text-zinc-500 uppercase mb-1">{t("research.topN")}</p>
          <NumberInput value={topN} onChange={(v) => setTopN(v ?? 30)} min={1} max={100} />
        </div>
      </div>
      <button
        onClick={startMining}
        disabled={submitting}
        className="px-4 py-2 bg-blue-600 text-white text-sm rounded-md hover:bg-blue-700 disabled:opacity-30"
      >
        {submitting ? t("common.starting") : t("research.startMining")}
      </button>
      <TaskStatus taskId={taskId} />
    </div>
  );
}

export function ResearchPage() {
  const { t } = useTranslation();
  const [activeTab, setActiveTab] = useState("library");

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-zinc-100">{t("research.title")}</h1>
        <Tabs tabs={RESEARCH_TABS} activeKey={activeTab} onChange={setActiveTab} />
      </div>

      {activeTab === "library" && <LibraryTab />}
      {activeTab === "icAnalysis" && <ICAnalysisTab />}
      {activeTab === "heatmap" && <HeatmapTab />}
      {activeTab === "mining" && <MiningTab />}
    </div>
  );
}
