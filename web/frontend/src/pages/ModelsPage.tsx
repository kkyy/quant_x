import { useEffect, useState, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Card } from "../components/ui/Card";
import { Tabs } from "../components/ui/Tabs";
import { Table } from "../components/ui/Table";
import { Select } from "../components/ui/Select";
import { MultiSelect } from "../components/ui/MultiSelect";
import { DatePicker } from "../components/ui/DatePicker";
import { NumberInput } from "../components/ui/NumberInput";
import { TaskStatus } from "../components/ui/TaskStatus";
import { Skeleton, SkeletonTable } from "../components/ui/Skeleton";
import { EChartsWrapper } from "../components/ui/EChartsWrapper";
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

const MODELS_TABS = [
  { key: "browser", label: "Browser" },
  { key: "train", label: "Train" },
  { key: "registry", label: "Registry" },
];

// ─── Model Browser Tab ─────────────────────────────────────────────────

function ModelBrowserTab() {
  const { t } = useTranslation();
  const [models, setModels] = useState<ModelInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [meta, setMeta] = useState<Record<string, unknown> | null>(null);
  const [importance, setImportance] = useState<Record<string, unknown> | null>(
    null
  );
  const [detailLoading, setDetailLoading] = useState(false);

  useEffect(() => {
    get<ModelInfo[]>("/models")
      .then(setModels)
      .catch(() => setModels([]))
      .finally(() => setLoading(false));
  }, []);

  const handleExpand = async (filename: string) => {
    if (expanded === filename) {
      setExpanded(null);
      setMeta(null);
      setImportance(null);
      return;
    }
    setExpanded(filename);
    setDetailLoading(true);
    try {
      const [m, imp] = await Promise.all([
        get<Record<string, unknown>>(`/models/${filename}/meta`),
        get<Record<string, unknown>>(`/models/${filename}/importance`),
      ]);
      setMeta(m);
      setImportance(imp);
    } catch {
      setMeta(null);
      setImportance(null);
    } finally {
      setDetailLoading(false);
    }
  };

  const importanceChartOption = useMemo(() => {
    if (!importance) return null;
    const entries = Object.entries(importance)
      .sort(([, a], [, b]) => (b as number) - (a as number))
      .slice(0, 20);
    if (entries.length === 0) return null;
    return {
      tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
      grid: { left: 140, right: 20, top: 10, bottom: 30 },
      xAxis: {
        type: "value",
        splitLine: { lineStyle: { color: "#1e1e22" } },
        axisLabel: { color: "#71717a" },
      },
      yAxis: {
        type: "category",
        data: entries.map(([feat]) => feat).reverse(),
        axisLine: { lineStyle: { color: "#27272a" } },
        axisLabel: { color: "#71717a", fontSize: 10 },
      },
      series: [
        {
          type: "bar",
          data: entries.map(([, val]) => val as number).reverse(),
          itemStyle: { color: "#22c55e" },
          barWidth: 14,
        },
      ],
    };
  }, [importance]);

  if (loading) return <SkeletonTable rows={6} />;
  if (models.length === 0)
    return (
      <p className="text-terminal-text-dim text-xs font-mono">{t("models.noModels")}</p>
    );

  return (
    <div className="space-y-3">
      <p className="text-xs font-mono text-terminal-text-dim">
        {t("models.count", { count: models.length })}
      </p>
      <Table
        columns={[
          {
            key: "filename",
            label: t("common.filename"),
            sortable: true,
            render: (row) => (
              <button
                onClick={() => handleExpand(row.filename as string)}
                className="flex items-center gap-2 text-left"
              >
                <span className="font-mono text-xs text-terminal-green">
                  {row.filename as string}
                </span>
                <span className="text-terminal-text-dim text-xs">
                  {expanded === (row.filename as string) ? "▼" : "▶"}
                </span>
              </button>
            ),
          },
          { key: "size_mb", label: t("common.sizeMb"), sortable: true, align: "right" as const },
          {
            key: "modified",
            label: t("common.modified"),
            sortable: true,
            render: (row) => (
              <span className="text-xs text-terminal-text-dim">
                {new Date(row.modified as string).toLocaleString()}
              </span>
            ),
          },
        ]}
        data={models as unknown as Record<string, unknown>[]}
        pageSize={15}
      />

      {expanded && (
        <Card title={`${expanded} — Details`}>
          {detailLoading ? (
            <div className="space-y-4">
              <Skeleton className="h-4 w-24" />
              <div className="grid grid-cols-3 gap-2">
                <Skeleton className="h-12" />
                <Skeleton className="h-12" />
                <Skeleton className="h-12" />
              </div>
              <Skeleton className="h-64" />
            </div>
          ) : (
            <div className="space-y-6">
              {/* Meta section */}
              <div>
                <h4 className="text-xs font-mono font-semibold text-terminal-text-dim uppercase tracking-wider mb-2">
                  {t("models.meta")}
                </h4>
                {meta && Object.keys(meta).length > 0 ? (
                  <div className="grid grid-cols-2 md:grid-cols-3 gap-2">
                    {Object.entries(meta).map(([k, v]) => (
                      <div
                        key={k}
                        className="bg-terminal-raised border border-terminal-border rounded-sm px-3 py-2"
                      >
                        <p className="text-[10px] font-mono text-terminal-text-dim uppercase">{k}</p>
                        <p className="text-sm font-mono text-terminal-text-bright truncate">
                          {typeof v === "object"
                            ? JSON.stringify(v)
                            : String(v)}
                        </p>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="text-xs font-mono text-terminal-text-dim">{t("models.noMeta")}</p>
                )}
              </div>

              {/* Feature importance chart */}
              <div>
                <h4 className="text-xs font-mono font-semibold text-terminal-text-dim uppercase tracking-wider mb-2">
                  {t("models.importance")}
                </h4>
                {importanceChartOption ? (
                  <EChartsWrapper option={importanceChartOption} height={500} />
                ) : (
                  <p className="text-xs font-mono text-terminal-text-dim">
                    {t("models.noImportance")}
                  </p>
                )}
              </div>
            </div>
          )}
        </Card>
      )}
    </div>
  );
}

// ─── Train Tab ─────────────────────────────────────────────────────────

function TrainTab() {
  const { t } = useTranslation();
  const [registry, setRegistry] = useState<RegistryInfo | null>(null);
  const [modelType, setModelType] = useState("lgbm");
  const [tag, setTag] = useState("");
  const [selectedFactors, setSelectedFactors] = useState<string[]>([]);
  const [fitStart, setFitStart] = useState("");
  const [fitEnd, setFitEnd] = useState("");
  const [qlibNative, setQlibNative] = useState(false);
  const [withSector, setWithSector] = useState(false);
  const [noExtraFactors, setNoExtraFactors] = useState(false);
  const [skipFactorPipeline, setSkipFactorPipeline] = useState(false);
  const [baggingFraction, setBaggingFraction] = useState<number | undefined>(
    undefined
  );
  const [ensembleSeeds, setEnsembleSeeds] = useState("");
  const [taskId, setTaskId] = useState<string | null>(null);

  useEffect(() => {
    get<RegistryInfo>("/models/registry")
      .then((reg) => {
        setRegistry(reg);
        if (reg.models.length > 0) setModelType(reg.models[0].name);
      })
      .catch(() => {});
  }, []);

  const handleTrain = async () => {
    const body: Record<string, unknown> = {
      model: modelType,
      qlib_native: qlibNative,
      factors: selectedFactors,
    };
    if (tag.trim()) body.tag = tag.trim();
    if (fitStart) body.fit_start = fitStart;
    if (fitEnd) body.fit_end = fitEnd;
    if (withSector) body.with_sector = withSector;
    if (noExtraFactors) body.no_extra_factors = noExtraFactors;
    if (skipFactorPipeline) body.skip_factor_pipeline = skipFactorPipeline;
    if (baggingFraction != null) body.bagging_fraction = baggingFraction;
    if (ensembleSeeds.trim()) {
      body.ensemble_seeds = ensembleSeeds
        .split(",")
        .map((s) => parseInt(s.trim()))
        .filter((n) => !isNaN(n));
    }
    const res = await post<{ task_id: string }>("/models/train", body);
    setTaskId(res.task_id);
  };

  const modelOptions =
    registry?.models.map((m) => ({ value: m.name, label: m.name })) ?? [];
  const factorOptions =
    registry?.factors.map((f) => ({ value: f.name, label: f.name })) ?? [];

  return (
    <div className="space-y-4 max-w-2xl">
      <Card title={t("models.trainTab")}>
        <div className="space-y-4">
          <div>
            <p className="text-[10px] font-mono text-terminal-text-dim uppercase tracking-wider mb-1">
              {t("models.modelType")}
            </p>
            <Select
              options={modelOptions}
              value={modelType}
              onChange={setModelType}
            />
          </div>
          <div>
            <p className="text-[10px] font-mono text-terminal-text-dim uppercase tracking-wider mb-1">
              {t("models.tag")}
            </p>
            <input
              type="text"
              value={tag}
              onChange={(e) => setTag(e.target.value)}
              placeholder={t("models.tagPlaceholder")}
              className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
            />
          </div>
          <div>
            <p className="text-[10px] font-mono text-terminal-text-dim uppercase tracking-wider mb-1">
              {t("models.factors")}
            </p>
            <MultiSelect
              options={factorOptions}
              values={selectedFactors}
              onChange={setSelectedFactors}
              placeholder="Select factors..."
            />
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <p className="text-[10px] font-mono text-terminal-text-dim uppercase tracking-wider mb-1">
                {t("models.fitStart")}
              </p>
              <DatePicker value={fitStart} onChange={setFitStart} />
            </div>
            <div>
              <p className="text-[10px] font-mono text-terminal-text-dim uppercase tracking-wider mb-1">
                {t("models.fitEnd")}
              </p>
              <DatePicker value={fitEnd} onChange={setFitEnd} />
            </div>
          </div>
          <div className="flex flex-wrap gap-4">
            <label className="flex items-center gap-2 text-xs font-mono text-terminal-text cursor-pointer">
              <input
                type="checkbox"
                checked={qlibNative}
                onChange={(e) => setQlibNative(e.target.checked)}
                className="accent-terminal-green"
              />
              {t("models.qlibNative")}
            </label>
            <label className="flex items-center gap-2 text-xs font-mono text-terminal-text cursor-pointer">
              <input
                type="checkbox"
                checked={withSector}
                onChange={(e) => setWithSector(e.target.checked)}
                className="accent-terminal-green"
              />
              With Sector
            </label>
            <label className="flex items-center gap-2 text-xs font-mono text-terminal-text cursor-pointer">
              <input
                type="checkbox"
                checked={noExtraFactors}
                onChange={(e) => setNoExtraFactors(e.target.checked)}
                className="accent-terminal-green"
              />
              No Extra Factors
            </label>
            <label className="flex items-center gap-2 text-xs font-mono text-terminal-text cursor-pointer">
              <input
                type="checkbox"
                checked={skipFactorPipeline}
                onChange={(e) => setSkipFactorPipeline(e.target.checked)}
                className="accent-terminal-green"
              />
              Skip Factor Pipeline
            </label>
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <p className="text-[10px] font-mono text-terminal-text-dim uppercase tracking-wider mb-1">
                Bagging Fraction
              </p>
              <NumberInput
                value={baggingFraction}
                onChange={(v) => setBaggingFraction(v)}
                step={0.05}
                min={0.1}
                max={1.0}
                placeholder="0.8"
              />
            </div>
            <div>
              <p className="text-[10px] font-mono text-terminal-text-dim uppercase tracking-wider mb-1">
                Ensemble Seeds
              </p>
              <input
                type="text"
                value={ensembleSeeds}
                onChange={(e) => setEnsembleSeeds(e.target.value)}
                placeholder="42,123,2024"
                className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
              />
            </div>
          </div>
          <button
            onClick={handleTrain}
            className="px-3 py-1.5 text-xs font-mono border border-terminal-green text-terminal-green hover:bg-terminal-green-glow transition-colors"
          >
            {t("models.trainBtn")}
          </button>
        </div>
      </Card>
      <TaskStatus taskId={taskId} />
    </div>
  );
}

// ─── Registry Tab ──────────────────────────────────────────────────────

function RegistryTab() {
  const { t } = useTranslation();
  const [registry, setRegistry] = useState<RegistryInfo | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    get<RegistryInfo>("/models/registry")
      .then(setRegistry)
      .catch(() => setRegistry(null))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <SkeletonTable rows={4} />;
  if (!registry) return null;

  return (
    <div className="space-y-6">
      <Card
        title={t("models.registeredModels", {
          count: registry.models.length,
        })}
      >
        <Table
          columns={[
            {
              key: "name",
              label: t("common.name"),
              sortable: true,
              render: (row) => (
                <span className="font-mono text-xs text-terminal-green">
                  {row.name as string}
                </span>
              ),
            },
          ]}
          data={registry.models as unknown as Record<string, unknown>[]}
          pageSize={20}
        />
      </Card>
      <Card
        title={t("models.registeredFactors", {
          count: registry.factors.length,
        })}
      >
        <Table
          columns={[
            {
              key: "name",
              label: t("common.name"),
              sortable: true,
              render: (row) => (
                <span className="font-mono text-xs text-terminal-cyan">
                  {row.name as string}
                </span>
              ),
            },
          ]}
          data={registry.factors as unknown as Record<string, unknown>[]}
          pageSize={20}
        />
      </Card>
    </div>
  );
}

// ─── Main Page ─────────────────────────────────────────────────────────

export function ModelsPage() {
  const { t } = useTranslation();
  const [activeTab, setActiveTab] = useState("browser");

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-sm font-mono font-semibold text-terminal-text-bright uppercase tracking-wider">
          {t("models.title")}
        </h1>
        <Tabs
          tabs={MODELS_TABS}
          activeKey={activeTab}
          onChange={setActiveTab}
        />
      </div>

      {activeTab === "browser" && <ModelBrowserTab />}
      {activeTab === "train" && <TrainTab />}
      {activeTab === "registry" && <RegistryTab />}
    </div>
  );
}
