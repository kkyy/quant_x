import { useEffect, useState, useRef } from "react";
import { Link } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { Brain, LineChart, Radio } from "lucide-react";
import { Card } from "../components/ui/Card";
import { Badge } from "../components/ui/Badge";
import { Table } from "../components/ui/Table";
import { Skeleton, SkeletonTable } from "../components/ui/Skeleton";
import { get } from "../api/client";

interface CacheInfo {
  file_count: number;
  total_size_mb: number;
  latest: string | null;
}

interface RuntimeInfo {
  python_version: string;
  qlib_data_path: string;
  models_count: number;
  cache_types: Record<string, CacheInfo>;
}

interface ModelInfo {
  filename: string;
  size_mb: number;
  modified: string;
  meta: Record<string, unknown>;
}

interface TaskInfo {
  task_id: string;
  task_type: string;
  status: string;
  created_at: string;
  error?: string;
}

const STATUS_VARIANT: Record<string, "success" | "warning" | "error" | "info" | "neutral"> = {
  done: "success",
  running: "info",
  pending: "warning",
  failed: "error",
  cancelled: "neutral",
};

export function OverviewPage() {
  const { t } = useTranslation();
  const [runtime, setRuntime] = useState<RuntimeInfo | null>(null);
  const [models, setModels] = useState<ModelInfo[]>([]);
  const [regime, setRegime] = useState<{ enabled: boolean; label?: string; error?: string } | null>(null);
  const [tasks, setTasks] = useState<TaskInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const modelPathRef = useRef<string | null>(null);

  useEffect(() => {
    Promise.all([
      get<RuntimeInfo>("/system/runtime"),
      get<ModelInfo[]>("/models"),
      get<{ enabled: boolean; label?: string; error?: string }>("/signals/regime"),
      get<TaskInfo[]>("/system/tasks"),
    ])
      .then(([rt, ms, reg, ts]) => {
        setRuntime(rt);
        setModels(ms);
        setRegime(reg);
        setTasks(ts.slice(0, 10));
        if (ms.length > 0) {
          modelPathRef.current = ms[ms.length - 1].filename;
        }
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="space-y-6 max-w-5xl">
        <h1 className="text-sm font-mono font-semibold text-terminal-text-bright uppercase tracking-wider">
          {t("overview.title")}
        </h1>
        <div className="grid grid-cols-3 gap-4">
          <Skeleton className="h-20" />
          <Skeleton className="h-20" />
          <Skeleton className="h-20" />
        </div>
        <div className="grid grid-cols-3 gap-4">
          <Skeleton className="h-16" />
          <Skeleton className="h-16" />
          <Skeleton className="h-16" />
        </div>
        <SkeletonTable rows={5} />
      </div>
    );
  }
  if (error) return <p className="text-terminal-red text-xs font-mono">{t("common.error")}: {error}</p>;

  const lastModel = models.length > 0 ? models[models.length - 1] : null;
  const cacheEntries = Object.entries(runtime?.cache_types ?? {}).sort(([a], [b]) => a.localeCompare(b));
  const totalCacheMb = cacheEntries.reduce((s, [, v]) => s + v.total_size_mb, 0);

  return (
    <div className="space-y-6 max-w-5xl">
      <h1 className="text-sm font-mono font-semibold text-terminal-text-bright uppercase tracking-wider">
        {t("overview.title")}
      </h1>

      {/* Quick-start actions */}
      <div className="grid grid-cols-3 gap-4">
        <Link to="/models" className="block">
          <Card accent="green" className="hover:border-terminal-green transition-colors cursor-pointer">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-sm bg-terminal-green-glow">
                <Brain size={20} className="text-terminal-green" />
              </div>
              <div>
                <p className="text-sm font-medium text-terminal-text-bright">{t("overview.trainModel")}</p>
                <p className="text-xs text-terminal-text-dim">{t("overview.trainModelDesc")}</p>
              </div>
            </div>
          </Card>
        </Link>
        <Link to="/backtest" className="block">
          <Card accent="amber" className="hover:border-terminal-amber transition-colors cursor-pointer">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-sm bg-terminal-amber-glow">
                <LineChart size={20} className="text-terminal-amber" />
              </div>
              <div>
                <p className="text-sm font-medium text-terminal-text-bright">{t("overview.runBacktest")}</p>
                <p className="text-xs text-terminal-text-dim">{t("overview.runBacktestDesc")}</p>
              </div>
            </div>
          </Card>
        </Link>
        <Link to="/signals" className="block">
          <Card accent="cyan" className="hover:border-terminal-cyan transition-colors cursor-pointer">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-sm bg-terminal-cyan-glow">
                <Radio size={20} className="text-terminal-cyan" />
              </div>
              <div>
                <p className="text-sm font-medium text-terminal-text-bright">{t("overview.generateSignals")}</p>
                <p className="text-xs text-terminal-text-dim">{t("overview.generateSignalsDesc")}</p>
              </div>
            </div>
          </Card>
        </Link>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-3 gap-4">
        <Card>
          <p className="text-[10px] font-mono text-terminal-text-dim uppercase tracking-wider mb-1">{t("dashboard.python")}</p>
          <p className="text-sm font-mono font-semibold text-terminal-text-bright">
            {runtime?.python_version?.split(" ")[0]}
          </p>
        </Card>
        <Card>
          <p className="text-[10px] font-mono text-terminal-text-dim uppercase tracking-wider mb-1">{t("dashboard.models")}</p>
          <p className="text-2xl font-mono font-bold text-terminal-green">{runtime?.models_count ?? 0}</p>
          {lastModel && (
            <p className="text-xs font-mono text-terminal-text-dim mt-1 truncate">
              {t("common.latest")}: {lastModel.filename}
            </p>
          )}
        </Card>
        <Card>
          <p className="text-[10px] font-mono text-terminal-text-dim uppercase tracking-wider mb-1">{t("dashboard.regime")}</p>
          {regime?.enabled ? (
            <Badge variant="warning">{regime.label || t("common.enabled")}</Badge>
          ) : (
            <Badge variant="neutral">
              {regime?.error ? `error: ${regime.error}` : t("common.disabled")}
            </Badge>
          )}
        </Card>
      </div>

      {/* Recent tasks */}
      {tasks.length > 0 && (
        <Card title={t("overview.recentTasks")}>
          <Table
            columns={[
              {
                key: "task_id",
                label: "ID",
                mono: true,
                render: (row) => (
                  <span className="font-mono text-xs text-terminal-text-dim">
                    {(row.task_id as string).slice(0, 8)}
                  </span>
                ),
              },
              { key: "task_type", label: t("common.type") },
              {
                key: "status",
                label: "Status",
                render: (row) => (
                  <Badge variant={STATUS_VARIANT[row.status as string] || "neutral"}>
                    {row.status as string}
                  </Badge>
                ),
              },
              {
                key: "created_at",
                label: t("common.modified"),
                render: (row) => (
                  <span className="text-xs text-terminal-text-dim">
                    {new Date(row.created_at as string).toLocaleString()}
                  </span>
                ),
              },
            ]}
            data={tasks as unknown as Record<string, unknown>[]}
            pageSize={10}
          />
        </Card>
      )}

      {/* Cache status */}
      <Card
        title={t("dashboard.cacheStatus")}
        actions={
          <span className="text-xs font-mono text-terminal-text-dim">
            {cacheEntries.length} {t("common.type")}, {totalCacheMb.toFixed(1)} MB
          </span>
        }
      >
        <Table
          columns={[
            { key: "type", label: t("common.type"), mono: true },
            {
              key: "file_count",
              label: t("common.files"),
              align: "right" as const,
              mono: true,
            },
            {
              key: "total_size_mb",
              label: t("common.sizeMb"),
              align: "right" as const,
              mono: true,
            },
            {
              key: "latest",
              label: t("common.latest"),
              render: (row) => (
                <span className="text-xs text-terminal-text-dim">
                  {row.latest ? new Date(row.latest as string).toLocaleDateString() : "-"}
                </span>
              ),
            },
          ]}
          data={cacheEntries.map(([type, info]) => ({
            type,
            file_count: info.file_count,
            total_size_mb: info.total_size_mb,
            latest: info.latest,
          }))}
          pageSize={20}
        />
      </Card>

      {/* Model list */}
      {models.length > 0 && (
        <Card title={t("dashboard.savedModels")}>
          <Table
            columns={[
              {
                key: "filename",
                label: t("common.filename"),
                sortable: true,
                mono: true,
                render: (row) => (
                  <span className="font-mono text-xs text-terminal-green">
                    {row.filename as string}
                  </span>
                ),
              },
              {
                key: "size_mb",
                label: t("common.sizeMb"),
                sortable: true,
                align: "right" as const,
                mono: true,
              },
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
        </Card>
      )}
    </div>
  );
}
