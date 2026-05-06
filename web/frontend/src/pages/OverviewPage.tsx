import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { Brain, LineChart, Radio } from "lucide-react";
import { Card } from "../components/ui/Card";
import { Badge } from "../components/ui/Badge";
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
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <p className="text-zinc-500 text-sm">{t("common.loading")}</p>;
  if (error) return <p className="text-red-400 text-sm">{t("common.error")}: {error}</p>;

  const lastModel = models.length > 0 ? models[models.length - 1] : null;
  const cacheEntries = Object.entries(runtime?.cache_types ?? {}).sort(([a], [b]) => a.localeCompare(b));
  const totalCacheMb = cacheEntries.reduce((s, [, v]) => s + v.total_size_mb, 0);

  return (
    <div className="p-6 space-y-6 max-w-5xl">
      <h1 className="text-xl font-semibold text-zinc-100">{t("overview.title")}</h1>

      {/* Quick-start actions */}
      <div className="grid grid-cols-3 gap-4">
        <Link to="/models" className="block">
          <Card className="hover:border-blue-600 transition-colors cursor-pointer">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-blue-900/50">
                <Brain size={20} className="text-blue-400" />
              </div>
              <div>
                <p className="text-sm font-medium text-zinc-200">{t("overview.trainModel")}</p>
                <p className="text-xs text-zinc-500">{t("overview.trainModelDesc")}</p>
              </div>
            </div>
          </Card>
        </Link>
        <Link to="/backtest" className="block">
          <Card className="hover:border-emerald-600 transition-colors cursor-pointer">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-emerald-900/50">
                <LineChart size={20} className="text-emerald-400" />
              </div>
              <div>
                <p className="text-sm font-medium text-zinc-200">{t("overview.runBacktest")}</p>
                <p className="text-xs text-zinc-500">{t("overview.runBacktestDesc")}</p>
              </div>
            </div>
          </Card>
        </Link>
        <Link to="/signals" className="block">
          <Card className="hover:border-amber-600 transition-colors cursor-pointer">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-lg bg-amber-900/50">
                <Radio size={20} className="text-amber-400" />
              </div>
              <div>
                <p className="text-sm font-medium text-zinc-200">{t("overview.generateSignals")}</p>
                <p className="text-xs text-zinc-500">{t("overview.generateSignalsDesc")}</p>
              </div>
            </div>
          </Card>
        </Link>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-3 gap-4">
        <Card>
          <p className="text-xs text-zinc-500 uppercase mb-1">{t("dashboard.python")}</p>
          <p className="text-sm font-mono font-semibold text-zinc-200">
            {runtime?.python_version?.split(" ")[0]}
          </p>
        </Card>
        <Card>
          <p className="text-xs text-zinc-500 uppercase mb-1">{t("dashboard.models")}</p>
          <p className="text-2xl font-bold text-zinc-100">{runtime?.models_count ?? 0}</p>
          {lastModel && (
            <p className="text-xs text-zinc-500 mt-1">
              {t("common.latest")}: {lastModel.filename}
            </p>
          )}
        </Card>
        <Card>
          <p className="text-xs text-zinc-500 uppercase mb-1">{t("dashboard.regime")}</p>
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
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-zinc-700">
                <th className="text-left px-3 py-2 text-xs text-zinc-400 uppercase">ID</th>
                <th className="text-left px-3 py-2 text-xs text-zinc-400 uppercase">Type</th>
                <th className="text-left px-3 py-2 text-xs text-zinc-400 uppercase">Status</th>
                <th className="text-left px-3 py-2 text-xs text-zinc-400 uppercase">Created</th>
              </tr>
            </thead>
            <tbody>
              {tasks.map((task) => (
                <tr key={task.task_id} className="border-b border-zinc-800">
                  <td className="px-3 py-2 font-mono text-xs text-zinc-400">
                    {task.task_id.slice(0, 8)}
                  </td>
                  <td className="px-3 py-2 text-zinc-300">{task.task_type}</td>
                  <td className="px-3 py-2">
                    <Badge variant={STATUS_VARIANT[task.status] || "neutral"}>
                      {task.status}
                    </Badge>
                  </td>
                  <td className="px-3 py-2 text-xs text-zinc-500">
                    {new Date(task.created_at).toLocaleString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </Card>
      )}

      {/* Cache status */}
      <Card
        title={t("dashboard.cacheStatus")}
        actions={
          <span className="text-sm text-zinc-500">
            {cacheEntries.length} {t("common.type")}, {totalCacheMb.toFixed(1)} MB
          </span>
        }
      >
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-zinc-700">
              <th className="text-left px-3 py-2 text-xs text-zinc-400 uppercase">{t("common.type")}</th>
              <th className="text-right px-3 py-2 text-xs text-zinc-400 uppercase">{t("common.files")}</th>
              <th className="text-right px-3 py-2 text-xs text-zinc-400 uppercase">{t("common.sizeMb")}</th>
              <th className="text-left px-3 py-2 text-xs text-zinc-400 uppercase">{t("common.latest")}</th>
            </tr>
          </thead>
          <tbody>
            {cacheEntries.map(([type, info]) => (
              <tr key={type} className="border-b border-zinc-800 hover:bg-zinc-800/50">
                <td className="px-3 py-2 font-mono text-xs text-zinc-300">{type}</td>
                <td className="text-right px-3 py-2 text-zinc-400">{info.file_count}</td>
                <td className="text-right px-3 py-2 text-zinc-400">{info.total_size_mb}</td>
                <td className="px-3 py-2 text-xs text-zinc-500">
                  {info.latest ? new Date(info.latest).toLocaleDateString() : "-"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>

      {/* Model list */}
      {models.length > 0 && (
        <Card title={t("dashboard.savedModels")}>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-zinc-700">
                <th className="text-left px-3 py-2 text-xs text-zinc-400 uppercase">{t("common.filename")}</th>
                <th className="text-right px-3 py-2 text-xs text-zinc-400 uppercase">{t("common.sizeMb")}</th>
                <th className="text-left px-3 py-2 text-xs text-zinc-400 uppercase">{t("common.modified")}</th>
              </tr>
            </thead>
            <tbody>
              {models.map((m) => (
                <tr key={m.filename} className="border-b border-zinc-800 hover:bg-zinc-800/50">
                  <td className="px-3 py-2 font-mono text-xs text-zinc-300">{m.filename}</td>
                  <td className="text-right px-3 py-2 text-zinc-400">{m.size_mb}</td>
                  <td className="px-3 py-2 text-xs text-zinc-500">{new Date(m.modified).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Card>
      )}
    </div>
  );
}
