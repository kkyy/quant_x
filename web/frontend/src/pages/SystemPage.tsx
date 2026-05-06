import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Card } from "../components/ui/Card";
import { Tabs } from "../components/ui/Tabs";
import { Badge } from "../components/ui/Badge";
import { Table } from "../components/ui/Table";
import { Skeleton, SkeletonTable, SkeletonCard } from "../components/ui/Skeleton";
import { get, del } from "../api/client";

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

interface LogsResponse {
  lines: string[];
  file: string | null;
}

interface DeleteResponse {
  deleted: number;
}

interface TaskInfo {
  task_id: string;
  task_type: string;
  status: string;
  created_at: string;
  error?: string;
}

const SYSTEM_TABS = [
  { key: "runtime", label: "Runtime" },
  { key: "tasks", label: "Tasks" },
  { key: "logs", label: "Logs" },
  { key: "cache", label: "Cache" },
];

// ─── Runtime Tab ───────────────────────────────────────────────────────

function RuntimeTab() {
  const { t } = useTranslation();
  const [runtime, setRuntime] = useState<RuntimeInfo | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    get<RuntimeInfo>("/system/runtime")
      .then(setRuntime)
      .catch(() => setRuntime(null))
      .finally(() => setLoading(false));
  }, []);

  if (loading)
    return (
      <div className="grid grid-cols-3 gap-4">
        <SkeletonCard rows={2} />
        <SkeletonCard rows={2} />
        <SkeletonCard rows={2} />
      </div>
    );
  if (!runtime) return null;

  return (
    <div className="grid grid-cols-3 gap-4">
      <Card>
        <h3 className="text-xs text-terminal-text-dim uppercase mb-1 font-mono tracking-wider">
          {t("system.pythonVersion")}
        </h3>
        <p className="text-sm font-mono text-terminal-text-bright">
          {runtime.python_version.split(" ")[0]}
        </p>
        <p className="text-xs text-terminal-text-dim mt-1 font-mono">
          {runtime.python_version.split(" ").slice(1).join(" ")}
        </p>
      </Card>
      <Card>
        <h3 className="text-xs text-terminal-text-dim uppercase mb-1 font-mono tracking-wider">
          {t("system.qlibPath")}
        </h3>
        <p className="text-xs font-mono text-terminal-text break-all">
          {runtime.qlib_data_path || "not configured"}
        </p>
      </Card>
      <Card>
        <h3 className="text-xs text-terminal-text-dim uppercase mb-1 font-mono tracking-wider">
          {t("system.savedModels")}
        </h3>
        <p className="text-2xl font-bold text-terminal-green font-mono">
          {runtime.models_count}
        </p>
      </Card>
    </div>
  );
}

// ─── Tasks Tab ─────────────────────────────────────────────────────────

function TasksTab() {
  const { t } = useTranslation();
  const [tasks, setTasks] = useState<TaskInfo[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = () => {
    setLoading(true);
    get<TaskInfo[]>("/system/tasks")
      .then(setTasks)
      .catch(() => setTasks([]))
      .finally(() => setLoading(false));
  };

  useEffect(refresh, []);

  const statusVariant = (status: string) => {
    switch (status) {
      case "done":
        return "success" as const;
      case "running":
        return "info" as const;
      case "failed":
        return "error" as const;
      case "cancelled":
        return "warning" as const;
      default:
        return "neutral" as const;
    }
  };

  return (
    <Card title="Background Tasks" actions={
      <button
        onClick={refresh}
        className="px-3 py-1.5 text-xs font-mono border border-terminal-border text-terminal-text-dim hover:border-terminal-text-dim hover:text-terminal-text transition-colors rounded-sm"
      >
        {t("common.refresh")}
      </button>
    }>
      {loading ? (
        <SkeletonTable rows={5} />
      ) : tasks.length === 0 ? (
        <p className="text-terminal-text-dim text-xs font-mono">No tasks found</p>
      ) : (
        <Table
          columns={[
            {
              key: "task_id",
              label: t("common.taskId"),
              render: (row) => (
                <span className="font-mono text-xs text-terminal-text-dim">
                  {(row.task_id as string).slice(0, 8)}
                </span>
              ),
            },
            {
              key: "task_type",
              label: "Type",
              sortable: true,
              render: (row) => (
                <span className="text-xs text-terminal-text font-mono">
                  {row.task_type as string}
                </span>
              ),
            },
            {
              key: "status",
              label: t("common.status"),
              sortable: true,
              render: (row) => (
                <Badge variant={statusVariant(row.status as string)}>
                  {row.status as string}
                </Badge>
              ),
            },
            {
              key: "created_at",
              label: t("common.modified"),
              sortable: true,
              render: (row) => (
                <span className="text-xs text-terminal-text-dim font-mono">
                  {new Date(row.created_at as string).toLocaleString()}
                </span>
              ),
            },
            {
              key: "error",
              label: "Error",
              render: (row) =>
                row.error ? (
                  <span className="text-xs text-terminal-red truncate max-w-xs block font-mono">
                    {row.error as string}
                  </span>
                ) : (
                  <span className="text-terminal-text-dim">-</span>
                ),
            },
          ]}
          data={tasks as unknown as Record<string, unknown>[]}
          pageSize={20}
        />
      )}
    </Card>
  );
}

// ─── Logs Tab ──────────────────────────────────────────────────────────

function LogsTab() {
  const { t } = useTranslation();
  const [logs, setLogs] = useState<string[]>([]);
  const [logFile, setLogFile] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const fetchLogs = () => {
    setLoading(true);
    get<LogsResponse>("/system/logs?lines=200")
      .then((data) => {
        setLogs(data.lines);
        setLogFile(data.file);
      })
      .catch(() => setLogs([]))
      .finally(() => setLoading(false));
  };

  useEffect(fetchLogs, []);

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        {logFile && (
          <span className="text-xs text-terminal-text-dim font-mono">
            {t("system.logFile", { file: logFile })}
          </span>
        )}
        <button
          onClick={fetchLogs}
          className="px-3 py-1.5 text-xs font-mono border border-terminal-border text-terminal-text-dim hover:border-terminal-text-dim hover:text-terminal-text transition-colors rounded-sm"
        >
          {t("common.refresh")}
        </button>
      </div>
      {loading ? (
        <Skeleton className="h-[400px] w-full" />
      ) : (
        <pre className="w-full bg-terminal-surface text-terminal-text-bright border border-terminal-border rounded-sm p-4 text-xs font-mono overflow-auto max-h-[600px] leading-relaxed">
          {logs.length > 0 ? logs.join("\n") : t("system.noLogs")}
        </pre>
      )}
    </div>
  );
}

// ─── Cache Tab ─────────────────────────────────────────────────────────

function CacheTab() {
  const { t } = useTranslation();
  const [cacheEntries, setCacheEntries] = useState<[string, CacheInfo][]>(
    []
  );
  const [loading, setLoading] = useState(true);
  const [deletingType, setDeletingType] = useState<string | null>(null);

  const refresh = () => {
    setLoading(true);
    get<RuntimeInfo>("/system/runtime")
      .then((data) =>
        setCacheEntries(
          Object.entries(data.cache_types).sort(([a], [b]) =>
            a.localeCompare(b)
          )
        )
      )
      .catch(() => setCacheEntries([]))
      .finally(() => setLoading(false));
  };

  useEffect(refresh, []);

  const handleDeleteExpired = async (type: string) => {
    setDeletingType(type);
    try {
      await del<DeleteResponse>(`/data/cache/${type}/expired`);
      refresh();
    } catch {
      // ignore
    } finally {
      setDeletingType(null);
    }
  };

  if (loading)
    return <SkeletonTable rows={5} />;

  return (
    <Table
      columns={[
        {
          key: "type",
          label: t("common.type"),
          sortable: true,
          render: (row) => (
            <span className="font-mono text-xs text-terminal-text">
              {row.type as string}
            </span>
          ),
        },
        {
          key: "file_count",
          label: t("common.files"),
          align: "right" as const,
          sortable: true,
        },
        {
          key: "total_size_mb",
          label: t("common.sizeMb"),
          align: "right" as const,
          sortable: true,
        },
        {
          key: "latest",
          label: t("common.latest"),
          render: (row) =>
            row.latest ? (
              <span className="text-xs text-terminal-text-dim font-mono">
                {new Date(row.latest as string).toLocaleDateString()}
              </span>
            ) : (
              <span className="text-terminal-text-dim">-</span>
            ),
        },
        {
          key: "actions",
          label: "",
          render: (row) => (
            <button
              onClick={() => handleDeleteExpired(row.type as string)}
              disabled={deletingType === (row.type as string)}
              className="px-3 py-1.5 text-xs font-mono border border-terminal-red text-terminal-red hover:bg-terminal-red-glow transition-colors rounded-sm disabled:opacity-30"
            >
              {deletingType === (row.type as string)
                ? t("common.deleting")
                : t("common.deleteExpired")}
            </button>
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
  );
}

// ─── Main Page ─────────────────────────────────────────────────────────

export function SystemPage() {
  const { t } = useTranslation();
  const [activeTab, setActiveTab] = useState("runtime");

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between">
        <h1 className="text-sm font-mono font-semibold text-terminal-text-bright uppercase tracking-wider">
          {t("system.title")}
        </h1>
        <Tabs
          tabs={SYSTEM_TABS}
          activeKey={activeTab}
          onChange={setActiveTab}
        />
      </div>

      {activeTab === "runtime" && <RuntimeTab />}
      {activeTab === "tasks" && <TasksTab />}
      {activeTab === "logs" && <LogsTab />}
      {activeTab === "cache" && <CacheTab />}
    </div>
  );
}
