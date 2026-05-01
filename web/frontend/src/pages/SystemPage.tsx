import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
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

type Tab = "runtime" | "logs" | "cache";

export function SystemPage() {
  const { t } = useTranslation();
  const [tab, setTab] = useState<Tab>("runtime");

  // Runtime state
  const [runtime, setRuntime] = useState<RuntimeInfo | null>(null);
  const [runtimeLoading, setRuntimeLoading] = useState(false);
  const [runtimeError, setRuntimeError] = useState<string | null>(null);

  // Logs state
  const [logs, setLogs] = useState<string[]>([]);
  const [logFile, setLogFile] = useState<string | null>(null);
  const [logsLoading, setLogsLoading] = useState(false);
  const [logsError, setLogsError] = useState<string | null>(null);

  // Cache state
  const [cacheEntries, setCacheEntries] = useState<[string, CacheInfo][]>([]);
  const [cacheLoading, setCacheLoading] = useState(false);
  const [cacheError, setCacheError] = useState<string | null>(null);
  const [deletingType, setDeletingType] = useState<string | null>(null);
  const [deleteMessages, setDeleteMessages] = useState<Record<string, string>>({});

  const fetchRuntime = () => {
    setRuntimeLoading(true);
    setRuntimeError(null);
    get<RuntimeInfo>("/system/runtime")
      .then((data) => {
        setRuntime(data);
        setCacheEntries(
          Object.entries(data.cache_types).sort(([a], [b]) => a.localeCompare(b))
        );
      })
      .catch((err) => setRuntimeError(err.message))
      .finally(() => setRuntimeLoading(false));
  };

  const fetchLogs = () => {
    setLogsLoading(true);
    setLogsError(null);
    get<LogsResponse>("/system/logs?lines=200")
      .then((data) => {
        setLogs(data.lines);
        setLogFile(data.file);
      })
      .catch((err) => setLogsError(err.message))
      .finally(() => setLogsLoading(false));
  };

  const fetchCache = () => {
    setCacheLoading(true);
    setCacheError(null);
    get<RuntimeInfo>("/system/runtime")
      .then((data) => {
        setCacheEntries(
          Object.entries(data.cache_types).sort(([a], [b]) => a.localeCompare(b))
        );
      })
      .catch((err) => setCacheError(err.message))
      .finally(() => setCacheLoading(false));
  };

  useEffect(() => {
    if (tab === "runtime") fetchRuntime();
    else if (tab === "logs") fetchLogs();
    else if (tab === "cache") fetchCache();
  }, [tab]);

  const handleDeleteExpired = async (type: string) => {
    setDeletingType(type);
    try {
      const res = await del<DeleteResponse>(`/data/cache/${type}/expired`);
      setDeleteMessages((prev) => ({
        ...prev,
        [type]: `Deleted ${res.deleted} expired files`,
      }));
      fetchCache();
    } catch (err: any) {
      setDeleteMessages((prev) => ({
        ...prev,
        [type]: `Error: ${err.message}`,
      }));
    } finally {
      setDeletingType(null);
    }
  };

  const tabs: { key: Tab; label: string }[] = [
    { key: "runtime", label: t("system.runtimeTab") },
    { key: "logs", label: t("system.logsTab") },
    { key: "cache", label: t("system.cacheTab") },
  ];

  return (
    <div className="space-y-4 max-w-5xl">
      <h2 className="text-2xl font-bold">{t("system.title")}</h2>

      {/* Tab bar */}
      <div className="flex gap-1 border-b border-zinc-200">
        {tabs.map((t) => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            className={`px-4 py-2 text-sm font-medium transition-colors ${
              tab === t.key
                ? "border-b-2 border-amber-500 text-amber-600"
                : "border-b-2 border-transparent text-zinc-500 hover:text-zinc-700"
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Runtime tab */}
      {tab === "runtime" && (
        <div className="space-y-4">
          {runtimeError && (
            <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-2 rounded text-sm">
              {runtimeError}
            </div>
          )}
          {runtimeLoading ? (
            <p className="text-zinc-500 text-sm">{t("common.loading")}</p>
          ) : runtime ? (
            <div className="grid grid-cols-3 gap-4">
              <div className="border border-zinc-200 rounded-lg p-4">
                <h3 className="text-sm text-zinc-500 mb-1">{t("system.pythonVersion")}</h3>
                <p className="text-sm font-mono">
                  {runtime.python_version.split(" ")[0]}
                </p>
                <p className="text-xs text-zinc-400 mt-1">
                  {runtime.python_version.split(" ").slice(1).join(" ")}
                </p>
              </div>
              <div className="border border-zinc-200 rounded-lg p-4">
                <h3 className="text-sm text-zinc-500 mb-1">{t("system.qlibPath")}</h3>
                <p className="text-xs font-mono break-all">
                  {runtime.qlib_data_path || "not configured"}
                </p>
              </div>
              <div className="border border-zinc-200 rounded-lg p-4">
                <h3 className="text-sm text-zinc-500 mb-1">{t("system.savedModels")}</h3>
                <p className="text-2xl font-bold">{runtime.models_count}</p>
              </div>
            </div>
          ) : null}
        </div>
      )}

      {/* Logs tab */}
      {tab === "logs" && (
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            {logFile && (
              <span className="text-sm text-zinc-500">File: {logFile}</span>
            )}
            <button
              onClick={fetchLogs}
              className="px-3 py-1.5 text-sm bg-white border border-zinc-300 rounded hover:bg-zinc-50"
            >
              {t("common.refresh")}
            </button>
          </div>

          {logsError && (
            <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-2 rounded text-sm">
              {logsError}
            </div>
          )}

          {logsLoading ? (
            <p className="text-zinc-500 text-sm">{t("common.loading")}</p>
          ) : (
            <pre className="w-full bg-zinc-900 text-zinc-100 border rounded-lg p-4 text-xs font-mono overflow-auto max-h-[600px] leading-relaxed">
              {logs.length > 0
                ? logs.join("\n")
                : t("system.noLogs")}
            </pre>
          )}
        </div>
      )}

      {/* Cache tab */}
      {tab === "cache" && (
        <div className="space-y-3">
          {cacheError && (
            <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-2 rounded text-sm">
              {cacheError}
            </div>
          )}

          {cacheLoading ? (
            <p className="text-zinc-500 text-sm">{t("common.loading")}</p>
          ) : (
            <div className="border border-zinc-200 rounded-lg overflow-hidden">
              <table className="w-full text-sm">
                <thead className="bg-zinc-50">
                  <tr>
                    <th className="text-left px-4 py-2">{t("common.type")}</th>
                    <th className="text-right px-4 py-2">{t("common.files")}</th>
                    <th className="text-right px-4 py-2">{t("common.sizeMb")}</th>
                    <th className="text-left px-4 py-2">{t("common.latest")}</th>
                    <th className="text-right px-4 py-2">{t("common.actions")}</th>
                  </tr>
                </thead>
                <tbody>
                  {cacheEntries.map(([type, info]) => (
                    <tr key={type} className="border-t border-zinc-200 hover:bg-zinc-50">
                      <td className="px-4 py-2 font-mono text-xs">{type}</td>
                      <td className="text-right px-4 py-2">{info.file_count}</td>
                      <td className="text-right px-4 py-2">{info.total_size_mb}</td>
                      <td className="px-4 py-2 text-xs text-zinc-600">
                        {info.latest
                          ? new Date(info.latest).toLocaleDateString()
                          : "-"}
                      </td>
                      <td className="text-right px-4 py-2">
                        <button
                          onClick={() => handleDeleteExpired(type)}
                          disabled={deletingType === type}
                          className="px-2 py-1 text-xs bg-red-50 text-red-600 border border-red-200 rounded hover:bg-red-100 disabled:opacity-50"
                        >
                          {deletingType === type ? t("common.deleting") : t("common.deleteExpired")}
                        </button>
                        {deleteMessages[type] && (
                          <span className="ml-2 text-xs text-zinc-500">
                            {deleteMessages[type]}
                          </span>
                        )}
                      </td>
                    </tr>
                  ))}
                  {cacheEntries.length === 0 && (
                    <tr>
                      <td colSpan={5} className="px-4 py-4 text-center text-zinc-400">
                        {t("system.noCache")}
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
