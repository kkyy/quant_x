import { useEffect, useState } from "react";
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

export function DashboardPage() {
  const [runtime, setRuntime] = useState<RuntimeInfo | null>(null);
  const [models, setModels] = useState<ModelInfo[]>([]);
  const [regime, setRegime] = useState<{ enabled: boolean; label?: string; error?: string } | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    Promise.all([
      get<RuntimeInfo>("/system/runtime"),
      get<ModelInfo[]>("/models"),
      get<{ enabled: boolean; label?: string; error?: string }>("/signals/regime"),
    ])
      .then(([rt, ms, reg]) => {
        setRuntime(rt);
        setModels(ms);
        setRegime(reg);
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <p className="text-gray-500">Loading...</p>;
  if (error) return <p className="text-red-600">Error: {error}</p>;

  const lastModel = models.length > 0 ? models[models.length - 1] : null;
  const cacheEntries = Object.entries(runtime?.cache_types ?? {}).sort(
    ([a], [b]) => a.localeCompare(b)
  );
  const totalCacheMb = cacheEntries.reduce((s, [, v]) => s + v.total_size_mb, 0);

  return (
    <div className="space-y-6 max-w-5xl">
      <h2 className="text-2xl font-bold">Dashboard</h2>

      {/* Summary cards */}
      <div className="grid grid-cols-3 gap-4">
        <div className="border rounded-lg p-4">
          <h3 className="text-sm text-gray-500 mb-1">Python</h3>
          <p className="text-sm font-mono">{runtime?.python_version?.split(" ")[0]}</p>
        </div>
        <div className="border rounded-lg p-4">
          <h3 className="text-sm text-gray-500 mb-1">Models</h3>
          <p className="text-2xl font-bold">{runtime?.models_count ?? 0}</p>
          {lastModel && (
            <p className="text-xs text-gray-500 mt-1">
              Latest: {lastModel.filename} ({new Date(lastModel.modified).toLocaleDateString()})
            </p>
          )}
        </div>
        <div className="border rounded-lg p-4">
          <h3 className="text-sm text-gray-500 mb-1">Regime Detection</h3>
          {regime?.enabled ? (
            <span className="inline-block px-2 py-1 text-xs bg-blue-100 text-blue-800 rounded">
              {regime.label || "enabled"}
            </span>
          ) : (
            <span className="inline-block px-2 py-1 text-xs bg-gray-100 text-gray-600 rounded">
              {regime?.error ? `error: ${regime.error}` : "disabled"}
            </span>
          )}
        </div>
      </div>

      {/* qlib data path */}
      <div className="border rounded-lg p-4">
        <h3 className="text-sm text-gray-500 mb-1">qlib Data Path</h3>
        <p className="text-xs font-mono break-all">{runtime?.qlib_data_path || "not configured"}</p>
      </div>

      {/* Cache status */}
      <div>
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-lg font-semibold">Cache Status</h3>
          <span className="text-sm text-gray-500">
            {cacheEntries.length} types, {totalCacheMb.toFixed(1)} MB total
          </span>
        </div>
        <div className="border rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50">
              <tr>
                <th className="text-left px-4 py-2">Type</th>
                <th className="text-right px-4 py-2">Files</th>
                <th className="text-right px-4 py-2">Size (MB)</th>
                <th className="text-left px-4 py-2">Latest</th>
              </tr>
            </thead>
            <tbody>
              {cacheEntries.map(([type, info]) => (
                <tr key={type} className="border-t hover:bg-gray-50">
                  <td className="px-4 py-2 font-mono text-xs">{type}</td>
                  <td className="text-right px-4 py-2">{info.file_count}</td>
                  <td className="text-right px-4 py-2">{info.total_size_mb}</td>
                  <td className="px-4 py-2 text-xs text-gray-600">
                    {info.latest ? new Date(info.latest).toLocaleDateString() : "-"}
                  </td>
                </tr>
              ))}
              {cacheEntries.length === 0 && (
                <tr>
                  <td colSpan={4} className="px-4 py-4 text-center text-gray-400">
                    No cache data
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Model list */}
      {models.length > 0 && (
        <div>
          <h3 className="text-lg font-semibold mb-3">Saved Models</h3>
          <div className="border rounded-lg overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-gray-50">
                <tr>
                  <th className="text-left px-4 py-2">Filename</th>
                  <th className="text-right px-4 py-2">Size (MB)</th>
                  <th className="text-left px-4 py-2">Modified</th>
                </tr>
              </thead>
              <tbody>
                {models.map((m) => (
                  <tr key={m.filename} className="border-t hover:bg-gray-50">
                    <td className="px-4 py-2 font-mono text-xs">{m.filename}</td>
                    <td className="text-right px-4 py-2">{m.size_mb}</td>
                    <td className="px-4 py-2 text-xs text-gray-600">
                      {new Date(m.modified).toLocaleString()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
