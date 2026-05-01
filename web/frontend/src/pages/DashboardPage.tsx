import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
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
  const { t } = useTranslation();
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
      .then(([rt, ms, reg]) => { setRuntime(rt); setModels(ms); setRegime(reg); })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <p className="text-zinc-500 text-sm">{t('common.loading')}</p>;
  if (error) return <p className="text-red-600 text-sm">{t('common.error')}: {error}</p>;

  const lastModel = models.length > 0 ? models[models.length - 1] : null;
  const cacheEntries = Object.entries(runtime?.cache_types ?? {}).sort(([a], [b]) => a.localeCompare(b));
  const totalCacheMb = cacheEntries.reduce((s, [, v]) => s + v.total_size_mb, 0);

  return (
    <div className="space-y-6 max-w-5xl">
      <h2 className="text-2xl font-bold text-zinc-900">{t('dashboard.title')}</h2>

      {/* Summary cards */}
      <div className="grid grid-cols-3 gap-4">
        <div className="bg-white border border-zinc-200 rounded-lg p-4 shadow-sm">
          <h3 className="text-xs text-zinc-500 uppercase tracking-wide mb-2">{t('dashboard.python')}</h3>
          <p className="text-sm font-mono font-semibold text-zinc-800">{runtime?.python_version?.split(" ")[0]}</p>
        </div>
        <div className="bg-white border border-zinc-200 rounded-lg p-4 shadow-sm">
          <h3 className="text-xs text-zinc-500 uppercase tracking-wide mb-2">{t('dashboard.models')}</h3>
          <p className="text-2xl font-bold text-zinc-900">{runtime?.models_count ?? 0}</p>
          {lastModel && (
            <p className="text-xs text-zinc-400 mt-1">
              {t('common.latest')}: {lastModel.filename} ({new Date(lastModel.modified).toLocaleDateString()})
            </p>
          )}
        </div>
        <div className="bg-white border border-zinc-200 rounded-lg p-4 shadow-sm">
          <h3 className="text-xs text-zinc-500 uppercase tracking-wide mb-2">{t('dashboard.regime')}</h3>
          {regime?.enabled ? (
            <span className="inline-block px-2 py-1 text-xs bg-amber-100 text-amber-700 rounded font-medium">
              {regime.label || t('common.enabled')}
            </span>
          ) : (
            <span className="inline-block px-2 py-1 text-xs bg-zinc-100 text-zinc-500 rounded">
              {regime?.error ? `error: ${regime.error}` : t('common.disabled')}
            </span>
          )}
        </div>
      </div>

      {/* qlib data path */}
      <div className="bg-white border border-zinc-200 rounded-lg p-4 shadow-sm">
        <h3 className="text-xs text-zinc-500 uppercase tracking-wide mb-2">{t('dashboard.qlibPath')}</h3>
        <p className="text-xs font-mono text-zinc-600 break-all">{runtime?.qlib_data_path || t('dashboard.notConfigured')}</p>
      </div>

      {/* Cache status */}
      <div>
        <div className="flex items-center justify-between mb-3">
          <h3 className="text-lg font-semibold text-zinc-800">{t('dashboard.cacheStatus')}</h3>
          <span className="text-sm text-zinc-400">
            {cacheEntries.length} {t('common.type')}, {totalCacheMb.toFixed(1)} MB
          </span>
        </div>
        <div className="bg-white border border-zinc-200 rounded-lg overflow-hidden shadow-sm">
          <table className="w-full text-sm">
            <thead className="bg-zinc-50 border-b border-zinc-200">
              <tr>
                <th className="text-left px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.type')}</th>
                <th className="text-right px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.files')}</th>
                <th className="text-right px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.sizeMb')}</th>
                <th className="text-left px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.latest')}</th>
              </tr>
            </thead>
            <tbody>
              {cacheEntries.map(([type, info]) => (
                <tr key={type} className="border-t border-zinc-100 hover:bg-zinc-50">
                  <td className="px-4 py-2 font-mono text-xs text-zinc-700">{type}</td>
                  <td className="text-right px-4 py-2 text-zinc-600">{info.file_count}</td>
                  <td className="text-right px-4 py-2 text-zinc-600">{info.total_size_mb}</td>
                  <td className="px-4 py-2 text-xs text-zinc-400">
                    {info.latest ? new Date(info.latest).toLocaleDateString() : "-"}
                  </td>
                </tr>
              ))}
              {cacheEntries.length === 0 && (
                <tr>
                  <td colSpan={4} className="px-4 py-4 text-center text-zinc-400 text-sm">{t('dashboard.noCache')}</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Model list */}
      {models.length > 0 && (
        <div>
          <h3 className="text-lg font-semibold text-zinc-800 mb-3">{t('dashboard.savedModels')}</h3>
          <div className="bg-white border border-zinc-200 rounded-lg overflow-hidden shadow-sm">
            <table className="w-full text-sm">
              <thead className="bg-zinc-50 border-b border-zinc-200">
                <tr>
                  <th className="text-left px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.filename')}</th>
                  <th className="text-right px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.sizeMb')}</th>
                  <th className="text-left px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.modified')}</th>
                </tr>
              </thead>
              <tbody>
                {models.map((m) => (
                  <tr key={m.filename} className="border-t border-zinc-100 hover:bg-zinc-50">
                    <td className="px-4 py-2 font-mono text-xs text-zinc-700">{m.filename}</td>
                    <td className="text-right px-4 py-2 text-zinc-600">{m.size_mb}</td>
                    <td className="px-4 py-2 text-xs text-zinc-400">{new Date(m.modified).toLocaleString()}</td>
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
