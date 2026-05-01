import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { get, post, del } from "../api/client";

interface CacheStatusEntry {
  type: string;
  file_count: number;
  total_size_mb: number;
  latest: string | null;
  ttl_days: number;
}

interface StockMatch {
  symbol: string;
  name: string;
  cache_files: { type: string; file: string; modified: string }[];
}

interface StockLookupResult {
  symbol: string;
  matches: StockMatch[];
}

type Tab = "cache" | "fetch" | "lookup";

export function DataPage() {
  const { t } = useTranslation();
  const [tab, setTab] = useState<Tab>("cache");

  const tabs: { key: Tab; label: string }[] = [
    { key: "cache", label: t('data.cacheTab') },
    { key: "fetch", label: t('data.fetchTab') },
    { key: "lookup", label: t('data.lookupTab') },
  ];

  return (
    <div className="space-y-4 max-w-5xl">
      <h2 className="text-2xl font-bold text-zinc-900">{t('data.title')}</h2>
      <div className="flex gap-1 border-b border-zinc-200">
        {tabs.map((tb) => (
          <button
            key={tb.key}
            onClick={() => setTab(tb.key)}
            className={`px-4 py-2 text-sm font-medium -mb-px border-b-2 transition-colors ${
              tab === tb.key
                ? "border-amber-500 text-amber-600"
                : "border-transparent text-zinc-500 hover:text-zinc-700"
            }`}
          >
            {tb.label}
          </button>
        ))}
      </div>
      {tab === "cache" && <CacheStatusTab />}
      {tab === "fetch" && <FetchTab />}
      {tab === "lookup" && <StockLookupTab />}
    </div>
  );
}

function CacheStatusTab() {
  const { t } = useTranslation();
  const [entries, setEntries] = useState<CacheStatusEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [deleting, setDeleting] = useState<string | null>(null);

  const load = () => {
    setLoading(true);
    setError(null);
    get<CacheStatusEntry[]>("/data/cache-status")
      .then(setEntries)
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => { load(); }, []);

  const handleDeleteExpired = async (type: string) => {
    setDeleting(type);
    try {
      const res = await del<{ deleted: number }>(`/data/cache/${type}/expired`);
      alert(`Deleted ${res.deleted} expired files for ${type}`);
      load();
    } catch (err: any) {
      alert(`Error: ${err.message}`);
    } finally {
      setDeleting(null);
    }
  };

  if (loading) return <p className="text-zinc-500 text-sm py-4">{t('common.loading')}</p>;
  if (error) return <p className="text-red-600 text-sm py-4">{t('common.error')}: {error}</p>;

  const totalSize = entries.reduce((s, e) => s + e.total_size_mb, 0);
  const totalFiles = entries.reduce((s, e) => s + e.file_count, 0);

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <span className="text-sm text-zinc-500">
          {t('data.cacheSummary', { types: entries.length, files: totalFiles, size: totalSize.toFixed(1) })}
        </span>
        <button onClick={load} className="text-sm px-3 py-1.5 border border-zinc-300 rounded-md hover:bg-zinc-50 text-zinc-700">
          {t('common.refresh')}
        </button>
      </div>
      <div className="bg-white border border-zinc-200 rounded-lg overflow-hidden shadow-sm">
        <table className="w-full text-sm">
          <thead className="bg-zinc-50 border-b border-zinc-200">
            <tr>
              <th className="text-left px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.type')}</th>
              <th className="text-right px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.files')}</th>
              <th className="text-right px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.sizeMb')}</th>
              <th className="text-left px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.latest')}</th>
              <th className="text-right px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('data.ttlDays')}</th>
              <th className="text-right px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.actions')}</th>
            </tr>
          </thead>
          <tbody>
            {entries.map((e) => (
              <tr key={e.type} className="border-t border-zinc-100 hover:bg-zinc-50">
                <td className="px-4 py-2 font-mono text-xs text-zinc-700">{e.type}</td>
                <td className="text-right px-4 py-2 text-zinc-600">{e.file_count}</td>
                <td className="text-right px-4 py-2 text-zinc-600">{e.total_size_mb}</td>
                <td className="px-4 py-2 text-xs text-zinc-400">{e.latest ? new Date(e.latest).toLocaleString() : "-"}</td>
                <td className="text-right px-4 py-2 text-zinc-600">{e.ttl_days}</td>
                <td className="text-right px-4 py-2">
                  <button
                    onClick={() => handleDeleteExpired(e.type)}
                    disabled={deleting === e.type}
                    className="text-xs px-2 py-1 text-red-600 border border-red-300 rounded hover:bg-red-50 disabled:opacity-50"
                  >
                    {deleting === e.type ? t('common.deleting') : t('common.deleteExpired')}
                  </button>
                </td>
              </tr>
            ))}
            {entries.length === 0 && (
              <tr>
                <td colSpan={6} className="px-4 py-6 text-center text-zinc-400 text-sm">{t('data.noCache')}</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function FetchTab() {
  const { t } = useTranslation();
  const [dataTypes, setDataTypes] = useState<string[]>([]);
  const [selectedType, setSelectedType] = useState("financial");
  const [ttl, setTtl] = useState("");
  const [force, setForce] = useState(false);
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    get<CacheStatusEntry[]>("/data/cache-status")
      .then((entries) => {
        const types = entries.map((e) => e.type);
        setDataTypes(types);
        if (types.length > 0) setSelectedType(types[0]);
      })
      .catch(() => {});
  }, []);

  const handleFetch = async () => {
    setStatus(t('data.fetching'));
    setError(null);
    try {
      const body: Record<string, unknown> = { type: selectedType, force };
      if (ttl && !force) body.ttl = parseInt(ttl, 10);
      const res = await post<{ task_id: string }>("/data/fetch", body);
      const tid = res.task_id;
      setStatus(`Task submitted: ${tid}`);
      pollStatus(tid);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err));
      setStatus(null);
    }
  };

  const pollStatus = (tid: string) => {
    const interval = setInterval(async () => {
      try {
        const tasks = await get<{ task_id: string; status: string; error?: string }[]>("/system/tasks");
        const task = tasks.find((tk) => tk.task_id === tid);
        if (!task) return;
        if (task.status === "done") { setStatus(t('data.fetchDone')); clearInterval(interval); }
        else if (task.status === "failed") { setError(task.error || t('common.error')); setStatus(null); clearInterval(interval); }
        else { setStatus(`Task ${tid}: ${task.status}...`); }
      } catch { clearInterval(interval); }
    }, 2000);
  };

  return (
    <div className="space-y-4 max-w-lg">
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('data.dataType')}</label>
        <select value={selectedType} onChange={(e) => setSelectedType(e.target.value)}
          className="w-full border border-zinc-300 rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-400">
          <option value="all">all</option>
          {dataTypes.map((tp) => <option key={tp} value={tp}>{tp}</option>)}
        </select>
      </div>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('data.ttlOverride')}</label>
        <input type="number" value={ttl} onChange={(e) => setTtl(e.target.value)} placeholder="default" disabled={force}
          className="w-full border border-zinc-300 rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-400 disabled:bg-zinc-50 disabled:text-zinc-400" />
      </div>
      <div className="flex items-center gap-2">
        <input type="checkbox" id="force-refresh" checked={force} onChange={(e) => setForce(e.target.checked)} className="rounded" />
        <label htmlFor="force-refresh" className="text-sm text-zinc-700">{t('data.forceRefresh')}</label>
      </div>
      <button onClick={handleFetch} disabled={status?.includes("Submitting") || status?.includes("running")}
        className="px-4 py-2 bg-amber-500 text-zinc-900 font-medium rounded-md text-sm hover:bg-amber-600 disabled:opacity-50">
        {t('data.fetchBtn')}
      </button>
      {status && <p className={`text-sm ${error ? "text-red-600" : "text-green-700"}`}>{status}</p>}
      {error && <p className="text-sm text-red-600">{error}</p>}
    </div>
  );
}

function StockLookupTab() {
  const { t } = useTranslation();
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<StockLookupResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSearch = async () => {
    if (!query.trim()) return;
    setLoading(true);
    setError(null);
    setResults(null);
    try {
      const res = await get<StockLookupResult>(`/data/stock-lookup/${encodeURIComponent(query.trim())}`);
      setResults(res);
    } catch (err: any) { setError(err.message); }
    finally { setLoading(false); }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => { if (e.key === "Enter") handleSearch(); };

  return (
    <div className="space-y-4">
      <div className="flex gap-2 max-w-lg">
        <input type="text" value={query} onChange={(e) => setQuery(e.target.value)} onKeyDown={handleKeyDown}
          placeholder={t('data.lookupPlaceholder')}
          className="flex-1 border border-zinc-300 rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-400" />
        <button onClick={handleSearch} disabled={loading}
          className="px-4 py-2 bg-amber-500 text-zinc-900 font-medium rounded-md text-sm hover:bg-amber-600 disabled:opacity-50">
          {loading ? t('common.loading') : t('common.search')}
        </button>
      </div>
      {error && <p className="text-sm text-red-600">{error}</p>}
      {results && (
        <div className="space-y-4">
          <p className="text-sm text-zinc-500">{t('data.found', { count: results.matches.length, symbol: results.symbol })}</p>
          {results.matches.length === 0 && <p className="text-sm text-zinc-400">{t('data.noMatch')}</p>}
          {results.matches.map((match) => (
            <div key={match.symbol} className="bg-white border border-zinc-200 rounded-lg p-4 shadow-sm space-y-2">
              <div className="flex items-baseline gap-3">
                <span className="font-mono font-bold text-sm text-zinc-800">{match.symbol}</span>
                <span className="text-zinc-700">{match.name}</span>
                <span className="text-xs text-zinc-400">{t('data.cachedFiles', { count: match.cache_files.length })}</span>
              </div>
              {match.cache_files.length > 0 && (
                <table className="w-full text-xs">
                  <thead className="bg-zinc-50">
                    <tr>
                      <th className="text-left px-3 py-1 text-zinc-500">{t('data.dataTypeCol')}</th>
                      <th className="text-left px-3 py-1 text-zinc-500">{t('data.fileCol')}</th>
                      <th className="text-left px-3 py-1 text-zinc-500">{t('common.modified')}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {match.cache_files.map((cf, i) => (
                      <tr key={i} className="border-t border-zinc-100">
                        <td className="px-3 py-1 font-mono text-zinc-700">{cf.type}</td>
                        <td className="px-3 py-1 font-mono text-zinc-700">{cf.file}</td>
                        <td className="px-3 py-1 text-zinc-400">{new Date(cf.modified).toLocaleString()}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
