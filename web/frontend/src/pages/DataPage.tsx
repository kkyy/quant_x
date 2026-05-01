import { useEffect, useState } from "react";
import { get, post, del } from "../api/client";

// ── Types ──────────────────────────────────────────────────────────────────────

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

// ── Component ──────────────────────────────────────────────────────────────────

export function DataPage() {
  const [tab, setTab] = useState<Tab>("cache");

  const tabs: { key: Tab; label: string }[] = [
    { key: "cache", label: "Cache Status" },
    { key: "fetch", label: "Fetch" },
    { key: "lookup", label: "Stock Lookup" },
  ];

  return (
    <div className="space-y-4 max-w-5xl">
      <h2 className="text-2xl font-bold">Data Management</h2>

      {/* Tab bar */}
      <div className="flex gap-1 border-b">
        {tabs.map((t) => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            className={`px-4 py-2 text-sm font-medium -mb-px border-b-2 transition-colors ${
              tab === t.key
                ? "border-blue-600 text-blue-600"
                : "border-transparent text-gray-500 hover:text-gray-700"
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Tab panels */}
      {tab === "cache" && <CacheStatusTab />}
      {tab === "fetch" && <FetchTab />}
      {tab === "lookup" && <StockLookupTab />}
    </div>
  );
}

// ── Cache Status Tab ───────────────────────────────────────────────────────────

function CacheStatusTab() {
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

  useEffect(() => {
    load();
  }, []);

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

  if (loading) return <p className="text-gray-500 py-4">Loading cache status...</p>;
  if (error) return <p className="text-red-600 py-4">Error: {error}</p>;

  const totalSize = entries.reduce((s, e) => s + e.total_size_mb, 0);
  const totalFiles = entries.reduce((s, e) => s + e.file_count, 0);

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <span className="text-sm text-gray-500">
          {entries.length} data types, {totalFiles} files, {totalSize.toFixed(1)} MB total
        </span>
        <button
          onClick={load}
          className="text-sm px-3 py-1 border rounded hover:bg-gray-50"
        >
          Refresh
        </button>
      </div>

      <div className="border rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-gray-50">
            <tr>
              <th className="text-left px-4 py-2">Type</th>
              <th className="text-right px-4 py-2">Files</th>
              <th className="text-right px-4 py-2">Size (MB)</th>
              <th className="text-left px-4 py-2">Latest</th>
              <th className="text-right px-4 py-2">TTL (days)</th>
              <th className="text-right px-4 py-2">Actions</th>
            </tr>
          </thead>
          <tbody>
            {entries.map((e) => (
              <tr key={e.type} className="border-t hover:bg-gray-50">
                <td className="px-4 py-2 font-mono text-xs">{e.type}</td>
                <td className="text-right px-4 py-2">{e.file_count}</td>
                <td className="text-right px-4 py-2">{e.total_size_mb}</td>
                <td className="px-4 py-2 text-xs text-gray-600">
                  {e.latest ? new Date(e.latest).toLocaleString() : "-"}
                </td>
                <td className="text-right px-4 py-2">{e.ttl_days}</td>
                <td className="text-right px-4 py-2">
                  <button
                    onClick={() => handleDeleteExpired(e.type)}
                    disabled={deleting === e.type}
                    className="text-xs px-2 py-1 text-red-600 border border-red-300 rounded hover:bg-red-50 disabled:opacity-50"
                  >
                    {deleting === e.type ? "Deleting..." : "Delete Expired"}
                  </button>
                </td>
              </tr>
            ))}
            {entries.length === 0 && (
              <tr>
                <td colSpan={6} className="px-4 py-6 text-center text-gray-400">
                  No cache entries found
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Fetch Tab ──────────────────────────────────────────────────────────────────

function FetchTab() {
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
    setStatus("Submitting...");
    setError(null);
    try {
      const body: Record<string, unknown> = {
        type: selectedType,
        force,
      };
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
        const tasks = await get<
          { task_id: string; status: string; error?: string }[]
        >("/system/tasks");
        const task = tasks.find((t) => t.task_id === tid);
        if (!task) return;
        if (task.status === "done") {
          setStatus("Fetch completed successfully.");
          clearInterval(interval);
        } else if (task.status === "failed") {
          setError(task.error || "Fetch failed");
          setStatus(null);
          clearInterval(interval);
        } else {
          setStatus(`Task ${tid}: ${task.status}...`);
        }
      } catch {
        clearInterval(interval);
      }
    }, 2000);
  };

  return (
    <div className="space-y-4 max-w-lg">
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">
          Data Type
        </label>
        <select
          value={selectedType}
          onChange={(e) => setSelectedType(e.target.value)}
          className="w-full border rounded px-3 py-2 text-sm"
        >
          <option value="all">all</option>
          {dataTypes.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
      </div>

      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">
          TTL Override (days, leave empty for default)
        </label>
        <input
          type="number"
          value={ttl}
          onChange={(e) => setTtl(e.target.value)}
          placeholder="default"
          className="w-full border rounded px-3 py-2 text-sm"
          disabled={force}
        />
      </div>

      <div className="flex items-center gap-2">
        <input
          type="checkbox"
          id="force-refresh"
          checked={force}
          onChange={(e) => setForce(e.target.checked)}
          className="rounded"
        />
        <label htmlFor="force-refresh" className="text-sm text-gray-700">
          Force refresh (ignore cache TTL)
        </label>
      </div>

      <button
        onClick={handleFetch}
        className="px-4 py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-700 disabled:opacity-50"
        disabled={status?.includes("Submitting") || status?.includes("running")}
      >
        Fetch Data
      </button>

      {status && (
        <p className={`text-sm ${error ? "text-red-600" : "text-green-700"}`}>
          {status}
        </p>
      )}
      {error && <p className="text-sm text-red-600">{error}</p>}
    </div>
  );
}

// ── Stock Lookup Tab ───────────────────────────────────────────────────────────

function StockLookupTab() {
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
      const res = await get<StockLookupResult>(
        `/data/stock-lookup/${encodeURIComponent(query.trim())}`
      );
      setResults(res);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") handleSearch();
  };

  return (
    <div className="space-y-4">
      <div className="flex gap-2 max-w-lg">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Enter symbol or name (e.g. 600519,茅台)"
          className="flex-1 border rounded px-3 py-2 text-sm"
        />
        <button
          onClick={handleSearch}
          disabled={loading}
          className="px-4 py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-700 disabled:opacity-50"
        >
          {loading ? "Searching..." : "Search"}
        </button>
      </div>

      {error && <p className="text-sm text-red-600">{error}</p>}

      {results && (
        <div className="space-y-4">
          <p className="text-sm text-gray-500">
            Found {results.matches.length} match
            {results.matches.length !== 1 ? "es" : ""} for "{results.symbol}"
          </p>

          {results.matches.length === 0 && (
            <p className="text-sm text-gray-400">No matching stocks found.</p>
          )}

          {results.matches.map((match) => (
            <div key={match.symbol} className="border rounded-lg p-4 space-y-2">
              <div className="flex items-baseline gap-3">
                <span className="font-mono font-bold text-sm">{match.symbol}</span>
                <span className="text-gray-700">{match.name}</span>
                <span className="text-xs text-gray-400">
                  {match.cache_files.length} cached file(s)
                </span>
              </div>

              {match.cache_files.length > 0 && (
                <table className="w-full text-xs">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="text-left px-3 py-1">Data Type</th>
                      <th className="text-left px-3 py-1">File</th>
                      <th className="text-left px-3 py-1">Modified</th>
                    </tr>
                  </thead>
                  <tbody>
                    {match.cache_files.map((cf, i) => (
                      <tr key={i} className="border-t">
                        <td className="px-3 py-1 font-mono">{cf.type}</td>
                        <td className="px-3 py-1 font-mono">{cf.file}</td>
                        <td className="px-3 py-1 text-gray-500">
                          {new Date(cf.modified).toLocaleString()}
                        </td>
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
