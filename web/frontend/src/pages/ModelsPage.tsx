import { useEffect, useState } from "react";
import { get, post } from "../api/client";

// ── Types ─────────────────────────────────���────────────────────────────────────

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

type Tab = "browser" | "train" | "registry";

// ── Component ──────────────────────────────────────────────────────────────────

export function ModelsPage() {
  const [tab, setTab] = useState<Tab>("browser");

  const tabs: { key: Tab; label: string }[] = [
    { key: "browser", label: "Model Browser" },
    { key: "train", label: "Train" },
    { key: "registry", label: "Registry" },
  ];

  return (
    <div className="space-y-4 max-w-5xl">
      <h2 className="text-2xl font-bold">Models</h2>

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
      {tab === "browser" && <ModelBrowserTab />}
      {tab === "train" && <TrainTab />}
      {tab === "registry" && <RegistryTab />}
    </div>
  );
}

// ── Model Browser Tab ──────────────────────────────────────────────────────────

function ModelBrowserTab() {
  const [models, setModels] = useState<ModelInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [meta, setMeta] = useState<Record<string, unknown> | null>(null);
  const [importance, setImportance] = useState<Record<string, unknown> | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  useEffect(() => {
    get<ModelInfo[]>("/models")
      .then(setModels)
      .catch((err) => setError(err.message))
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
    } catch (err: any) {
      setError(err.message);
    } finally {
      setDetailLoading(false);
    }
  };

  if (loading) return <p className="text-gray-500 py-4">Loading models...</p>;
  if (error) return <p className="text-red-600 py-4">Error: {error}</p>;

  if (models.length === 0) {
    return <p className="text-gray-500 py-4">No saved models found.</p>;
  }

  return (
    <div className="space-y-2">
      <p className="text-sm text-gray-500">{models.length} saved model(s)</p>

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
              <ModelRow
                key={m.filename}
                model={m}
                isExpanded={expanded === m.filename}
                onToggle={() => handleExpand(m.filename)}
                meta={expanded === m.filename ? meta : null}
                importance={expanded === m.filename ? importance : null}
                detailLoading={detailLoading}
              />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Model Row (expandable) ────────────────────────────────────────────────────

function ModelRow({
  model,
  isExpanded,
  onToggle,
  meta,
  importance,
  detailLoading,
}: {
  model: ModelInfo;
  isExpanded: boolean;
  onToggle: () => void;
  meta: Record<string, unknown> | null;
  importance: Record<string, unknown> | null;
  detailLoading: boolean;
}) {
  const importanceEntries = importance
    ? Object.entries(importance)
        .sort(([, a], [, b]) => (b as number) - (a as number))
        .slice(0, 20)
    : [];

  return (
    <>
      <tr
        className="border-t hover:bg-gray-50 cursor-pointer"
        onClick={onToggle}
      >
        <td className="px-4 py-2">
          <span className="font-mono text-xs">{model.filename}</span>
          <span className="ml-2 text-xs text-gray-400">
            {isExpanded ? "▼" : "▶"}
          </span>
        </td>
        <td className="text-right px-4 py-2">{model.size_mb}</td>
        <td className="px-4 py-2 text-xs text-gray-600">
          {new Date(model.modified).toLocaleString()}
        </td>
      </tr>

      {isExpanded && (
        <tr className="border-t bg-gray-50">
          <td colSpan={3} className="px-6 py-4">
            {detailLoading ? (
              <p className="text-sm text-gray-500">Loading details...</p>
            ) : (
              <div className="grid grid-cols-2 gap-6">
                {/* Meta section */}
                <div>
                  <h4 className="text-sm font-semibold mb-2">Meta</h4>
                  {meta && Object.keys(meta).length > 0 ? (
                    <table className="w-full text-xs">
                      <tbody>
                        {Object.entries(meta).map(([k, v]) => (
                          <tr key={k} className="border-t">
                            <td className="py-1 pr-3 font-medium text-gray-600">
                              {k}
                            </td>
                            <td className="py-1 font-mono">
                              {typeof v === "object"
                                ? JSON.stringify(v)
                                : String(v)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  ) : (
                    <p className="text-xs text-gray-400">No meta file found</p>
                  )}
                </div>

                {/* Feature importance section */}
                <div>
                  <h4 className="text-sm font-semibold mb-2">
                    Feature Importance (top 20)
                  </h4>
                  {importanceEntries.length > 0 ? (
                    <table className="w-full text-xs">
                      <thead>
                        <tr>
                          <th className="text-left py-1">Feature</th>
                          <th className="text-right py-1">Importance</th>
                        </tr>
                      </thead>
                      <tbody>
                        {importanceEntries.map(([feat, val]) => (
                          <tr key={feat} className="border-t">
                            <td className="py-1 font-mono">{feat}</td>
                            <td className="text-right py-1">
                              {(val as number).toFixed(4)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  ) : (
                    <p className="text-xs text-gray-400">
                      No importance file found
                    </p>
                  )}
                </div>
              </div>
            )}
          </td>
        </tr>
      )}
    </>
  );
}

// ── Train Tab ──────────────────────────────────────────────────────────────────

function TrainTab() {
  const [registry, setRegistry] = useState<RegistryInfo | null>(null);
  const [modelType, setModelType] = useState("lgbm");
  const [tag, setTag] = useState("");
  const [selectedFactors, setSelectedFactors] = useState<string[]>([]);
  const [fitStart, setFitStart] = useState("");
  const [fitEnd, setFitEnd] = useState("");
  const [qlibNative, setQlibNative] = useState(false);
  const [dryRun, setDryRun] = useState(true);
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    get<RegistryInfo>("/models/registry")
      .then((reg) => {
        setRegistry(reg);
        if (reg.models.length > 0) setModelType(reg.models[0].name);
      })
      .catch(() => {});
  }, []);

  const toggleFactor = (name: string) => {
    setSelectedFactors((prev) =>
      prev.includes(name) ? prev.filter((f) => f !== name) : [...prev, name]
    );
  };

  const handleTrain = async () => {
    setStatus("Submitting training task...");
    setError(null);
    try {
      const body: any = {
        model: modelType,
        qlib_native: qlibNative,
        factors: selectedFactors,
      };
      if (tag.trim()) body.tag = tag.trim();
      if (fitStart) body.fit_start = fitStart;
      if (fitEnd) body.fit_end = fitEnd;

      const res = await post<{ task_id: string }>("/models/train", body);
      setStatus(`Training task submitted: ${res.task_id}`);
      pollTrainStatus(res.task_id);
    } catch (err: any) {
      setError(err.message);
      setStatus(null);
    }
  };

  const pollTrainStatus = (tid: string) => {
    const interval = setInterval(async () => {
      try {
        const tasks = await get<
          { task_id: string; status: string; error?: string }[]
        >("/system/tasks");
        const task = tasks.find((t) => t.task_id === tid);
        if (!task) return;
        if (task.status === "done") {
          setStatus("Training completed successfully.");
          clearInterval(interval);
        } else if (task.status === "failed") {
          setError(task.error || "Training failed");
          setStatus(null);
          clearInterval(interval);
        } else {
          setStatus(`Task ${tid}: ${task.status}...`);
        }
      } catch {
        clearInterval(interval);
      }
    }, 3000);
  };

  return (
    <div className="space-y-4 max-w-lg">
      {/* Model type */}
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">
          Model Type
        </label>
        <select
          value={modelType}
          onChange={(e) => setModelType(e.target.value)}
          className="w-full border rounded px-3 py-2 text-sm"
        >
          {registry?.models.map((m) => (
            <option key={m.name} value={m.name}>
              {m.name}
            </option>
          ))}
        </select>
      </div>

      {/* Tag */}
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">
          Tag (optional)
        </label>
        <input
          type="text"
          value={tag}
          onChange={(e) => setTag(e.target.value)}
          placeholder="e.g. baseline, sector_full"
          className="w-full border rounded px-3 py-2 text-sm"
        />
      </div>

      {/* Factors */}
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-2">
          Factors
        </label>
        <div className="flex flex-wrap gap-2">
          {registry?.factors.map((f) => (
            <label
              key={f.name}
              className={`inline-flex items-center gap-1 px-2 py-1 border rounded text-xs cursor-pointer transition-colors ${
                selectedFactors.includes(f.name)
                  ? "bg-blue-50 border-blue-400 text-blue-700"
                  : "bg-white border-gray-300 text-gray-600 hover:bg-gray-50"
              }`}
            >
              <input
                type="checkbox"
                checked={selectedFactors.includes(f.name)}
                onChange={() => toggleFactor(f.name)}
                className="rounded"
              />
              {f.name}
            </label>
          ))}
        </div>
      </div>

      {/* Date range */}
      <div className="grid grid-cols-2 gap-3">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Fit Start
          </label>
          <input
            type="date"
            value={fitStart}
            onChange={(e) => setFitStart(e.target.value)}
            className="w-full border rounded px-3 py-2 text-sm"
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Fit End
          </label>
          <input
            type="date"
            value={fitEnd}
            onChange={(e) => setFitEnd(e.target.value)}
            className="w-full border rounded px-3 py-2 text-sm"
          />
        </div>
      </div>

      {/* Toggles */}
      <div className="space-y-2">
        <div className="flex items-center gap-2">
          <input
            type="checkbox"
            id="qlib-native"
            checked={qlibNative}
            onChange={(e) => setQlibNative(e.target.checked)}
            className="rounded"
          />
          <label htmlFor="qlib-native" className="text-sm text-gray-700">
            qlib-native mode (MLflow tracked)
          </label>
        </div>
        <div className="flex items-center gap-2">
          <input
            type="checkbox"
            id="dry-run-train"
            checked={dryRun}
            onChange={(e) => setDryRun(e.target.checked)}
            className="rounded"
          />
          <label htmlFor="dry-run-train" className="text-sm text-gray-700">
            Dry run (preview only)
          </label>
        </div>
      </div>

      {/* Submit */}
      <button
        onClick={handleTrain}
        disabled={!!status && status.includes("Submitting")}
        className="px-4 py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-700 disabled:opacity-50"
      >
        {dryRun ? "Train (Dry Run)" : "Train"}
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

// ── Registry Tab ───────────────────────────────────────────────────────────────

function RegistryTab() {
  const [registry, setRegistry] = useState<RegistryInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    get<RegistryInfo>("/models/registry")
      .then(setRegistry)
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <p className="text-gray-500 py-4">Loading registry...</p>;
  if (error) return <p className="text-red-600 py-4">Error: {error}</p>;
  if (!registry) return null;

  return (
    <div className="space-y-6">
      {/* Registered models */}
      <div>
        <h3 className="text-lg font-semibold mb-3">
          Registered Models ({registry.models.length})
        </h3>
        <div className="border rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50">
              <tr>
                <th className="text-left px-4 py-2">Name</th>
                <th className="text-left px-4 py-2">Class</th>
              </tr>
            </thead>
            <tbody>
              {registry.models.map((m) => (
                <tr key={m.name} className="border-t hover:bg-gray-50">
                  <td className="px-4 py-2 font-mono text-sm">{m.name}</td>
                  <td className="px-4 py-2 text-xs text-gray-500">
                    {m.name.charAt(0).toUpperCase() + m.name.slice(1)}AlphaModel
                  </td>
                </tr>
              ))}
              {registry.models.length === 0 && (
                <tr>
                  <td colSpan={2} className="px-4 py-4 text-center text-gray-400">
                    No models registered
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Registered factors */}
      <div>
        <h3 className="text-lg font-semibold mb-3">
          Registered Factors ({registry.factors.length})
        </h3>
        <div className="border rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50">
              <tr>
                <th className="text-left px-4 py-2">Name</th>
                <th className="text-left px-4 py-2">Class</th>
              </tr>
            </thead>
            <tbody>
              {registry.factors.map((f) => (
                <tr key={f.name} className="border-t hover:bg-gray-50">
                  <td className="px-4 py-2 font-mono text-sm">{f.name}</td>
                  <td className="px-4 py-2 text-xs text-gray-500">
                    {f.name.charAt(0).toUpperCase() + f.name.slice(1)}Factor
                  </td>
                </tr>
              ))}
              {registry.factors.length === 0 && (
                <tr>
                  <td colSpan={2} className="px-4 py-4 text-center text-gray-400">
                    No factors registered
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
