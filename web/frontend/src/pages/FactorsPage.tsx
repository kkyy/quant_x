import { useEffect, useState } from "react";
import { get, post } from "../api/client";

interface FactorItem {
  name: string;
  class: string;
}

interface LibraryItem extends FactorItem {
  enabled: boolean;
}

interface TaskResponse {
  task_id: string;
}

type Tab = "library" | "evaluation" | "mining";

export function FactorsPage() {
  const [tab, setTab] = useState<Tab>("library");
  const [library, setLibrary] = useState<LibraryItem[]>([]);
  const [factors, setFactors] = useState<FactorItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Evaluation state
  const [selectedFactor, setSelectedFactor] = useState("");
  const [evalTaskId, setEvalTaskId] = useState<string | null>(null);
  const [evalSubmitting, setEvalSubmitting] = useState(false);

  // Mining state
  const [minIc, setMinIc] = useState("0.03");
  const [minIcir, setMinIcir] = useState("0.4");
  const [topN, setTopN] = useState("30");
  const [mineTaskId, setMineTaskId] = useState<string | null>(null);
  const [mineSubmitting, setMineSubmitting] = useState(false);

  useEffect(() => {
    setLoading(true);
    setError(null);
    if (tab === "library") {
      get<LibraryItem[]>("/factors/library")
        .then(setLibrary)
        .catch((err) => setError(err.message))
        .finally(() => setLoading(false));
    } else if (tab === "evaluation") {
      get<FactorItem[]>("/factors")
        .then((data) => {
          setFactors(data);
          if (data.length > 0 && !selectedFactor) {
            setSelectedFactor(data[0].name);
          }
        })
        .catch((err) => setError(err.message))
        .finally(() => setLoading(false));
    } else {
      setLoading(false);
    }
  }, [tab]);

  const handleEvaluate = async () => {
    if (!selectedFactor) return;
    setEvalSubmitting(true);
    setEvalTaskId(null);
    try {
      const res = await post<TaskResponse>("/factors/evaluate", { name: selectedFactor });
      setEvalTaskId(res.task_id);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setEvalSubmitting(false);
    }
  };

  const handleMine = async () => {
    setMineSubmitting(true);
    setMineTaskId(null);
    try {
      const res = await post<TaskResponse>("/factors/mine", {
        min_ic: parseFloat(minIc),
        min_icir: parseFloat(minIcir),
        top_n: parseInt(topN, 10),
      });
      setMineTaskId(res.task_id);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setMineSubmitting(false);
    }
  };

  const tabs: { key: Tab; label: string }[] = [
    { key: "library", label: "Library" },
    { key: "evaluation", label: "Evaluation" },
    { key: "mining", label: "Mining" },
  ];

  return (
    <div className="space-y-4 max-w-5xl">
      <h2 className="text-2xl font-bold">Factors</h2>

      {/* Tab bar */}
      <div className="flex gap-1 border-b">
        {tabs.map((t) => (
          <button
            key={t.key}
            onClick={() => { setTab(t.key); setError(null); }}
            className={`px-4 py-2 text-sm font-medium transition-colors ${
              tab === t.key
                ? "border-b-2 border-blue-600 text-blue-600"
                : "text-gray-500 hover:text-gray-700"
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-2 rounded text-sm">
          {error}
        </div>
      )}

      {loading && <p className="text-gray-500 text-sm">Loading...</p>}

      {/* Library tab */}
      {tab === "library" && !loading && (
        <div className="border rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50">
              <tr>
                <th className="text-left px-4 py-2">Name</th>
                <th className="text-left px-4 py-2">Class</th>
                <th className="text-left px-4 py-2">Status</th>
              </tr>
            </thead>
            <tbody>
              {library.map((f) => (
                <tr key={f.name} className="border-t hover:bg-gray-50">
                  <td className="px-4 py-2 font-mono text-xs">{f.name}</td>
                  <td className="px-4 py-2 text-xs text-gray-600">{f.class}</td>
                  <td className="px-4 py-2">
                    {f.enabled ? (
                      <span className="inline-block px-2 py-0.5 text-xs bg-green-100 text-green-800 rounded">
                        enabled
                      </span>
                    ) : (
                      <span className="inline-block px-2 py-0.5 text-xs bg-gray-100 text-gray-500 rounded">
                        disabled
                      </span>
                    )}
                  </td>
                </tr>
              ))}
              {library.length === 0 && (
                <tr>
                  <td colSpan={3} className="px-4 py-4 text-center text-gray-400">
                    No factors registered
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* Evaluation tab */}
      {tab === "evaluation" && !loading && (
        <div className="space-y-4">
          <div className="border rounded-lg p-4 space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Select Factor
              </label>
              <select
                value={selectedFactor}
                onChange={(e) => setSelectedFactor(e.target.value)}
                className="w-full max-w-xs border rounded px-3 py-2 text-sm"
              >
                {factors.map((f) => (
                  <option key={f.name} value={f.name}>
                    {f.name} ({f.class})
                  </option>
                ))}
              </select>
            </div>
            <button
              onClick={handleEvaluate}
              disabled={evalSubmitting || !selectedFactor}
              className="px-4 py-2 bg-blue-600 text-white text-sm rounded hover:bg-blue-700 disabled:opacity-50"
            >
              {evalSubmitting ? "Evaluating..." : "Evaluate"}
            </button>
            {evalTaskId && (
              <p className="text-xs text-gray-500">
                Task submitted. ID: <span className="font-mono">{evalTaskId}</span>
              </p>
            )}
          </div>
        </div>
      )}

      {/* Mining tab */}
      {tab === "mining" && (
        <div className="space-y-4">
          <div className="border rounded-lg p-4 space-y-4">
            <div className="grid grid-cols-3 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Min IC
                </label>
                <input
                  type="number"
                  step="0.01"
                  value={minIc}
                  onChange={(e) => setMinIc(e.target.value)}
                  className="w-full border rounded px-3 py-2 text-sm"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Min ICIR
                </label>
                <input
                  type="number"
                  step="0.1"
                  value={minIcir}
                  onChange={(e) => setMinIcir(e.target.value)}
                  className="w-full border rounded px-3 py-2 text-sm"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Top N
                </label>
                <input
                  type="number"
                  value={topN}
                  onChange={(e) => setTopN(e.target.value)}
                  className="w-full border rounded px-3 py-2 text-sm"
                />
              </div>
            </div>
            <button
              onClick={handleMine}
              disabled={mineSubmitting}
              className="px-4 py-2 bg-blue-600 text-white text-sm rounded hover:bg-blue-700 disabled:opacity-50"
            >
              {mineSubmitting ? "Mining..." : "Start Mining"}
            </button>
            {mineTaskId && (
              <p className="text-xs text-gray-500">
                Task submitted. ID: <span className="font-mono">{mineTaskId}</span>
              </p>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
