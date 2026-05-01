import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
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
  const { t } = useTranslation();
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
    { key: "library", label: t("factors.libraryTab") },
    { key: "evaluation", label: t("factors.evaluationTab") },
    { key: "mining", label: t("factors.miningTab") },
  ];

  return (
    <div className="space-y-4 max-w-5xl">
      <h2 className="text-2xl font-bold">{t("factors.title")}</h2>

      {/* Tab bar */}
      <div className="flex gap-1 border-b border-zinc-200">
        {tabs.map((t) => (
          <button
            key={t.key}
            onClick={() => { setTab(t.key); setError(null); }}
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

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-2 rounded text-sm">
          {error}
        </div>
      )}

      {loading && <p className="text-zinc-500 text-sm">{t("common.loading")}</p>}

      {/* Library tab */}
      {tab === "library" && !loading && (
        <div className="border rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-zinc-50">
              <tr>
                <th className="text-left px-4 py-2">{t("common.name")}</th>
                <th className="text-left px-4 py-2">{t("common.class")}</th>
                <th className="text-left px-4 py-2">{t("common.status")}</th>
              </tr>
            </thead>
            <tbody>
              {library.map((f) => (
                <tr key={f.name} className="border-t border-zinc-200 hover:bg-zinc-50">
                  <td className="px-4 py-2 font-mono text-xs">{f.name}</td>
                  <td className="px-4 py-2 text-xs text-zinc-600">{f.class}</td>
                  <td className="px-4 py-2">
                    {f.enabled ? (
                      <span className="inline-block px-2 py-0.5 text-xs bg-green-100 text-green-800 rounded">
                        {t("common.enabled")}
                      </span>
                    ) : (
                      <span className="inline-block px-2 py-0.5 text-xs bg-zinc-100 text-zinc-500 rounded">
                        {t("common.disabled")}
                      </span>
                    )}
                  </td>
                </tr>
              ))}
              {library.length === 0 && (
                <tr>
                  <td colSpan={3} className="px-4 py-4 text-center text-zinc-400">
                    {t("factors.noFactors")}
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
              <label className="block text-sm font-medium text-zinc-700 mb-1">
                {t("factors.selectFactor")}
              </label>
              <select
                value={selectedFactor}
                onChange={(e) => setSelectedFactor(e.target.value)}
                className="w-full max-w-xs border border-zinc-300 rounded-md bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-amber-400"
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
              className="px-4 py-2 bg-amber-500 text-zinc-900 font-medium text-sm rounded hover:bg-amber-600 disabled:opacity-50"
            >
              {evalSubmitting ? t("common.evaluating") : t("common.evaluate")}
            </button>
            {evalTaskId && (
              <p className="text-xs text-zinc-500">
                {t("factors.taskSubmitted", { id: evalTaskId })}
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
                <label className="block text-sm font-medium text-zinc-700 mb-1">
                  {t("factors.minIc")}
                </label>
                <input
                  type="number"
                  step="0.01"
                  value={minIc}
                  onChange={(e) => setMinIc(e.target.value)}
                  className="w-full border border-zinc-300 rounded-md bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-amber-400"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-zinc-700 mb-1">
                  {t("factors.minIcir")}
                </label>
                <input
                  type="number"
                  step="0.1"
                  value={minIcir}
                  onChange={(e) => setMinIcir(e.target.value)}
                  className="w-full border border-zinc-300 rounded-md bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-amber-400"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-zinc-700 mb-1">
                  {t("factors.topN")}
                </label>
                <input
                  type="number"
                  value={topN}
                  onChange={(e) => setTopN(e.target.value)}
                  className="w-full border border-zinc-300 rounded-md bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-amber-400"
                />
              </div>
            </div>
            <button
              onClick={handleMine}
              disabled={mineSubmitting}
              className="px-4 py-2 bg-amber-500 text-zinc-900 font-medium text-sm rounded hover:bg-amber-600 disabled:opacity-50"
            >
              {mineSubmitting ? t("factors.mining") : t("factors.startMining")}
            </button>
            {mineTaskId && (
              <p className="text-xs text-zinc-500">
                {t("factors.taskSubmitted", { id: mineTaskId })}
              </p>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
