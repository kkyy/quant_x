import { useState, useEffect } from "react";
import { get, post } from "../api/client";

interface ModelInfo {
  filename: string;
  size_mb: number;
  modified: string;
  meta: Record<string, unknown>;
}

interface ResultFile {
  filename: string;
  size_kb: number;
  modified: string;
}

interface ResultData {
  columns: string[];
  rows: Record<string, unknown>[];
}

type Tab = "grid" | "results" | "wfv";

export function BacktestPage() {
  const [tab, setTab] = useState<Tab>("grid");

  return (
    <div>
      <h2 className="text-2xl font-bold mb-4">Backtest</h2>

      <div className="flex gap-1 mb-6 border-b border-gray-700">
        {(["grid", "results", "wfv"] as Tab[]).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
              tab === t
                ? "border-blue-500 text-blue-400"
                : "border-transparent text-gray-400 hover:text-gray-200"
            }`}
          >
            {t === "grid" ? "Grid Search" : t === "results" ? "Results" : "Walk-Forward"}
          </button>
        ))}
      </div>

      {tab === "grid" && <GridSearchTab />}
      {tab === "results" && <ResultsTab />}
      {tab === "wfv" && <WalkForwardTab />}
    </div>
  );
}

/* ---------- Grid Search Tab ---------- */

function GridSearchTab() {
  const [models, setModels] = useState<ModelInfo[]>([]);
  const [modelPath, setModelPath] = useState("");
  const [market, setMarket] = useState("csi300");
  const [topk, setTopk] = useState("5,10,15,20");
  const [nDrop, setNDrop] = useState("1,3,5");
  const [holdThresh, setHoldThresh] = useState("3,5,10");
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [multiSeed, setMultiSeed] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [taskId, setTaskId] = useState<string | null>(null);
  const [taskStatus, setTaskStatus] = useState<string | null>(null);

  useEffect(() => {
    get<ModelInfo[]>("/models").then((data) => {
      setModels(data);
      if (data.length > 0 && !modelPath) {
        setModelPath(data[0].filename);
      }
    });
  }, []);

  const handleSubmit = async () => {
    setSubmitting(true);
    setTaskStatus(null);
    try {
      const res = await post<{ task_id: string }>("/backtest/grid", {
        model_path: modelPath,
        topk: topk.split(",").map((s) => parseInt(s.trim())).filter((n) => !isNaN(n)),
        n_drop: nDrop.split(",").map((s) => parseInt(s.trim())).filter((n) => !isNaN(n)),
        hold_thresh: holdThresh.split(",").map((s) => parseInt(s.trim())).filter((n) => !isNaN(n)),
        start: startDate || null,
        end: endDate || null,
        market,
        multi_seed: multiSeed,
      });
      setTaskId(res.task_id);
      pollStatus(res.task_id);
    } catch (err) {
      setTaskStatus(`Error: ${err}`);
    } finally {
      setSubmitting(false);
    }
  };

  const pollStatus = (tid: string) => {
    const interval = setInterval(async () => {
      try {
        const tasks = await get<{ task_id: string; status: string; error?: string }[]>("/system/tasks");
        const t = tasks.find((x) => x.task_id === tid);
        if (t) {
          setTaskStatus(t.status);
          if (t.status === "done" || t.status === "failed" || t.status === "cancelled") {
            clearInterval(interval);
          }
        }
      } catch {
        clearInterval(interval);
      }
    }, 2000);
  };

  return (
    <div className="max-w-2xl space-y-4">
      <div>
        <label className="block text-sm font-medium text-gray-300 mb-1">Model</label>
        <select
          value={modelPath}
          onChange={(e) => setModelPath(e.target.value)}
          className="w-full bg-gray-800 border border-gray-600 rounded px-3 py-2 text-sm"
        >
          {models.length === 0 && <option value="">No models found</option>}
          {models.map((m) => (
            <option key={m.filename} value={m.filename}>
              {m.filename} ({m.size_mb} MB)
            </option>
          ))}
        </select>
      </div>

      <div>
        <label className="block text-sm font-medium text-gray-300 mb-1">Market</label>
        <select
          value={market}
          onChange={(e) => setMarket(e.target.value)}
          className="w-full bg-gray-800 border border-gray-600 rounded px-3 py-2 text-sm"
        >
          <option value="csi300">CSI 300</option>
          <option value="csi500">CSI 500</option>
          <option value="csi800">CSI 800</option>
          <option value="csi1000">CSI 1000</option>
        </select>
      </div>

      <div className="grid grid-cols-3 gap-4">
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-1">Top-K</label>
          <input
            type="text"
            value={topk}
            onChange={(e) => setTopk(e.target.value)}
            className="w-full bg-gray-800 border border-gray-600 rounded px-3 py-2 text-sm"
            placeholder="5,10,15,20"
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-1">N-Drop</label>
          <input
            type="text"
            value={nDrop}
            onChange={(e) => setNDrop(e.target.value)}
            className="w-full bg-gray-800 border border-gray-600 rounded px-3 py-2 text-sm"
            placeholder="1,3,5"
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-1">Hold Thresh</label>
          <input
            type="text"
            value={holdThresh}
            onChange={(e) => setHoldThresh(e.target.value)}
            className="w-full bg-gray-800 border border-gray-600 rounded px-3 py-2 text-sm"
            placeholder="3,5,10"
          />
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-1">Start Date</label>
          <input
            type="date"
            value={startDate}
            onChange={(e) => setStartDate(e.target.value)}
            className="w-full bg-gray-800 border border-gray-600 rounded px-3 py-2 text-sm"
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-1">End Date</label>
          <input
            type="date"
            value={endDate}
            onChange={(e) => setEndDate(e.target.value)}
            className="w-full bg-gray-800 border border-gray-600 rounded px-3 py-2 text-sm"
          />
        </div>
      </div>

      <div className="flex items-center gap-3">
        <label className="flex items-center gap-2 text-sm text-gray-300 cursor-pointer">
          <input
            type="checkbox"
            checked={multiSeed}
            onChange={(e) => setMultiSeed(e.target.checked)}
            className="rounded border-gray-600"
          />
          Multi-seed robustness
        </label>
      </div>

      <button
        onClick={handleSubmit}
        disabled={submitting || !modelPath}
        className="bg-blue-600 hover:bg-blue-700 disabled:bg-gray-600 disabled:cursor-not-allowed text-white px-6 py-2 rounded text-sm font-medium"
      >
        {submitting ? "Starting..." : "Run Grid Search"}
      </button>

      {taskId && (
        <div className="mt-4 p-3 bg-gray-800 rounded text-sm">
          <p>
            Task ID: <span className="font-mono text-blue-400">{taskId}</span>
          </p>
          {taskStatus && (
            <p className="mt-1">
              Status:{" "}
              <span
                className={
                  taskStatus === "done"
                    ? "text-green-400"
                    : taskStatus === "failed"
                    ? "text-red-400"
                    : taskStatus === "running"
                    ? "text-yellow-400"
                    : "text-gray-400"
                }
              >
                {taskStatus}
              </span>
            </p>
          )}
        </div>
      )}
    </div>
  );
}

/* ---------- Results Tab ---------- */

function ResultsTab() {
  const [results, setResults] = useState<ResultFile[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [data, setData] = useState<ResultData | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    get<ResultFile[]>("/backtest/results").then(setResults);
  }, []);

  const loadResult = async (filename: string) => {
    setSelected(filename);
    setLoading(true);
    try {
      const res = await get<ResultData>(`/backtest/results/${filename}`);
      setData(res);
    } catch {
      setData(null);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div>
      <h3 className="text-lg font-semibold mb-3">Backtest Results</h3>

      {results.length === 0 ? (
        <p className="text-gray-500 text-sm">No results found.</p>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          {/* File list */}
          <div className="lg:col-span-1">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-gray-400 border-b border-gray-700">
                  <th className="text-left py-2">File</th>
                  <th className="text-right py-2">Size</th>
                </tr>
              </thead>
              <tbody>
                {results.map((r) => (
                  <tr
                    key={r.filename}
                    onClick={() => loadResult(r.filename)}
                    className={`cursor-pointer border-b border-gray-800 hover:bg-gray-800 ${
                      selected === r.filename ? "bg-gray-800" : ""
                    }`}
                  >
                    <td className="py-2 text-blue-400 font-mono text-xs">{r.filename}</td>
                    <td className="text-right py-2 text-gray-400">{r.size_kb} KB</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Data display */}
          <div className="lg:col-span-2">
            {loading && <p className="text-gray-400 text-sm">Loading...</p>}
            {!loading && !selected && (
              <p className="text-gray-500 text-sm">Select a file to view results.</p>
            )}
            {!loading && data && data.columns && (
              <div className="overflow-auto max-h-[600px]">
                <table className="w-full text-xs">
                  <thead className="sticky top-0 bg-gray-900">
                    <tr className="text-gray-400 border-b border-gray-700">
                      {data.columns.map((col) => (
                        <th key={col} className="text-left py-2 px-2 whitespace-nowrap">
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {data.rows.map((row, i) => (
                      <tr key={i} className="border-b border-gray-800 hover:bg-gray-800">
                        {data.columns.map((col) => (
                          <td key={col} className="py-1 px-2 whitespace-nowrap">
                            {typeof row[col] === "number"
                              ? row[col].toFixed(4)
                              : String(row[col] ?? "")}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

/* ---------- Walk-Forward Tab ---------- */

function WalkForwardTab() {
  const [trainUniverses, setTrainUniverses] = useState("csi300");
  const [evalMarket, setEvalMarket] = useState("csi300");
  const [topk, setTopk] = useState("5,15,20");
  const [nDrop, setNDrop] = useState("1,3");
  const [holdThresh, setHoldThresh] = useState("5,8,10");
  const [workers, setWorkers] = useState(1);
  const [submitting, setSubmitting] = useState(false);
  const [taskId, setTaskId] = useState<string | null>(null);
  const [taskStatus, setTaskStatus] = useState<string | null>(null);

  const handleSubmit = async () => {
    setSubmitting(true);
    setTaskStatus(null);
    try {
      const res = await post<{ task_id: string }>("/backtest/walk-forward", {
        train_universes: trainUniverses.split(",").map((s) => s.trim()),
        eval_market: evalMarket,
        topk: topk.split(",").map((s) => parseInt(s.trim())).filter((n) => !isNaN(n)),
        n_drop: nDrop.split(",").map((s) => parseInt(s.trim())).filter((n) => !isNaN(n)),
        hold_thresh: holdThresh.split(",").map((s) => parseInt(s.trim())).filter((n) => !isNaN(n)),
        workers,
      });
      setTaskId(res.task_id);
      const interval = setInterval(async () => {
        try {
          const tasks = await get<{ task_id: string; status: string; error?: string }[]>("/system/tasks");
          const t = tasks.find((x) => x.task_id === res.task_id);
          if (t) {
            setTaskStatus(t.status);
            if (t.status === "done" || t.status === "failed" || t.status === "cancelled") {
              clearInterval(interval);
            }
          }
        } catch {
          clearInterval(interval);
        }
      }, 2000);
    } catch (err) {
      setTaskStatus(`Error: ${err}`);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="max-w-2xl space-y-4">
      <div>
        <label className="block text-sm font-medium text-gray-300 mb-1">Train Universes (comma-separated)</label>
        <input
          type="text"
          value={trainUniverses}
          onChange={(e) => setTrainUniverses(e.target.value)}
          className="w-full bg-gray-800 border border-gray-600 rounded px-3 py-2 text-sm"
          placeholder="csi300,csi800"
        />
      </div>

      <div>
        <label className="block text-sm font-medium text-gray-300 mb-1">Eval Market</label>
        <select
          value={evalMarket}
          onChange={(e) => setEvalMarket(e.target.value)}
          className="w-full bg-gray-800 border border-gray-600 rounded px-3 py-2 text-sm"
        >
          <option value="csi300">CSI 300</option>
          <option value="csi500">CSI 500</option>
          <option value="csi800">CSI 800</option>
          <option value="csi1000">CSI 1000</option>
        </select>
      </div>

      <div className="grid grid-cols-3 gap-4">
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-1">Top-K</label>
          <input
            type="text"
            value={topk}
            onChange={(e) => setTopk(e.target.value)}
            className="w-full bg-gray-800 border border-gray-600 rounded px-3 py-2 text-sm"
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-1">N-Drop</label>
          <input
            type="text"
            value={nDrop}
            onChange={(e) => setNDrop(e.target.value)}
            className="w-full bg-gray-800 border border-gray-600 rounded px-3 py-2 text-sm"
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-300 mb-1">Hold Thresh</label>
          <input
            type="text"
            value={holdThresh}
            onChange={(e) => setHoldThresh(e.target.value)}
            className="w-full bg-gray-800 border border-gray-600 rounded px-3 py-2 text-sm"
          />
        </div>
      </div>

      <div>
        <label className="block text-sm font-medium text-gray-300 mb-1">Workers</label>
        <input
          type="number"
          value={workers}
          onChange={(e) => setWorkers(parseInt(e.target.value) || 1)}
          min={1}
          max={8}
          className="w-full bg-gray-800 border border-gray-600 rounded px-3 py-2 text-sm"
        />
      </div>

      <button
        onClick={handleSubmit}
        disabled={submitting}
        className="bg-blue-600 hover:bg-blue-700 disabled:bg-gray-600 disabled:cursor-not-allowed text-white px-6 py-2 rounded text-sm font-medium"
      >
        {submitting ? "Starting..." : "Run Walk-Forward Validation"}
      </button>

      {taskId && (
        <div className="mt-4 p-3 bg-gray-800 rounded text-sm">
          <p>
            Task ID: <span className="font-mono text-blue-400">{taskId}</span>
          </p>
          {taskStatus && (
            <p className="mt-1">
              Status:{" "}
              <span
                className={
                  taskStatus === "done"
                    ? "text-green-400"
                    : taskStatus === "failed"
                    ? "text-red-400"
                    : taskStatus === "running"
                    ? "text-yellow-400"
                    : "text-gray-400"
                }
              >
                {taskStatus}
              </span>
            </p>
          )}
        </div>
      )}
    </div>
  );
}
