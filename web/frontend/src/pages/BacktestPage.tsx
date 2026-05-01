import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { get, post } from "../api/client";

interface ModelInfo { filename: string; size_mb: number; modified: string; meta: Record<string, unknown>; }
interface ResultFile { filename: string; size_kb: number; modified: string; }
interface ResultData { columns: string[]; rows: Record<string, unknown>[]; }
type Tab = "grid" | "results" | "wfv";

export function BacktestPage() {
  const { t } = useTranslation();
  const [tab, setTab] = useState<Tab>("grid");

  return (
    <div className="space-y-4 max-w-5xl">
      <h2 className="text-2xl font-bold text-zinc-900">{t('backtest.title')}</h2>
      <div className="flex gap-1 border-b border-zinc-200">
        {(["grid", "results", "wfv"] as Tab[]).map((tb) => (
          <button key={tb} onClick={() => setTab(tb)}
            className={`px-4 py-2 text-sm font-medium -mb-px border-b-2 transition-colors ${
              tab === tb ? "border-amber-500 text-amber-600" : "border-transparent text-zinc-500 hover:text-zinc-700"
            }`}>
            {tb === "grid" ? t('backtest.gridTab') : tb === "results" ? t('backtest.resultsTab') : t('backtest.wfvTab')}
          </button>
        ))}
      </div>
      {tab === "grid" && <GridSearchTab />}
      {tab === "results" && <ResultsTab />}
      {tab === "wfv" && <WalkForwardTab />}
    </div>
  );
}

function GridSearchTab() {
  const { t } = useTranslation();
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
    get<ModelInfo[]>("/models").then((data) => { setModels(data); if (data.length > 0 && !modelPath) setModelPath(data[0].filename); });
  }, []);

  const handleSubmit = async () => {
    setSubmitting(true); setTaskStatus(null);
    try {
      const res = await post<{ task_id: string }>("/backtest/grid", {
        model_path: modelPath,
        topk: topk.split(",").map((s) => parseInt(s.trim())).filter((n) => !isNaN(n)),
        n_drop: nDrop.split(",").map((s) => parseInt(s.trim())).filter((n) => !isNaN(n)),
        hold_thresh: holdThresh.split(",").map((s) => parseInt(s.trim())).filter((n) => !isNaN(n)),
        start: startDate || null, end: endDate || null, market, multi_seed: multiSeed,
      });
      setTaskId(res.task_id);
      const interval = setInterval(async () => {
        try {
          const tasks = await get<{ task_id: string; status: string; error?: string }[]>("/system/tasks");
          const tk = tasks.find((x) => x.task_id === res.task_id);
          if (tk) { setTaskStatus(tk.status); if (["done","failed","cancelled"].includes(tk.status)) clearInterval(interval); }
        } catch { clearInterval(interval); }
      }, 2000);
    } catch (err) { setTaskStatus(`Error: ${err}`); }
    finally { setSubmitting(false); }
  };

  const inputCls = "w-full border border-zinc-300 rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-400";

  return (
    <div className="max-w-2xl space-y-4">
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.model')}</label>
        <select value={modelPath} onChange={(e) => setModelPath(e.target.value)} className={inputCls}>
          {models.length === 0 && <option value="">{t('backtest.noModels')}</option>}
          {models.map((m) => <option key={m.filename} value={m.filename}>{m.filename} ({m.size_mb} MB)</option>)}
        </select>
      </div>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.market')}</label>
        <select value={market} onChange={(e) => setMarket(e.target.value)} className={inputCls}>
          <option value="csi300">CSI 300</option>
          <option value="csi500">CSI 500</option>
          <option value="csi800">CSI 800</option>
          <option value="csi1000">CSI 1000</option>
        </select>
      </div>
      <div className="grid grid-cols-3 gap-4">
        <div>
          <label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.topk')}</label>
          <input type="text" value={topk} onChange={(e) => setTopk(e.target.value)} className={inputCls} placeholder="5,10,15,20" />
        </div>
        <div>
          <label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.nDrop')}</label>
          <input type="text" value={nDrop} onChange={(e) => setNDrop(e.target.value)} className={inputCls} placeholder="1,3,5" />
        </div>
        <div>
          <label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.holdThresh')}</label>
          <input type="text" value={holdThresh} onChange={(e) => setHoldThresh(e.target.value)} className={inputCls} placeholder="3,5,10" />
        </div>
      </div>
      <div className="grid grid-cols-2 gap-4">
        <div>
          <label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.startDate')}</label>
          <input type="date" value={startDate} onChange={(e) => setStartDate(e.target.value)} className={inputCls} />
        </div>
        <div>
          <label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.endDate')}</label>
          <input type="date" value={endDate} onChange={(e) => setEndDate(e.target.value)} className={inputCls} />
        </div>
      </div>
      <label className="flex items-center gap-2 text-sm text-zinc-700 cursor-pointer">
        <input type="checkbox" checked={multiSeed} onChange={(e) => setMultiSeed(e.target.checked)} className="rounded" />
        {t('backtest.multiSeed')}
      </label>
      <button onClick={handleSubmit} disabled={submitting || !modelPath}
        className="px-4 py-2 bg-amber-500 text-zinc-900 font-medium rounded-md text-sm hover:bg-amber-600 disabled:opacity-50">
        {submitting ? t('common.starting') : t('backtest.runGrid')}
      </button>
      {taskId && (
        <div className="mt-4 p-3 bg-zinc-50 border border-zinc-200 rounded-lg text-sm">
          <p className="text-zinc-600">{t('common.taskId')}: <span className="font-mono text-amber-600">{taskId}</span></p>
          {taskStatus && (
            <p className="mt-1 text-zinc-600">{t('common.status')}: <span className={
              taskStatus === "done" ? "text-green-600" : taskStatus === "failed" ? "text-red-600" : taskStatus === "running" ? "text-amber-600" : "text-zinc-400"
            }>{taskStatus}</span></p>
          )}
        </div>
      )}
    </div>
  );
}

function ResultsTab() {
  const { t } = useTranslation();
  const [results, setResults] = useState<ResultFile[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [data, setData] = useState<ResultData | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => { get<ResultFile[]>("/backtest/results").then(setResults); }, []);

  const loadResult = async (filename: string) => {
    setSelected(filename); setLoading(true);
    try { const res = await get<ResultData>(`/backtest/results/${filename}`); setData(res); }
    catch { setData(null); }
    finally { setLoading(false); }
  };

  return (
    <div>
      <h3 className="text-lg font-semibold text-zinc-800 mb-3">{t('backtest.resultsTab')}</h3>
      {results.length === 0 ? (
        <p className="text-zinc-500 text-sm">{t('backtest.noResults')}</p>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <div className="lg:col-span-1">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-zinc-400 border-b border-zinc-200">
                  <th className="text-left py-2 text-xs uppercase tracking-wide">{t('common.filename')}</th>
                  <th className="text-right py-2 text-xs uppercase tracking-wide">{t('common.sizeKb')}</th>
                </tr>
              </thead>
              <tbody>
                {results.map((r) => (
                  <tr key={r.filename} onClick={() => loadResult(r.filename)}
                    className={`cursor-pointer border-b border-zinc-100 hover:bg-zinc-50 ${selected === r.filename ? "bg-zinc-50" : ""}`}>
                    <td className="py-2 text-amber-600 font-mono text-xs">{r.filename}</td>
                    <td className="text-right py-2 text-zinc-400">{r.size_kb} KB</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="lg:col-span-2">
            {loading && <p className="text-zinc-400 text-sm">{t('common.loading')}</p>}
            {!loading && !selected && <p className="text-zinc-500 text-sm">{t('backtest.selectResult')}</p>}
            {!loading && data && data.columns && (
              <div className="overflow-auto max-h-[600px] border border-zinc-200 rounded-lg">
                <table className="w-full text-xs">
                  <thead className="sticky top-0 bg-zinc-50 border-b border-zinc-200">
                    <tr>
                      {data.columns.map((col) => (
                        <th key={col} className="text-left py-2 px-2 whitespace-nowrap text-zinc-500 font-medium">{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {data.rows.map((row, i) => (
                      <tr key={i} className="border-b border-zinc-100 hover:bg-zinc-50">
                        {data.columns.map((col) => (
                          <td key={col} className="py-1 px-2 whitespace-nowrap text-zinc-700">
                            {typeof row[col] === "number" ? (row[col] as number).toFixed(4) : String(row[col] ?? "")}
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

function WalkForwardTab() {
  const { t } = useTranslation();
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
    setSubmitting(true); setTaskStatus(null);
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
          const tk = tasks.find((x) => x.task_id === res.task_id);
          if (tk) { setTaskStatus(tk.status); if (["done","failed","cancelled"].includes(tk.status)) clearInterval(interval); }
        } catch { clearInterval(interval); }
      }, 2000);
    } catch (err) { setTaskStatus(`Error: ${err}`); }
    finally { setSubmitting(false); }
  };

  const inputCls = "w-full border border-zinc-300 rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-400";

  return (
    <div className="max-w-2xl space-y-4">
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.trainUniverses')}</label>
        <input type="text" value={trainUniverses} onChange={(e) => setTrainUniverses(e.target.value)} className={inputCls} placeholder="csi300,csi800" />
      </div>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.evalMarket')}</label>
        <select value={evalMarket} onChange={(e) => setEvalMarket(e.target.value)} className={inputCls}>
          <option value="csi300">CSI 300</option>
          <option value="csi500">CSI 500</option>
          <option value="csi800">CSI 800</option>
          <option value="csi1000">CSI 1000</option>
        </select>
      </div>
      <div className="grid grid-cols-3 gap-4">
        <div><label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.topk')}</label><input type="text" value={topk} onChange={(e) => setTopk(e.target.value)} className={inputCls} /></div>
        <div><label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.nDrop')}</label><input type="text" value={nDrop} onChange={(e) => setNDrop(e.target.value)} className={inputCls} /></div>
        <div><label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.holdThresh')}</label><input type="text" value={holdThresh} onChange={(e) => setHoldThresh(e.target.value)} className={inputCls} /></div>
      </div>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('backtest.workers')}</label>
        <input type="number" value={workers} onChange={(e) => setWorkers(parseInt(e.target.value) || 1)} min={1} max={8} className={inputCls} />
      </div>
      <button onClick={handleSubmit} disabled={submitting}
        className="px-4 py-2 bg-amber-500 text-zinc-900 font-medium rounded-md text-sm hover:bg-amber-600 disabled:opacity-50">
        {submitting ? t('common.starting') : t('backtest.runWfv')}
      </button>
      {taskId && (
        <div className="mt-4 p-3 bg-zinc-50 border border-zinc-200 rounded-lg text-sm">
          <p className="text-zinc-600">{t('common.taskId')}: <span className="font-mono text-amber-600">{taskId}</span></p>
          {taskStatus && (
            <p className="mt-1 text-zinc-600">{t('common.status')}: <span className={taskStatus === "done" ? "text-green-600" : taskStatus === "failed" ? "text-red-600" : taskStatus === "running" ? "text-amber-600" : "text-zinc-400"}>{taskStatus}</span></p>
          )}
        </div>
      )}
    </div>
  );
}
