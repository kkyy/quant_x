import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { get, post } from "../api/client";

interface ModelInfo { filename: string; size_mb: number; modified: string; meta: Record<string, unknown>; }
interface SignalFile { filename: string; size_kb: number; modified: string; }
interface SignalContent { content: string; }
interface RegimeInfo { enabled: boolean; regime: number | null; label: string | null; error?: string; }
type Tab = "generate" | "history" | "rebalance" | "notification";

export function SignalsPage() {
  const { t } = useTranslation();
  const [tab, setTab] = useState<Tab>("generate");

  const tabs: { key: Tab; label: string }[] = [
    { key: "generate", label: t('signals.generateTab') },
    { key: "history", label: t('signals.historyTab') },
    { key: "rebalance", label: t('signals.rebalanceTab') },
    { key: "notification", label: t('signals.notificationTab') },
  ];

  return (
    <div className="space-y-4 max-w-5xl">
      <h2 className="text-2xl font-bold text-zinc-900">{t('signals.title')}</h2>
      <div className="flex gap-1 border-b border-zinc-200">
        {tabs.map((tb) => (
          <button key={tb.key} onClick={() => setTab(tb.key)}
            className={`px-4 py-2 text-sm font-medium -mb-px border-b-2 transition-colors ${
              tab === tb.key ? "border-amber-500 text-amber-600" : "border-transparent text-zinc-500 hover:text-zinc-700"
            }`}>
            {tb.label}
          </button>
        ))}
      </div>
      {tab === "generate" && <GenerateTab />}
      {tab === "history" && <HistoryTab />}
      {tab === "rebalance" && <RebalanceTab />}
      {tab === "notification" && <NotificationTab />}
    </div>
  );
}

function GenerateTab() {
  const { t } = useTranslation();
  const [models, setModels] = useState<ModelInfo[]>([]);
  const [modelPath, setModelPath] = useState("");
  const [account, setAccount] = useState("1000000");
  const [positions, setPositions] = useState("");
  const [dryRun, setDryRun] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [taskId, setTaskId] = useState<string | null>(null);
  const [taskStatus, setTaskStatus] = useState<string | null>(null);
  const [regime, setRegime] = useState<RegimeInfo | null>(null);

  useEffect(() => {
    get<ModelInfo[]>("/models").then((data) => { setModels(data); if (data.length > 0 && !modelPath) setModelPath(data[0].filename); });
    get<RegimeInfo>("/signals/regime").then(setRegime).catch(() => {});
  }, []);

  const handleSubmit = async () => {
    setSubmitting(true); setTaskStatus(null);
    try {
      const res = await post<{ task_id: string }>("/signals/generate", {
        model_path: modelPath, account: parseFloat(account) || 1000000, positions: positions || null, dry_run: dryRun,
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
      {regime && (
        <div className="p-3 bg-zinc-50 border border-zinc-200 rounded-lg text-sm">
          <span className="text-zinc-500">{t('signals.regime')}: </span>
          {regime.enabled ? (
            <span className="inline-block px-2 py-0.5 bg-amber-100 text-amber-700 rounded text-xs font-medium">{regime.label ?? t('common.enabled')}</span>
          ) : (
            <span className="text-zinc-400">{t('common.disabled')}</span>
          )}
        </div>
      )}
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('signals.model')}</label>
        <select value={modelPath} onChange={(e) => setModelPath(e.target.value)} className={inputCls}>
          {models.length === 0 && <option value="">{t('signals.noModels')}</option>}
          {models.map((m) => <option key={m.filename} value={m.filename}>{m.filename} ({m.size_mb} MB)</option>)}
        </select>
      </div>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('signals.account')}</label>
        <input type="number" value={account} onChange={(e) => setAccount(e.target.value)} className={inputCls} />
      </div>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('signals.positions')}</label>
        <textarea value={positions} onChange={(e) => setPositions(e.target.value)} rows={3}
          className={`${inputCls} font-mono`} placeholder={t('signals.positionsPlaceholder')} />
      </div>
      <label className="flex items-center gap-2 text-sm text-zinc-700 cursor-pointer">
        <input type="checkbox" checked={dryRun} onChange={(e) => setDryRun(e.target.checked)} className="rounded" />
        {t('signals.dryRun')}
      </label>
      <button onClick={handleSubmit} disabled={submitting || !modelPath}
        className="px-4 py-2 bg-amber-500 text-zinc-900 font-medium rounded-md text-sm hover:bg-amber-600 disabled:opacity-50">
        {submitting ? t('common.starting') : t('signals.generateBtn')}
      </button>
      {taskId && (
        <div className="mt-4 p-3 bg-zinc-50 border border-zinc-200 rounded-lg text-sm">
          <p className="text-zinc-600">{t('common.taskId')}: <span className="font-mono text-amber-600">{taskId}</span></p>
          {taskStatus && <p className="mt-1 text-zinc-600">{t('common.status')}: <span className={taskStatus === "done" ? "text-green-600" : taskStatus === "failed" ? "text-red-600" : "text-amber-600"}>{taskStatus}</span></p>}
        </div>
      )}
    </div>
  );
}

function HistoryTab() {
  const { t } = useTranslation();
  const [files, setFiles] = useState<SignalFile[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [content, setContent] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => { get<SignalFile[]>("/signals/history").then(setFiles); }, []);

  const loadFile = async (filename: string) => {
    setSelected(filename); setLoading(true);
    try { const res = await get<SignalContent>(`/signals/history/${filename}`); setContent(res.content); }
    catch { setContent(null); }
    finally { setLoading(false); }
  };

  return (
    <div>
      <h3 className="text-lg font-semibold text-zinc-800 mb-3">{t('signals.historyTab')}</h3>
      {files.length === 0 ? (
        <p className="text-zinc-500 text-sm">{t('signals.noHistory')}</p>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <div className="lg:col-span-1">
            <table className="w-full text-sm">
              <thead><tr className="text-zinc-400 border-b border-zinc-200">
                <th className="text-left py-2 text-xs uppercase tracking-wide">{t('common.filename')}</th>
                <th className="text-right py-2 text-xs uppercase tracking-wide">{t('common.sizeKb')}</th>
              </tr></thead>
              <tbody>
                {files.map((f) => (
                  <tr key={f.filename} onClick={() => loadFile(f.filename)}
                    className={`cursor-pointer border-b border-zinc-100 hover:bg-zinc-50 ${selected === f.filename ? "bg-zinc-50" : ""}`}>
                    <td className="py-2 text-amber-600 font-mono text-xs">{f.filename}</td>
                    <td className="text-right py-2 text-zinc-400">{f.size_kb} KB</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="lg:col-span-2">
            {loading && <p className="text-zinc-400 text-sm">{t('common.loading')}</p>}
            {!loading && !selected && <p className="text-zinc-500 text-sm">{t('common.selectFile')}</p>}
            {!loading && content && (
              <pre className="bg-zinc-900 border border-zinc-700 rounded-lg p-4 text-xs text-zinc-300 overflow-auto max-h-[600px] whitespace-pre-wrap">{content}</pre>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function RebalanceTab() {
  const { t } = useTranslation();
  const [mock, setMock] = useState(false);
  const [dryRun, setDryRun] = useState(true);
  const [message, setMessage] = useState<string | null>(null);

  const handleRun = () => {
    setMessage(`Scheduled rebalance with mock=${mock}, dry-run=${dryRun}.\n${t('signals.rebalanceNote')}`);
  };

  return (
    <div className="max-w-2xl space-y-4">
      <p className="text-zinc-500 text-sm">{t('signals.rebalanceNote')}</p>
      <div className="flex items-center gap-6">
        <label className="flex items-center gap-2 text-sm text-zinc-700 cursor-pointer">
          <input type="checkbox" checked={mock} onChange={(e) => setMock(e.target.checked)} className="rounded" />
          {t('signals.mockMode')}
        </label>
        <label className="flex items-center gap-2 text-sm text-zinc-700 cursor-pointer">
          <input type="checkbox" checked={dryRun} onChange={(e) => setDryRun(e.target.checked)} className="rounded" />
          {t('signals.dryRun')}
        </label>
      </div>
      <button onClick={handleRun} className="px-4 py-2 bg-amber-500 text-zinc-900 font-medium rounded-md text-sm hover:bg-amber-600">
        {t('signals.runRebalance')}
      </button>
      {message && <pre className="bg-zinc-50 border border-zinc-200 rounded-lg p-4 text-sm text-zinc-700 whitespace-pre-wrap">{message}</pre>}
    </div>
  );
}

function NotificationTab() {
  const { t } = useTranslation();
  const [title, setTitle] = useState("");
  const [content, setContent] = useState("");
  const [sending, setSending] = useState(false);
  const [result, setResult] = useState<string | null>(null);

  const handleSend = async () => {
    setSending(true); setResult(null);
    try { setResult(`Test notification sent.\nTitle: ${title}\nContent: ${content}`); }
    catch (err) { setResult(`Error: ${err}`); }
    finally { setSending(false); }
  };

  const inputCls = "w-full border border-zinc-300 rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-400";

  return (
    <div className="max-w-2xl space-y-4">
      <p className="text-zinc-500 text-sm">{t('signals.notifNote')}</p>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('signals.notifTitle')}</label>
        <input type="text" value={title} onChange={(e) => setTitle(e.target.value)} className={inputCls} placeholder={t('signals.notifTitlePlaceholder')} />
      </div>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('signals.notifContent')}</label>
        <textarea value={content} onChange={(e) => setContent(e.target.value)} rows={5} className={inputCls} placeholder={t('signals.notifPlaceholder')} />
      </div>
      <button onClick={handleSend} disabled={sending || !title}
        className="px-4 py-2 bg-amber-500 text-zinc-900 font-medium rounded-md text-sm hover:bg-amber-600 disabled:opacity-50">
        {sending ? t('common.sending') : t('signals.sendTest')}
      </button>
      {result && <pre className="bg-zinc-50 border border-zinc-200 rounded-lg p-4 text-sm text-zinc-700 whitespace-pre-wrap">{result}</pre>}
    </div>
  );
}
