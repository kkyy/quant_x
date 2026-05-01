import React, { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { get, post } from "../api/client";

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

export function ModelsPage() {
  const { t } = useTranslation();
  const [tab, setTab] = useState<Tab>("browser");

  const tabs: { key: Tab; label: string }[] = [
    { key: "browser", label: t('models.browserTab') },
    { key: "train",   label: t('models.trainTab') },
    { key: "registry",label: t('models.registryTab') },
  ];

  return (
    <div className="space-y-4 max-w-5xl">
      <h2 className="text-2xl font-bold text-zinc-900">{t('models.title')}</h2>
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
      {tab === "browser" && <ModelBrowserTab />}
      {tab === "train" && <TrainTab />}
      {tab === "registry" && <RegistryTab />}
    </div>
  );
}

function ModelBrowserTab() {
  const { t } = useTranslation();
  const [models, setModels] = useState<ModelInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [meta, setMeta] = useState<Record<string, unknown> | null>(null);
  const [importance, setImportance] = useState<Record<string, unknown> | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  useEffect(() => {
    get<ModelInfo[]>("/models").then(setModels).catch((err) => setError(err.message)).finally(() => setLoading(false));
  }, []);

  const handleExpand = async (filename: string) => {
    if (expanded === filename) { setExpanded(null); setMeta(null); setImportance(null); return; }
    setExpanded(filename);
    setDetailLoading(true);
    try {
      const [m, imp] = await Promise.all([
        get<Record<string, unknown>>(`/models/${filename}/meta`),
        get<Record<string, unknown>>(`/models/${filename}/importance`),
      ]);
      setMeta(m); setImportance(imp);
    } catch (err: any) { setError(err.message); }
    finally { setDetailLoading(false); }
  };

  if (loading) return <p className="text-zinc-500 text-sm py-4">{t('common.loading')}</p>;
  if (error) return <p className="text-red-600 text-sm py-4">{t('common.error')}: {error}</p>;
  if (models.length === 0) return <p className="text-zinc-500 text-sm py-4">{t('models.noModels')}</p>;

  const importanceEntries = importance
    ? Object.entries(importance).sort(([, a], [, b]) => (b as number) - (a as number)).slice(0, 20)
    : [];

  return (
    <div className="space-y-2">
      <p className="text-sm text-zinc-500">{t('models.count', { count: models.length })}</p>
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
              <React.Fragment key={m.filename}>
                <tr className="border-t border-zinc-100 hover:bg-zinc-50 cursor-pointer"
                  onClick={() => handleExpand(m.filename)}>
                  <td className="px-4 py-2">
                    <span className="font-mono text-xs text-zinc-700">{m.filename}</span>
                    <span className="ml-2 text-xs text-zinc-400">{expanded === m.filename ? "▼" : "▶"}</span>
                  </td>
                  <td className="text-right px-4 py-2 text-zinc-600">{m.size_mb}</td>
                  <td className="px-4 py-2 text-xs text-zinc-400">{new Date(m.modified).toLocaleString()}</td>
                </tr>
                {expanded === m.filename && (
                  <tr className="border-t border-zinc-100 bg-zinc-50">
                    <td colSpan={3} className="px-6 py-4">
                      {detailLoading ? (
                        <p className="text-sm text-zinc-500">{t('models.loadingDetails')}</p>
                      ) : (
                        <div className="grid grid-cols-2 gap-6">
                          <div>
                            <h4 className="text-sm font-semibold text-zinc-700 mb-2">{t('models.meta')}</h4>
                            {meta && Object.keys(meta).length > 0 ? (
                              <table className="w-full text-xs">
                                <tbody>
                                  {Object.entries(meta).map(([k, v]) => (
                                    <tr key={k} className="border-t border-zinc-100">
                                      <td className="py-1 pr-3 font-medium text-zinc-500">{k}</td>
                                      <td className="py-1 font-mono text-zinc-700">{typeof v === "object" ? JSON.stringify(v) : String(v)}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            ) : <p className="text-xs text-zinc-400">{t('models.noMeta')}</p>}
                          </div>
                          <div>
                            <h4 className="text-sm font-semibold text-zinc-700 mb-2">{t('models.importance')}</h4>
                            {importanceEntries.length > 0 ? (
                              <table className="w-full text-xs">
                                <thead>
                                  <tr>
                                    <th className="text-left py-1 text-zinc-500">{t('models.feature')}</th>
                                    <th className="text-right py-1 text-zinc-500">{t('models.importance')}</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {importanceEntries.map(([feat, val]) => (
                                    <tr key={feat} className="border-t border-zinc-100">
                                      <td className="py-1 font-mono text-zinc-700">{feat}</td>
                                      <td className="text-right py-1 text-zinc-600">{(val as number).toFixed(4)}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            ) : <p className="text-xs text-zinc-400">{t('models.noImportance')}</p>}
                          </div>
                        </div>
                      )}
                    </td>
                  </tr>
                )}
              </React.Fragment>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function TrainTab() {
  const { t } = useTranslation();
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
    get<RegistryInfo>("/models/registry").then((reg) => { setRegistry(reg); if (reg.models.length > 0) setModelType(reg.models[0].name); }).catch(() => {});
  }, []);

  const toggleFactor = (name: string) => {
    setSelectedFactors((prev) => prev.includes(name) ? prev.filter((f) => f !== name) : [...prev, name]);
  };

  const handleTrain = async () => {
    setStatus(t('models.submitting'));
    setError(null);
    try {
      const body: any = { model: modelType, qlib_native: qlibNative, factors: selectedFactors };
      if (tag.trim()) body.tag = tag.trim();
      if (fitStart) body.fit_start = fitStart;
      if (fitEnd) body.fit_end = fitEnd;
      const res = await post<{ task_id: string }>("/models/train", body);
      setStatus(`Training task submitted: ${res.task_id}`);
      const interval = setInterval(async () => {
        try {
          const tasks = await get<{ task_id: string; status: string; error?: string }[]>("/system/tasks");
          const task = tasks.find((tk) => tk.task_id === res.task_id);
          if (!task) return;
          if (task.status === "done") { setStatus("Training completed successfully."); clearInterval(interval); }
          else if (task.status === "failed") { setError(task.error || "Training failed"); setStatus(null); clearInterval(interval); }
          else { setStatus(`Task ${res.task_id}: ${task.status}...`); }
        } catch { clearInterval(interval); }
      }, 3000);
    } catch (err: any) { setError(err.message); setStatus(null); }
  };

  const inputCls = "w-full border border-zinc-300 rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-amber-400";

  return (
    <div className="space-y-4 max-w-lg">
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('models.modelType')}</label>
        <select value={modelType} onChange={(e) => setModelType(e.target.value)} className={inputCls}>
          {registry?.models.map((m) => <option key={m.name} value={m.name}>{m.name}</option>)}
        </select>
      </div>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-1">{t('models.tag')}</label>
        <input type="text" value={tag} onChange={(e) => setTag(e.target.value)} placeholder={t('models.tagPlaceholder')} className={inputCls} />
      </div>
      <div>
        <label className="block text-sm font-medium text-zinc-700 mb-2">{t('models.factors')}</label>
        <div className="flex flex-wrap gap-2">
          {registry?.factors.map((f) => (
            <label key={f.name}
              className={`inline-flex items-center gap-1 px-2 py-1 border rounded text-xs cursor-pointer transition-colors ${
                selectedFactors.includes(f.name) ? "bg-amber-50 border-amber-400 text-amber-700" : "bg-white border-zinc-300 text-zinc-600 hover:bg-zinc-50"
              }`}>
              <input type="checkbox" checked={selectedFactors.includes(f.name)} onChange={() => toggleFactor(f.name)} className="rounded" />
              {f.name}
            </label>
          ))}
        </div>
      </div>
      <div className="grid grid-cols-2 gap-3">
        <div>
          <label className="block text-sm font-medium text-zinc-700 mb-1">{t('models.fitStart')}</label>
          <input type="date" value={fitStart} onChange={(e) => setFitStart(e.target.value)} className={inputCls} />
        </div>
        <div>
          <label className="block text-sm font-medium text-zinc-700 mb-1">{t('models.fitEnd')}</label>
          <input type="date" value={fitEnd} onChange={(e) => setFitEnd(e.target.value)} className={inputCls} />
        </div>
      </div>
      <div className="space-y-2">
        <label className="flex items-center gap-2 text-sm text-zinc-700 cursor-pointer">
          <input type="checkbox" checked={qlibNative} onChange={(e) => setQlibNative(e.target.checked)} className="rounded" />
          {t('models.qlibNative')}
        </label>
        <label className="flex items-center gap-2 text-sm text-zinc-700 cursor-pointer">
          <input type="checkbox" checked={dryRun} onChange={(e) => setDryRun(e.target.checked)} className="rounded" />
          {t('models.dryRun')}
        </label>
      </div>
      <button onClick={handleTrain} disabled={!!status && status.includes(t('models.submitting'))}
        className="px-4 py-2 bg-amber-500 text-zinc-900 font-medium rounded-md text-sm hover:bg-amber-600 disabled:opacity-50">
        {dryRun ? t('models.trainDryBtn') : t('models.trainBtn')}
      </button>
      {status && <p className={`text-sm ${error ? "text-red-600" : "text-green-700"}`}>{status}</p>}
      {error && <p className="text-sm text-red-600">{error}</p>}
    </div>
  );
}

function RegistryTab() {
  const { t } = useTranslation();
  const [registry, setRegistry] = useState<RegistryInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    get<RegistryInfo>("/models/registry").then(setRegistry).catch((err) => setError(err.message)).finally(() => setLoading(false));
  }, []);

  if (loading) return <p className="text-zinc-500 text-sm py-4">{t('common.loading')}</p>;
  if (error) return <p className="text-red-600 text-sm py-4">{t('common.error')}: {error}</p>;
  if (!registry) return null;

  const TableSection = ({ title, items }: { title: string; items: { name: string }[] }) => (
    <div>
      <h3 className="text-lg font-semibold text-zinc-800 mb-3">{title}</h3>
      <div className="bg-white border border-zinc-200 rounded-lg overflow-hidden shadow-sm">
        <table className="w-full text-sm">
          <thead className="bg-zinc-50 border-b border-zinc-200">
            <tr>
              <th className="text-left px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.name')}</th>
              <th className="text-left px-4 py-2 text-xs text-zinc-500 uppercase tracking-wide font-medium">{t('common.class')}</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item) => (
              <tr key={item.name} className="border-t border-zinc-100 hover:bg-zinc-50">
                <td className="px-4 py-2 font-mono text-sm text-zinc-700">{item.name}</td>
                <td className="px-4 py-2 text-xs text-zinc-400">{item.name.charAt(0).toUpperCase() + item.name.slice(1)}AlphaModel</td>
              </tr>
            ))}
            {items.length === 0 && (
              <tr><td colSpan={2} className="px-4 py-4 text-center text-zinc-400 text-sm">{t('models.noRegistered')}</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );

  return (
    <div className="space-y-6">
      <TableSection title={t('models.registeredModels', { count: registry.models.length })} items={registry.models} />
      <TableSection title={t('models.registeredFactors', { count: registry.factors.length })} items={registry.factors} />
    </div>
  );
}
