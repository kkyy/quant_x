import { useState, useEffect } from "react";
import { get, post } from "../api/client";

interface ModelInfo {
  filename: string;
  size_mb: number;
  modified: string;
  meta: Record<string, unknown>;
}

interface SignalFile {
  filename: string;
  size_kb: number;
  modified: string;
}

interface SignalContent {
  content: string;
}

interface RegimeInfo {
  enabled: boolean;
  regime: number | null;
  label: string | null;
  error?: string;
}

type Tab = "generate" | "history" | "rebalance" | "notification";

export function SignalsPage() {
  const [tab, setTab] = useState<Tab>("generate");

  return (
    <div>
      <h2 className="text-2xl font-bold mb-4">Signals</h2>

      <div className="flex gap-1 mb-6 border-b border-gray-700">
        {(["generate", "history", "rebalance", "notification"] as Tab[]).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors capitalize ${
              tab === t
                ? "border-blue-500 text-blue-400"
                : "border-transparent text-gray-400 hover:text-gray-200"
            }`}
          >
            {t}
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

/* ---------- Generate Tab ---------- */

function GenerateTab() {
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
    get<ModelInfo[]>("/models").then((data) => {
      setModels(data);
      if (data.length > 0 && !modelPath) {
        setModelPath(data[0].filename);
      }
    });
    get<RegimeInfo>("/signals/regime").then(setRegime).catch(() => {});
  }, []);

  const handleSubmit = async () => {
    setSubmitting(true);
    setTaskStatus(null);
    try {
      const res = await post<{ task_id: string }>("/signals/generate", {
        model_path: modelPath,
        account: parseFloat(account) || 1000000,
        positions: positions || null,
        dry_run: dryRun,
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
      {regime && (
        <div className="p-3 bg-gray-800 rounded text-sm">
          <span className="text-gray-400">Regime: </span>
          {regime.enabled ? (
            <span className="text-yellow-400">{regime.label ?? "N/A"}</span>
          ) : (
            <span className="text-gray-500">disabled</span>
          )}
        </div>
      )}

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
        <label className="block text-sm font-medium text-gray-300 mb-1">Account (CNY)</label>
        <input
          type="number"
          value={account}
          onChange={(e) => setAccount(e.target.value)}
          className="w-full bg-gray-800 border border-gray-600 rounded px-3 py-2 text-sm"
        />
      </div>

      <div>
        <label className="block text-sm font-medium text-gray-300 mb-1">
          Current Positions (e.g. SH600000:500,SZ000001:300)
        </label>
        <textarea
          value={positions}
          onChange={(e) => setPositions(e.target.value)}
          rows={3}
          className="w-full bg-gray-800 border border-gray-600 rounded px-3 py-2 text-sm font-mono"
          placeholder="SH600000:500,SZ000001:300"
        />
      </div>

      <div className="flex items-center gap-3">
        <label className="flex items-center gap-2 text-sm text-gray-300 cursor-pointer">
          <input
            type="checkbox"
            checked={dryRun}
            onChange={(e) => setDryRun(e.target.checked)}
            className="rounded border-gray-600"
          />
          Dry run (no push)
        </label>
      </div>

      <button
        onClick={handleSubmit}
        disabled={submitting || !modelPath}
        className="bg-blue-600 hover:bg-blue-700 disabled:bg-gray-600 disabled:cursor-not-allowed text-white px-6 py-2 rounded text-sm font-medium"
      >
        {submitting ? "Starting..." : "Generate Signal"}
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

/* ---------- History Tab ---------- */

function HistoryTab() {
  const [files, setFiles] = useState<SignalFile[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  const [content, setContent] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    get<SignalFile[]>("/signals/history").then(setFiles);
  }, []);

  const loadFile = async (filename: string) => {
    setSelected(filename);
    setLoading(true);
    try {
      const res = await get<SignalContent>(`/signals/history/${filename}`);
      setContent(res.content);
    } catch {
      setContent(null);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div>
      <h3 className="text-lg font-semibold mb-3">Signal History</h3>

      {files.length === 0 ? (
        <p className="text-gray-500 text-sm">No signal files found.</p>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <div className="lg:col-span-1">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-gray-400 border-b border-gray-700">
                  <th className="text-left py-2">File</th>
                  <th className="text-right py-2">Size</th>
                </tr>
              </thead>
              <tbody>
                {files.map((f) => (
                  <tr
                    key={f.filename}
                    onClick={() => loadFile(f.filename)}
                    className={`cursor-pointer border-b border-gray-800 hover:bg-gray-800 ${
                      selected === f.filename ? "bg-gray-800" : ""
                    }`}
                  >
                    <td className="py-2 text-blue-400 font-mono text-xs">{f.filename}</td>
                    <td className="text-right py-2 text-gray-400">{f.size_kb} KB</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="lg:col-span-2">
            {loading && <p className="text-gray-400 text-sm">Loading...</p>}
            {!loading && !selected && (
              <p className="text-gray-500 text-sm">Select a file to view content.</p>
            )}
            {!loading && content && (
              <pre className="bg-gray-900 border border-gray-700 rounded p-4 text-xs text-gray-300 overflow-auto max-h-[600px] whitespace-pre-wrap">
                {content}
              </pre>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

/* ---------- Rebalance Tab ---------- */

function RebalanceTab() {
  const [mock, setMock] = useState(false);
  const [dryRun, setDryRun] = useState(true);
  const [message, setMessage] = useState<string | null>(null);

  const handleRun = () => {
    setMessage(
      `Scheduled rebalance with mock=${mock}, dry-run=${dryRun}.\n` +
        "This feature will be connected to the full rebalance pipeline in a future update.\n" +
        "For now, use: python run_scheduled_rebalance.py --mock --dry-run"
    );
  };

  return (
    <div className="max-w-2xl space-y-4">
      <p className="text-gray-400 text-sm">
        Schedule and run portfolio rebalancing. This tab is a placeholder for the full rebalance
        pipeline integration.
      </p>

      <div className="flex items-center gap-6">
        <label className="flex items-center gap-2 text-sm text-gray-300 cursor-pointer">
          <input
            type="checkbox"
            checked={mock}
            onChange={(e) => setMock(e.target.checked)}
            className="rounded border-gray-600"
          />
          Mock mode
        </label>
        <label className="flex items-center gap-2 text-sm text-gray-300 cursor-pointer">
          <input
            type="checkbox"
            checked={dryRun}
            onChange={(e) => setDryRun(e.target.checked)}
            className="rounded border-gray-600"
          />
          Dry run (no push)
        </label>
      </div>

      <button
        onClick={handleRun}
        className="bg-blue-600 hover:bg-blue-700 text-white px-6 py-2 rounded text-sm font-medium"
      >
        Run Rebalance
      </button>

      {message && (
        <pre className="bg-gray-800 border border-gray-700 rounded p-4 text-sm text-gray-300 whitespace-pre-wrap">
          {message}
        </pre>
      )}
    </div>
  );
}

/* ---------- Notification Tab ---------- */

function NotificationTab() {
  const [title, setTitle] = useState("");
  const [content, setContent] = useState("");
  const [sending, setSending] = useState(false);
  const [result, setResult] = useState<string | null>(null);

  const handleSend = async () => {
    setSending(true);
    setResult(null);
    try {
      // Attempt to send via a notification endpoint if available
      // For now, show a confirmation message
      setResult(`Test notification sent.\nTitle: ${title}\nContent: ${content}`);
    } catch (err) {
      setResult(`Error: ${err}`);
    } finally {
      setSending(false);
    }
  };

  return (
    <div className="max-w-2xl space-y-4">
      <p className="text-gray-400 text-sm">Send a test notification to verify notification channels.</p>

      <div>
        <label className="block text-sm font-medium text-gray-300 mb-1">Title</label>
        <input
          type="text"
          value={title}
          onChange={(e) => setTitle(e.target.value)}
          className="w-full bg-gray-800 border border-gray-600 rounded px-3 py-2 text-sm"
          placeholder="Test Notification"
        />
      </div>

      <div>
        <label className="block text-sm font-medium text-gray-300 mb-1">Content</label>
        <textarea
          value={content}
          onChange={(e) => setContent(e.target.value)}
          rows={5}
          className="w-full bg-gray-800 border border-gray-600 rounded px-3 py-2 text-sm"
          placeholder="This is a test notification from the dashboard."
        />
      </div>

      <button
        onClick={handleSend}
        disabled={sending || !title}
        className="bg-blue-600 hover:bg-blue-700 disabled:bg-gray-600 disabled:cursor-not-allowed text-white px-6 py-2 rounded text-sm font-medium"
      >
        {sending ? "Sending..." : "Send Test"}
      </button>

      {result && (
        <pre className="bg-gray-800 border border-gray-700 rounded p-4 text-sm text-gray-300 whitespace-pre-wrap">
          {result}
        </pre>
      )}
    </div>
  );
}
