import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { Card } from "../components/ui/Card";
import { Tabs } from "../components/ui/Tabs";
import { Badge } from "../components/ui/Badge";
import { Select } from "../components/ui/Select";
import { NumberInput } from "../components/ui/NumberInput";
import { DatePicker } from "../components/ui/DatePicker";
import { TaskStatus } from "../components/ui/TaskStatus";
import { Skeleton } from "../components/ui/Skeleton";
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

const SIGNALS_TABS = [
  { key: "generate", label: "Generate" },
  { key: "daily", label: "Daily" },
  { key: "rebalance", label: "Rebalance" },
  { key: "regime", label: "Regime" },
  { key: "notification", label: "Notification" },
];

// ─── Generate Tab ──────────────────────────────────────────────────────

function GenerateTab() {
  const { t } = useTranslation();
  const [models, setModels] = useState<ModelInfo[]>([]);
  const [modelPath, setModelPath] = useState("");
  const [account, setAccount] = useState(1000000);
  const [positions, setPositions] = useState("");
  const [dryRun, setDryRun] = useState(true);
  const [taskId, setTaskId] = useState<string | null>(null);

  useEffect(() => {
    get<ModelInfo[]>("/models").then((data) => {
      setModels(data);
      if (data.length > 0 && !modelPath) setModelPath(data[0].filename);
    });
  }, []);

  const handleGenerate = async () => {
    const res = await post<{ task_id: string }>("/signals/generate", {
      model_path: modelPath,
      account,
      positions: positions || null,
      dry_run: dryRun,
    });
    setTaskId(res.task_id);
  };

  const modelOptions = models.map((m) => ({
    value: m.filename,
    label: `${m.filename} (${m.size_mb} MB)`,
  }));

  return (
    <div className="space-y-4 max-w-2xl">
      <Card title={t("signals.generateTab")}>
        <div className="space-y-4">
          <div>
            <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
              {t("signals.model")}
            </p>
            <Select
              options={modelOptions}
              value={modelPath}
              onChange={setModelPath}
              searchable
            />
          </div>
          <div>
            <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
              {t("signals.account")}
            </p>
            <NumberInput
              value={account}
              onChange={(v) => setAccount(v ?? 1000000)}
              min={10000}
              step={100000}
            />
          </div>
          <div>
            <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
              {t("signals.positions")}
            </p>
            <textarea
              value={positions}
              onChange={(e) => setPositions(e.target.value)}
              rows={3}
              className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
              placeholder={t("signals.positionsPlaceholder")}
            />
          </div>
          <label className="flex items-center gap-2 text-xs font-mono text-terminal-text cursor-pointer">
            <input
              type="checkbox"
              checked={dryRun}
              onChange={(e) => setDryRun(e.target.checked)}
              className="accent-terminal-green"
            />
            {t("signals.dryRun")}
          </label>
          <button
            onClick={handleGenerate}
            disabled={!modelPath}
            className="px-3 py-1.5 text-xs font-mono border border-terminal-green text-terminal-green hover:bg-terminal-green-glow transition-colors rounded-sm disabled:opacity-30"
          >
            {t("signals.generateBtn")}
          </button>
        </div>
      </Card>
      <TaskStatus taskId={taskId} />
    </div>
  );
}

// ─── Daily Tab (full params) ───────────────────────────────────────────

function DailyTab() {
  const { t } = useTranslation();
  const [models, setModels] = useState<ModelInfo[]>([]);
  const [modelPath, setModelPath] = useState("");
  const [account, setAccount] = useState(1000000);
  const [positions, setPositions] = useState("");
  const [config, setConfig] = useState("");
  const [dryRun, setDryRun] = useState(true);
  const [taskId, setTaskId] = useState<string | null>(null);

  useEffect(() => {
    get<ModelInfo[]>("/models").then((data) => {
      setModels(data);
      if (data.length > 0 && !modelPath) setModelPath(data[0].filename);
    });
  }, []);

  const handleDaily = async () => {
    const body: Record<string, unknown> = {
      model_path: modelPath,
      account,
      positions: positions || null,
      dry_run: dryRun,
    };
    if (config.trim()) body.config = config.trim();
    const res = await post<{ task_id: string }>("/signals/generate", body);
    setTaskId(res.task_id);
  };

  const modelOptions = models.map((m) => ({
    value: m.filename,
    label: `${m.filename} (${m.size_mb} MB)`,
  }));

  return (
    <div className="space-y-4 max-w-2xl">
      <Card title={t("signals.dailySignal")}>
        <div className="space-y-4">
          <div>
            <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
              {t("signals.model")}
            </p>
            <Select
              options={modelOptions}
              value={modelPath}
              onChange={setModelPath}
              searchable
            />
          </div>
          <div>
            <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
              {t("signals.account")}
            </p>
            <NumberInput
              value={account}
              onChange={(v) => setAccount(v ?? 1000000)}
              min={10000}
              step={100000}
            />
          </div>
          <div>
            <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
              {t("signals.positions")}
            </p>
            <textarea
              value={positions}
              onChange={(e) => setPositions(e.target.value)}
              rows={2}
              className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
              placeholder={t("signals.positionsPlaceholder")}
            />
          </div>
          <div>
            <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
              {t("signals.configOverride")}
            </p>
            <input
              type="text"
              value={config}
              onChange={(e) => setConfig(e.target.value)}
              placeholder="config/daily_csi1000.yaml"
              className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
            />
          </div>
          <label className="flex items-center gap-2 text-xs font-mono text-terminal-text cursor-pointer">
            <input
              type="checkbox"
              checked={dryRun}
              onChange={(e) => setDryRun(e.target.checked)}
              className="accent-terminal-green"
            />
            {t("signals.dryRun")}
          </label>
          <button
            onClick={handleDaily}
            disabled={!modelPath}
            className="px-3 py-1.5 text-xs font-mono border border-terminal-green text-terminal-green hover:bg-terminal-green-glow transition-colors rounded-sm disabled:opacity-30"
          >
            {t("signals.generateDailyBtn")}
          </button>
        </div>
      </Card>
      <TaskStatus taskId={taskId} />
    </div>
  );
}

// ─── Rebalance Tab ─────────────────────────────────────────────────────

function RebalanceTab() {
  const { t } = useTranslation();
  const [mock, setMock] = useState(false);
  const [dryRun, setDryRun] = useState(true);
  const [config, setConfig] = useState("");
  const [positions, setPositions] = useState("");
  const [positionDate, setPositionDate] = useState("");
  const [minActionValue, setMinActionValue] = useState<number | undefined>(1000);
  const [skipUpdate, setSkipUpdate] = useState(true);
  const [force, setForce] = useState(false);
  const [notifyChannel, setNotifyChannel] = useState("bark");
  const [taskId, setTaskId] = useState<string | null>(null);

  const handleRun = async () => {
    const res = await post<{ task_id: string }>("/signals/rebalance", {
      mock,
      dry_run: dryRun,
      config: config.trim() || undefined,
      positions: positions.trim() || undefined,
      position_date: positionDate || undefined,
      min_action_value: minActionValue,
      skip_update: skipUpdate,
      force,
      notify_channel: notifyChannel,
    });
    setTaskId(res.task_id);
  };

  return (
    <div className="space-y-4 max-w-2xl">
      <Card title={t("signals.rebalanceTab")}>
        <div className="space-y-4">
          <p className="text-terminal-text-dim text-xs font-mono">{t("signals.rebalanceNote")}</p>
          <div>
            <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
              {t("signals.configOverride")}
            </p>
            <input
              type="text"
              value={config}
              onChange={(e) => setConfig(e.target.value)}
              placeholder="config/daily_csi1000.yaml"
              className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
            />
          </div>
          <div>
            <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
              {t("signals.positions")}
            </p>
            <textarea
              value={positions}
              onChange={(e) => setPositions(e.target.value)}
              rows={2}
              className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
              placeholder={t("signals.positionsPlaceholder")}
            />
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
                {t("signals.positionDate")}
              </p>
              <DatePicker value={positionDate} onChange={setPositionDate} />
            </div>
            <div>
              <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
                {t("signals.minActionValue")}
              </p>
              <NumberInput
                value={minActionValue}
                onChange={(v) => setMinActionValue(v)}
                min={0}
                step={500}
                placeholder="1000"
              />
            </div>
          </div>
          <div>
            <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
              {t("signals.notifyChannel")}
            </p>
            <Select
              options={[
                { value: "bark", label: "Bark" },
                { value: "all", label: t("signals.allChannels") },
              ]}
              value={notifyChannel}
              onChange={setNotifyChannel}
            />
          </div>
          <div className="flex flex-wrap items-center gap-6">
            <label className="flex items-center gap-2 text-xs font-mono text-terminal-text cursor-pointer">
              <input
                type="checkbox"
                checked={mock}
                onChange={(e) => setMock(e.target.checked)}
                className="accent-terminal-green"
              />
              {t("signals.mockMode")}
            </label>
            <label className="flex items-center gap-2 text-xs font-mono text-terminal-text cursor-pointer">
              <input
                type="checkbox"
                checked={dryRun}
                onChange={(e) => setDryRun(e.target.checked)}
                className="accent-terminal-green"
              />
              {t("signals.dryRun")}
            </label>
            <label className="flex items-center gap-2 text-xs font-mono text-terminal-text cursor-pointer">
              <input
                type="checkbox"
                checked={skipUpdate}
                onChange={(e) => setSkipUpdate(e.target.checked)}
                className="accent-terminal-green"
              />
              {t("signals.skipUpdate")}
            </label>
            <label className="flex items-center gap-2 text-xs font-mono text-terminal-text cursor-pointer">
              <input
                type="checkbox"
                checked={force}
                onChange={(e) => setForce(e.target.checked)}
                className="accent-terminal-green"
              />
              {t("signals.forceRun")}
            </label>
          </div>
          <button
            onClick={handleRun}
            className="px-3 py-1.5 text-xs font-mono border border-terminal-green text-terminal-green hover:bg-terminal-green-glow transition-colors rounded-sm"
          >
            {t("signals.runRebalance")}
          </button>
        </div>
      </Card>
      <TaskStatus taskId={taskId} />
    </div>
  );
}

// ─── Regime Tab ────────────────────────────────────────────────────────

function RegimeTab() {
  const { t } = useTranslation();
  const [regime, setRegime] = useState<RegimeInfo | null>(null);

  useEffect(() => {
    get<RegimeInfo>("/signals/regime")
      .then(setRegime)
      .catch(() => {});
  }, []);

  const regimeLabels: Record<number, string> = {
    0: "Calm Bull",
    1: "Calm Bear",
    2: "Volatile Bull",
    3: "Volatile Bear",
  };

  const regimeColors: Record<number, string> = {
    0: "bg-terminal-green-glow text-terminal-green border-terminal-green",
    1: "bg-terminal-red-glow text-terminal-red border-terminal-red",
    2: "bg-terminal-amber-glow text-terminal-amber border-terminal-amber",
    3: "bg-terminal-cyan-glow text-terminal-cyan border-terminal-cyan",
  };

  return (
    <Card title={t("signals.regime")}>
      <div className="space-y-4">
        {regime ? (
          <>
            <div className="flex items-center gap-4">
              <Badge variant={regime.enabled ? "success" : "neutral"}>
                {regime.enabled
                  ? t("common.enabled")
                  : t("common.disabled")}
              </Badge>
              {regime.enabled && regime.regime != null && (
                <span
                  className={`px-3 py-1 rounded-sm border text-xs font-mono font-semibold ${
                    regimeColors[regime.regime] ?? "bg-terminal-raised text-terminal-text-dim border-terminal-border"
                  }`}
                >
                  {regimeLabels[regime.regime] ?? `Regime ${regime.regime}`}
                </span>
              )}
            </div>
            {regime.error && (
              <p className="text-terminal-red text-xs font-mono">{regime.error}</p>
            )}
            {regime.enabled && regime.label === "requires_price_data" && (
              <p className="text-terminal-text-dim text-xs font-mono">
                Regime detection requires real-time price data. Run during
                market hours or with cached data.
              </p>
            )}
          </>
        ) : (
          <Skeleton className="h-6 w-48" />
        )}
      </div>
    </Card>
  );
}

// ─── Notification Tab ──────────────────────────────────────────────────

function NotificationTab() {
  const { t } = useTranslation();
  const [title, setTitle] = useState("");
  const [content, setContent] = useState("");
  const [channel, setChannel] = useState("");
  const [dryRun, setDryRun] = useState(true);
  const [confirmSend, setConfirmSend] = useState(false);
  const [sending, setSending] = useState(false);
  const [result, setResult] = useState<{ success: boolean; dry_run?: boolean; sent?: boolean; channels?: string[]; error?: string } | null>(null);

  const handleSend = async () => {
    setSending(true);
    setResult(null);
    try {
      const res = await post<{ success: boolean; error?: string }>(
        "/signals/notify-test",
        {
          title,
          content,
          channel: channel || undefined,
          dry_run: dryRun,
          confirm_send: confirmSend,
        }
      );
      setResult(res);
    } catch (err) {
      setResult({ success: false, error: String(err) });
    } finally {
      setSending(false);
    }
  };

  return (
    <Card title={t("signals.notificationTab")}>
      <div className="space-y-4 max-w-lg">
        <p className="text-terminal-text-dim text-xs font-mono">{t("signals.notifNote")}</p>
        <div>
          <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
            {t("signals.notifTitle")}
          </p>
          <input
            type="text"
            value={title}
            onChange={(e) => setTitle(e.target.value)}
            placeholder={t("signals.notifTitlePlaceholder")}
            className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
          />
        </div>
        <div>
          <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
            {t("signals.notifContent")}
          </p>
          <textarea
            value={content}
            onChange={(e) => setContent(e.target.value)}
            rows={5}
            className="w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
            placeholder={t("signals.notifPlaceholder")}
          />
        </div>
        <div>
          <p className="text-xs font-mono text-terminal-text-dim uppercase mb-1">
            {t("signals.notifyChannel")}
          </p>
          <Select
            options={[
              { value: "", label: t("signals.allChannels") },
              { value: "bark", label: "Bark" },
              { value: "pushplus", label: "PushPlus" },
              { value: "dingtalk", label: "DingTalk" },
              { value: "serverchan", label: "ServerChan" },
              { value: "wechat_mp", label: "WeChat MP" },
            ]}
            value={channel}
            onChange={setChannel}
          />
        </div>
        <div className="flex flex-wrap gap-4">
          <label className="flex items-center gap-2 text-xs font-mono text-terminal-text cursor-pointer">
            <input
              type="checkbox"
              checked={dryRun}
              onChange={(e) => {
                setDryRun(e.target.checked);
                if (e.target.checked) setConfirmSend(false);
              }}
              className="accent-terminal-green"
            />
            {t("signals.dryRun")}
          </label>
          {!dryRun && (
            <label className="flex items-center gap-2 text-xs font-mono text-terminal-red cursor-pointer">
              <input
                type="checkbox"
                checked={confirmSend}
                onChange={(e) => setConfirmSend(e.target.checked)}
                className="accent-terminal-red"
              />
              {t("signals.confirmRealNotify")}
            </label>
          )}
        </div>
        <button
          onClick={handleSend}
          disabled={sending || !title || (!dryRun && !confirmSend)}
          className="px-3 py-1.5 text-xs font-mono border border-terminal-green text-terminal-green hover:bg-terminal-green-glow transition-colors rounded-sm disabled:opacity-30"
        >
          {sending ? t("common.sending") : dryRun ? t("signals.previewTest") : t("signals.sendTest")}
        </button>
        {result && (
          <div
            className={`px-4 py-3 rounded-sm text-xs font-mono border ${
              result.success
                ? "bg-terminal-green-glow text-terminal-green border-terminal-green"
                : "bg-terminal-red-glow text-terminal-red border-terminal-red"
            }`}
          >
            {result.success
              ? result.dry_run
                ? `${t("signals.previewOk")} ${result.channels?.join(", ") || t("common.noData")}`
                : t("common.sent")
              : `Error: ${result.error}`}
          </div>
        )}
      </div>
    </Card>
  );
}

// ─── History (inline) ──────────────────────────────────────────────────

function HistorySection() {
  const { t } = useTranslation();
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
      const res = await get<SignalContent>(
        `/signals/history/${filename}`
      );
      setContent(res.content);
    } catch {
      setContent(null);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Card title={t("signals.historyTab")}>
      {files.length === 0 ? (
        <p className="text-terminal-text-dim text-xs font-mono">{t("signals.noHistory")}</p>
      ) : (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          <div className="lg:col-span-1 space-y-1 max-h-[500px] overflow-y-auto">
            {files.map((f) => (
              <button
                key={f.filename}
                onClick={() => loadFile(f.filename)}
                className={`w-full text-left px-3 py-2 rounded-sm text-xs font-mono transition-colors ${
                  selected === f.filename
                    ? "bg-terminal-green-glow text-terminal-green"
                    : "text-terminal-text-dim hover:bg-terminal-raised hover:text-terminal-text"
                }`}
              >
                <span className="block truncate">{f.filename}</span>
                <span className="text-terminal-text-dim">{f.size_kb} KB</span>
              </button>
            ))}
          </div>
          <div className="lg:col-span-2">
            {loading && (
              <div className="space-y-2">
                <Skeleton className="h-4 w-3/4" />
                <Skeleton className="h-4 w-full" />
                <Skeleton className="h-4 w-5/6" />
                <Skeleton className="h-4 w-2/3" />
                <Skeleton className="h-4 w-full" />
              </div>
            )}
            {!loading && !selected && (
              <p className="text-terminal-text-dim text-xs font-mono">
                {t("common.selectFile")}
              </p>
            )}
            {!loading && content && (
              <pre className="bg-terminal-surface border border-terminal-border rounded-sm p-4 text-xs font-mono text-terminal-text overflow-auto max-h-[500px] whitespace-pre-wrap">
                {content}
              </pre>
            )}
          </div>
        </div>
      )}
    </Card>
  );
}

// ─── Main Page ─────────────────────────────────────────────────────────

export function SignalsPage() {
  const { t } = useTranslation();
  const [activeTab, setActiveTab] = useState("generate");

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-sm font-mono font-semibold text-terminal-text-bright uppercase tracking-wider">
          {t("signals.title")}
        </h1>
        <Tabs
          tabs={SIGNALS_TABS}
          activeKey={activeTab}
          onChange={setActiveTab}
        />
      </div>

      {activeTab === "generate" && <GenerateTab />}
      {activeTab === "daily" && <DailyTab />}
      {activeTab === "rebalance" && <RebalanceTab />}
      {activeTab === "regime" && <RegimeTab />}
      {activeTab === "notification" && <NotificationTab />}

      {/* History always visible below tabs */}
      <HistorySection />
    </div>
  );
}
