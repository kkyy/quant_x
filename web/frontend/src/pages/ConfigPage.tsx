import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Card } from "../components/ui/Card";
import { Tabs } from "../components/ui/Tabs";
import { Badge } from "../components/ui/Badge";
import { Table } from "../components/ui/Table";
import { SkeletonCard } from "../components/ui/Skeleton";
import { get, put } from "../api/client";

interface ConfigResponse {
  content: string;
  exists: boolean;
}

interface SaveResponse {
  saved: boolean;
}

interface RegimeRule {
  key: string;
  label: string;
  topk: number;
  n_drop: number;
  hold_thresh: number;
}

const CONFIG_TABS = [
  { key: "editor", label: "Editor" },
  { key: "strategy", label: "Strategy" },
  { key: "regime", label: "Regime" },
];

type ConfigName = "base" | "model" | "notify";

// ─── Editor Tab ────────────────────────────────────────────────────────

function EditorTab() {
  const { t } = useTranslation();
  const [selectedConfig, setSelectedConfig] = useState<ConfigName>("base");
  const [content, setContent] = useState("");
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState<{ type: "ok" | "err"; text: string } | null>(null);

  const fetchConfig = async (name: ConfigName) => {
    setLoading(true);
    setMessage(null);
    try {
      const res = await get<ConfigResponse>(`/config/${name}`);
      setContent(res.content);
    } catch (err) {
      setMessage({ type: "err", text: String(err) });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchConfig(selectedConfig);
  }, [selectedConfig]);

  const handleSave = async () => {
    setSaving(true);
    setMessage(null);
    try {
      await put<SaveResponse>(`/config/${selectedConfig}`, { content });
      setMessage({ type: "ok", text: t("config.savedOk") });
    } catch (err) {
      setMessage({ type: "err", text: String(err) });
    } finally {
      setSaving(false);
    }
  };

  const configNames: { key: ConfigName; label: string }[] = [
    { key: "base", label: "base.yaml" },
    { key: "model", label: "model.yaml" },
    { key: "notify", label: "notify.yaml" },
  ];

  return (
    <div className="space-y-3">
      <div className="flex gap-2">
        {configNames.map((c) => (
          <button
            key={c.key}
            onClick={() => setSelectedConfig(c.key)}
            className={`px-3 py-1.5 text-xs font-mono border rounded-sm transition-colors ${
              selectedConfig === c.key
                ? "border-terminal-green text-terminal-green bg-terminal-green-glow"
                : "border-terminal-border text-terminal-text-dim hover:border-terminal-text-dim hover:text-terminal-text"
            }`}
          >
            {c.label}
          </button>
        ))}
      </div>

      {message && (
        <div
          className={`px-4 py-2 rounded-sm text-xs font-mono border ${
            message.type === "ok"
              ? "bg-terminal-green-glow text-terminal-green border-terminal-green/30"
              : "bg-terminal-red-glow text-terminal-red border-terminal-red/30"
          }`}
        >
          {message.text}
        </div>
      )}

      {loading ? (
        <SkeletonCard rows={10} />
      ) : (
        <>
          <textarea
            value={content}
            onChange={(e) => setContent(e.target.value)}
            className="w-full h-96 font-mono text-xs bg-terminal-surface border border-terminal-border rounded-sm p-3 text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors"
            spellCheck={false}
          />
          <div className="flex gap-2">
            <button
              onClick={handleSave}
              disabled={saving}
              className="px-3 py-1.5 text-xs font-mono border border-terminal-green text-terminal-green hover:bg-terminal-green-glow transition-colors rounded-sm disabled:opacity-30"
            >
              {saving ? t("common.saving") : t("common.save")}
            </button>
            <button
              onClick={() => fetchConfig(selectedConfig)}
              className="px-3 py-1.5 text-xs font-mono border border-terminal-border text-terminal-text-dim hover:border-terminal-text-dim hover:text-terminal-text transition-colors rounded-sm"
            >
              {t("common.reload")}
            </button>
          </div>
        </>
      )}
    </div>
  );
}

// ─── Strategy Tab ──────────────────────────────────────────────────────

function StrategyTab() {
  const [content, setContent] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    get<ConfigResponse>("/config/strategy_candidates")
      .then((res) =>
        setContent(res.content || "# No strategy_candidates.yaml found")
      )
      .catch((err) => setError(String(err)))
      .finally(() => setLoading(false));
  }, []);

  if (error)
    return (
      <div className="bg-terminal-red-glow text-terminal-red border border-terminal-red/30 px-4 py-2 rounded-sm text-xs font-mono">
        {error}
      </div>
    );
  if (loading)
    return <SkeletonCard rows={8} />;

  return (
    <pre className="w-full bg-terminal-surface border border-terminal-border text-terminal-text rounded-sm p-4 text-xs font-mono overflow-auto max-h-[600px] whitespace-pre-wrap">
      {content}
    </pre>
  );
}

// ─── Regime Tab ────────────────────────────────────────────────────────

function RegimeTab() {
  const { t } = useTranslation();
  const [regimeRules, setRegimeRules] = useState<RegimeRule[]>([]);
  const [regimeEnabled, setRegimeEnabled] = useState(false);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    get<ConfigResponse>("/config/base")
      .then((res) => {
        try {
          const regimeSection = res.content;
          const enabled = regimeSection.includes("enabled: true");
          setRegimeEnabled(enabled);

          const rules: RegimeRule[] = [];
          const lines = regimeSection.split("\n");
          let inRegime = false;
          let currentRule: Partial<RegimeRule> | null = null;

          for (const line of lines) {
            if (line.includes("regime_switch")) {
              inRegime = true;
              continue;
            }
            if (inRegime && line.match(/^\s{6}\d/)) {
              if (currentRule && currentRule.topk !== undefined) {
                rules.push(currentRule as RegimeRule);
              }
              const keyMatch = line.match(/^\s*(\d+):\s*#\s*(\w+)/);
              const bareMatch = line.match(/^\s*(\d+):/);
              const key = keyMatch?.[1] ?? bareMatch?.[1] ?? "";
              const label = keyMatch?.[2] ?? `Rule ${key}`;
              currentRule = { key, label };
              continue;
            }
            if (currentRule) {
              const topkMatch = line.match(/topk:\s*(\d+)/);
              const ndropMatch = line.match(/n_drop:\s*(\d+)/);
              const holdMatch = line.match(/hold_thresh:\s*(\d+)/);
              if (topkMatch) currentRule.topk = parseInt(topkMatch[1], 10);
              if (ndropMatch) currentRule.n_drop = parseInt(ndropMatch[1], 10);
              if (holdMatch) currentRule.hold_thresh = parseInt(holdMatch[1], 10);
            }
          }
          if (currentRule && currentRule.topk !== undefined) {
            rules.push(currentRule as RegimeRule);
          }

          setRegimeRules(rules);
        } catch {
          // ignore parse errors
        }
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const regimeLabels: Record<string, string> = {
    "0": t("config.regimeLabelCalm_bull"),
    "1": t("config.regimeLabelCalm_bear"),
    "2": t("config.regimeLabelVolatile_bull"),
    "3": t("config.regimeLabelVolatile_bear"),
  };

  if (loading)
    return <SkeletonCard rows={4} />;

  return (
    <div className="space-y-4">
      <Card>
        <div className="flex items-center gap-3">
          <span className="text-xs text-terminal-text font-mono">
            {t("config.regimeSwitching")}
          </span>
          <Badge variant={regimeEnabled ? "success" : "neutral"}>
            {regimeEnabled ? t("common.enabled") : t("common.disabled")}
          </Badge>
        </div>
      </Card>

      {regimeRules.length > 0 ? (
        <Table
          columns={[
            {
              key: "key",
              label: t("config.key"),
              render: (row) => (
                <span className="font-mono text-xs text-terminal-text-dim">
                  {row.key as string}
                </span>
              ),
            },
            {
              key: "label",
              label: t("config.regime"),
              render: (row) => (
                <span className="text-xs text-terminal-text-bright">
                  {regimeLabels[row.key as string] || (row.label as string)}
                </span>
              ),
            },
            { key: "topk", label: t("config.topk"), align: "right" as const },
            {
              key: "n_drop",
              label: t("config.nDrop"),
              align: "right" as const,
            },
            {
              key: "hold_thresh",
              label: t("config.holdThresh"),
              align: "right" as const,
            },
          ]}
          data={regimeRules as unknown as Record<string, unknown>[]}
          pageSize={10}
        />
      ) : (
        <Card>
          <p className="text-terminal-text-dim text-xs font-mono">
            {t("config.noRegimeRules")}
          </p>
        </Card>
      )}

      <p className="text-xs text-terminal-text-dim font-mono">{t("config.regimeNote")}</p>
    </div>
  );
}

// ─── Main Page ─────────────────────────────────────────────────────────

export function ConfigPage() {
  const { t } = useTranslation();
  const [activeTab, setActiveTab] = useState("editor");

  return (
    <div className="space-y-5">
      <div className="flex items-center justify-between">
        <h1 className="text-sm font-mono font-semibold text-terminal-text-bright uppercase tracking-wider">
          {t("config.title")}
        </h1>
        <Tabs
          tabs={CONFIG_TABS}
          activeKey={activeTab}
          onChange={setActiveTab}
        />
      </div>

      {activeTab === "editor" && <EditorTab />}
      {activeTab === "strategy" && <StrategyTab />}
      {activeTab === "regime" && <RegimeTab />}
    </div>
  );
}
