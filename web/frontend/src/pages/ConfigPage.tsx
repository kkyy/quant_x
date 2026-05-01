import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { get, put } from "../api/client";

interface ConfigResponse {
  content: string;
  exists: boolean;
}

interface SaveResponse {
  saved: boolean;
}

type Tab = "editor" | "strategy_candidates" | "regime_rules";
type ConfigName = "base" | "model" | "notify";

interface RegimeRule {
  key: string;
  label: string;
  topk: number;
  n_drop: number;
  hold_thresh: number;
}

export function ConfigPage() {
  const { t } = useTranslation();
  const [tab, setTab] = useState<Tab>("editor");

  // Editor state
  const [selectedConfig, setSelectedConfig] = useState<ConfigName>("base");
  const [content, setContent] = useState("");
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [editorError, setEditorError] = useState<string | null>(null);
  const [saveMessage, setSaveMessage] = useState<string | null>(null);

  // Strategy candidates state
  const [strategyContent, setStrategyContent] = useState("");
  const [strategyLoading, setStrategyLoading] = useState(false);
  const [strategyError, setStrategyError] = useState<string | null>(null);

  // Regime rules state
  const [regimeRules, setRegimeRules] = useState<RegimeRule[]>([]);
  const [regimeEnabled, setRegimeEnabled] = useState(false);
  const [regimeLoading, setRegimeLoading] = useState(false);
  const [regimeError, setRegimeError] = useState<string | null>(null);

  const configNames: { key: ConfigName; label: string }[] = [
    { key: "base", label: "base.yaml" },
    { key: "model", label: "model.yaml" },
    { key: "notify", label: "notify.yaml" },
  ];

  // Fetch config content for editor tab
  const fetchConfig = async (name: ConfigName) => {
    setLoading(true);
    setEditorError(null);
    setSaveMessage(null);
    try {
      const res = await get<ConfigResponse>(`/config/${name}`);
      setContent(res.content);
    } catch (err: any) {
      setEditorError(err.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (tab === "editor") {
      fetchConfig(selectedConfig);
    }
  }, [selectedConfig, tab]);

  // Fetch strategy candidates
  useEffect(() => {
    if (tab === "strategy_candidates") {
      setStrategyLoading(true);
      setStrategyError(null);
      get<ConfigResponse>("/config/strategy_candidates")
        .then((res) => setStrategyContent(res.content || "# No strategy_candidates.yaml found"))
        .catch((err) => setStrategyError(err.message))
        .finally(() => setStrategyLoading(false));
    }
  }, [tab]);

  // Fetch regime rules
  useEffect(() => {
    if (tab === "regime_rules") {
      setRegimeLoading(true);
      setRegimeError(null);
      get<ConfigResponse>("/config/base")
        .then((res) => {
          // Parse regime rules from YAML content
          try {
            const regimeSection = res.content;
            const enabled = regimeSection.includes("enabled: true");
            setRegimeEnabled(enabled);

            // Simple regex-based parsing for regime rules
            const rules: RegimeRule[] = [];
            const ruleRegex = /(\d+):\s*#\s*(\w+)\s+topk:\s*(\d+)\s+n_drop:\s*(\d+)\s+hold_thresh:\s*(\d+)/g;
            let match;
            while ((match = ruleRegex.exec(regimeSection)) !== null) {
              rules.push({
                key: match[1],
                label: match[2],
                topk: parseInt(match[3], 10),
                n_drop: parseInt(match[4], 10),
                hold_thresh: parseInt(match[5], 10),
              });
            }

            // Fallback: try alternate parsing if the above didn't work
            if (rules.length === 0) {
              const lines = regimeSection.split("\n");
              let inRegime = false;
              let currentKey = "";
              let currentLabel = "";
              let currentRule: Partial<RegimeRule> | null = null;

              for (const line of lines) {
                if (line.includes("regime_switch")) {
                  inRegime = true;
                  continue;
                }
                if (inRegime && !line.startsWith("      ") && !line.startsWith("    ") && line.trim() === "") {
                  continue;
                }
                if (inRegime && line.match(/^\s{6}\d/)) {
                  if (currentRule && currentRule.topk !== undefined) {
                    rules.push(currentRule as RegimeRule);
                  }
                  const keyMatch = line.match(/^\s*(\d+):\s*#\s*(\w+)/);
                  if (keyMatch) {
                    currentKey = keyMatch[1];
                    currentLabel = keyMatch[2];
                  } else {
                    const bareMatch = line.match(/^\s*(\d+):/);
                    if (bareMatch) {
                      currentKey = bareMatch[1];
                      currentLabel = `Rule ${bareMatch[1]}`;
                    }
                  }
                  currentRule = { key: currentKey, label: currentLabel };
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
            }

            setRegimeRules(rules);
          } catch {
            setRegimeError("Failed to parse regime rules from config");
          }
        })
        .catch((err) => setRegimeError(err.message))
        .finally(() => setRegimeLoading(false));
    }
  }, [tab]);

  const handleSave = async () => {
    setSaving(true);
    setSaveMessage(null);
    setEditorError(null);
    try {
      await put<SaveResponse>(`/config/${selectedConfig}`, { content });
      setSaveMessage(t("config.savedOk"));
    } catch (err: any) {
      setEditorError(err.message);
    } finally {
      setSaving(false);
    }
  };

  const handleReload = () => {
    fetchConfig(selectedConfig);
  };

  const tabs: { key: Tab; label: string }[] = [
    { key: "editor", label: t("config.editorTab") },
    { key: "strategy_candidates", label: t("config.strategyTab") },
    { key: "regime_rules", label: t("config.regimeTab") },
  ];

  const regimeLabels: Record<string, string> = {
    "0": t('config.regimeLabelCalm_bull'),
    "1": t('config.regimeLabelCalm_bear'),
    "2": t('config.regimeLabelVolatile_bull'),
    "3": t('config.regimeLabelVolatile_bear'),
  };

  return (
    <div className="space-y-4 max-w-5xl">
      <h2 className="text-2xl font-bold text-zinc-900">{t("config.title")}</h2>

      {/* Tab bar */}
      <div className="flex gap-1 border-b border-zinc-200">
        {tabs.map((tb) => (
          <button
            key={tb.key}
            onClick={() => { setTab(tb.key); setEditorError(null); setSaveMessage(null); }}
            className={`px-4 py-2 text-sm font-medium transition-colors ${
              tab === tb.key
                ? "border-b-2 border-amber-500 text-amber-600"
                : "border-b-2 border-transparent text-zinc-500 hover:text-zinc-700"
            }`}
          >
            {tb.label}
          </button>
        ))}
      </div>

      {/* Editor tab */}
      {tab === "editor" && (
        <div className="space-y-3">
          <div className="flex gap-2">
            {configNames.map((c) => (
              <button
                key={c.key}
                onClick={() => setSelectedConfig(c.key)}
                className={`px-3 py-1.5 text-sm rounded border ${
                  selectedConfig === c.key
                    ? "bg-amber-500 text-zinc-900 border-amber-500"
                    : "bg-white text-zinc-700 border-zinc-300 hover:bg-zinc-50"
                }`}
              >
                {c.label}
              </button>
            ))}
          </div>

          {editorError && (
            <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-2 rounded text-sm">
              {editorError}
            </div>
          )}

          {saveMessage && (
            <div className="bg-green-50 border border-green-200 text-green-700 px-4 py-2 rounded text-sm">
              {saveMessage}
            </div>
          )}

          {loading ? (
            <p className="text-zinc-500 text-sm">{t("common.loading")}</p>
          ) : (
            <>
              <textarea
                value={content}
                onChange={(e) => setContent(e.target.value)}
                className="w-full h-96 font-mono text-sm border border-zinc-300 rounded-lg p-3 focus:outline-none focus:ring-2 focus:ring-amber-400"
                spellCheck={false}
              />
              <div className="flex gap-2">
                <button
                  onClick={handleSave}
                  disabled={saving}
                  className="px-4 py-2 bg-amber-500 text-zinc-900 font-medium text-sm rounded hover:bg-amber-600 disabled:opacity-50"
                >
                  {saving ? t("common.saving") : t("common.save")}
                </button>
                <button
                  onClick={handleReload}
                  className="px-4 py-2 bg-white text-zinc-700 text-sm rounded border border-zinc-300 hover:bg-zinc-50"
                >
                  {t("common.reload")}
                </button>
              </div>
            </>
          )}
        </div>
      )}

      {/* Strategy Candidates tab */}
      {tab === "strategy_candidates" && (
        <div className="space-y-3">
          {strategyError && (
            <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-2 rounded text-sm">
              {strategyError}
            </div>
          )}
          {strategyLoading ? (
            <p className="text-zinc-500 text-sm">{t("common.loading")}</p>
          ) : (
            <pre className="w-full bg-zinc-50 border border-zinc-200 rounded-lg p-4 text-sm font-mono overflow-auto max-h-[600px] whitespace-pre-wrap">
              {strategyContent}
            </pre>
          )}
        </div>
      )}

      {/* Regime Rules tab */}
      {tab === "regime_rules" && (
        <div className="space-y-3">
          {regimeError && (
            <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-2 rounded text-sm">
              {regimeError}
            </div>
          )}
          {regimeLoading ? (
            <p className="text-zinc-500 text-sm">{t("common.loading")}</p>
          ) : (
            <>
              <div className="border border-zinc-200 rounded-lg p-4">
                <div className="flex items-center gap-2 mb-2">
                  <span className="text-sm font-medium">{t("config.regimeSwitching")}</span>
                  {regimeEnabled ? (
                    <span className="inline-block px-2 py-0.5 text-xs bg-green-100 text-green-800 rounded">
                      {t("common.enabled")}
                    </span>
                  ) : (
                    <span className="inline-block px-2 py-0.5 text-xs bg-zinc-100 text-zinc-500 rounded">
                      {t("common.disabled")}
                    </span>
                  )}
                </div>
              </div>

              {regimeRules.length > 0 ? (
                <div className="border border-zinc-200 rounded-lg overflow-hidden">
                  <table className="w-full text-sm">
                    <thead className="bg-zinc-50">
                      <tr>
                        <th className="text-left px-4 py-2">{t("config.key")}</th>
                        <th className="text-left px-4 py-2">{t("config.regime")}</th>
                        <th className="text-right px-4 py-2">{t("config.topk")}</th>
                        <th className="text-right px-4 py-2">{t("config.nDrop")}</th>
                        <th className="text-right px-4 py-2">{t("config.holdThresh")}</th>
                      </tr>
                    </thead>
                    <tbody>
                      {regimeRules.map((r) => (
                        <tr key={r.key} className="border-t border-zinc-200 hover:bg-zinc-50">
                          <td className="px-4 py-2 font-mono text-xs">{r.key}</td>
                          <td className="px-4 py-2">
                            {regimeLabels[r.key] || r.label}
                          </td>
                          <td className="text-right px-4 py-2">{r.topk}</td>
                          <td className="text-right px-4 py-2">{r.n_drop}</td>
                          <td className="text-right px-4 py-2">{r.hold_thresh}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="border border-zinc-200 rounded-lg p-4 text-center text-zinc-400 text-sm">
                  {t("config.noRegimeRules")}
                </div>
              )}

              <p className="text-xs text-zinc-400">
                {t("config.regimeNote")}
              </p>
            </>
          )}
        </div>
      )}
    </div>
  );
}
