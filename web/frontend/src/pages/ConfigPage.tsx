import { useEffect, useState } from "react";
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
      setSaveMessage("Saved successfully");
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
    { key: "editor", label: "Editor" },
    { key: "strategy_candidates", label: "Strategy Candidates" },
    { key: "regime_rules", label: "Regime Rules" },
  ];

  const regimeLabels: Record<string, string> = {
    "0": "Calm Bull",
    "1": "Calm Bear",
    "2": "Volatile Bull",
    "3": "Volatile Bear",
  };

  return (
    <div className="space-y-4 max-w-5xl">
      <h2 className="text-2xl font-bold">Config</h2>

      {/* Tab bar */}
      <div className="flex gap-1 border-b">
        {tabs.map((t) => (
          <button
            key={t.key}
            onClick={() => { setTab(t.key); setEditorError(null); setSaveMessage(null); }}
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
                    ? "bg-blue-600 text-white border-blue-600"
                    : "bg-white text-gray-700 border-gray-300 hover:bg-gray-50"
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
            <p className="text-gray-500 text-sm">Loading...</p>
          ) : (
            <>
              <textarea
                value={content}
                onChange={(e) => setContent(e.target.value)}
                className="w-full h-96 font-mono text-sm border rounded-lg p-3 focus:outline-none focus:ring-2 focus:ring-blue-500"
                spellCheck={false}
              />
              <div className="flex gap-2">
                <button
                  onClick={handleSave}
                  disabled={saving}
                  className="px-4 py-2 bg-blue-600 text-white text-sm rounded hover:bg-blue-700 disabled:opacity-50"
                >
                  {saving ? "Saving..." : "Save"}
                </button>
                <button
                  onClick={handleReload}
                  className="px-4 py-2 bg-white text-gray-700 text-sm rounded border border-gray-300 hover:bg-gray-50"
                >
                  Reload
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
            <p className="text-gray-500 text-sm">Loading...</p>
          ) : (
            <pre className="w-full bg-gray-50 border rounded-lg p-4 text-sm font-mono overflow-auto max-h-[600px] whitespace-pre-wrap">
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
            <p className="text-gray-500 text-sm">Loading...</p>
          ) : (
            <>
              <div className="border rounded-lg p-4">
                <div className="flex items-center gap-2 mb-2">
                  <span className="text-sm font-medium">Regime Switching:</span>
                  {regimeEnabled ? (
                    <span className="inline-block px-2 py-0.5 text-xs bg-green-100 text-green-800 rounded">
                      enabled
                    </span>
                  ) : (
                    <span className="inline-block px-2 py-0.5 text-xs bg-gray-100 text-gray-500 rounded">
                      disabled
                    </span>
                  )}
                </div>
              </div>

              {regimeRules.length > 0 ? (
                <div className="border rounded-lg overflow-hidden">
                  <table className="w-full text-sm">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="text-left px-4 py-2">Key</th>
                        <th className="text-left px-4 py-2">Regime</th>
                        <th className="text-right px-4 py-2">Top K</th>
                        <th className="text-right px-4 py-2">N Drop</th>
                        <th className="text-right px-4 py-2">Hold Thresh</th>
                      </tr>
                    </thead>
                    <tbody>
                      {regimeRules.map((r) => (
                        <tr key={r.key} className="border-t hover:bg-gray-50">
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
                <div className="border rounded-lg p-4 text-center text-gray-400 text-sm">
                  No regime rules found in config
                </div>
              )}

              <p className="text-xs text-gray-400">
                Regime rules are read-only. Edit base.yaml to modify.
              </p>
            </>
          )}
        </div>
      )}
    </div>
  );
}
