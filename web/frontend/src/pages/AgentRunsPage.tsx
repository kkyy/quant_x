import { useCallback, useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Badge } from "../components/ui/Badge";
import { Card } from "../components/ui/Card";
import { Skeleton, SkeletonTable } from "../components/ui/Skeleton";
import { Tabs } from "../components/ui/Tabs";
import { get, post } from "../api/client";
import type { AgentRunCreateRequest, AgentRunDetail, AgentRunSummary } from "../api/types";

type AgentTabKey = "plan" | "commands" | "summary" | "feedback" | "approval" | "raw";

const DETAIL_TABS: { key: AgentTabKey; labelKey: string }[] = [
  { key: "plan", labelKey: "agentRuns.plan" },
  { key: "commands", labelKey: "agentRuns.commands" },
  { key: "summary", labelKey: "agentRuns.summary" },
  { key: "feedback", labelKey: "agentRuns.feedback" },
  { key: "approval", labelKey: "agentRuns.approval" },
  { key: "raw", labelKey: "agentRuns.raw" },
];

const ARTIFACT_KEYS = [
  { key: "plan", flag: "has_plan", fields: ["plan.md", "plan_markdown", "plan", "plan_path"] },
  { key: "cmd", flag: "has_commands", fields: ["commands.md", "commands.json", "commands_markdown", "commands", "commands_path"] },
  { key: "sum", flag: "has_execution_summary", fields: ["execution_summary.md", "execution_summary", "summary", "execution_summary_path"] },
  { key: "fb", flag: "has_feedback", fields: ["feedback.md", "feedback.json", "feedback", "feedback_path"] },
  { key: "tpl", flag: "has_approval_template", fields: ["approval_template.yaml", "approval_template", "approval", "approval_template_path"] },
];

function normalizeRunList(payload: AgentRunSummary[] | { runs?: AgentRunSummary[] }) {
  return Array.isArray(payload) ? payload : payload.runs ?? [];
}

function coerceText(value: unknown) {
  if (typeof value === "string") return value;
  if (value == null) return "";
  return JSON.stringify(value, null, 2);
}

function firstText(run: AgentRunDetail | null, fields: string[]) {
  if (!run) return "";
  for (const field of fields) {
    const value = run[field];
    if (typeof value === "string" && value.trim()) return value;
  }
  if (run.artifacts && typeof run.artifacts === "object" && !Array.isArray(run.artifacts)) {
    const artifacts = run.artifacts as Record<string, unknown>;
    for (const field of fields) {
      const value = artifacts[field];
      if (typeof value === "string" && value.trim()) return value;
    }
  }
  return "";
}

function formatJson(value: unknown) {
  if (value == null) return "";
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function artifactValue(run: AgentRunDetail | null, key: string) {
  if (!run?.artifacts || Array.isArray(run.artifacts) || typeof run.artifacts !== "object") return undefined;
  return (run.artifacts as Record<string, unknown>)[key];
}

function statusVariant(status?: string) {
  switch ((status ?? "").toLowerCase()) {
    case "done":
    case "completed":
    case "success":
      return "success" as const;
    case "running":
    case "started":
    case "pending":
      return "info" as const;
    case "failed":
    case "error":
      return "error" as const;
    case "cancelled":
    case "needs_approval":
    case "waiting":
      return "warning" as const;
    default:
      return "neutral" as const;
  }
}

function hasArtifact(run: AgentRunSummary | AgentRunDetail | null, fields: string[]) {
  if (!run) return false;
  for (const artifact of ARTIFACT_KEYS) {
    if (fields === artifact.fields && Boolean(run[artifact.flag])) return true;
  }
  if (fields.some((field) => Boolean(run[field]))) return true;
  const artifacts = run.artifacts;
  if (Array.isArray(artifacts)) {
    return fields.some((field) => artifacts.some((item) => item.includes(field.replace("_path", ""))));
  }
  if (artifacts && typeof artifacts === "object") {
    return fields.some((field) => Boolean((artifacts as Record<string, unknown>)[field]));
  }
  return false;
}

function displayTime(run: AgentRunSummary | AgentRunDetail | null, kind: "created" | "updated") {
  if (!run) return "-";
  if (kind === "created") return run.created_at ?? run.generated_at ?? "-";
  return run.updated_at ?? run.modified_at ?? "-";
}

function FieldLabel({ children }: { children: React.ReactNode }) {
  return (
    <p className="mb-1 text-[10px] font-mono uppercase tracking-wider text-terminal-text-dim">
      {children}
    </p>
  );
}

function TextBlock({ value, empty }: { value: string; empty: string }) {
  return (
    <pre className="min-h-[360px] max-h-[620px] overflow-auto whitespace-pre-wrap rounded-sm border border-terminal-border bg-terminal-bg p-4 font-mono text-xs leading-relaxed text-terminal-text">
      {value.trim() ? value : empty}
    </pre>
  );
}

function ToggleField({
  checked,
  label,
  onChange,
}: {
  checked: boolean;
  label: string;
  onChange: (checked: boolean) => void;
}) {
  return (
    <label className="flex items-center gap-2 font-mono text-xs text-terminal-text-dim">
      <input
        type="checkbox"
        checked={checked}
        onChange={(event) => onChange(event.target.checked)}
        className="h-3.5 w-3.5 accent-terminal-green"
      />
      {label}
    </label>
  );
}

export function AgentRunsPage() {
  const { t } = useTranslation();
  const [runs, setRuns] = useState<AgentRunSummary[]>([]);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [detail, setDetail] = useState<AgentRunDetail | null>(null);
  const [activeTab, setActiveTab] = useState<AgentTabKey>("plan");
  const [loadingRuns, setLoadingRuns] = useState(true);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [objective, setObjective] = useState("");
  const [runId, setRunId] = useState("");
  const [useLlm, setUseLlm] = useState(false);
  const [proposeActions, setProposeActions] = useState(true);
  const [writeApprovalTemplate, setWriteApprovalTemplate] = useState(true);
  const [appendMemory, setAppendMemory] = useState(false);
  const [creating, setCreating] = useState(false);
  const [regeneratingTemplate, setRegeneratingTemplate] = useState(false);

  const fetchRuns = useCallback((nextSelected?: string) => {
    setLoadingRuns(true);
    setError(null);
    get<AgentRunSummary[] | { runs?: AgentRunSummary[] }>("/agents/runs")
      .then((payload) => {
        const nextRuns = normalizeRunList(payload);
        setRuns(nextRuns);
        setSelectedRunId((current) => nextSelected ?? current ?? nextRuns[0]?.run_id ?? null);
      })
      .catch((err: Error) => {
        setRuns([]);
        setError(err.message);
      })
      .finally(() => setLoadingRuns(false));
  }, []);

  useEffect(() => {
    fetchRuns();
  }, [fetchRuns]);

  useEffect(() => {
    if (!selectedRunId) {
      setDetail(null);
      return;
    }
    setLoadingDetail(true);
    get<AgentRunDetail>(`/agents/runs/${encodeURIComponent(selectedRunId)}`)
      .then(setDetail)
      .catch((err: Error) => {
        setDetail(null);
        setError(err.message);
      })
      .finally(() => setLoadingDetail(false));
  }, [selectedRunId]);

  const selectedSummary = useMemo(
    () => runs.find((run) => run.run_id === selectedRunId) ?? null,
    [runs, selectedRunId]
  );

  const detailTabs = useMemo(
    () => DETAIL_TABS.map((tab) => ({ key: tab.key, label: t(tab.labelKey) })),
    [t]
  );

  const currentText = useMemo(() => {
    if (activeTab === "plan") return firstText(detail, ["plan.md", "plan_markdown", "plan"]);
    if (activeTab === "commands") return firstText(detail, ["commands.md", "commands_markdown", "commands"]);
    if (activeTab === "summary") return firstText(detail, ["execution_summary.md", "execution_summary", "summary"]);
    if (activeTab === "feedback") return firstText(detail, ["feedback.md", "feedback"]) || coerceText(artifactValue(detail, "feedback.json"));
    if (activeTab === "approval") return firstText(detail, ["approval_template.yaml", "approval_template", "approval"]);
    return formatJson(detail?.raw ?? detail);
  }, [activeTab, detail]);

  const handleCreate = () => {
    if (!objective.trim()) return;
    const payload: AgentRunCreateRequest = {
      objective: objective.trim(),
      use_llm: useLlm,
      propose_actions: proposeActions,
      write_approval_template: writeApprovalTemplate,
      append_memory: appendMemory,
    };
    if (runId.trim()) payload.run_id = runId.trim();

    setCreating(true);
    setError(null);
    post<AgentRunDetail | AgentRunSummary>("/agents/runs", payload)
      .then((created) => {
        const createdId = created.run_id || payload.run_id;
        setObjective("");
        setRunId("");
        if (createdId) {
          setSelectedRunId(createdId);
          setDetail(created as AgentRunDetail);
          fetchRuns(createdId);
        } else {
          fetchRuns();
        }
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setCreating(false));
  };

  const handleRegenerateTemplate = () => {
    if (!selectedRunId) return;
    setRegeneratingTemplate(true);
    setError(null);
    post<AgentRunSummary>(`/agents/runs/${encodeURIComponent(selectedRunId)}/approval-template`)
      .then(() => {
        fetchRuns(selectedRunId);
        return get<AgentRunDetail>(`/agents/runs/${encodeURIComponent(selectedRunId)}`);
      })
      .then(setDetail)
      .catch((err: Error) => setError(err.message))
      .finally(() => setRegeneratingTemplate(false));
  };

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-xl font-semibold text-terminal-text-bright">{t("agentRuns.title")}</h1>
        <p className="mt-1 font-mono text-xs text-terminal-text-dim">{t("agentRuns.subtitle")}</p>
      </div>

      {error && (
        <div className="rounded-sm border border-terminal-red bg-terminal-red-glow px-3 py-2 font-mono text-xs text-terminal-red">
          {error}
        </div>
      )}

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-[380px_minmax(0,1fr)]">
        <div className="space-y-4">
          <Card title={t("agentRuns.createRun")} accent="green">
            <div className="space-y-3">
              <div>
                <FieldLabel>{t("agentRuns.objective")}</FieldLabel>
                <textarea
                  value={objective}
                  onChange={(event) => setObjective(event.target.value)}
                  placeholder={t("agentRuns.objectivePlaceholder")}
                  rows={4}
                  className="w-full rounded-sm border border-terminal-border bg-terminal-bg px-3 py-2 font-mono text-xs text-terminal-text outline-none focus:border-terminal-green"
                />
              </div>
              <div>
                <FieldLabel>{t("agentRuns.runIdOptional")}</FieldLabel>
                <input
                  value={runId}
                  onChange={(event) => setRunId(event.target.value)}
                  placeholder={t("agentRuns.runIdPlaceholder")}
                  className="w-full rounded-sm border border-terminal-border bg-terminal-bg px-3 py-2 font-mono text-xs text-terminal-text outline-none focus:border-terminal-green"
                />
              </div>
              <div className="grid grid-cols-2 gap-2">
                <ToggleField checked={useLlm} label={t("agentRuns.useLlm")} onChange={setUseLlm} />
                <ToggleField checked={proposeActions} label={t("agentRuns.proposeActions")} onChange={setProposeActions} />
                <ToggleField checked={writeApprovalTemplate} label={t("agentRuns.approvalTemplate")} onChange={setWriteApprovalTemplate} />
                <ToggleField checked={appendMemory} label={t("agentRuns.appendMemory")} onChange={setAppendMemory} />
              </div>
              <button
                onClick={handleCreate}
                disabled={creating || !objective.trim()}
                className="w-full rounded-sm border border-terminal-green px-3 py-2 font-mono text-xs text-terminal-green transition-colors hover:bg-terminal-green-glow disabled:cursor-not-allowed disabled:opacity-40"
              >
                {creating ? t("agentRuns.creating") : t("agentRuns.create")}
              </button>
            </div>
          </Card>

          <Card
            title={t("agentRuns.runs")}
            actions={
              <button
                onClick={() => fetchRuns()}
                className="rounded-sm border border-terminal-border px-3 py-1.5 font-mono text-xs text-terminal-text-dim transition-colors hover:border-terminal-text-dim hover:text-terminal-text"
              >
                {t("common.refresh")}
              </button>
            }
          >
            {loadingRuns ? (
              <SkeletonTable rows={6} />
            ) : runs.length === 0 ? (
              <p className="font-mono text-xs text-terminal-text-dim">{t("agentRuns.noRuns")}</p>
            ) : (
              <div className="max-h-[560px] space-y-2 overflow-auto pr-1">
                {runs.map((run) => (
                  <button
                    key={run.run_id}
                    onClick={() => setSelectedRunId(run.run_id)}
                    className={`w-full rounded-sm border px-3 py-2 text-left transition-colors ${
                      selectedRunId === run.run_id
                        ? "border-terminal-green bg-terminal-green-glow"
                        : "border-terminal-border bg-terminal-bg hover:border-terminal-text-dim"
                    }`}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <span className="truncate font-mono text-xs text-terminal-green">{run.run_id}</span>
                      <Badge variant={statusVariant(run.status)}>{run.status ?? "unknown"}</Badge>
                    </div>
                    <p className="mt-1 line-clamp-2 text-xs text-terminal-text-dim">
                      {run.objective || t("agentRuns.noObjective")}
                    </p>
                    <div className="mt-2 flex flex-wrap gap-1.5">
                      {ARTIFACT_KEYS.map((artifact) => (
                        <Badge
                          key={artifact.key}
                          variant={hasArtifact(run, artifact.fields) ? "success" : "neutral"}
                          className="px-1.5"
                        >
                          {artifact.key}
                        </Badge>
                      ))}
                    </div>
                  </button>
                ))}
              </div>
            )}
          </Card>
        </div>

        <div className="min-w-0 space-y-4">
          <Card
            title={t("agentRuns.detail")}
            accent="cyan"
            actions={
              selectedRunId ? (
                <button
                  onClick={handleRegenerateTemplate}
                  disabled={regeneratingTemplate}
                  className="rounded-sm border border-terminal-border px-3 py-1.5 font-mono text-xs text-terminal-text-dim transition-colors hover:border-terminal-cyan hover:text-terminal-cyan disabled:cursor-not-allowed disabled:opacity-40"
                >
                  {regeneratingTemplate ? t("agentRuns.regenerating") : t("agentRuns.regenerateApproval")}
                </button>
              ) : null
            }
          >
            {!selectedRunId ? (
              <p className="font-mono text-xs text-terminal-text-dim">{t("agentRuns.selectRun")}</p>
            ) : (
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-2 lg:grid-cols-4">
                  <div className="rounded-sm border border-terminal-border bg-terminal-bg px-3 py-2">
                    <FieldLabel>{t("agentRuns.runId")}</FieldLabel>
                    <p className="truncate font-mono text-xs text-terminal-green">{selectedRunId}</p>
                  </div>
                  <div className="rounded-sm border border-terminal-border bg-terminal-bg px-3 py-2">
                    <FieldLabel>{t("common.status")}</FieldLabel>
                    <Badge variant={statusVariant(detail?.status ?? selectedSummary?.status)}>
                      {detail?.status ?? selectedSummary?.status ?? "unknown"}
                    </Badge>
                  </div>
                  <div className="rounded-sm border border-terminal-border bg-terminal-bg px-3 py-2">
                    <FieldLabel>{t("agentRuns.created")}</FieldLabel>
                    <p className="truncate font-mono text-xs text-terminal-text">
                      {displayTime(detail ?? selectedSummary, "created")}
                    </p>
                  </div>
                  <div className="rounded-sm border border-terminal-border bg-terminal-bg px-3 py-2">
                    <FieldLabel>{t("agentRuns.updated")}</FieldLabel>
                    <p className="truncate font-mono text-xs text-terminal-text">
                      {displayTime(detail ?? selectedSummary, "updated")}
                    </p>
                  </div>
                </div>

                <div className="rounded-sm border border-terminal-border bg-terminal-bg px-3 py-2">
                  <FieldLabel>{t("agentRuns.objective")}</FieldLabel>
                  <p className="whitespace-pre-wrap font-mono text-xs text-terminal-text">
                    {detail?.objective ?? selectedSummary?.objective ?? t("agentRuns.noObjective")}
                  </p>
                </div>

                <div className="flex flex-wrap gap-1.5">
                  {ARTIFACT_KEYS.map((artifact) => (
                    <Badge
                      key={artifact.key}
                      variant={hasArtifact(detail ?? selectedSummary, artifact.fields) ? "success" : "neutral"}
                    >
                      {artifact.key}
                    </Badge>
                  ))}
                </div>
              </div>
            )}
          </Card>

          <Card>
            <Tabs tabs={detailTabs} activeKey={activeTab} onChange={(key) => setActiveTab(key as AgentTabKey)} />
            <div className="pt-4">
              {loadingDetail ? (
                <Skeleton className="h-[420px] w-full" />
              ) : (
                <TextBlock value={currentText} empty={t("agentRuns.emptySection")} />
              )}
            </div>
          </Card>
        </div>
      </div>
    </div>
  );
}
