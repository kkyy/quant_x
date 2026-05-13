# Agent Strategy Iteration Implementation Plan

Date: 2026-05-13

## Goal

Integrate the agent strategy iteration design into `quant_ex` in controlled phases, starting with an offline and auditable planning layer that does not bypass the existing validation stack.

## Phase 1

Objective:
- Establish the research-planning substrate.

Deliverables:
- role and plan schemas
- prompt catalog in-repo
- local context pack builder
- append-only agent memory log
- optional OpenAI-compatible client, disabled by default
- `run_agent_strategy_iteration.py`
- per-run bundle under `docs/strategy_log/agent_runs/`
- focused tests

Exit criteria:
- offline CLI run writes `run.json`, `plan.md`, `context.json`, and `prompts.json`
- no network access required in default mode
- no secrets written to disk

## Phase 2

Objective:
- Make prompts and role execution stronger without coupling to expensive runs.

Deliverables:
- richer context pack slices for result CSVs and config diffs
- role-specific JSON schema validation
- better bull/bear/risk carry-over between roles
- prompt regression fixtures

Exit criteria:
- stable prompt outputs across repeated offline runs
- structured LLM mode degrades cleanly when env vars are absent

## Phase 3

Objective:
- Add evaluation feedback and memory reflection.

Deliverables:
- parse same-model backtest CSVs and WFV summaries into feedback objects
- append delayed reflections to agent memory
- seed the next run from prior validated outcomes

Exit criteria:
- a completed planning run can be updated with validated outcomes
- memory remains separate from durable strategy logs

## Phase 4

Objective:
- Add semi-automated execution adapters behind approval gates.

Deliverables:
- command generation and optional execution wrappers
- dry-run-only support by default
- approval tagging for WFV, qlib updates, data fetches, and notifications

Exit criteria:
- cheap validations can be executed safely from the agent layer
- expensive actions require explicit user intent

## Phase 5

Objective:
- Integrate with the dashboard and long-term research workflow.

Deliverables:
- API endpoints to create and browse agent runs
- Web Dashboard surfaces for prompts, context, decisions, and memory
- links from agent runs to strategy/system iteration logs

Exit criteria:
- planning runs are visible and traceable from the existing research UI

## Current Execution Choice

Phase 1 through Phase 5 have been implemented. The current agent layer can build offline plans, parse feedback CSVs, produce gated command proposals with `commands.json` / `commands.md`, write `approval_template.yaml`, execute only explicitly approved commands whose `command_id` and `command_sha256` match the current plan, summarize execution in `execution_summary.md`, detect backtest/WFV CSV candidates for Phase 3 feedback handoff, and expose run artifacts in the Web Dashboard. `--execute-safe` remains limited to local low-risk checks; training, backtest, WFV, data fetch/update, notifications, and trading-like commands require an approval file entry before execution. The dashboard currently supports browse/create/regenerate approval template only, not command execution.

## LLM Tier Configuration

Agent role model assignment is configured in `config/agent_strategy_iteration.yaml`.

- Each role uses `model_tier`, currently `quick` or `deep`.
- Tier definitions live under `llm.tiers`.
- The local real config can directly store `api_key` and `base_url`; this file is gitignored. The committed example keeps `api_key` empty.
- `api_key_env` / `base_url_env` are still accepted as optional fallback fields for environment-driven setups.
- Tier-specific environment variables such as `QUANT_EX_AGENT_DEEP_MODEL` and `QUANT_EX_AGENT_QUICK_MODEL` can override the model names without changing the YAML.

Current default mapping:

```yaml
llm:
  api_key: ""
  base_url: "https://your-openai-compatible-endpoint.example"
  tiers:
    quick:
      model: "gpt-5.4-mini"
      reasoning_effort: "low"
      temperature: 0.1
      max_tokens: 1200
    deep:
      model: "gpt-5.5"
      reasoning_effort: "high"
      temperature: 0.2
      max_tokens: 2400
```
