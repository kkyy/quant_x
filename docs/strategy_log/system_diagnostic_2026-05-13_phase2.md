# System Diagnostic: 2026-05-13 Phase 2

## Layer Scores
| Layer | Score | Weakest Link | Highest Leverage Fix |
|---|---:|---|---|
| Data | 3 | Agent context previously exposed paths but not enough artifact summaries for evidence-aware prompts. | Add local CSV summaries and config summaries to the context pack. |
| Factors | 3 | Factor-agent proposals still need stronger evidence discipline before any implementation. | Carry prior role reports and schema warnings through every role output. |
| Model | 4 | Model changes are still not the right near-term bottleneck. | Keep model analyst as a review role and enrich its context with current controls. |
| Backtest | 4 | Comparability guardrails existed in prose but were not yet encoded in role outputs. | Add warning-only role schema validation and stronger backtest context. |
| Execution | 4 | Execution approvals are protected, but downstream roles need memory/context carry-over. | Include agent memory tail and upstream role order in run bundles. |
| Web | 3 | Agent run browsing remains a future dashboard task. | Defer Web work until Phase 3/4 bundle shape stabilizes. |

## Key Findings
1. Phase 1 produced an offline planner, but role outputs were still too loose for reliable LLM use.
2. Phase 2 strengthens the planner by adding artifact summaries, config summaries, memory tail, upstream role carry-over, and warning-only role schema validation.
3. No training, WFV, qlib update, data fetch, notification, or live-like execution was run in this phase.

## Change Plan
| Change | Files | Validation |
|---|---|---|
| Add richer local context slices. | `agent/strategy_iteration/context.py`, `agent/strategy_iteration/schemas.py` | Context import smoke and targeted tests. |
| Add role schema validation and carry-over metadata. | `agent/strategy_iteration/validation.py`, `agent/strategy_iteration/roles.py` | `test_role_runner_attaches_schema_and_carryover`. |
| Persist Phase 2 run bundle. | `docs/strategy_log/agent_runs/phase2_context_schema_carryover/` | CLI run with `--no-llm`. |
| Record system iteration. | `docs/strategy_log/system_iteration_log.csv` | CSV parse check. |
