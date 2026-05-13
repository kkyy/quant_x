# System Diagnostic: 2026-05-13 Phase 4-C

## Layer Scores
| Layer | Score | Weakest Link | Highest Leverage Fix |
|---|---:|---|---|
| Data | 3 | Data refresh commands need explicit authorization because they may use network and mutate cache. | Require approval-file entries with command hashes before execution. |
| Factors | 3 | Factor experiments need a repeatable handoff from proposal to validation. | Preserve proposed commands and approvals beside each agent run bundle. |
| Model | 4 | Training remains too expensive for implicit execution. | Keep training protected unless a concrete command hash is approved. |
| Backtest | 4 | Backtest/WFV command drafts are useful but need stale-approval protection. | Match both `command_id` and `command_sha256` before execution. |
| Execution | 5 | Phase 4-A/B proposed commands but lacked a durable approval mechanism. | Add approval templates and `--execute-approved --approval-file` execution. |
| Web | 3 | Approval review is still file-based. | Defer dashboard approval review to Phase 5. |

## Key Findings
1. Phase 4-C adds file-based approvals with command hashes, so a user approves a specific command string rather than a mutable command id.
2. The CLI can now write `approval_template.yaml` beside `commands.json` and `commands.md`; all entries default to `approved: false`.
3. `--execute-approved` executes only entries whose `run_id`, `command_id`, and `command_sha256` match. Unapproved, denied, or stale-hash commands are recorded as skipped.

## Change Plan
| Change | Files | Validation |
|---|---|---|
| Add command hash and approval schema. | `agent/strategy_iteration/schemas.py` | Tests validate hash serialization and mismatch behavior. |
| Add approval loader, template writer, and approved-command runner. | `agent/strategy_iteration/execution.py` | Tests cover approved execution and stale-hash skip. |
| Extend CLI approval flow. | `run_agent_strategy_iteration.py` | CLI test covers `--execute-approved --approval-file`. |
| Record Phase 4-C sample artifacts. | `docs/strategy_log/agent_runs/phase4c_approval_gate/` | Sample includes `approval_template.yaml`. |

## Validation
- `./.venv/bin/python -m pytest test/test_agent_strategy_iteration.py test/test_grid_search.py test/test_walk_forward_validation.py`
- `./.venv/bin/python -m compileall agent/strategy_iteration run_agent_strategy_iteration.py`
- `./.venv/bin/python run_agent_strategy_iteration.py --objective "phase4c approval file command gate integration" --run-id phase4c_approval_gate --no-llm --no-memory --propose-actions --write-approval-template`
