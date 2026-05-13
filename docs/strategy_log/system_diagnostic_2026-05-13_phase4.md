# System Diagnostic: 2026-05-13 Phase 4

## Layer Scores
| Layer | Score | Weakest Link | Highest Leverage Fix |
|---|---:|---|---|
| Data | 3 | Data refresh commands can mutate cache or use network if agent execution is too permissive. | Classify fetch/update commands as protected proposal-only actions. |
| Factors | 3 | Factor ideas still need controlled validation before implementation. | Convert plans into explicit validation commands with success criteria and audit artifacts. |
| Model | 4 | Training commands are expensive and can overwrite local artifacts if run casually. | Keep training proposal-only unless the user explicitly approves a concrete command. |
| Backtest | 4 | Same-model backtests are useful but can be mistaken for promotion evidence. | Generate backtest templates behind approval and route completed CSVs through feedback mode. |
| Execution | 4 | The agent layer previously had no command boundary between planning and running. | Add a command proposal layer, risk tags, and safe-local execution gate. |
| Web | 3 | Agent runs are still file-based and not browsable in the dashboard. | Leave dashboard browsing for Phase 5 after command/feedback artifacts stabilize. |

## Key Findings
1. Phase 4 adds the first execution boundary: agent plans now produce `commands.json` and `commands.md` with per-command risk tags and approval reasons.
2. The safe runner is intentionally narrow. It can run local import/pytest/registry checks, while training, backtest, WFV, data fetch/update, notifications, and trading-like commands remain proposal-only.
3. Guarded command templates make the next research action visible without performing it: same-model backtest, WFV, scheduled rebalance dry-run, and data refresh are all present but marked as requiring approval.

## Change Plan
| Change | Files | Validation |
|---|---|---|
| Add command proposal and execution schemas. | `agent/strategy_iteration/schemas.py` | Serialization covered by focused tests. |
| Add risk classifier, guarded command templates, safe runner, and artifact writer. | `agent/strategy_iteration/execution.py` | Tests for protected WFV, safe registry/import, and skipped protected commands. |
| Extend CLI with action proposal and safe execution flags. | `run_agent_strategy_iteration.py` | CLI sample run writes `commands.json` and `commands.md`. |
| Record a Phase 4 sample run. | `docs/strategy_log/agent_runs/phase4_command_gate/` | Command plan shows safe-local and protected commands distinctly. |

## Validation
- `./.venv/bin/python -m pytest test/test_agent_strategy_iteration.py test/test_grid_search.py test/test_walk_forward_validation.py`
- `./.venv/bin/python run_agent_strategy_iteration.py --objective "phase4 command proposal and approval gate integration" --run-id phase4_command_gate --no-llm --no-memory --propose-actions`
