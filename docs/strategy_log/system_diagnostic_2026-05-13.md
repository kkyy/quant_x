# System Diagnostic: 2026-05-13

## Layer Scores
| Layer | Score | Weakest Link | Highest Leverage Fix |
|---|---:|---|---|
| Data | 3 | No new orthogonal source is yet production-ready; the immediate gap is research discipline, not fetch breadth. | Add a data-factor analyst role with context and prompt discipline before new source work. |
| Factors | 3 | Recent evidence still shows redundancy and WFV fragility when changes are not tightly scoped. | Force factor proposals through trace, screening, and kill-test language before execution. |
| Model | 4 | The validated LGBM line remains the best default; the gap is hypothesis quality, not missing model code. | Keep model review as an analyst role rather than defaulting to new model implementation. |
| Backtest | 4 | Comparability can still drift across benchmark, rank metric, deal price, and cost assumptions. | Make backtest comparability a first-class analyst output in every agent run. |
| Execution | 4 | Rebalance and notification semantics remain risky if planning output is mistaken for executable intent. | Keep Phase 1 offline and approval-gated, with execution concerns surfaced in dedicated roles. |
| Web | 3 | No dashboard surface exists yet for agent planning artifacts. | Defer Web integration until the offline bundle format stabilizes. |

## Key Findings
1. The most immediate improvement is not a new alpha experiment, but a disciplined planning layer that borrows RD-Agent's hypothesis-feedback trace and TradingAgents-ex's role-based debate.
2. Prompt system design, context packs, and append-only research memory are required before optional LLM execution is trustworthy.
3. Phase 1 should remain offline-by-default and write auditable run bundles under `docs/strategy_log/agent_runs/`.

## Change Plan
| Change | Files | Validation |
|---|---|---|
| Add Phase 1 implementation roadmap. | `docs/agent_strategy_iteration_implementation_plan_2026-05-13.md` | Manual review. |
| Add prompt catalog, context pack, memory log, CLI, and config. | `agent/strategy_iteration/*`, `run_agent_strategy_iteration.py`, `config/agent_strategy_iteration.yaml` | Import check + CLI run. |
| Add focused tests. | `test/test_agent_strategy_iteration.py` | `pytest` on targeted files. |
| Record system iteration. | `docs/strategy_log/system_iteration_log.csv` | CSV parse check. |
