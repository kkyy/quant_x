# System Diagnostic: 2026-05-13 Phase 3

## Layer Scores
| Layer | Score | Weakest Link | Highest Leverage Fix |
|---|---:|---|---|
| Data | 3 | Agent memory could remember plans but not measured outcomes. | Add CSV-driven feedback records and append-only outcome memory. |
| Factors | 3 | Factor/postprocess ideas can look promising in same-model results without durable evidence. | Compare treatment CSVs against explicit control CSVs and record deltas. |
| Model | 4 | Model-layer changes remain secondary; evaluation discipline is the current system need. | Keep feedback generic across model/factor/backtest outputs without launching training. |
| Backtest | 4 | Result interpretation was manual and easy to lose between iterations. | Parse backtest and WFV summaries into structured `StrategyFeedback`. |
| Execution | 4 | Planner still must not trigger external side effects. | Feedback mode reads existing CSV artifacts only. |
| Web | 3 | Dashboard browsing for feedback artifacts is still future work. | Defer Web integration until feedback format stabilizes. |

## Key Findings
1. Phase 3 closes the first research loop: plan bundles can now be followed by measured feedback and memory reflection.
2. Feedback generation is conservative: same-model treatment gains are held back unless rank metric and Sharpe improvement clear the configured gates, and WFV downside such as weak min Sharpe can refute an arm.
3. The sample feedback for `fundamental_gate_top70` versus its control is `hold/mixed`, matching the historical conclusion that same-model uplift was not enough for promotion.

## Change Plan
| Change | Files | Validation |
|---|---|---|
| Add metric snapshots and strategy feedback schemas. | `agent/strategy_iteration/schemas.py` | Unit tests for parsing and serialization. |
| Add CSV result evaluator. | `agent/strategy_iteration/evaluator.py` | Backtest/control and WFV summary tests. |
| Append outcome memory entries. | `agent/strategy_iteration/memory.py` | CLI feedback test and memory inspection. |
| Extend CLI feedback mode. | `run_agent_strategy_iteration.py` | `--feedback-run-id` smoke run. |
| Record sample feedback. | `docs/strategy_log/agent_runs/phase3_feedback_sample/` | Generated `feedback.json` and `feedback.md`. |
