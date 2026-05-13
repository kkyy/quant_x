# System Diagnostic: Full Agent Train/Backtest Cycle

Date: 2026-05-13

## Scope

Validate the complete strategy-iteration agent path with real LLM roles and actual quant_ex execution:

- multi-role LLM planning
- prompt/context/role trace persistence
- command proposal and approval template generation
- real LGBM training
- real same-window backtest
- feedback parsing and agent memory reflection

No qlib data update, notification, scheduled rebalance execution, or trading-like action was run.

## Run

- Run id: `full_agent_train_backtest_20260513`
- Run bundle: `docs/strategy_log/agent_runs/full_agent_train_backtest_20260513/`
- Roles: 12 / 12 real LLM calls
- Model tiers: `quick=gpt-5.4-mini`, `deep=gpt-5.5`
- Key artifacts: `plan.md`, `role_traces.md/json`, `commands.md/json`, `approval_template.yaml`, `feedback.md/json`

## Strict Training And Backtest

- Training config: `docs/strategy_log/agent_runs/full_agent_train_backtest_20260513/train_csi1000_eval_csi300.yaml`
- Train universe: `csi1000`
- Eval market: `csi300`
- Benchmark: `SH000300`
- Model: `models/lgbm_agent_full_iter_csi1000_20260513_20260513_210545.pkl`
- Strategy params: `topk=15`, `n_drop=3`, `hold_thresh=8`
- Result CSV: `backtest_results/agent_runs/full_agent_train_backtest_20260513_csi1000_model_csi300_eval.csv`

Metrics:

| Metric | Value |
|---|---:|
| cumulative return | 72.77% |
| annual return | 27.45% |
| Sharpe | 1.2490 |
| information ratio | 0.5774 |
| max drawdown | -20.86% |
| alpha | 10.18% |
| Rank IC | 0.0521 |

Control:

- `backtest_results/ablation/fundamental_control_15_3_8_20260511.csv`
- Control Sharpe: `1.6229`
- Control IR: `1.1574`

Deltas:

- Delta Sharpe: `-0.3739`
- Delta IR: `-0.5800`
- Delta MaxDD: `-0.0051`

## Decision

The workflow is operational, but the strict csi1000 retrain candidate is rejected.

Feedback:

- Decision: `reject`
- Hypothesis evaluation: `refuted`
- Next ablation: do not rerun this exact configuration; return to the existing baseline control or design a smaller orthogonal ablation.

## Important Caveat

An earlier diagnostic command in the same run used `config/daily_csi1000.yaml`. Despite the file name, that config currently resolves `market.name` to `csi300`, so the resulting `full_agent_train_backtest_20260513_same_model.csv` is superseded and should not be used as the strict csi1000 conclusion.

Future strict csi1000 training should always verify `market.name: "csi1000"` in the override and confirm the saved model `_meta.json`.
