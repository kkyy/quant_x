# System Diagnostic: 2026-05-11

## Layer Scores
| Layer | Score | Weakest Link | Highest Leverage Fix |
|---|---:|---|---|
| Data | 3 | No new orthogonal data source was validated after the prior Alpha158 ceiling diagnosis. | Avoid new fetches in this cycle; preserve cached/offline evidence and focus on execution correctness. |
| Factors | 3 | Always-on SVS behaves like regime leverage rather than stable alpha. | Use drawdown-gated SVS only as a risk/stability overlay, not as a default alpha boost. |
| Model | 4 | LGBM + Alpha158 remains the validated model line; alternative models/factors previously degraded WFV. | Reuse `models/lgbm_universe-csi1000_20260428_155547.pkl` and avoid retraining for this iteration. |
| Backtest | 4 | Candidate evidence existed, but the reusable config path did not match the WFV-winning parameters. | Align `config/csi1000_adaptive_overlay_20.yaml` to WFV winner `topk=15/n_drop=3/hold=8`. |
| Execution | 3 | Daily command surface still pointed at an aggressive overlay path; adaptive dd20 lacked a safe dry-run command. | Add a dry-run scheduled rebalance command for the adaptive dd20 candidate. |
| Web | 3 | Dashboard files are already locally modified; not the constraint for this strategy iteration. | Leave web surface untouched and validate backend import only. |

## Key Findings
1. The 2026-05-05 WFV run found a distinct stability candidate: `csi1000_adaptive_dd20` with 7/7 positive return and Sharpe folds, mean Sharpe 0.984, min Sharpe 0.0217, p=0.0247, and worst drawdown -23.47%.
2. The candidate's config path was stale: `config/csi1000_adaptive_overlay_20.yaml` used `topk=10/n_drop=5/hold=8`, while the WFV-winning row and strategy log record `topk=15/n_drop=3/hold=8`.
3. The right system action is not another factor/model experiment; it is promoting the stability candidate as a reusable research branch while keeping `adaptive_baseline_wf` / `csi1000_balanced` as return-seeking controls.

## Change Plan
| Change | Files | Validation |
|---|---|---|
| Align adaptive dd20 config with WFV-winning parameters. | `config/csi1000_adaptive_overlay_20.yaml` | Load merged config and assert strategy/daily params match `15/3/8`. |
| Add adaptive dd20 to candidate index. | `config/strategy_candidates.yaml` | Parse YAML and verify candidate fields. |
| Add safe dry-run command. | `command/daily/csi1000_adaptive_dd20_0511.sh` | Shell syntax check. |
| Record system iteration. | `docs/strategy_log/system_iteration_log.csv` | CSV parse check. |
