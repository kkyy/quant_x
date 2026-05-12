# Fundamental Factor Iteration Plan - 2026-05-11

## Context

The current project already has rich fundamental data, but the direct
"append all fundamental columns to Alpha158" path has produced noise rather
than alpha.

Observed baseline evidence:

- `csi1000_ablation_control`: technical extra factors only, 2024-01-01 to
  2026-04-29, `topk=15/n_drop=3/hold=5`, annual return `0.0032`, Sharpe
  `0.0095`, MaxDD `-0.3405`, RankIC `0.0428`.
- `csi1000_ablation_fundamental`: technical + current fundamental block,
  same window and strategy parameters, annual return `-0.0570`, Sharpe
  `-0.1741`, MaxDD `-0.3466`, RankIC `0.0376`.
- The saved top-50 feature importance for the fundamental run contains no
  fundamental column, suggesting LightGBM mostly ignored the block while still
  suffering ranking noise from it.
- System logs also conclude that direct valuation/fundamental feature
  addition degraded WFV or same-model performance.

Current stable controls:

- Return-seeking control: `adaptive_baseline_wf`, config
  `config/daily_csi1000.yaml`, WFV 2020-2026, `topk=15/n_drop=3/hold=8`,
  mean Sharpe `1.2184`, min Sharpe `-0.6518`, positive folds `5/7`, p-value
  `0.0553`.
- Stability control: `adaptive_dd20_wf`, config
  `config/csi1000_adaptive_overlay_20.yaml`, WFV 2020-2026,
  `topk=15/n_drop=3/hold=8`, mean Sharpe `0.9844`, min Sharpe `0.0217`,
  positive folds `7/7`, p-value `0.0247`.

All new fundamental experiments must keep benchmark, rank metric, deal price,
costs, slippage, universe, and WFV folds consistent with the chosen control.

## Core Hypothesis

Fundamentals should not be treated as another daily high-frequency feature
block beside price-volume factors. They should be tested as:

1. Low-frequency quality/value/growth state descriptors.
2. Cross-sectional gates or risk filters.
3. Orthogonal residual signals after removing size, industry, and Alpha158
   exposure.
4. Slow-horizon signals whose label horizon is 5/10/20 trading days rather
   than default 1-day noise.

The goal is not to maximize in-window Sharpe. The goal is to find a
fundamental use mode that improves WFV mean Sharpe or worst-fold stability
without increasing drawdown or concentration.

## Stage 0 - Data And Leakage Audit

Before training:

- Check coverage by metric, year, and universe. Reject columns with training
  coverage below `60%` unless they are explicit event flags.
- Check staleness. Financial-statement fields must carry an `asof` or report
  publication date; daily forward-fill from period end is not acceptable for
  deployment-grade tests.
- Check monotonic release lag. If only report date is available, use a
  conservative lag assumption, e.g. quarterly report date + 45 calendar days
  for interim reports and +120 calendar days for annual reports.
- Winsorize per date at `1%/99%`, then daily rank or z-score transform.
- For absolute values such as market cap, revenue, net profit, total assets:
  test only transformed versions, e.g. log scale, sector-relative percentile,
  or residual after size neutralization.

Pass/fail:

- No WFV until a data audit CSV is saved under
  `optimization_results/research_cycles/fundamental_data_audit_YYYYMMDD.csv`.
- Any metric with suspicious look-ahead risk is excluded from Stage 1.

## Stage 1 - Factor IC Audit, No Model Training

Run a train-only factor audit for horizons `1, 5, 10, 20`:

- `valuation`: `pe_ttm`, `pb`, `ps_ttm`, `dyr`, plus inverse/value ranks.
- `profitability`: `roe`, `roa`, `gross_margin`, `net_margin`.
- `growth`: `revenue_growth`, `profit_growth`, `rev_accel`.
- `cashflow`: `ocf_to_np`, `fcf_yield`.
- Event/proxy groups: `dividend`, `analyst`, `earnings_guidance`,
  `balance_sheet`, `repurchase`, `institutional`, `shareholder`, `pledge`.

Evaluation rules:

- Compute RankIC, ICIR, coverage, and turnover/staleness for each column.
- Evaluate raw, daily-rank, industry-neutral, size-neutral, and
  industry+size-neutral variants.
- Greedy correlation pruning: `max_corr=0.7`.
- A column can enter Stage 2 only if `abs(IC) >= 0.01`, `abs(ICIR) >= 0.15`,
  coverage >= `60%`, and the sign is stable in at least `4/7` WFV-like yearly
  train slices.
- Prefer signs that make economic sense: cheaper valuation, higher quality,
  improving profitability, better cashflow, and fewer distress flags.

This stage is deliberately cheap and should be run before any training.

## Stage 2 - Four Controlled Application Modes

All arms include a control that reproduces the current baseline exactly.
Limit each batch to four to six treatment arms.

### Arm A - Fundamental Quality Gate

Use fundamentals only as an exclusion or narrowing layer before TopkDropout.
Do not append raw columns to model training.

Candidate gates:

- Exclude bottom `20%` by sector-relative profitability composite.
- Exclude bottom `20%` by cashflow quality.
- Exclude top `20%` by distress composite: leverage, goodwill, pledge ratio,
  shareholder deterioration.
- Keep top `70%` by fundamental composite, then run the same model score.

Reason:

The prior direct-input run suggests fundamentals may be more useful for
removing bad candidates than for daily ranking.

Primary comparison:

- Control: `adaptive_baseline_wf`.
- Treatment: same trained Alpha158 model path and same `topk=15/n_drop=3/hold=8`;
  only the pre-trade candidate filter changes.

Promotion threshold:

- Mean Sharpe improves by at least `0.10`, or MaxDD improves by at least `0.03`
  with no worse mean Sharpe.
- Positive folds must be at least `5/7`.

### Arm B - Fundamental Score Overlay

Build 3-5 composite scores, then blend them with the model score after daily
rank transform:

- `quality_score`: profitability + margins + cashflow.
- `value_score`: cheap valuation, excluding negative or invalid denominators.
- `growth_quality_score`: growth only when profitability and cashflow pass.
- `distress_score`: leverage, pledge, goodwill, falling shareholder quality.

Test small weights only:

- `score = 0.90 * model_rank + 0.10 * fundamental_rank`
- `score = 0.80 * model_rank + 0.20 * fundamental_rank`
- `score = model_rank`, but use fundamental score only as tie-breaker.

Reason:

This treats fundamentals as a slow prior, not as high-dimensional daily model
features.

Promotion threshold:

- Must beat control on WFV robust score, not only same-model 2024-2026.
- If `sharpe_ttest_pvalue > 0.3`, do not promote.

### Arm C - Orthogonal Residual Fundamental Model

Two-stage approach:

1. Train the current Alpha158/LGBM control.
2. On training/validation splits only, compute residual returns or residual
   ranks after removing the base model score, industry, and size effects.
3. Train a small regularized model using only selected fundamental composites
   to predict that residual.
4. Blend residual score back into the base score with weights `0.05`, `0.10`,
   and `0.20`.

Recommended model:

- Ridge or Lasso first, not LightGBM. If the linear model cannot extract
  stable residual alpha, a flexible model is unlikely to be trustworthy.

Reason:

This explicitly asks whether fundamentals add information not already captured
by Alpha158 and market microstructure factors.

Promotion threshold:

- Residual model must have positive validation RankIC in at least `4/7` folds.
- Final blended WFV must not reduce min Sharpe below the control by more than
  `0.10`.

### Arm D - Slow-Horizon Fundamental Labels

Train variants with `training.label_horizon` equal to `5`, `10`, and `20`.
Use only selected fundamental composites, not all raw fields.

Reason:

Fundamental data should not be judged solely by 1-day forward returns. It is
more likely to affect medium-horizon selection quality.

Promotion threshold:

- The selected horizon must improve RankIC and WFV Sharpe after transaction
  costs. A higher gross IC with higher turnover is not sufficient.

## Stage 3 - Minimal Executable Matrix

Batch 1: audit only.

```bash
./.venv/bin/python run_train.py --list-registry
./.venv/bin/python -m pytest \
  test/test_fundamental_factor_extended.py \
  test/test_valuation_factor.py \
  test/test_balance_sheet_factor.py \
  test/test_dividend_factor.py \
  test/test_earnings_guidance_factor.py \
  test/test_analyst_factor.py
```

Batch 2: same-model quick replay, no WFV promotion.

```bash
./.venv/bin/python run_train.py --model lgbm \
  --config config/ablation_fundamental_quality_gate.yaml \
  --tag fund_quality_gate

./.venv/bin/python run_backtest.py \
  --model-path models/lgbm_fund_quality_gate_*.pkl \
  --market csi300 --topk 15 --n-drop 3 --hold-thresh 8 \
  --start 2024-01-01 --end 2026-05-11 \
  --output-csv backtest_results/ablation/fund_quality_gate.csv
```

Batch 3: WFV only for survivors from Batch 2.

```bash
./.venv/bin/python run_walk_forward_validation.py \
  --train-universes csi1000 \
  --eval-market csi300 \
  --train-config config/ablation_fundamental_quality_gate.yaml \
  --with-extra-factors \
  --run-id fund_quality_gate_wf \
  --topk 15 \
  --n-drop 3 \
  --hold-thresh 8
```

Batch 4: compare against stability control.

```bash
./.venv/bin/python run_walk_forward_validation.py \
  --train-universes csi1000 \
  --eval-market csi300 \
  --train-config config/csi1000_adaptive_overlay_20.yaml \
  --run-id adaptive_dd20_control_wf \
  --topk 15 \
  --n-drop 3 \
  --hold-thresh 8
```

Use a distinct output directory/tag for each arm so results do not overwrite
existing WFV artifacts.

## Stage 4 - Decision Rules

Promote to `compare_next` only if:

- WFV mean Sharpe improves by at least `0.10` vs the relevant control, or
  worst MaxDD improves by at least `0.03` without worse mean Sharpe.
- Positive Sharpe folds are at least `5/7`.
- `sharpe_ttest_pvalue <= 0.30`.
- No hidden dependency on fresh network fetches during training or daily
  signal generation.
- Concentration and max position assumptions are unchanged or stricter than
  the control.

Promote to `keep` only if:

- It beats the control on robust score and either mean Sharpe or min Sharpe.
- It remains acceptable under current benchmark-aware `information_ratio`
  sorting, `deal_price=close`, and configured costs.
- The effect is visible in at least two market regimes, not only 2024-2026.

Mark `do_not_promote` if:

- Same-model Sharpe improves but WFV mean Sharpe falls.
- Fundamental features dominate feature importance without improving RankIC.
- The benefit comes from a single year or from concentration increase.
- The arm requires suspicious point-in-time assumptions.

## Implementation Notes

The existing `FactorScreener` is not currently wired into
`ModelTrainer.train()`, which calls `factor_pipeline.compute()` directly.
Before using IC screening in live experiments, add a train-split-only screening
path so the screener cannot see validation/test labels.

`run_walk_forward_validation.py` now keeps its historical default by passing
`--no-extra-factors`, but supports `--with-extra-factors` for factor ablations.
Use that flag whenever a WFV arm is meant to compute fundamental, sector, CSV,
or other configured factor blocks.

Recommended implementation order:

1. Add a standalone audit script that computes coverage, staleness, IC/ICIR,
   and correlation reports for fundamental columns.
2. Add composite fundamental factor output via `csv` or a small new factor
   module, rather than appending every raw field.
3. Add postprocess support for fundamental gate/overlay modes.
4. Run same-model smoke comparisons.
5. Run WFV only for the two best smoke-test survivors.
6. Append durable results to `docs/strategy_log/strategy_iteration_log.csv`
   and update `config/strategy_candidates.yaml` only for `keep`,
   `compare_next`, or deliberate fallback arms.

## Recommended First Arms

Start with the least fragile path:

1. `fund_quality_gate_top70`: composite gate, keep top 70%; initial config is
   `config/ablation_fundamental_gate.yaml`.
2. `fund_score_overlay_w10`: rank blend with 10% fundamental score; initial
   config is `config/ablation_fundamental_overlay_w10.yaml`.
3. `fund_quality_gate_drop_bottom20`: only remove the worst 20%.
4. `fund_score_overlay_w20`: rank blend with 20% fundamental score.
5. `fund_residual_ridge_w10`: residual model blend with 10% weight.

Do not retry the old `technical + raw fundamental full block` arm unless the
data audit or transformation layer changes materially.

## Cache Audit Result - 2026-05-11

Ran cache-only CSi1000 audit over `2024-01-01..2026-05-11`, using valuation
cache with no lag and financial cache with a conservative 45-day report lag.
Artifacts:

- `optimization_results/research_cycles/fundamental_audit_20260511_csi1000_cache_metrics.csv`
- `optimization_results/research_cycles/fundamental_audit_20260511_csi1000_cache_selected.csv`
- `optimization_results/research_cycles/fundamental_audit_20260511_csi1000_cache_coverage.csv`
- Short-window selected run:
  `optimization_results/research_cycles/fundamental_audit_20260511_csi1000_cache_rank_stable3_selected.csv`

Strict `min_stable_years=4` selects nothing because this short smoke window has
only three calendar years. With `min_stable_years=3`, selected candidates are:

- Positive signs: `peg`, `revenue_growth`, `profit_growth`.
- Negative signs: `pb`, `pcf`, `gross_margin`.

First configs intentionally exclude `gross_margin` despite passing the short
audit because its negative sign is economically suspicious and may be a sector
composition artifact. Keep `market_cap` / `float_market_cap` out of the first
fundamental composite despite strong ICIR because they are stable in only `2/3`
years; use them as size-control diagnostics instead.

## WFV Result - Fundamental Gate Top70

Ran on 2026-05-12:

```bash
./.venv/bin/python run_walk_forward_validation.py \
  --train-universes csi1000 \
  --eval-market csi300 \
  --train-config config/ablation_fundamental_gate.yaml \
  --run-id fundamental_gate_top70_wf \
  --topk 15 \
  --n-drop 3 \
  --hold-thresh 8
```

Result source:

- `optimization_results/walk_forward_fundamental_gate_top70_wf/walk_forward_summary.csv`
- `optimization_results/walk_forward_fundamental_gate_top70_wf/walk_forward_all_results.csv`

Summary:

- Mean annual return `2.70%`
- Mean Sharpe `0.301`
- Min Sharpe `-1.200`
- Worst MaxDD `-45.46%`
- Positive Sharpe folds `4/7`
- RankIC `0.0278`
- Sharpe t-test p-value `0.515`

Decision: downgrade. The same-model 2024-2026 improvement was not robust. The
gate badly underperformed in 2020 and 2021, and it is much worse than both
`adaptive_baseline_wf` and `adaptive_dd20_wf`. Do not promote this top70
fundamental gate. If fundamentals are revisited, start with a much weaker
drop-bottom filter or a longer point-in-time audit before any WFV.

## V2/V3/V4 Results - Coverage And Growth Gates

After the broad top70 gate failed, tested progressively safer variants:

1. `config/ablation_fundamental_gate_drop20_cov4.yaml`
   - Same-model result identical to same-day control.
   - Interpretation: `min_metric_count=4` is too strict for current caches and
     makes the filter effectively inactive.

2. `config/ablation_fundamental_gate_drop20_cov3.yaml`
   - Same-model result also identical to control.
   - Interpretation: broad composite bottom-20 filtering does not affect the
     `topk=15` portfolio enough to matter.

3. `config/ablation_fundamental_growth_gate_top70_cov2.yaml`
   - WFV source:
     `optimization_results/walk_forward_fundamental_growth_gate_top70_cov2_wf/walk_forward_summary.csv`
   - Mean Sharpe `0.760`, min Sharpe `-0.445`, worst MaxDD `-30.55%`,
     positive folds `6/7`, p-value `0.045`.
   - Interpretation: growth-only plus coverage guard fixes much of the broad
     gate failure, but still trails baseline and dd20.

4. `config/ablation_fundamental_growth_gate_drop20_cov2.yaml`
   - Same-model source:
     `backtest_results/ablation/fundamental_growth_gate_drop20_cov2_20260512.csv`
   - Same-model 2024-2026 Sharpe `2.177`, IR `1.818`.
   - WFV source:
     `optimization_results/walk_forward_fundamental_growth_gate_drop20_cov2_wf/walk_forward_summary.csv`
   - WFV mean Sharpe `0.872`, min Sharpe `-0.529`, worst MaxDD `-29.57%`,
     positive folds `5/7`, p-value `0.068`.
   - Interpretation: best fundamental-only postprocess branch so far, but
     still not good enough versus `adaptive_baseline_wf` (mean Sharpe `1.218`)
     or `adaptive_dd20_wf` (mean Sharpe `0.984`, all folds positive).

Current decision: do not promote any fundamental gate. The most informative
next experiment, if continuing this branch, is a drawdown/regime-gated growth
filter that disables the growth filter in weak markets, because the drop20
version is strong in 2024-2026 but still loses in 2022/2023.
