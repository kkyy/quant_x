# System Diagnostic: 2026-05-02

## Layer Scores (1-5, 5=strong)
| Layer | Score | Weakest Link | Highest Leverage Fix |
|-------|-------|-------------|-------------------|
| Data | 2 | 10 of 15 cache domains never fetched; only financial + northbound have data | Run `run_fetch_data.py --type all` to populate missing caches; fetch new domain data for ablation |
| Factors | 2 | Only 1 of 20 factor modules active (technical/Alpha158); mining pipeline never run | Run factor mining; test ablation on valuation/margin/analyst individually |
| Model | 3 | No HP tuning, ensemble disabled, alternative models untested; only LGBM ever used | Enable bootstrap ensemble; try XGBoost; run Optuna/grid HP search |
| Backtest | 3 | No position cap (50-63% single-stock weight); regime switching disabled | Implement max_position_pct; enable and validate regime switching |
| Execution | 3 | Scheduled rebalance uses conservative config not best candidate; base/daily config divergence | Align daily config with best candidate; add liquidity filters |

## Key Findings

1. **Data is the biggest bottleneck**: 10 of 15 registered fetcher domains have never been run. The model relies entirely on Alpha158 price/volume features. Fundamental, valuation, margin, analyst, and other domain data exist as fetcher code but have zero cache data. This severely limits the factor layer.

2. **Factor ablation failures were premature**: Fundamental and northbound factors were rejected after short-window ablation (2024-01 to 2026-04) with weak statistical significance (northbound p=0.41). The fundamental rejection was based on stale/incomplete financial data. Before concluding these factors have no value, they need proper data population and longer-horizon WFV.

3. **No position concentration control**: The best walk-forward candidate (csi800/5/3/8) has 50-63% single-stock weights. This is a critical practical risk. The `strategy.portfolio` section is missing from all configs. TopkDropoutStrategy assigns equal weight, but with topk=5, each position is 20% — the 50%+ weights suggest the strategy holds fewer than topk stocks in some periods.

4. **Ensemble disabled despite being configured**: `model.ensemble.seeds: [42, 123, 2024]` is in config but `enabled: false`. Bootstrap bagging is also off. Multi-seed averaging could reduce prediction variance at ~3x training cost.

5. **Factor mining pipeline never executed**: `cache/mined_factors.json` does not exist. The `FactorMiner` has 9 expression templates (momentum, mean-reversion, volume-price) but has never been run. This is free alpha that hasn't been explored.

## Historical Context (from strategy_iteration_log.csv)

**Best strategies by Sharpe (same-model 2024-2026 window):**
- csi1000_relative_strength_overlay: Sharpe 3.10-3.66 (topk=5, extreme concentration)
- csi1000_overlay_plus: Sharpe 2.91-3.22 (topk=10, best tradeoff)
- csi1000_overlay_practical: Sharpe 2.54-2.94 (topk=15, fallback)
- csi1000_balanced: Sharpe 1.03-1.48 (conservative baseline)

**Rejected factors:**
- Full sector factor block: do_not_promote (degraded baseline)
- Sector-relative-only: do_not_promote (trailed no-sector baseline)
- Fundamental factor block: do_not_promote (degraded control, likely due to data quality)
- Northbound factor block: downgrade (WFV p=0.41, statistically insignificant)
- Northbound as overlay training feature: do_not_promote (Sharpe collapsed 1.67→0.64)

**Decision surface**: The system has converged on a single-model (LGBM/Alpha158/CSI1000) architecture. Overlay strategies (stock_vs_sector post-filter) provide the strongest alpha uplift but lack WFV validation. The remaining high-leverage improvements are: (1) data population, (2) factor mining, (3) ensemble, (4) position caps.
