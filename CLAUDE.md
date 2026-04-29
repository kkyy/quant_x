# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`quant_ex` is an A-share quantitative stock selection framework built on qlib + Alpha158. It supports multi-model training, factor mining, strategy backtesting, walk-forward validation, daily signal generation, and Claude AI-powered parameter optimization.

## Environment

- **Python**: Use `/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9` for syntax/import checks — this venv has pandas, numpy, sklearn, lightgbm. The project's own `.venv` has no packages installed.
- **qlib data**: `/Users/weidian/code/algorithms/investment_data/qlib_data/qlib_bin`
- **Setup**: `pip install -r requirements.txt` or `pip install -e .[dev]` for dev tools

## Commands

```bash
# Training
python run_train.py --model lgbm --tag baseline
python run_train.py --model lgbm --with-sector --tag sector_full
python run_train.py --qlib-native                    # MLflow mode; outputs Recorder ID
python run_train.py --list-registry                  # Show all registered models/factors

# Backtesting
python run_backtest.py --model-path models/lgbm_*.pkl
python run_backtest.py --topk 5,10,15 --n-drop 1,3,5 --seeds  # multi-seed robustness
python run_backtest.py --output-csv results/my.csv   # write to specific path (for WFV isolation)
python run_backtest.py --optimize --n-iters 3        # requires ANTHROPIC_API_KEY

# Walk-forward validation
python run_walk_forward_validation.py \
  --train-universes csi300,csi800,csi1000 \
  --eval-market csi300 \
  --topk 5,15,20 --n-drop 1,3 --hold-thresh 5,8,10
python run_walk_forward_validation.py --folds-config config/walk_forward_folds.yaml  # custom folds
python run_walk_forward_validation.py \
  --robust-weights '{"mean_sharpe":1.0,"sharpe_std":-0.3,"min_sharpe":0.5,"positive_sharpe_folds":0.05}'

# Daily signals (run after market close)
python run_daily.py --model-path models/lgbm_*.pkl --dry-run
python run_daily.py --config config/daily_csi1000.yaml --model-path models/lgbm_*.pkl --dry-run
python run_daily.py --account 500000 --positions SH600000:500,SZ000001:300

# Scheduled rebalance (with launchd)
python run_scheduled_rebalance.py --mock --dry-run     # test format
python run_scheduled_rebalance.py --dry-run            # full pipeline, no push
scripts/install_daily_rebalance_launchd.sh             # register 20:00 / 09:00 / 14:00 jobs

# Factor mining
python run_factor_mining.py --min-ic 0.03 --min-icir 0.4 --top-n 30

# Data update
python run_update_qlib_data.py                       # full pipeline
python run_update_qlib_data.py --skip-dolt-pull      # skip dolt sync
python run_update_qlib_data.py --supplement-source akshare   # fill gaps with akshare

# Tests
python -m pytest test/test_universe_filter.py test/test_trainer.py test/test_data_sources.py

# Lint/format (if dev deps installed)
ruff check .
black .
```

## Architecture

Config deep-merges `base.yaml → model.yaml → notify.yaml` (later overrides earlier). Strategy/backtest/universe params are all in `base.yaml`.

```
Data Layer      DataLoader (qlib D.features) → UniverseFilter (liquidity filter) → SectorDataProvider (concurrent akshare, 7d cache)
                ↓ data/utils.py: unified code_to_qlib_instrument() + cached load_stock_names()
                ↓ qlib_update/: Dolt → SQL → CSV → normalize → dump_bin
                ↓ data/sources/: GapFiller (akshare / eastmoney) bridges Dolt gaps
Feature Layer   Alpha158 (qlib native) + FactorPipeline [technical, sector, mined, regime]
                ↓ (optional) FactorScreener: IC/ICIR threshold + corr dedup
Model Layer     ModelTrainer → qlib-native path (MLflow .recorder) or custom path (.pkl)
                LGBMAlphaModel supports bootstrap bagging (bagging_fraction param)
Backtest Layer  BacktestEngine (qlib TopkDropoutStrategy) → GridSearchBacktest → AutoOptimizer (Claude)
                backtest/metrics.py: benchmark excess return, IR, turnover
                backtest/attribution.py: Brinson sector attribution (NEW)
                backtest/signal_diagnostics.py: IC decay, rolling IC monitor
Signal Layer    SignalGenerator (price_data reuse, suspended filter)
                → postprocess: industry_neutralize, size_neutralize (NEW)
                → concentration check (NEW): Herfindahl + hard limit warnings
                → NotificationPusher
```

Two training modes:
- **qlib-native** (`--qlib-native`): Uses `qlib.LGBModel`, tracked via MLflow. After training, paste the Recorder ID into `config/base.yaml → experiment.latest_recorder_id`.
- **custom** (default): Uses registry-based models, saves to `models/*.pkl` + `_meta.json` + `_feature_importance.json` sidecars. Reference via `--model-path`.

## Key Patterns

**Registry pattern** — models and factors use decorator-based auto-registration:
```python
@ModelRegistry.register("lgbm")
class LGBMAlphaModel(BaseAlphaModel): ...

@FactorRegistry.register("regime")
class RegimeFeatureEngine(BaseFactor): ...
```
`ModelTrainer.__init__` auto-imports all model/factor modules via `importlib`. New factor modules must be added to the loop in `models/trainer.py`.

**Adding a new model**: Subclass `BaseAlphaModel`, implement `fit()` and `predict()`, decorate with `@ModelRegistry.register("name")`, place in `models/`.

**Adding a new factor**: Subclass `BaseFactor`, implement `compute(price_data) → DataFrame` (must return `(instrument, datetime)` MultiIndex), decorate with `@FactorRegistry.register("name")`, place in `features/`. Add to `config/model.yaml → features.factors` and to the `importlib` loop in `models/trainer.py`.

**FactorPipeline** builds from config list; supports optional `screener_config` kwarg:
```python
pipeline = FactorPipeline.from_config(factor_configs,
    screener_config={"min_ic": 0.02, "min_icir": 0.3, "max_corr": 0.7})
kept = pipeline.compute_with_screening(price_data, forward_returns=label)
```

**Backward compat for old pickles**: `LGBMAlphaModel.__setstate__` + `_ensure_runtime_defaults()` fills missing attributes at load time. New attributes added to `__init__` must also be added there with their default values. Same pattern for `SectorFactorEngine._ensure_compat_attrs()`.

**Multi-seed isolation**: `run_backtest.py --seeds` spawns a subprocess per seed with `PYTHONHASHSEED` set.

## Configuration

`config/notify.yaml` is gitignored — copy from `config/notify.yaml.example`.

`config/strategy_candidates.yaml` records walk-forward research conclusions (not auto-loaded). Transient results go in `optimization_results/`; long-term conclusions go here.

`config/walk_forward_folds.yaml.example` — template for custom fold definitions; pass via `--folds-config`.

### UniverseFilter options (`strategy.universe_filter`)
- `exclude_kcb`: drop 科创板 `SH688xxx`
- `exclude_list`: manual blacklist
- `min_price`: price floor
- `exclude_st`: drop ST stocks (name-based via `sector_stocks.json`)
- `exclude_suspended`: drop zero-volume stocks (latest trading day)
- `min_avg_volume` / `avg_volume_window`: N-day average volume floor (default disabled)
- `min_avg_amount` / `avg_amount_window`: N-day average amount floor in CNY (requires `$amount` column, default disabled)

### Signal postprocess options (`signal.postprocess`)
- `daily_transform`: `rank` | `zscore` | `none`
- `industry_neutralize`: subtract same-day sector mean
- `size_neutralize`: OLS residualization against log-market-cap (default disabled, falls back to qlib `$market_cap` if `size_data` not passed)

### Portfolio concentration options (`strategy.portfolio`)
- `max_position_pct`: single-stock weight cap (WARNING logged when exceeded)
- `concentration_hard_limit`: absolute hard cap (ERROR logged when exceeded)

### LGBMAlphaModel bootstrap ensemble (`model.ensemble`)
```yaml
model:
  ensemble:
    enabled: true
    seeds: [42, 123, 2024]
    bagging_fraction: 0.8   # bootstrap sample fraction per member (optional)
```

### SectorFactorEngine ablation flags (`features.factors[name=sector]`)
`include_sector_momentum`, `include_sector_relative`, `include_stock_vs_sector`, `include_sector_reversal`, `include_sector_volatility`, `include_sector_id`, `include_concept`, `include_concept_id`

### Regime factor options (`features.factors[name=regime]`)
- `windows`: list of look-back windows (default `[20, 60]`)
- `dd_window`: rolling high window for drawdown (default `120`)

Outputs: `regime_trend_{w}d`, `regime_vol_{w}d`, `regime_breadth_{w}d`, `regime_corr_{w}d`, `regime_drawdown`, `regime_label` (0=calm_bull, 1=calm_bear, 2=volatile_bull, 3=volatile_bear)

## New Analytical Modules

**Brinson attribution** (`backtest/attribution.py`):
```python
from quant_ex.backtest.attribution import brinson_attribution, format_attribution
result = brinson_attribution(portfolio_weights, benchmark_weights, returns, sector_map)
print(format_attribution(result))  # allocation / selection / interaction by sector
```

**IC decay analysis** (`backtest/signal_diagnostics.py`):
```python
from quant_ex.backtest import compute_ic_decay, compute_rolling_ic
decay = compute_ic_decay(pred, price_data)   # IC at horizons [1,2,3,5,10,15,20]
monitor = compute_rolling_ic(pred, price_data, horizon=5, window=20)
```

**Walk-forward statistical significance**: `summarize()` now returns `sharpe_ttest_pvalue` and `return_ttest_pvalue` columns (one-sample t-test H₀=0). Report table includes `sharpe_p` column.

## Crawler Module

`crawler/eastmoney/` is an independent East Money API SDK (no qlib dependency). Use without proxy — direct connection works; proxy causes empty replies (exit 52).

```bash
python crawler/scripts/fetch_sector_enums.py          # refresh sector_codes.json
python crawler/scripts/fetch_sector_stocks.py --resume # resumable constituent fetch
```

## Data Update

`run_update_qlib_data.py` handles the full pipeline: Dolt clone/pull → SQL server → export → normalize → qlib bin.

Dolt lock troubleshooting:
- `dolt status` creates a stale `.dolt/noms/LOCK` file. The script auto-cleans it when no dolt process is running.
- If a real `dolt sql-server` is still running, kill it first: `pkill -f 'dolt sql-server'`
- To reuse an already-running server: `--reuse-dolt-server`

**Supplementary gap filler** (`--supplement-source akshare|eastmoney`): fetches missing days from akshare/eastmoney before normalization. Lives in `data/sources/gap_filler.py`.

**Calendar lag fix**: fixed in `data/qlib_update/normalize.py` via `NoopNormalize` which overrides `format_data` to avoid qlib dropping the last row due to `"tradedate"` vs `"date"` column name mismatch.

## AI Optimizer

`agent/auto_optimizer.py` uses `claude-opus-4-6` to analyze grid search CSV results and suggest the next parameter grid. Set `ANTHROPIC_API_KEY` env var before using `--optimize`.

## Per-Strategy Signal Configs

Create an override YAML (e.g. `config/daily_csi1000.yaml`) and pass via `--config` to run csi1000-trained models against csi300 evaluation universes with the correct strategy params.

## Scheduled Rebalance Notes

**`start_date` must be earlier than today.** `TopkDropoutStrategy` does not open positions on the very first day of the backtest. Set `start_date` to at least a few trading days before the signal date.

**Following the strategy**: On your first day, execute only the **买入** actions from "次交易日调仓动作". On subsequent days, follow both buy and sell actions.

**Model backward compat**: Old `.pkl` files trained before `SectorFactorEngine` attributes existed (e.g., `include_sector_momentum`) will crash at inference. `SectorFactorEngine._ensure_compat_attrs()` fills missing attributes at `compute()` time. Same for `LGBMAlphaModel._ensure_runtime_defaults()` (covers `bagging_fraction`, `ensemble_seeds`, etc.).
