# CLAUDE.md

This file is the compact operating guide for Claude Code in this repo. Use `README.md` and `docs/` for full background.

## Project

`quant_ex` is a qlib + Alpha158 A-share low-frequency stock-selection research framework. It supports model training, backtesting, walk-forward validation, daily signals, scheduled rebalance reports, Web Dashboard workflows, and a lightweight multi-role agent strategy iteration layer.

It is a research and decision-support system, not an automatic live-trading system.

## Environment

- Use `./.venv/bin/python` by default.
- Do not switch to an external Python environment unless explicitly asked.
- qlib data path is normally `/Users/weidian/code/algorithms/investment_data/qlib_data/qlib_bin` or the repo-local `./qlib_data/qlib_bin`.
- Ask before network crawls, dependency installs, qlib data updates, full WFV, real notifications, or anything trading-like.
- Never write API keys, notify credentials, or account secrets into committed files.

## Core Commands

```bash
# quick validation
./.venv/bin/python -m pytest test/test_agent_strategy_iteration.py test/test_web_dashboard.py
./.venv/bin/python run_train.py --list-registry

# train
./.venv/bin/python run_train.py --model lgbm --tag baseline
./.venv/bin/python run_train.py --config path/to/override.yaml --model lgbm --tag my_tag

# backtest
./.venv/bin/python run_backtest.py --model-path models/lgbm_xxx.pkl --market csi300 \
  --topk 15 --n-drop 3 --hold-thresh 8 --output-csv backtest_results/my.csv

# walk-forward
./.venv/bin/python run_walk_forward_validation.py \
  --train-universes csi300,csi800,csi1000 --eval-market csi300 \
  --topk 5,15,20 --n-drop 1,3 --hold-thresh 5,8,10

# daily / scheduled rebalance; prefer dry-run first
./.venv/bin/python run_daily.py --model-path models/lgbm_xxx.pkl --dry-run
./.venv/bin/python run_scheduled_rebalance.py --config config/daily_csi1000.yaml --dry-run

# agent strategy iteration
./.venv/bin/python run_agent_strategy_iteration.py --objective "..." --no-llm \
  --propose-actions --write-approval-template
./.venv/bin/python run_agent_strategy_iteration.py --objective "..." --use-llm \
  --propose-actions --write-approval-template
./.venv/bin/python run_agent_strategy_iteration.py --feedback-run-id RUN \
  --result-csv result.csv --control-csv control.csv --result-kind backtest \
  --rank-metric information_ratio

# web
./.venv/bin/python web/run_web.py
cd web/frontend && npm run build
```

## Where To Look First

- Current candidates: `config/strategy_candidates.yaml`
- Durable strategy log: `docs/strategy_log/strategy_iteration_log.csv`
- System iteration log: `docs/strategy_log/system_iteration_log.csv`
- Agent memory: `docs/strategy_log/agent_memory.md`
- Agent design/implementation: `docs/agent_strategy_iteration_design_2026-05-13.md`, `docs/agent_strategy_iteration_implementation_plan_2026-05-13.md`
- Full project docs: `README.md`

## Architecture Map

```text
DataLoader / UniverseFilter
  -> Alpha158 + FactorPipeline
  -> ModelTrainer
  -> BacktestEngine / GridSearch / WFV
  -> SignalGenerator / postprocess / regime switch
  -> run_scheduled_rebalance

agent/strategy_iteration/
  -> multi-role planner
  -> prompt/context/role traces
  -> command proposals + approval templates
  -> backtest/WFV feedback + memory

web/
  -> FastAPI backend + React frontend
  -> Agent Runs page can browse/create runs and regenerate approval templates
```

## Research Rules

- Keep comparisons controlled: same benchmark, rank metric, deal price, costs/slippage, train universe, eval universe, and strategy params unless that axis is the experiment.
- Main backtest ranking uses `information_ratio`; do not mix Sharpe-only and IR-ranked conclusions without saying so.
- Durable strategy conclusions go into `docs/strategy_log/strategy_iteration_log.csv`. Temporary debug runs stay in generated result folders.
- `config/strategy_candidates.yaml` is a research index, not auto-loaded runtime config.
- Current stable control: `csi1000_balanced` / `adaptive_baseline_wf`.
- SVS overlay is a possible amplifier, not a default stable alpha. Promote only with WFV evidence.
- Latest full-cycle agent result, 2026-05-13: `full_agent_train_backtest_20260513` proved the agent -> train -> backtest -> feedback path works, but the strict csi1000 retrain candidate was rejected: Sharpe `1.2490`, IR `0.5774`, weaker than same-parameter control.
- Important config trap: `config/daily_csi1000.yaml` currently has `market.name: csi300`. For strict csi1000 training, create/check an override with `market.name: "csi1000"` and verify the saved model `_meta.json`.

## Development Rules

- Models use `BaseAlphaModel` + `@ModelRegistry.register`.
- Factors use `BaseFactor` + `@FactorRegistry.register` and return `(instrument, datetime)` MultiIndex DataFrames.
- New config options default to safe/disabled values and must not break old models.
- Preserve pickle compatibility through `__setstate__` / `_ensure_runtime_defaults()` patterns.
- For Web backend imports, use `from quant_ex.xxx import yyy`.
- Long-running Web operations should use `TaskManager.start_sync_task()` and SSE.
- Frontend text changes must update both `web/frontend/src/i18n/en.json` and `zh.json`.
- Do not batch-format unrelated files.

## Do Not Commit

- `config/notify.yaml`
- `config/agent_strategy_iteration.yaml`
- `.env`, `config/local*.yaml`, `config/secret*.yaml`
- `docs/strategy_log/agent_runs/`
- `backtest_results/`, `optimization_results/`, `logs/`
- `models/*.pkl`, `*.joblib`
- `signals/*.txt`
- `web/frontend/dist/`, `node_modules/`, `__pycache__/`

`cache/` is allow-listed. Currently tracked cache should be limited to `cache/financial/*.csv`, `cache/northbound/*.csv`, and `cache/sector_map.json`.

## Collaboration

- Expect local uncommitted user changes. Inspect before editing.
- Never revert or delete user changes unless explicitly asked.
- Be conservative with models, notebooks, cache, and generated results.
- If a task is high-impact or expensive, explain the next action before running it.
