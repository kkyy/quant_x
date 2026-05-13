# AGENTS.md

本文件给 Codex 等代码代理提供**高信号操作规约**。完整项目说明见 `README.md`；设计和长期结论见 `docs/` 与 `docs/strategy_log/`。

## 项目定位

`quant_ex` 是基于 qlib + Alpha158 的 A 股低频量化选股研究框架，用于训练模型、回测/WFV、生成每日信号、辅助调仓和沉淀研究结论。它是研究与辅助决策系统，不是自动实盘交易系统。

核心链路：

```text
DataLoader / UniverseFilter
  -> Alpha158 + FactorPipeline
  -> ModelTrainer
  -> BacktestEngine / GridSearch / WFV
  -> SignalGenerator / postprocess / regime switch
  -> run_scheduled_rebalance
```

新增 agent 层位于 `agent/strategy_iteration/`：多角色 planner、prompt/context/trace、命令审批模板、回测/WFV feedback 回灌。Agent 只辅助研究设计和审议，不直接绕过训练、回测、WFV 和审批边界。

## 运行环境

- 默认 Python：`./.venv/bin/python`。不要默认切换外部环境。
- qlib 数据路径：`/Users/weidian/code/algorithms/investment_data/qlib_data/qlib_bin`，本仓库也常用 `./qlib_data/qlib_bin`。
- 涉及网络、下载依赖、外部 API、真实推送、数据更新、完整 WFV、真实资金/实盘语义时，先确认用户意图。
- 不要把 API key、通知凭证、账号信息写入可提交文件。

## 常用命令

```bash
# 轻量验证
./.venv/bin/python -m pytest test/test_agent_strategy_iteration.py test/test_web_dashboard.py
./.venv/bin/python run_train.py --list-registry

# 训练
./.venv/bin/python run_train.py --model lgbm --tag baseline
./.venv/bin/python run_train.py --config path/to/override.yaml --model lgbm --tag my_tag

# 回测
./.venv/bin/python run_backtest.py --model-path models/lgbm_xxx.pkl --market csi300 \
  --topk 15 --n-drop 3 --hold-thresh 8 --output-csv backtest_results/my.csv

# WFV
./.venv/bin/python run_walk_forward_validation.py \
  --train-universes csi300,csi800,csi1000 --eval-market csi300 \
  --topk 5,15,20 --n-drop 1,3 --hold-thresh 5,8,10

# 每日/调仓，真实推送前优先 dry-run
./.venv/bin/python run_daily.py --model-path models/lgbm_xxx.pkl --dry-run
./.venv/bin/python run_scheduled_rebalance.py --config config/daily_csi1000.yaml --dry-run

# Agent 策略迭代
./.venv/bin/python run_agent_strategy_iteration.py --objective "..." --no-llm \
  --propose-actions --write-approval-template
./.venv/bin/python run_agent_strategy_iteration.py --objective "..." --use-llm \
  --propose-actions --write-approval-template
./.venv/bin/python run_agent_strategy_iteration.py --feedback-run-id RUN \
  --result-csv result.csv --control-csv control.csv --result-kind backtest \
  --rank-metric information_ratio

# Web
./.venv/bin/python web/run_web.py
cd web/frontend && npm run build
```

## 先读哪里

- 策略候选：`config/strategy_candidates.yaml`
- 策略级历史：`docs/strategy_log/strategy_iteration_log.csv`
- 系统级历史：`docs/strategy_log/system_iteration_log.csv`
- Agent 设计/实施：`docs/agent_strategy_iteration_design_2026-05-13.md`、`docs/agent_strategy_iteration_implementation_plan_2026-05-13.md`
- Agent memory：`docs/strategy_log/agent_memory.md`
- Web 约定：`README.md`、`web/frontend/README.md`

## 策略研究规则

- 新增长期候选或明确迭代结论时，追加 `docs/strategy_log/strategy_iteration_log.csv`；临时调试不入表。
- 比较策略必须写清：benchmark、`rank_metric`、`deal_price`、成本/滑点、训练股票池、评估股票池、topk/n_drop/hold_thresh、是否启用 SVS overlay。
- 主回测链路默认带 benchmark，并以 `information_ratio` 排序；不要混用 Sharpe-only 与 IR-ranked 结论。
- `config/strategy_candidates.yaml` 是研究结论索引，不是运行时自动加载配置。
- 近期稳定对照臂：`csi1000_balanced` / `adaptive_baseline_wf`。SVS overlay 是放大器，不是默认稳定 alpha；推广必须有 WFV 证据。
- 2026-05-13 完整 agent→训练→回测→feedback 闭环已跑通：`full_agent_train_backtest_20260513` strict csi1000 重训候选 Sharpe `1.2490`、IR `0.5774`，弱于同参数 control，feedback 为 `reject/refuted`。该 run 验证通路，不推广策略。
- 注意：`config/daily_csi1000.yaml` 当前 `market.name` 实际为 `csi300`。strict csi1000 训练必须显式检查 override，并核对模型 `_meta.json`。

## 开发约定

- 遵循现有注册表模式：模型继承 `BaseAlphaModel` + `@ModelRegistry.register`；因子继承 `BaseFactor` + `@FactorRegistry.register`。
- 新参数必须有默认值且向后兼容；旧 `.pkl` 兼容逻辑放在 `__setstate__` / `_ensure_runtime_defaults()` 一类入口。
- 回测、信号、通知改动优先 dry-run 或小参数验证。
- 处理日期、股票代码、MultiIndex 时谨慎，避免改变 qlib 期望格式。
- Web 后端项目导入使用 `from quant_ex.xxx import yyy`。耗时接口走 `TaskManager.start_sync_task()` + SSE，不阻塞 request。
- 前端文本改动同步 `web/frontend/src/i18n/en.json` 和 `zh.json`。

## 敏感与生成产物

不要提交：

- `config/notify.yaml`
- `config/agent_strategy_iteration.yaml`
- `.env`、`config/local*.yaml`、`config/secret*.yaml`
- `docs/strategy_log/agent_runs/`
- `logs/`、`mlruns/`、`mlartifacts/`、`qlib_workflow/`
- `backtest_results/`、`optimization_results/`
- `signals/*.txt`、`*.pkl`、`*.joblib`
- `web/frontend/dist/`、`node_modules/`、`__pycache__/`

`cache/` 是白名单策略：当前允许入库的只有 `cache/financial/*.csv`、`cache/northbound/*.csv`、`cache/sector_map.json`。

## 协作底线

- 可能存在用户本地未提交改动。编辑前看相关文件，不要覆盖用户工作。
- 不要删除或回滚用户改动，除非用户明确要求。
- 不要批量格式化无关文件。
- 对模型、缓存、notebook、回测结果保持克制；修改或清理前说明原因。
