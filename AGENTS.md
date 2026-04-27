# AGENTS.md

本文件用于指导 Codex 等代码代理在本仓库中进行后续开发、调试和维护。优先遵守用户的当前请求；当请求没有明确细节时，按本文件和现有代码风格执行。

## 项目定位

`quant_ex` 是基于 qlib + Alpha158 的 A 股量化选股框架，支持：

- 多模型训练：qlib-native `LGBModel` 与自定义 `lgbm` / `xgb` / `ridge` / `lasso`
- 额外因子：技术因子、行业/概念轮动因子、挖掘因子
- TopkDropout 策略回测、网格搜索、多 seed 稳健性评估
- 每日信号生成、目标持仓与买卖差分
- 多渠道通知推送
- 东方财富数据 SDK 与板块/成分股缓存
- Claude API 驱动的回测参数自动优化

## 运行环境

- Python 目标版本：`>=3.9`
- 本机推荐解释器：`/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9`
  - 该环境已包含 pandas、numpy、sklearn、lightgbm 等常用依赖，适合做语法、导入和轻量测试。
  - 当前项目自己的 `.venv` 可能未安装完整依赖，不要默认依赖它。
- qlib 数据路径：`/Users/weidian/code/algorithms/investment_data/qlib_data/qlib_bin`
- 安装依赖：
  - `pip install -r requirements.txt`
  - 或开发安装：`pip install -e .[dev]`

涉及网络、下载依赖、外部 API、真实推送、真实资金/实盘语义的操作，应先确认用户意图。不要把 API key、通知凭证、账号信息写入入库文件。

## 常用命令

优先使用上述推荐 Python 解释器运行检查，例如：

```bash
/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -m pytest test/test_universe_filter.py test/test_trainer.py
```

训练：

```bash
python run_train.py --model lgbm --tag baseline
python run_train.py --model lgbm --with-sector --tag sector_full
python run_train.py --qlib-native
python run_train.py --list-registry
```

回测：

```bash
python run_backtest.py --model-path models/lgbm_*.pkl
python run_backtest.py --topk 5,10,15 --n-drop 1,3,5 --seeds
python run_backtest.py --optimize --n-iters 3
```

每日信号：

```bash
python run_daily.py --model-path models/lgbm_*.pkl --dry-run
python run_daily.py --account 500000 --positions SH600000:500,SZ000001:300
```

因子挖掘：

```bash
python run_factor_mining.py --min-ic 0.03 --min-icir 0.4 --top-n 30
```

格式与静态检查：

```bash
ruff check .
black .
```

## 目录职责

- `config/`：配置文件。`base.yaml` 放 qlib 路径、市场、训练区间、实验、路径、回测/策略等基础配置；`model.yaml` 放模型和因子配置；`notify.yaml.example` 是通知配置模板。
- `data/`：qlib 数据加载、股票池过滤、行业数据提供。
- `features/`：因子注册、技术因子、行业因子、挖掘因子。
- `models/`：模型基类、注册表、训练器及各模型实现；也可能存在模型元信息文件。
- `backtest/`：回测引擎、指标、网格搜索。
- `signals/`：信号生成逻辑；运行后可能输出信号文件，注意不要提交生成产物。
- `notify/`：通知推送通道。
- `crawler/`：东方财富 API SDK、脚本和缓存数据；该模块应尽量保持独立，不引入 qlib 依赖。
- `agent/`：AI 参数优化器。
- `test/`：pytest 测试。
- `notebooks/`：实验笔记本；`.gitignore` 默认忽略 `*.ipynb`，编辑前注意用户是否有未提交的本地实验内容。

## 架构与数据流

配置深度合并顺序以代码为准：

```text
config/base.yaml -> config/model.yaml -> config/strategy.yaml(若存在，兼容旧版) -> config/notify.yaml(若存在) -> 用户自定义配置
```

当前仓库通常不再使用 `strategy.yaml`，策略、回测和股票池过滤参数应优先维护在 `config/base.yaml`。

核心数据流：

```text
DataLoader(qlib D.features / DatasetH)
  -> UniverseFilter
  -> SectorDataProvider(akshare / cache)
  -> Alpha158 + FactorPipeline
  -> ModelTrainer(qlib-native 或 custom)
  -> BacktestEngine / GridSearchBacktest
  -> SignalGenerator
  -> NotificationPusher
```

两种训练模式：

- `--qlib-native`：使用 qlib 原生 `LGBModel`，通过 MLflow recorder 追踪。训练完成后把 Recorder ID 写入 `config/base.yaml` 的 `experiment.latest_recorder_id`。
- 默认 custom 模式：使用项目内模型注册表，模型保存为 `models/*.pkl`，后续通过 `--model-path` 传给回测或每日信号脚本。

## 开发约定

### 注册表模式

新增模型：

- 放在 `models/`
- 继承 `BaseAlphaModel`
- 实现 `fit()` 和 `predict()`
- 使用 `@ModelRegistry.register("name")`
- 保持模型配置位于 `config/model.yaml` 对应小节

新增因子：

- 放在 `features/`
- 继承 `BaseFactor`
- 实现 `compute(price_data) -> DataFrame`
- 返回结果必须使用 `(instrument, datetime)` MultiIndex
- 使用 `@FactorRegistry.register("name")`
- 在 `config/model.yaml -> model.features.factors` 中添加配置项

`ModelTrainer.__init__` 会通过 `importlib` 自动导入模型和因子模块。新增文件后优先用 `python run_train.py --list-registry` 验证注册是否生效。

### 因子流水线

`FactorPipeline` 从配置列表构建，每个 entry 的 `name` 必须匹配因子注册 key，其余字段作为因子构造参数传入。

行业因子需要同时满足：

- CLI 使用 `--with-sector`
- `config/model.yaml -> model.features.factors` 中启用 `sector` entry

挖掘因子需要先运行 `run_factor_mining.py` 生成 `cache/mined_factors.json`，再启用 `mined` entry。

### 配置与敏感文件

- 不要提交 `config/notify.yaml`、`.env`、`config/local*.yaml`、`config/secret*.yaml`。
- 修改默认配置时，优先保持样例可运行，不要把个人凭证、临时绝对输出路径或一次性实验参数写死。
- qlib 数据路径当前是本机绝对路径；若为通用化改动，应说明兼容影响。

### 生成产物

以下目录/文件通常是运行产物或缓存，不应作为普通代码改动提交：

- `cache/`
- `logs/`
- `mlruns/`
- `mlartifacts/`
- `qlib_workflow/`
- `backtest_results/`
- `optimization_results/`
- `signals/*.txt`
- `*.pkl` / `*.joblib`

## 测试与验证

修改后按风险选择验证范围：

- 配置、注册表、轻量逻辑改动：运行相关单测和 `--list-registry`。
- 模型、因子、数据集构建改动：至少做导入检查和针对性单测；如依赖 qlib 数据，使用本机 qlib 路径运行最小脚本。
- 回测、信号、通知改动：优先使用 `--dry-run` 或小范围参数验证，避免真实推送。
- 爬虫改动：优先测试 SDK 参数构造、解析和缓存逻辑；真实请求需要用户允许或明确请求。

建议基础检查：

```bash
/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -m pytest test/test_universe_filter.py test/test_trainer.py
/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 run_train.py --list-registry
```

## 代码风格

- 保持 Python 3.9 兼容。
- 遵循现有模块风格：类型注解、简洁 docstring、清晰日志。
- 优先使用项目已有工具：`utils.config.load_config`、`utils.logger.setup_logger`、注册表、数据加载器和基类。
- 不做无关重构，不批量格式化未触及文件。
- 处理日期、股票代码、MultiIndex 时要谨慎，避免隐式改变 qlib 期望格式。
- 对外部服务、文件缓存、模型文件和回测输出增加错误处理时，保持失败信息可读且便于定位。

## 东方财富 Crawler

`crawler/eastmoney/` 是独立 SDK，不依赖 qlib。直接连接通常可用，代理可能导致空回复。

刷新缓存：

```bash
python crawler/scripts/fetch_sector_enums.py
python crawler/scripts/fetch_sector_stocks.py --resume
```

修改 crawler 时，尽量不要让主训练/回测路径强依赖实时网络；应继续支持缓存或离线数据。

## AI 优化器

`agent/auto_optimizer.py` 使用 Anthropic API 分析网格搜索 CSV 并建议下一轮参数。运行 `run_backtest.py --optimize` 前需要 `ANTHROPIC_API_KEY`。

除非用户明确要求，不要改写优化器使用的模型、密钥读取方式或把密钥写入配置文件。

## 协作注意事项

- 可能存在用户本地未提交改动。编辑前先查看相关文件，避免覆盖用户工作。
- 不要删除或回滚用户改动，除非用户明确要求。
- 对 notebook、模型文件、缓存和回测结果保持克制；需要修改或清理时先说明原因。
- 如果必须运行耗时训练、完整回测、联网爬取或真实通知，先征得用户同意。
