# AGENTS.md

本文件用于指导 Codex 等代码代理在本仓库中进行后续开发、调试和维护。优先遵守用户的当前请求；当请求没有明确细节时，按本文件和现有代码风格执行。

## 项目定位

`quant_ex` 是基于 qlib + Alpha158 的 A 股量化选股框架，支持：

- 多模型训练：qlib-native `LGBModel` 与自定义 `lgbm` / `xgb` / `ridge` / `lasso` / `mlp`
- 额外因子：技术因子、行业/概念轮动因子、挖掘因子、市场状态感知因子（regime）、北向资金、基本面、质押、融资融券、内部交易、分析师预期、股东户数、分红、估值、资产负债表、业绩预告、机构持仓、回购、机构调研、CSV 自定义因子
- FactorScreener：IC/ICIR 阈值 + 相关性去重，自动过滤低质量因子
- TopkDropout 策略回测、网格搜索、多 seed 稳健性评估
- Walk-forward 时间交叉验证（支持自定义折叠 YAML、t 检验显著性）
- 每日信号生成、目标持仓与买卖差分
- 集中度风险检查、流动性过滤
- 信号后处理：行业中性化、市值中性化
- Brinson 绩效归因（板块级 allocation/selection/interaction）
- IC 衰减分析、滚动 IC 监控
- 多渠道通知推送
- 东方财富数据 SDK 与板块/成分股缓存
- Claude API 驱动的回测参数自动优化
- Web Dashboard：React + FastAPI 本地可视化面板（数据管理、模型训练、回测、信号、因子、配置），支持中英文切换

## 运行环境

- Python 目标版本：`>=3.9`
- 默认解释器：`./.venv/bin/python`（当前为 Python 3.11）
  - 后续代理默认使用项目根目录下的 `.venv` 进行运行、验证、测试和数据脚本执行，除非用户明确要求使用其他环境。
  - 当前项目 `.venv` 已用于训练、回测、akshare 数据补全和轻量验证；不要再默认切换到外部 `Qbot` 环境。
- qlib 数据路径：`/Users/weidian/code/algorithms/investment_data/qlib_data/qlib_bin`
- 安装依赖：`pip install -r requirements.txt` 或 `pip install -e .[dev]`

涉及网络、下载依赖、外部 API、真实推送、真实资金/实盘语义的操作，应先确认用户意图。不要把 API key、通知凭证、账号信息写入入库文件。

## 常用命令

优先使用项目 `.venv` 运行检查：

```bash
./.venv/bin/python -m pytest test/test_universe_filter.py test/test_trainer.py
```

训练：

```bash
python run_train.py --model lgbm --tag baseline
python run_train.py --model lgbm --with-sector --tag sector_full
python run_train.py --qlib-native
python run_train.py --list-registry    # 验证模型/因子注册（含 regime）
```

回测：

```bash
python run_backtest.py --model-path models/lgbm_*.pkl
python run_backtest.py --topk 5,10,15 --n-drop 1,3,5 --seeds
python run_backtest.py --output-csv results/my_run.csv  # 指定输出路径
python run_backtest.py --optimize --n-iters 3
```

Walk-forward：

```bash
python run_walk_forward_validation.py \
  --train-universes csi300,csi800,csi1000 \
  --eval-market csi300 \
  --topk 5,15,20 --n-drop 1,3 --hold-thresh 5,8,10

# 自定义折叠（--folds-config）
python run_walk_forward_validation.py --folds-config config/walk_forward_folds.yaml

# 调整稳健得分权重
python run_walk_forward_validation.py \
  --robust-weights '{"mean_sharpe": 1.0, "sharpe_std": -0.3, "min_sharpe": 0.5, "positive_sharpe_folds": 0.05}'
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

Web Dashboard：

```bash
# 生产模式（单一进程，同时提供 API + 静态前端）
python web/run_web.py                    # http://localhost:8000

# 开发模式（两个终端）
python web/run_web.py                    # 后端 :8000
cd web/frontend && npm run dev           # 前端 :5173（自动代理 /api → :8000）

# 构建前端
cd web/frontend && npm run build         # 输出到 web/frontend/dist/
```

## 目录职责

- `config/`：配置文件。`base.yaml` 放 qlib 路径、市场、训练区间、策略、回测；`model.yaml` 放模型和因子配置；`walk_forward_folds.yaml.example` 是自定义折叠示例；`notify.yaml.example` 是通知配置模板。
- `docs/strategy_log/`：长期保留的策略迭代表格日志。`strategy_iteration_log.csv` 记录策略配置路径、迭代日期、模型、参数、效果指标、结论和下一步对照实验方向；`system_iteration_log.csv` 记录系统级迭代周期（变更内容、基线范围、前后最佳 Sharpe、诊断评分、决策、收敛状态），通过 `strategy_iteration_ids` 列与策略日志关联。这是后续策略比较与 ablation 决策的首选入口。
- `data/`：qlib 数据加载、股票池过滤（含流动性过滤）、行业数据提供；`utils.py` 是统一代码转换+缓存模块。`data/fetchers/`：15 个领域特定 fetcher（BaseDataFetcher 子类），各自缓存到 `cache/<domain>/`，TTL 可配置；入口为 `run_fetch_data.py --type <type>`。
- `features/`：因子基类、注册表、技术因子、行业因子、挖掘因子（含 qlib init 保护）、市场状态因子；12 个 akshare 数据驱动因子（pledge, margin, insider, analyst, shareholder, dividend, valuation, balance_sheet, earnings_guidance, institutional, repurchase, visit）、CSV 自定义因子；`library/` 含 FactorScreener / FactorCleaner / FactorEvaluator。
- `models/`：模型基类、注册表、训练器及各模型实现；训练产物含 `_meta.json` 和 `_feature_importance.json` sidecar。
- `backtest/`：回测引擎、指标（含基准超额/IR/换手率）、网格搜索、信号诊断（IC 衰减/滚动 IC）、Brinson 归因。
- `signals/`：信号生成（含停牌过滤/price_data 复用）、后处理（含市值中性化）。
- `notify/`：通知推送渠道。
- `crawler/`：东方财富 API SDK，应保持独立，不引入 qlib 依赖。
- `agent/`：AI 参数优化器（Claude API）。
- `web/`：Web Dashboard（FastAPI + React）。`web/api/` 是后端路由与服务层；`web/frontend/` 是 React 前端（Vite + TypeScript + Tailwind + react-i18next 中英文切换）。入口 `web/run_web.py`。
- `test/`：pytest 测试。

## 架构与数据流

配置深度合并顺序：

```text
config/base.yaml → config/model.yaml → config/notify.yaml(若存在) → 用户自定义配置(--config)
```

核心数据流：

```text
DataLoader(qlib D.features / DatasetH)
  → UniverseFilter (含流动性过滤)
  → SectorDataProvider(akshare / cache, 并发抓取)
  → Alpha158 + FactorPipeline [technical, sector, mined, regime, northbound, fundamental,
                                pledge, margin, insider, analyst, shareholder, dividend,
                                valuation, balance_sheet, earnings_guidance, institutional,
                                repurchase, visit, csv]
      → (可选) FactorScreener
  → ModelTrainer (qlib-native 或 custom, 支持 bootstrap bagging)
  → BacktestEngine / GridSearchBacktest → AutoOptimizer (Claude)
  → SignalGenerator (price_data 复用, 停牌过滤)
      → postprocess (industry_neutralize / size_neutralize)
      → 集中度风险检查
  → NotificationPusher
  → (可选) brinson_attribution
```

两种训练模式：
- `--qlib-native`：qlib 原生 `LGBModel`，MLflow recorder 追踪。训练后把 Recorder ID 写入 `config/base.yaml → experiment.latest_recorder_id`。
- 默认 custom 模式：注册表模型，保存为 `models/*.pkl`，后续通过 `--model-path` 使用。

## 开发约定

### 注册表模式

新增模型：继承 `BaseAlphaModel`，实现 `fit()` / `predict()`，`@ModelRegistry.register("name")`，放在 `models/`。

新增因子：继承 `BaseFactor`，实现 `compute(price_data) → DataFrame`（必须返回 `(instrument, datetime)` MultiIndex），`@FactorRegistry.register("name")`，放在 `features/`，在 `config/model.yaml → features.factors` 中添加配置项，并在 `models/trainer.py` 的 `importlib` 循环中注册模块名。

验证注册：

```bash
python run_train.py --list-registry
```

预期因子注册包含：`sector, technical, mined, regime, northbound, fundamental, pledge, margin, insider, analyst, shareholder, dividend, valuation, balance_sheet, earnings_guidance, institutional, repurchase, visit, csv`

### 因子流水线与 FactorScreener

`FactorPipeline.from_config()` 支持 `screener_config` 参数，自动构建 `FactorScreener`：

```python
pipeline = FactorPipeline.from_config(
    factor_configs,
    screener_config={"min_ic": 0.02, "min_icir": 0.3, "max_corr": 0.7},
)
kept = pipeline.compute_with_screening(price_data, forward_returns=label)
```

`pipeline.compute_with_cleaning(price_data, cleaner)` 用于后期清洗（winsorize/standardize）。

### 新增能力的模块位置

| 能力 | 模块 |
|---|---|
| 市场状态感知因子 | `features/regime_features.py` |
| 因子质量过滤 | `features/library/screener.py` |
| 市值中性化 | `signals/postprocess.neutralize_by_size()` |
| 流动性过滤 | `data/universe.UniverseFilter` (`min_avg_volume`/`min_avg_amount`) |
| 集中度风险检查 | `run_daily._check_concentration()` |
| Brinson 绩效归因 | `backtest/attribution.brinson_attribution()` |
| IC 衰减分析 | `backtest/signal_diagnostics.compute_ic_decay()` |
| 滚动 IC 监控 | `backtest/signal_diagnostics.compute_rolling_ic()` |
| Walk-forward 统计显著性 | `run_walk_forward_validation.summarize()` (`sharpe_ttest_pvalue`) |
| 自定义折叠 YAML | `run_walk_forward_validation.load_folds()` |
| Bootstrap Bagging | `LGBMAlphaModel(bagging_fraction=0.8)` |
| 质押因子 | `features/pledge_factor.py` (pledge_ratio 等) |
| 融资融券因子 | `features/margin_factor.py` (margin_balance, short_balance 等) |
| 内部交易因子 | `features/insider_factor.py` (insider_net_buy_pct 等) |
| 分析师预期因子 | `features/analyst_factor.py` (analyst_coverage, buy_rating_ratio 等) |
| 股东户数因子 | `features/shareholder_factor.py` (sh_count, sh_count_chg_pct 等) |
| 分红因子 | `features/dividend_factor.py` (div_yield_ttm, div_consistency 等) |
| 估值因子 | `features/valuation_factor.py` (pe_ttm, pb, market_cap 等) |
| 资产负债表因子 | `features/balance_sheet_factor.py` (leverage_ratio, current_ratio 等) |
| 业绩预告因子 | `features/earnings_guidance_factor.py` (guidance_type, earnings_surprise_pct 等) |
| 机构持仓因子 | `features/institutional_factor.py` (fund_hold_count, qfii_hold_flag 等) |
| 回购因子 | `features/repurchase_factor.py` (repurchase_completion_pct, repurchase_active 等) |
| 机构调研因子 | `features/visit_factor.py` (visit_count_{w}d, visit_count_chg 等) |
| CSV 自定义因子 | `features/csv_factor.py` (从 CSV 文件加载自定义因子) |
| 系统迭代日志 | `docs/strategy_log/system_iteration_log.csv` (全系统迭代周期记录) |

### 向后兼容原则

- 新增参数必须有 `None` 或合理默认值，不破坏现有代码。
- 旧 `.pkl` 模型通过 `__setstate__` + `_ensure_runtime_defaults()` 补全缺失属性。
- 新配置项默认不启用（如 `size_neutralize: false`, `min_avg_volume: null`）。

### 配置与敏感文件

- 不要提交 `config/notify.yaml`、`.env`、`config/local*.yaml`、`config/secret*.yaml`。
- qlib 数据路径当前是本机绝对路径；通用化改动应说明兼容影响。

### 策略迭代记录约定

- 新增或修改长期保留的策略配置时，不要只改 `config/*.yaml` 或 `config/strategy_candidates.yaml`。
- 必须同步更新 `docs/strategy_log/strategy_iteration_log.csv`，记录：配置路径、迭代日期、模型路径、训练/评估股票池、核心参数、关键效果指标、结论、下一步 ablation 方向。
- 该 CSV 按时间追加维护，用于后续代理快速选择比较对象和实验优先级。
- 临时调试、不准备保留的参数试跑不要写入；只有“值得后续比较或复用”的策略版本才入表。

### 生成产物（默认不应提交）

- 默认不提交：`logs/`, `mlruns/`, `mlartifacts/`, `qlib_workflow/`, `backtest_results/`, `optimization_results/`, `signals/*.txt`, `*.pkl`, `*.joblib`
- `cache/` 采用白名单策略，不要把它当成“整个目录都不提交”处理。
- 当前允许入库的 cache 仅有：`cache/financial/*.csv`、`cache/northbound/*.csv`、`cache/sector_map.json`
- 其他 cache 产物默认继续忽略；如需新增可入库缓存，必须同步更新 `.gitignore` 与 `cache/README.md`

## 测试与验证

- 配置、注册表、轻量逻辑改动：运行相关单测和 `--list-registry`。
- 模型、因子、数据集改动：导入检查 + 针对性单测；依赖 qlib 数据时使用本机路径运行最小脚本。
- 回测、信号、通知改动：优先 `--dry-run` 或小参数验证，避免真实推送。
- 爬虫改动：优先测试 SDK 构造、解析和缓存；真实请求需用户允许。

```bash
./.venv/bin/python -m pytest test/test_universe_filter.py test/test_trainer.py
./.venv/bin/python run_train.py --list-registry
```

## 代码风格

- 默认按项目 `.venv` 的 Python 3.11 运行与验证；若涉及版本兼容性讨论或修改，应明确说明影响范围。
- 遵循现有模块风格：类型注解、简洁 docstring、清晰日志。
- 优先使用项目已有工具：`utils.config.load_config`、`utils.logger.setup_logger`、注册表、数据加载器和基类。
- 不做无关重构，不批量格式化未触及文件。
- 处理日期、股票代码、MultiIndex 时要谨慎，避免隐式改变 qlib 期望格式。

## Web Dashboard 开发约定

### 后端（FastAPI）

- `web/api/routers/` 中所有项目模块导入使用 `from quant_ex.xxx import yyy`（非裸 `from models.xxx`），因为 `app.py` 的 `sys.path` 同时包含项目根目录和其父目录以支持 quant_ex 包内的相对导入。
- 新增 API 端点在对应 router 文件中添加，按 RESTful 风格组织。
- 后台任务使用 `TaskManager.start_sync_task()` 包装阻塞函数，前端通过 SSE 流获取进度。
- 路径常量定义在 `web/api/deps.py`（MODELS_DIR、CACHE_DIR 等）。

### 前端（React 19 + TypeScript）

- 页面组件放在 `web/frontend/src/pages/`，每个页面对应一个 tabbed 布局。
- API 调用统一通过 `src/api/client.ts` 的 `get`/`post`/`put`/`del` 函数。
- SSE 流使用 `src/hooks/useSSE.ts` hook。
- 国际化使用 `react-i18next`，翻译文件在 `src/i18n/`（en.json / zh.json），切换组件为 `LanguageToggle`。
- 共享 UI 组件放在 `src/components/`。
- 使用 Tailwind CSS 类，不引入额外 CSS 框架。

### 验证

- 后端验证：`.venv/bin/python -c "from web.api.app import app; print('OK')"`
- API 端点测试：使用 `fastapi.testclient.TestClient`
- 前端验证：`cd web/frontend && npm run build`

## 东方财富 Crawler

`crawler/eastmoney/` 是独立 SDK，不依赖 qlib。**直连可用，代理可能导致空回复（exit 52）**。

```bash
python crawler/scripts/fetch_sector_enums.py
python crawler/scripts/fetch_sector_stocks.py --resume
```

修改 crawler 时，不要让主训练/回测路径强依赖实时网络；继续支持缓存/离线数据。

## AI 优化器

`agent/auto_optimizer.py` 使用 Anthropic API 分析网格搜索 CSV 并建议下一轮参数。运行前需要 `ANTHROPIC_API_KEY`。

```bash
python run_backtest.py --optimize --n-iters 3
```

## 协作注意事项

- 可能存在用户本地未提交改动。编辑前先查看相关文件，避免覆盖用户工作。
- 不要删除或回滚用户改动，除非用户明确要求。
- 对 notebook、模型文件、缓存和回测结果保持克制；需要修改或清理时先说明原因。
- 如果必须运行耗时训练、完整回测、联网爬取或真实通知，先征得用户同意。
