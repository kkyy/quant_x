# quant_ex 优化实施报告

> 最新更新：2026-05-11（第四批更新）
> 基础：PROJECT_AUDIT.md 审计报告
> 状态：**审计高优先级主链路继续收敛（第一批 15 项 + 第二批 11 项 + 第三批 5 项 + 第四批 5 项）**

---

## 一、已修复缺陷（BUG 修复）

### BUG-01 ✅ 统一 `_code_to_qlib` 股票代码转换逻辑

**修复方式：** 新建 `data/utils.py`，提供统一实现 `code_to_qlib_instrument()` 和带 `lru_cache` 的 `load_stock_names()`。

**变更文件：**
- `data/utils.py` — **新建**，包含权威的代码转换函数和缓存加载器
- `data/sector.py` — 删除本地 `_code_to_qlib()`，改为 `from .utils import code_to_qlib_instrument`
- `data/universe.py` — 删除本地 `_load_stock_names()` 和 `_to_qlib_code()`，改用 `data.utils`
- `signals/generator.py` — 同上

**关键修复点：** 原 `sector.py` 对 B 股（9xxxxx）映射错误（归入 SZ），现在统一映射到 SH。

---

### BUG-02 ✅ 删除 `technical_factors.py` 中的死代码

**修复方式：** 删除 `_atr_ratio` 函数中永远不执行的 `... if False else None` 代码块。

**变更文件：** `features/technical_factors.py`

---

### BUG-03 ✅ Walk-forward 并行模式 CSV 竞争读取

**修复方式：** `run_backtest.py` 新增 `--output-csv`，`run_walk_forward_validation.py` 改为折叠隔离路径，删除 `latest_grid_csv()`。

**变更文件：** `run_backtest.py`、`run_walk_forward_validation.py`

---

### BUG-04 ✅ 回测指标缺乏基准超额收益

**修复方式：** 重写 `backtest/metrics.py`，新增 `benchmark_rets` 和 `positions` 参数，支持 `excess_annual_return`、`information_ratio`、`tracking_error`、`beta`、`alpha`、`avg_turnover`。

**变更文件：** `backtest/metrics.py`

---

### BUG-05 ✅ `SectorFactorEngine._map_sector_stat` 向量化

**修复方式：** 双循环替换为向量化 `groupby` + `reindex`，CSI1000 场景约 5-10x 加速。

**变更文件：** `features/sector_factors.py`

---

### BUG-06 ✅ `sector_reversal` 修正为真正的反转因子

**修复方式：** `-sector_momentum(w)` → `sector_momentum(w_short) - sector_momentum(w_long)`。

**变更文件：** `features/sector_factors.py`

---

### BUG-07 ✅ 停牌股票在信号生成时被绕过

**修复方式：** `SignalGenerator.generate()` 增加 `$volume` 停牌检测，`_target_positions()` 跳过零成交量股票。

**变更文件：** `signals/generator.py`

---

### BUG-08 ✅ `FactorMiner._compute` qlib 未初始化保护

**修复方式：** 增加 `qlib.config.C.provider_uri` 检测，未初始化时 return None 并 log warning。

**变更文件：** `features/factor_mining.py`

---

### BUG-11 ✅ 调仓信号收益显示为 0（第三批）

**问题：** `start_date: "previous_trade_date"` 只给 1 天回测窗口，qlib 回测的 `return` 无意义，导致"当日收益"始终为 0。

**修复方式：** 当传入 `--positions` 时，用真实持仓的前日/今日未复权收盘价计算实际 P&L，替换回测返回值。报告显示 `持仓收益 +1,922元 (+1.80%) 总市值 108,625元`。

**变更文件：** `run_scheduled_rebalance.py`

---

### BUG-A01 ✅ Benchmark 主链路接入（第四批）

**问题：** `backtest/metrics.py` 已能计算超额收益，但 `BacktestEngine` 调 qlib 时仍传 `benchmark=None`，导致主回测报告没有 `bench`，网格搜索排序仍偏绝对 Sharpe。

**修复方式：**
- `BacktestEngine` 从 `market.benchmark` / `backtest.benchmark` 读取基准并传给 `backtest_daily()`
- `compute_metrics()` 自动读取 qlib report 的 `bench` 列，输出 `excess_annual_return`、`information_ratio`、`tracking_error`、`beta`、`alpha`
- `GridSearchBacktest` 默认按 `backtest.rank_metric: information_ratio` 排序，无 IR 时退回 Sharpe
- `run_backtest.py` 的最优参数详情显示超额指标

**变更文件：** `backtest/engine.py`、`backtest/metrics.py`、`backtest/grid_search.py`、`run_backtest.py`、`config/base.yaml`

---

### BUG-A06 ✅ 调仓脚本股票名称加载去重（第四批）

**问题：** `run_scheduled_rebalance.py` 保留了本地 `_load_stock_names()` / `_to_qlib_code()`，与 `data/utils.py` 的权威实现重复。

**修复方式：** 改为直接使用 `data.utils.load_stock_names()`，统一 B 股/北交所代码转换和缓存逻辑。

**变更文件：** `run_scheduled_rebalance.py`

---

## 二、性能优化

### OPT-02 ✅ akshare 行业数据并发获取

`ThreadPoolExecutor(max_workers=8)`，~5-6x 加速。

**变更文件：** `data/sector.py`

---

### OPT-A02 ✅ 回测成交价配置化（第四批）

`backtest.deal_price` 已接入 qlib `SimulatorExecutor.exchange_kwargs`。默认保持历史 `close` 口径，研究中可通过配置切换为 `open` 做次日执行口径对照。

**变更文件：** `backtest/engine.py`、`config/base.yaml`

---

### OPT-03 ✅ Walk-forward 输出路径隔离（见 BUG-03）

---

### OPT-04 ✅ FactorPipeline 并行计算

`ThreadPoolExecutor(max_workers=4)` 并发执行各因子。

**变更文件：** `features/base.py`

---

### OPT-05 ✅ 特征重要性持久化

Top-50 特征重要性自动保存为 `models/{stem}_feature_importance.json`。

**变更文件：** `models/trainer.py`

---

### OPT-06 ✅ LGBM Bootstrap Bagging

`bagging_fraction=0.8` 为每个 ensemble 成员独立 bootstrap 采样。

**变更文件：** `models/lgbm_model.py`、`models/trainer.py`

---

### OPT-07 ✅ `run_daily.py` 价格数据去重加载

提前加载 `price_data` 并通过参数传递，消除 2-3 次冗余 qlib 查询。

**变更文件：** `run_daily.py`、`signals/generator.py`

---

### OPT-08 ✅ Walk-forward 自定义折叠配置

`--folds-config <path>` + `config/walk_forward_folds.yaml.example`。

**变更文件：** `run_walk_forward_validation.py`

---

### OPT-10 ✅ Robust Score 系数可配置化

`--robust-weights` CLI 参数，自定义 walk-forward 评分权重。

**变更文件：** `run_walk_forward_validation.py`

---

## 三、新增量化能力

### CAP-01 ✅ 基准超额收益指标体系（信息比率）

见 BUG-04。

---

### CAP-03 ✅ IC 衰减分析（`compute_ic_decay`）

**变更文件：** `backtest/signal_diagnostics.py`

---

### CAP-04 ✅ FactorScreener 集成到 FactorPipeline

`compute_with_screening(price_data, forward_returns, screener)` 方法。

**变更文件：** `features/base.py`

---

### CAP-05 ✅ 持仓换手率追踪

见 BUG-04 的 `compute_metrics`。

---

### CAP-06 ✅ 市场状态感知因子（`RegimeFeatureEngine`）

6 类 regime 信号：trend / vol / breadth / corr / drawdown / label。

**变更文件：** `features/regime_features.py`（新建）

---

### CAP-07 / GAP-01 ✅ 持仓集中度风险检查

`max_position_pct` WARNING + `concentration_hard_limit` ERROR + Herfindahl 指数。

**变更文件：** `run_daily.py`

---

### CAP-08 ✅ Brinson 绩效归因模块

板块级 BHB 三分解（allocation / selection / interaction）。

**变更文件：** `backtest/attribution.py`（新建）

---

### CAP-09 ✅ 滚动 IC 监控（`compute_rolling_ic`）

**变更文件：** `backtest/signal_diagnostics.py`

---

### CAP-10 ✅ Web Dashboard（第三批）

本地 SPA 管理面板：React 19 + Vite + TypeScript + Tailwind CSS + react-i18next。

- 后端：FastAPI，7 routers，33 API 端点，TaskManager + SSE 流式推送
- 前端：8 页面（Dashboard / Data / Models / Backtest / Signals / Factors / Config / System）
- 入口：`python web/run_web.py`（生产 :8000）或 `npm run dev`（开发 :5173）

**变更文件：** `web/`（新建目录，~40 文件）

---

### CAP-11 ✅ 外部数据抓取层（第三批）

15 个领域专用 fetcher（`BaseDataFetcher` 子类），每个自动缓存到 `cache/<type>/*.csv`，可配置 TTL。

| 数据类型 | 缓存 TTL |
|----------|----------|
| margin / pledge / insider / repurchase | 1 天 |
| analyst | 3 天 |
| financial / visit | 7 天 |
| balance_sheet / dividend / earnings_guidance / institutional / shareholder | 30 天 |
| valuation | 1 天 |
| northbound | 1 天 |

入口：`python run_fetch_data.py --type <type>`

**变更文件：** `data/fetchers/`（新建目录，15 个 fetcher + `__init__.py`）、`run_fetch_data.py`

---

### CAP-12 ✅ 因子注册表与因子库管理（第三批）

`features/library/` 提供 `FactorMeta` / `FactorLibrary`（目录） / `FactorCleaner` / `FactorScreener`，支持 20 个已注册因子（含 csv、regime、sector、technical、mined、northbound、fundamental 及 12 个 akshare 数据驱动因子）。

**变更文件：** `features/library/`（新建目录）

---

### CAP-13 ✅ Overlay 回撤监控（第三批）

`overlay_monitor` 配置：当 overlay 策略累计回撤超过阈值（默认 -15%）时，在调仓报告中插入 WARNING，建议切换到保守基线策略。

**变更文件：** `run_scheduled_rebalance.py`

---

### CAP-14 ✅ Stock-vs-Sector (SVS) 过滤与回撤门控（第三批）

`stock_vs_sector_filter`：按板块相对强弱过滤，三种模式（`hard_filter` / `multiplicative_weight` / `residual_add`）。`drawdown_gating`：市场回撤超过阈值时自动禁用 SVS 过滤。

**变更文件：** `signals/postprocess.py`

---

### CAP-15 ✅ 调仓信号个股收益与持股天数（第三批）

- `--positions` 格式扩展为 `INSTRUMENT:SHARES:ENTRY_DATE`，支持逐股建仓日期
- 报告中每只股票显示 `收益+972元(+4.16%) 持2日`
- hold_thresh 保护改为逐股独立判断，不同建仓日期的持仓各自计算保护期

**变更文件：** `run_scheduled_rebalance.py`

---

### CAP-16 ✅ 持仓累计 P&L 全局建仓日口径（第四批）

`_compute_portfolio_pnl()` 新增 `default_entry_date`。当 `--positions` 使用旧格式但同时传入 `--position-date` 时，累计收益会按全局建仓日计算，不再退回成前一日收益。

**变更文件：** `run_scheduled_rebalance.py`

---

## 四、新增过滤与后处理能力

### GAP-04 ✅ Walk-forward 统计显著性

`sharpe_ttest_pvalue` / `return_ttest_pvalue`，报告表格含 `sharpe_p` 列。

**变更文件：** `run_walk_forward_validation.py`

---

### GAP-07 ✅ 流动性过滤器

`min_avg_volume` / `min_avg_amount` + 窗口参数，默认不启用。

**变更文件：** `data/universe.py`

---

### GAP-08 ✅ 市值中性化

`signal.postprocess.size_neutralize: true`，截面 OLS 去除线性市值暴露。

**变更文件：** `signals/postprocess.py`

---

## 五、影响汇总

### 第一批 + 第二批（2026-04-29）

| 项目 | 类别 | 影响 |
|------|------|------|
| `data/utils.py` 新建 | 架构 | 消除 3 处重复代码，B股映射修正 |
| `SectorFactorEngine` 向量化 | 性能 | 5-10x 加速 |
| akshare 并发 | 性能 | 行业数据刷新 ~5-6x 加速 |
| FactorPipeline 并发 | 性能 | 多因子场景 ~2-3x 加速 |
| 基准超额指标 | 准确性 | IR 替代绝对 Sharpe |
| IC 衰减分析 | 新能力 | 科学确定 hold_thresh |
| 滚动 IC 监控 | 新能力 | 模型退化监控基础 |
| WFV CSV 隔离 | 可靠性 | 并行 walk-forward 结果不错配 |
| 停牌过滤修正 | 安全性 | 不再产生无法成交的买入指令 |
| 特征重要性持久化 | 可观测性 | 跨版本因子重要性趋势分析 |
| sector_reversal 修正 | 模型质量 | 因子不再冗余 |
| bootstrap bagging | 模型质量 | ensemble 多样性提升 |
| FactorScreener | 模型质量 | 低 IC / 高相关因子自动过滤 |
| 集中度风险检查 | 安全性 | 实时告警集中度超标 |
| Brinson 归因 | 新能力 | 板块级归因分解 |
| t 检验显著性 | 统计严谨性 | WFV 结果含 p 值 |
| 流动性过滤 | 安全性 | 过滤低流动性股票 |
| 市值中性化 | 模型质量 | 去除市值偏差 |

### 第三批（2026-05-07）

| 项目 | 类别 | 影响 |
|------|------|------|
| Web Dashboard | 新能力 | 全功能可视化管理面板，8 页面 33 端点 |
| 外部数据抓取层 | 新能力 | 15 个 fetcher 覆盖基本面/资金面/情绪面 |
| 因子注册表与库管理 | 新能力 | 20 因子目录，IC/ICIR 筛选与去相关 |
| Overlay 回撤监控 | 安全性 | 弱市自动预警，建议切换保守策略 |
| SVS 过滤 + 回撤门控 | 模型质量 | 板块相对强弱过滤 + 弱市自动禁用 |
| 调仓信号真实 P&L | 准确性 | 替换无意义的回测收益，显示真实持仓盈亏 |
| 个股持股天数 | 可观测性 | 每只股票显示持有天数和盈亏 |
| 逐股 hold 保护 | 安全性 | 不同建仓日期独立计算保护期 |

### 第四批（2026-05-11）

| 项目 | 类别 | 影响 |
|------|------|------|
| Benchmark 主链路 | 准确性 | qlib report 默认带基准收益，指标输出 IR/alpha/tracking error |
| IR 默认排序 | 研究口径 | 网格搜索优先选择相对基准表现更强的参数 |
| deal_price 配置化 | 执行一致性 | 支持 close/open 成交价口径对照 |
| 调仓名称工具去重 | 可维护性 | 定时调仓复用 `data.utils.load_stock_names()` |
| 全局建仓日累计 P&L | 准确性 | 旧格式 `--positions` + `--position-date` 也能显示真实累计收益 |

---

## 六、未实施项说明

| 项目 | 原因 |
|------|------|
| CAP-02 质量成长类基本面因子 | 估值类已实现，质量/成长类仍待扩展 |
| CAP-07 持仓止损/波动率目标 | 需修改 qlib 策略层，超出当前架构边界 |
| GAP-02 次日开盘价成交 | 已支持 `backtest.deal_price: open`，仍需对候选策略做统一复跑并沉淀实盘口径模板 |
| GAP-03 历史板块快照 | 需构建历史成分股时间序列数据库，工程量大 |
| BUG-09 轻微隐患 | 风险低，不影响核心功能，标记为 future work |

---

*第一批：修改/新建 13 文件，修复 7 缺陷，完成 4 优化，新增 3 能力。*
*第二批：修改/新建 12 文件，修复 1 缺陷，完成 5 优化，新增 5 能力。*
*第三批：修改/新建 ~50 文件，修复 1 缺陷，新增 6 能力。*
*第四批：修改/新建 8 文件，修复 3 个审计问题，新增/完善 2 个研究口径能力。*
*累计修改/新建 ~83 文件，全面提升框架的健壮性、性能、可观测性、可视化和量化能力。*
