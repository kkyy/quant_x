# quant_ex 项目审计报告（复核更新版）

> 复核时间：2026-05-11
> 补充更新：2026-05-13，Agent 策略迭代层与完整训练/回测/feedback 闭环已落地
> 复核基线：当前工作区 HEAD + 2026-05-07 复核报告 + 本轮审计迭代
> 复核范围：`models/`、`features/`、`data/`、`backtest/`、`signals/`、`run_*.py`、`config/`、`test/`、`web/`

---

## 目录

1. [复核摘要](#1-复核摘要)
2. [已关闭或过时的旧结论](#2-已关闭或过时的旧结论)
3. [当前仍然成立的缺陷与风险](#3-当前仍然成立的缺陷与风险)
4. [优化机会（按收益/成本排序）](#4-优化机会按收益成本排序)
5. [能力盘点：已具备 / 部分具备 / 缺失](#5-能力盘点已具备--部分具备--缺失)
6. [与量化盈利目标的主要差距](#6-与量化盈利目标的主要差距)
7. [建议的下一轮工作顺序](#7-建议的下一轮工作顺序)

---

## 1. 复核摘要

与 2026-04-30 的旧版报告相比，本次复核的核心变化：

- **Web Dashboard 全面上线**：9 页面 37 端点的本地 SPA 管理面板已落地，之前缺失的可视化能力大幅补齐；新增 Agent Runs 页面。
- **Agent 策略迭代层落地**：`agent/strategy_iteration/` 已支持多角色 LLM/离线计划、prompt/context/trace 记录、命令审批模板、execution summary、回测/WFV feedback 回灌和 append-only memory。
- **外部数据抓取层完成**：15 个领域 fetcher + 可配置缓存 TTL，覆盖基本面/资金面/情绪面全维度。
- **调仓信号真实性大幅提升**：真实持仓 P&L 替换了无意义的回测收益，个股持股天数可见，逐股 hold 保护更精确。
- **Overlay 回撤监控已落地**：弱市自动预警 + SVS 回撤门控，从"有概念"变成"有机制"。
- **Benchmark 主链路已接通**：`BacktestEngine` 现在向 qlib 传入 `market.benchmark`，`compute_metrics()` 自动读取报告 `bench` 列，网格搜索默认按 `information_ratio` 排序。
- **回测成交价已配置化**：`backtest.deal_price` 可在配置中切换 `close` / `open` 等 qlib 支持字段，默认保持历史 `close` 口径。
- **完整 agent 闭环已验证但未产生新候选**：`full_agent_train_backtest_20260513` 完成 real LLM agent → LGBM 训练 → csi300 回测 → feedback。strict csi1000 重训候选 Sharpe 1.2490、IR 0.5774，弱于现有同参数 control，结论为 reject/refuted。
- 旧报告中若干高优先级问题已修复或降级。

本次复核后，问题重新分成三类：

- 已关闭：确认已修复或原判断为误报。
- 活跃问题：当前代码中仍存在，且会影响研究结论或实盘可信度。
- 中长期缺口：不是 bug，但会限制策略质量或落地能力。

---

## 2. 已关闭或过时的旧结论

| 旧编号 | 当前状态 | 复核结论 |
|---|---|---|
| BUG-01 | 已修复 | 股票代码转换已统一到 `data/utils.py:code_to_qlib_instrument()`。|
| BUG-02 | 已修复 | `features/technical_factors.py` 中 ATR 死代码已删除。|
| BUG-03 | 已修复 | Walk-forward 已通过 `--output-csv` 写入折叠隔离路径。|
| BUG-05 | 已修复 | `features/sector_factors.py:_map_sector_stat()` 已改为向量化 `groupby + reindex`。|
| BUG-06 | 已修复 | `sector_reversal` 已改为短窗动量减长窗动量。|
| BUG-07 | 已修复 | `signals/generator.py` 用 `$volume` 识别停牌股票并跳过。|
| BUG-10 | 误报 | `_merge_extra()` 存在于 `models/base.py`，调用链成立。|
| OPT-03 | 已完成 | CSV 输出路径隔离已实现。|
| OPT-05 | 已完成 | Top-N 特征重要性写入 `models/*_feature_importance.json`。|
| OPT-06 | 已完成 | `LGBMAlphaModel` 支持 `bagging_fraction` bootstrap bagging。|
| OPT-08 | 已完成 | `--folds-config` + `config/walk_forward_folds.yaml.example`。|
| CAP-03 | 已完成 | `backtest/signal_diagnostics.py` 已有 `compute_ic_decay()`。|
| CAP-05 | 部分完成 | `avg_turnover`、`--slippage-sensitivity` 已有，但仍未系统纳入候选策略决策流程。|
| CAP-06 | 已完成 | `features/regime_features.py` + `strategy/regime_switch.py` 已完整落地。|
| CAP-08 | 部分完成 | Brinson 板块归因已有，个股/因子级归因闭环未形成。|
| CAP-09 | 部分完成 | `compute_rolling_ic()` 已有，自动告警、持久化、阈值治理未接通。|
| GAP-04 | 部分缓解 | WFV 汇总含 t-test p-value，但折叠数量偏少。|
| GAP-08 | 部分缓解 | `size_neutralize` 已有，未默认启用。|
| **BUG-A01** | **已修复** | benchmark 已从配置传入 qlib 回测报告，指标与排序默认使用 IR。 |
| **BUG-A06** | **已修复** | `run_scheduled_rebalance.py` 已改用 `data.utils.load_stock_names()`。 |
| **OPT-A07** | **已关闭** | `scripts/install_daily_rebalance_launchd.sh` 已按脚本位置动态解析项目路径，未发现旧路径 plist 模板。 |

---

## 3. 当前仍然成立的缺陷与风险

### BUG-A02 🟡 回测成交假设已可配置，但默认仍保持 `deal_price="close"`

**位置：** `backtest/engine.py`

**现状：** `backtest.deal_price` 已接入 `SimulatorExecutor`，可切换到 `open` 等 qlib 支持字段；为了兼容历史结果，默认值仍是 `close`。

**影响：** 如果研究仍沿用默认 `close`，回测与次日调仓执行之间仍存在乐观偏差。

**建议：**
1. 为正式候选统一增加 `deal_price: "open"` 对照复跑。
2. 将开盘滑点和量能约束纳入标准评估模板。
3. 在策略日志中记录成交价口径，避免 close/open 结果混用。

---

### BUG-A03 🔴 历史板块/ST 过滤仍存在前视偏差与幸存者偏差风险

**位置：** `data/utils.py`、`data/sector.py`、`data/universe.py`

**现状：** 股票名称和部分行业映射依赖当前 `crawler/data/sector_stocks.json` 快照。

**影响：** 历史回测使用了"今天知道的 ST 名称/板块归属"，隐性前视信息抬高回测可信度。

**建议：**
1. 为板块归属、ST 状态引入按日期索引的历史快照。
2. 优先切换到时间一致的数据源。
3. 在研究报告中显式标注行业/名称数据是否为"当下快照"。

---

### BUG-A04 ⚠️ `factor_mining.py` 失败时以 debug 日志静默跳过

**位置：** `features/factor_mining.py:_compute()`

**建议：** 将失败表达式计数和样本规模写入最终摘要，减少对全局 qlib 状态的直接依赖。

---

### BUG-A05 ⚠️ `min_price` 过滤跨时间步 fallback 增加理解成本

**位置：** `data/universe.py`

**建议：** 明确过滤口径，优先逐交易日局部对齐，增加专门单测。

---

### BUG-A07 ⚠️ 回测侧缺少真正的持仓约束执行

**位置：** `backtest/engine.py`

**现状：** 信号侧已有集中度检查，回测执行层无仓位硬约束。

**建议：** 将仓位上限下沉到回测执行逻辑，对已有候选重新按硬约束复跑。

---

## 4. 优化机会（按收益/成本排序）

### OPT-A01 已完成：把超额收益口径接到所有主报表和候选策略排序

已完成链路贯通：qlib report `bench` → `compute_metrics()` → grid CSV → `information_ratio` 默认排序。

---

### OPT-A02 高收益中成本：回测成交价格、滑点、量能约束统一"实盘口径模板"

已有 `--slippage-sensitivity`，再往前一步统一化可显著提高研究与落地一致性。

---

### OPT-A03 已完成：继续完成数据工具去重

`run_scheduled_rebalance.py` 已改用共享 `data.utils.load_stock_names()`。

---

### OPT-A04 中收益中成本：为 `FactorPipeline` 引入可控并行

前提：确认各因子 `compute()` 无共享可变状态。

---

### OPT-A05 已完成：`SectorDataProvider` akshare 抓取改为并发

`SectorDataProvider._fetch_akshare()` 已使用 `ThreadPoolExecutor(max_workers=8)` 并发抓取。

---

### OPT-A06 中收益中成本：补齐"失败可观测性"

最典型的是 `factor_mining.py`，"静默跳过"比报错更危险。

---

## 5. 能力盘点：已具备 / 部分具备 / 缺失

### 5.1 已具备

- 因子 IC 衰减分析：`compute_ic_decay()`
- 滚动 IC 监控基础函数：`compute_rolling_ic()`
- Brinson 板块归因：`backtest/attribution.py`
- 市值中性化：`signals/postprocess.py` 的 `size_neutralize`
- 市场状态切换：`strategy/regime_switch.py`，已接入 `run_daily.py` 与 `run_scheduled_rebalance.py`
- Walk-forward 自定义折叠：`--folds-config`
- 统计显著性：`sharpe_ttest_pvalue` / `return_ttest_pvalue`
- 换手率与滑点敏感性：`avg_turnover`、`--slippage-sensitivity`
- 基础基本面因子：`features/fundamental_factor.py`（估值类 + 扩展财务因子）
- 外部数据抓取：15 个领域 fetcher，`data/fetchers/` 完整覆盖
- CSV 自定义因子：`features/csv_factor.py`
- 因子注册表与库管理：20 个已注册因子 + `FactorLibrary` + `FactorScreener`
- 系统迭代日志：`docs/strategy_log/system_iteration_log.csv`
- **Web Dashboard**：React 19 + FastAPI，8 页面 33 端点，TaskManager + SSE
- **Overlay 回撤监控**：累计回撤超阈值自动预警
- **SVS 过滤 + 回撤门控**：板块相对强弱过滤，弱市自动禁用
- **真实持仓 P&L**：用实际价格变化计算每日盈亏，替换无意义的回测收益
- **个股持股天数**：`--positions` 支持逐股建仓日期，报告显示持有天数
- **逐股 hold 保护**：不同建仓日期独立计算保护期
- **Benchmark 超额收益主链路**：回测报告默认带 `bench`，指标输出 IR/alpha/tracking error，网格搜索默认按 IR 排序
- **回测成交价配置**：`backtest.deal_price` 可控制 qlib 成交价字段

### 5.2 部分具备

#### CAP-B02 基本面因子库

已有估值类和扩展财务因子，仍缺更完整的质量/成长因子框架和财报滞后处理。

#### CAP-B03 归因分析

板块级 Brinson 已有，因子级、个股级、按月稳定输出的归因产品未闭环。

#### CAP-B04 模型退化监控

已有滚动 IC 计算函数，定时任务、阈值治理、告警通知未接通。

#### CAP-B05 风险控制

信号侧已有集中度检查，回测/执行侧未形成统一硬约束。

### 5.3 仍然缺失或明显不足

#### CAP-C01 历史快照式行业/ST/证券状态数据

当前最影响研究可信度的数据层能力缺口。已有申万一级行业 fetcher，仍缺时间序列式历史快照。

#### CAP-C02 多标签或多持有期训练框架

主标签仍偏向单一持有期。已有 IC 衰减分析能力，下一步应让训练标签与持有周期协同。

#### CAP-C03 实盘口径统一模板

"研究回测""日常信号""定时调仓提醒"三条链路在价格口径、风险约束、执行口径上仍不完全统一。

#### CAP-C04 压力测试与极端成交情景模拟

缺跌停无法卖出、开盘跳空、流动性骤降等情景压力测试。

#### CAP-C05 统一的研究可观测性与失败审计

表达式失败率、因子缺失率、数据源 fallback 次数、缓存命中率等。

---

## 6. 与量化盈利目标的主要差距

### GAP-A01 🟡 研究结果仍可能高估真实 Alpha

benchmark 链路已接通；剩余主要风险是默认 `deal_price="close"` 偏理想化，以及历史行业/ST 数据仍使用当前快照。

### GAP-A02 🔴 回测风险约束与实盘约束仍未统一

研究端允许极端集中，提醒端只做报警，"回测最优参数不可执行"的结构性偏差仍在。

### GAP-A03 🟡 持有期与标签期的协同还没形成闭环

已有 IC 衰减分析能力，但还没落实到训练与选参制度里。

### GAP-A04 🟡 基本面与非价格 Alpha 仍不够厚

Alpha 主体仍偏价格/成交量，跨年份稳定性不够。

### GAP-A05 🟡 自动化研究闭环还不够强

网格搜索、walk-forward、显著性检验、IC 衰减、滑点敏感性尚未汇总为标准化"研究准入门槛"。

---

## 7. 建议的下一轮工作顺序

1. 用 `deal_price: "open"` + 滑点敏感性复跑当前候选，建立实盘口径模板。
2. 为行业归属、ST 状态、证券状态补历史快照，先消除前视偏差。
3. 将仓位硬约束下沉到回测执行层，重跑候选策略。
4. 把 IC 衰减真正接入标签设计和持有期选择。
5. 在已有估值因子基础上扩展质量/成长类基本面因子。
6. 将失败可观测性纳入标准研究报告：因子失败率、缺失率、fallback 次数、缓存命中率。

---

## 结论

与 2026-04-30 旧版报告相比，项目已从"基础设施补齐"进入"研究可信度和落地一致性决定上限"的阶段。

本次复核最重要的更新：

- Web Dashboard、数据抓取层、调仓信号真实性、Overlay 监控已全面落地，可观测性和可控性大幅提升。
- benchmark 已从缺口变成默认研究口径；最高优先级转为执行口径、历史快照和风险约束三条关键链路。
- 一旦这三项补齐，后续做基本面扩展、标签体系升级、自动重训与退化告警，收益会更大也更可信。

*本报告为复核更新版，目的是反映当前代码状态。后续如再修复 benchmark、执行口径或历史快照问题，建议继续更新本文件。*
