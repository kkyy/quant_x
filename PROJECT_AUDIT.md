# quant_ex 项目审计报告（复核更新版）

> 复核时间：2026-04-30  
> 复核基线：当前工作区 HEAD + 2026-04-29 旧版审计报告  
> 复核范围：`models/`、`features/`、`data/`、`backtest/`、`signals/`、`run_*.py`、`config/`、`test/`

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

这次复核的核心结论与 2026-04-29 的旧版报告相比有明显变化：

- 旧报告中一批高优先级问题已经修复，不能再继续视为未完成项。
- 旧报告中若干“缺失能力”实际上已经落地，只是旧报告没有反映最新代码状态。
- 当前最值得优先处理的问题，已经从“代码重复、并行竞态、基础分析能力缺失”，转移为“回测基准链路未贯通、执行假设偏理想化、历史成分前视偏差、实盘风险约束不足”。

本次复核后，将问题重新分成三类：

- 已关闭：确认已修复或原判断为误报。
- 活跃问题：当前代码中仍存在，且会影响研究结论或实盘可信度。
- 中长期缺口：不是 bug，但会限制策略质量或落地能力。

---

## 2. 已关闭或过时的旧结论

下列结论在当前代码中已经不再成立，或需要改写为“部分完成”。

| 旧编号 | 当前状态 | 复核结论 |
|---|---|---|
| BUG-01 | 已修复 | 股票代码转换已统一到 `data/utils.py:code_to_qlib_instrument()`，`data/universe.py`、`signals/generator.py`、`data/sector.py` 已改为复用统一实现。|
| BUG-02 | 已修复 | `features/technical_factors.py` 中 ATR 的死代码已删除，当前实现只保留逐股真实计算路径。|
| BUG-03 | 已修复 | Walk-forward 已通过 `run_backtest.py --output-csv` 写入折叠隔离路径，`run_walk_forward_validation.py` 不再依赖“最新 CSV”猜测。|
| BUG-05 | 已修复 | `features/sector_factors.py:_map_sector_stat()` 已改为向量化 `groupby + reindex`，原双循环瓶颈已消除。|
| BUG-06 | 已修复 | `sector_reversal` 已改为短窗动量减长窗动量，不再等于 `-sector_momentum`。|
| BUG-07 | 已修复 | `signals/generator.py` 现在会用最新 `$volume` 识别停牌股票，并在构造目标持仓时跳过。|
| BUG-10 | 误报 | `_merge_extra()` 存在于 `models/base.py`，`lgbm_model.py` 的调用链是成立的。|
| OPT-03 | 已完成 | CSV 输出路径隔离已经实现，旧优化项应从待办中移除。|
| OPT-05 | 已完成 | `models/trainer.py` 已将 Top-N 特征重要性写入 `models/*_feature_importance.json`。|
| OPT-06 | 已完成 | `LGBMAlphaModel` 已支持 `bagging_fraction` 的 bootstrap bagging。|
| OPT-08 | 已完成 | `run_walk_forward_validation.py` 已支持 `--folds-config`，并提供 `config/walk_forward_folds.yaml.example`。|
| CAP-03 | 已完成 | `backtest/signal_diagnostics.py` 已有 `compute_ic_decay()`。|
| CAP-05 | 部分完成 | `backtest/metrics.py` 已有 `avg_turnover`，`run_backtest.py` 已有 `--slippage-sensitivity`；但结果仍未系统纳入候选策略决策流程。|
| CAP-06 | 已完成 | `features/regime_features.py` 与 `strategy/regime_switch.py` 已提供市场状态感知与参数切换能力。|
| CAP-08 | 部分完成 | `backtest/attribution.py` 已提供 Brinson 归因，但仍偏板块级，未覆盖个股/因子级归因闭环。|
| CAP-09 | 部分完成 | `compute_rolling_ic()` 已存在，但自动告警、定时持久化、阈值治理尚未接通。|
| GAP-04 | 部分缓解 | Walk-forward 汇总已包含 `sharpe_ttest_pvalue` / `return_ttest_pvalue`，但折叠数量偏少的问题仍然存在。|
| GAP-08 | 部分缓解 | `signals/postprocess.py` 已支持 `size_neutralize`，但并未默认启用，也未形成标准归因口径。|

结论：旧报告里至少有一半以上条目需要降级或移除，继续直接使用会高估项目当前缺口。

---

## 3. 当前仍然成立的缺陷与风险

以下问题在本次复核中仍然成立，且优先级高于其他待办。

### BUG-A01 🔴 基准超额收益链路只做了一半，回测主链仍未真正接入 benchmark

**位置：** `backtest/metrics.py`、`backtest/engine.py`

**现状：**

- `backtest/metrics.py` 已支持 `benchmark_rets`，可计算 `excess_annual_return`、`information_ratio`、`tracking_error`、`beta`、`alpha`。
- 但 `backtest/engine.py` 仍调用 `backtest_daily(..., benchmark=None, ...)`。
- 当前主流程没有看到稳定的 benchmark return 序列被传入 `compute_metrics()`，也就意味着“支持超额收益指标”和“实际在回测中使用超额收益指标”仍是两回事。

**影响：**

- 研究输出仍可能主要依赖绝对 Sharpe。
- 牛市环境下参数优选仍可能高估策略质量。
- `strategy_candidates.yaml` 中的历史结论很可能混合了绝对收益口径与超额收益口径。

**建议：**

1. `BacktestEngine.run()` 传入正确 benchmark，而不是 `None`。
2. 在回测报告和候选策略汇总中强制同时展示绝对 Sharpe 与 IR。
3. 将“候选策略排名依据”切换为包含超额指标的组合口径。

---

### BUG-A02 🔴 回测成交假设仍然偏理想化，默认 `deal_price="close"`

**位置：** `backtest/engine.py`

**现状：**

当前 `exchange_kwargs` 仍固定：

```python
"deal_price": "close"
```

这对收盘后生成信号、次日执行的实际流程并不贴合。虽然近期已经补了 `min_cost`、净收益成本扣减、滑点敏感性分析，但成交时点假设本身没有改。

**影响：**

- 回测与实盘调仓时间错位。
- 中小盘策略的真实成交偏差仍可能被低估。
- 高换手策略的研究结果仍偏乐观。

**建议：**

1. 提供 `deal_price` 可配置项，至少支持 `open` / `close`。
2. 对默认研究口径切换到“次日开盘成交”或明确区分“研究口径”和“实盘口径”。
3. 将开盘滑点和量能约束纳入标准评估模板。

---

### BUG-A03 🔴 历史板块/ST 过滤仍存在前视偏差与幸存者偏差风险

**位置：** `data/utils.py`、`data/sector.py`、`data/universe.py`

**现状：**

- 股票名称和部分行业映射依赖当前 `crawler/data/sector_stocks.json`。
- 这份文件是当前快照，不是按时间切片的历史快照。

**影响：**

- 历史回测使用了“今天知道的 ST 名称/板块归属”。
- 行业因子、行业中性化、ST 过滤都可能引入隐性前视信息。
- 这类偏差不会让程序报错，但会系统性抬高回测可信度。

**建议：**

1. 为板块归属、ST 状态引入按日期索引的历史快照。
2. 能用 qlib 历史成分股/历史证券状态时，优先切换到时间一致的数据源。
3. 在所有研究报告中显式标注当前行业/名称数据是否为“当下快照”。

---

### BUG-A04 ⚠️ `factor_mining.py` 仍依赖全局 qlib 状态，失败时以 debug 日志静默跳过

**位置：** `features/factor_mining.py:_compute()`

**现状：**

- 当前代码已经增加了 qlib 初始化检查，较旧版更安全。
- 但 `_compute()` 仍直接调用全局 `D.features()`。
- 若表达式失败、provider 状态不一致、日期范围不匹配，当前逻辑主要是 `debug` 记录后返回 `None`。

**影响：**

- 因子挖掘结果仍可能“不报错但缺失候选”。
- 多环境、多折叠、多数据源场景下可复现性仍不足。

**建议：**

1. 将失败表达式计数和样本规模写入最终摘要，而不是只打 debug。
2. 将挖掘输入对齐到统一的数据加载链路，减少对全局 qlib 状态的直接依赖。
3. 对“因子有效但未计算”和“因子无效”做显式区分。

---

### BUG-A05 ⚠️ `min_price` 过滤逻辑仍偏复杂，跨时间步 fallback 会增加理解和维护成本

**位置：** `data/universe.py`

**现状：**

- 当前实现会先按 `pred.index` 对齐 `real_close`。
- 若存在任何 `NaN`，则退回到“每只股票最新可用价格”做补洞。

**风险：**

- 不同时间点的信号可能混入不同口径的价格回填。
- 逻辑正确性依赖对 MultiIndex 对齐细节的理解，维护门槛较高。
- 性能上也会在有局部缺失时触发全量 fallback。

**建议：**

1. 明确过滤口径是“当日价格不足则剔除”还是“允许最近有效价格回填”。
2. 若目标是回测一致性，优先使用逐交易日局部对齐，而不是全局 fallback。
3. 为 `min_price` 增加专门单测，覆盖多日期、多股票、局部缺失场景。

---

### BUG-A06 ⚠️ 去重工作只完成了一部分，`run_scheduled_rebalance.py` 仍保留本地 `_load_stock_names()`

**位置：** `run_scheduled_rebalance.py`

**现状：**

- `data/utils.py` 已提供统一的 `load_stock_names()`。
- 但 `run_scheduled_rebalance.py` 仍保留本地 `_load_stock_names()`。

**影响：**

- 代码层面仍有一份额外维护点。
- 后续如果 stock-name 来源或缓存规则调整，这里仍可能与公共实现脱节。

**建议：**

1. 将 `run_scheduled_rebalance.py` 也切换到 `data.utils.load_stock_names()`。
2. 明确整个项目的“股票名称/代码转换”只保留一份权威入口。

---

### BUG-A07 ⚠️ 回测侧缺少真正的持仓约束执行，只在信号/提醒侧有集中度检查

**位置：** `run_daily.py`、`signals/generator.py`、`backtest/engine.py`

**现状：**

- `signals/generator.py` 和 `run_daily.py` 已能检查 `max_position_pct`、`concentration_hard_limit`。
- 但 `backtest/engine.py` 的策略执行层没有看到真正的仓位硬约束注入。

**影响：**

- 研究侧和实盘提醒侧的风险口径不一致。
- 回测中允许出现“研究上能持有、实盘上不会允许”的极端集中仓位。

**建议：**

1. 将仓位上限下沉到回测执行逻辑，而不是只在信号层报警。
2. 对 `strategy_candidates.yaml` 中已有候选重新按硬约束复跑。
3. 将“约束前”和“约束后”的收益/风险损失分开记录。

---

## 4. 优化机会（按收益/成本排序）

以下不是致命 bug，但投入产出比较高。

### OPT-A01 高收益低成本：把超额收益口径接到所有主报表和候选策略排序

原因：指标能力已经在 `backtest/metrics.py` 里实现了一半，剩下主要是链路贯通和报表口径统一，投入小、研究收益大。

---

### OPT-A02 高收益中成本：把回测成交价格、滑点、量能约束做成统一“实盘口径模板”

原因：当前已经有 `--slippage-sensitivity`，说明基础设施存在。再往前一步，把 `deal_price`、量能上限、最小成交额过滤统一化，能显著提高研究与落地的一致性。

---

### OPT-A03 中收益低成本：继续完成数据工具去重

重点对象：`run_scheduled_rebalance.py` 的 `_load_stock_names()`、其他可能残存的本地股票代码转换/名称加载实现。

---

### OPT-A04 中收益中成本：为 `FactorPipeline` 引入可控并行

前提：确认各因子 `compute()` 无共享可变状态、无 qlib 全局副作用后，再并行化 `technical` / `sector` / `fundamental` / `mined`。

预期收益：缩短训练前特征准备时间，尤其是带 sector/fundamental 因子的组合。

---

### OPT-A05 中收益中成本：将 `SectorDataProvider` 的 akshare 抓取改为并发

旧报告这条仍然成立。当前 `fundamental_factor.py` 已经使用线程池并发抓取，说明项目并不排斥这种实现方式；行业数据层也适合做同类改造。

---

### OPT-A06 中收益中成本：补齐“失败可观测性”而不是继续依赖 debug 日志

最典型的是 `factor_mining.py`。研究框架里“静默跳过”比报错更危险，因为它会直接污染结论而不暴露异常。

---

## 5. 能力盘点：已具备 / 部分具备 / 缺失

### 5.1 已具备

- 因子 IC 衰减分析：`compute_ic_decay()` 已存在。
- 滚动 IC 监控基础函数：`compute_rolling_ic()` 已存在。
- Brinson 板块归因：`backtest/attribution.py` 已存在。
- 市值中性化：`signals/postprocess.py` 已支持 `size_neutralize`。
- 市场状态切换：`strategy/regime_switch.py` 已接入 `run_daily.py` 与 `run_scheduled_rebalance.py`。
- Walk-forward 自定义折叠：`--folds-config` 已支持。
- 统计显著性：walk-forward 汇总已有 `sharpe_ttest_pvalue` / `return_ttest_pvalue`。
- 换手率与滑点敏感性基础分析：`avg_turnover`、`--slippage-sensitivity` 已存在。
- 基础基本面因子：`features/fundamental_factor.py` 已提供估值类与扩展财务因子（ROE、ROA、gross_margin、revenue_growth 等）。
- 外部数据抓取：15 个领域 fetcher（含 sw1_industry 申万行业），`data/fetchers/` 已完整覆盖。
- CSV 自定义因子：`features/csv_factor.py` 支持从 CSV 文件加载自定义因子。
- 系统迭代日志：`docs/strategy_log/system_iteration_log.csv` 记录全系统迭代周期。
- Web Dashboard 国际化：React 19 + react-i18next，支持中英文切换。
- 因子注册表：20 个已注册因子（含 csv、regime、sector、technical、mined、northbound、fundamental 及 12 个 akshare 数据驱动因子）。

### 5.2 部分具备

#### CAP-B01 基准超额收益体系

状态：部分具备。

- 指标函数已支持。
- 主回测链路未完全接通。
- 研究排名口径仍未强制切换到超额收益优先。

#### CAP-B02 基本面因子库

状态：部分具备。

- 已有估值类因子（如 `pe_ttm`、`pb`、`ps_ttm`、`dyr`）和扩展财务因子（`roe`、`roa`、`gross_margin`、`net_margin`、`revenue_growth`、`profit_growth`、`ocf_to_np`、`fcf_yield`）。
- 仍缺更完整的质量/成长因子框架和财报滞后处理。

#### CAP-B03 归因分析

状态：部分具备。

- 板块级 Brinson 已有。
- 因子级、个股级、按月稳定输出的归因产品还没有形成闭环。

#### CAP-B04 模型退化监控

状态：部分具备。

- 已有滚动 IC 计算函数。
- 未见定时任务、阈值治理、告警通知和历史监控文件沉淀的完整链路。

#### CAP-B05 风险控制

状态：部分具备。

- 信号侧已有集中度检查与提醒。
- 回测/执行侧尚未形成统一硬约束。

### 5.3 仍然缺失或明显不足

#### CAP-C01 历史快照式行业/ST/证券状态数据

这是当前最影响研究可信度的”数据层能力缺口”，优先级高于再增加几个新因子。当前已有申万一级行业 fetcher（`sw1_fetcher`），但仍缺时间序列式历史快照。

#### CAP-C02 多标签或多持有期训练框架

当前主标签仍偏向单一持有期。既然已经有 IC 衰减分析能力，下一步自然应该是让训练标签与持有周期协同，而不是长期停留在单标签评估。

#### CAP-C03 实盘口径统一模板

当前“研究回测”“日常信号”“定时调仓提醒”三条链路在价格口径、风险约束、执行口径上仍不完全统一。

#### CAP-C04 压力测试与极端成交情景模拟

当前还缺少跌停无法卖出、开盘跳空、流动性骤降等情景压力测试。

#### CAP-C05 统一的研究可观测性与失败审计

包括：表达式失败率、因子缺失率、数据源 fallback 次数、缓存命中率、行业映射覆盖率等。

---

## 6. 与量化盈利目标的主要差距

从“代码完整性”角度看，项目已经明显强于旧报告描述的状态；但从“稳定盈利与可信落地”角度看，仍有四个核心差距。

### GAP-A01 🔴 研究结果仍可能高估真实 Alpha

根因不是单一 bug，而是三件事叠加：

- benchmark 链路未完全接通；
- `deal_price="close"` 偏理想化；
- 历史行业/ST 数据仍可能使用当前快照。

这三项一起存在时，即使代码本身没有报错，研究结果也可能系统性偏乐观。

---

### GAP-A02 🔴 回测风险约束与实盘约束仍未统一

如果研究端允许极端集中，而提醒端只做报警，最终会出现“回测最优参数不可执行”的结构性偏差。

---

### GAP-A03 🟡 持有期与标签期的协同还没形成闭环

现在项目已经具备 IC 衰减分析能力，因此这个问题从“看不到”变成了“已经能看见，但还没落实到训练与选参制度里”。

---

### GAP-A04 🟡 基本面与非价格 Alpha 仍不够厚

虽然估值类基本面因子已经有基础实现，但目前 Alpha 主体仍偏价格/成交量。若要提升跨年份稳定性，仅靠技术面和行业相对强弱通常不够。

---

### GAP-A05 🟡 自动化研究闭环还不够强

已有网格搜索、walk-forward、显著性检验、IC 衰减、滑点敏感性，但这些能力还没有完全汇总为一套标准化“研究准入门槛”。当前仍较依赖人工解释结果。

---

## 7. 建议的下一轮工作顺序

如果按“先提高研究可信度，再扩展 Alpha 来源”的原则，建议顺序如下：

1. 打通 benchmark 主链路，把超额收益指标变成默认口径。
2. 把回测成交口径从固定收盘价扩展为可配置，并建立实盘口径模板。
3. 为行业归属、ST 状态、证券状态补历史快照，先消除前视偏差。
4. 将仓位硬约束下沉到回测执行层，重跑候选策略。
5. 清理最后一批数据工具重复实现，降低维护分叉。
6. 把 IC 衰减真正接入标签设计和持有期选择。
7. 在已有估值因子基础上扩展质量/成长类基本面因子。

---

## 结论

当前 `quant_ex` 已经不是“基础设施明显缺失”的状态，而是进入了“研究可信度和落地一致性决定上限”的阶段。

与旧版报告相比，本次复核最重要的更新有三点：

- 很多基础能力其实已经落地，旧报告低估了项目完成度。
- 目前最高优先级不再是继续堆功能，而是把 benchmark、执行口径、历史快照和风险约束这四条关键链路做扎实。
- 一旦这四项补齐，后续再做基本面扩展、标签体系升级、自动重训与退化告警，收益会更大，也更可信。

*本报告为复核更新版，目的是反映当前代码状态，而不是保留历史问题清单。后续如再修复 benchmark、执行口径或历史快照问题，建议继续更新本文件，而不是在旧结论上叠加补丁。*
