# quant_ex 项目全面审计报告

> 生成时间：2026-04-29  
> 审计范围：全部 Python 源码、配置文件、测试文件  
> 审计目标：缺陷/隐患、优化点、缺失能力、盈利目标差距

---

## 目录

1. [代码缺陷与潜在隐患](#1-代码缺陷与潜在隐患)
2. [现有功能的优化机会](#2-现有功能的优化机会)
3. [缺失的量化能力规划](#3-缺失的量化能力规划)
4. [与量化盈利目标的差距分析](#4-与量化盈利目标的差距分析)

---

## 1. 代码缺陷与潜在隐患

### BUG-01 ⚠️ `_code_to_qlib` 逻辑不一致（双份代码，映射规则有差异）

**位置：** `data/sector.py:22-27` vs `data/universe.py:51-62` vs `signals/generator.py:64-77`

**问题描述：**  
三处独立的股票代码转换函数，映射规则不完全一致：

| 文件 | B股 (9xxxxx) | BJ股 (4/8xxxxx) |
|------|------------|----------------|
| `sector.py:_code_to_qlib` | 以 "9" 开头 → 映射到 `SZ`（❌ 错误，应为 SH） | 正确 |
| `universe.py:_to_qlib_code` | `prefix in (6,9)` → `SH`（✅ 正确） | 正确 |
| `signals/generator.py:_to_qlib_code` | 同 universe.py（✅ 正确） | 正确 |

`sector.py` 的 `_code_to_qlib` 对 B 股（SH9xxxxx，如 SH900001）处理有误，会将其错误归入 SZ 系列，导致板块数据关联错误。

**影响范围：** 板块因子、sector_map 计算、行业中性化处理  
**修复建议：** 统一成一个 `data/utils.py` 中的 `code_to_qlib_instrument(code: str) -> str` 函数，三处引用。

---

### BUG-02 ⚠️ `technical_factors.py` ATR 计算中存在死代码

**位置：** `features/technical_factors.py:177-182`

**问题代码：**
```python
tr = pd.concat(
    [...],
    axis=1,
).groupby(level=0, axis=1).max() if False else None
```

`if False else None` 使整段代码永远不会执行，`tr` 始终为 `None`。之后正文的逐股循环才是真正被执行的代码。这段死代码还使用了 pandas 已废弃的 `axis=1` groupby 参数（pandas 2.0+ 会报 `FutureWarning` 甚至 `TypeError`）。

**影响范围：** 无功能影响（dead code），但引发混淆，维护风险高  
**修复建议：** 直接删除 `tr = ...` 的死代码块。

---

### BUG-03 ⚠️ Walk-forward 并行模式下 CSV 竞争读取

**位置：** `run_walk_forward_validation.py:277-288`，函数 `latest_grid_csv()`

**问题描述：**  
`latest_grid_csv()` 读取全局 `backtest_results/` 目录下时间最新的 grid CSV 文件。当 `--workers > 1` 时，多个并行 worker 同时写入该目录，一个 worker 可能读取到另一个 worker 刚写完的 CSV，导致结果混乱。

```python
def latest_grid_csv() -> Path:
    candidates = list((REPO_ROOT / "backtest_results").glob("grid_*.csv"))
    return max(candidates, key=lambda path: path.stat().st_mtime)  # 竞争条件！
```

**影响范围：** 并行 walk-forward 时折叠结果可能错配  
**修复建议：** 在 `run_backtest.py` 中增加 `--output-csv <path>` 参数，让每次 grid search 将结果写到指定路径；在 `_run_one_fold_universe` 中传入隔离路径，避免读全局最新文件。

---

### BUG-04 🔴 Backtest 指标不含基准超额收益（误导性 Sharpe）

**位置：** `backtest/metrics.py:31-33`

**问题描述：**  
当前 Sharpe 计算为 `ann_ret / ann_vol`，风险无风险利率设为 0，没有扣除任何基准收益。这意味着：
- 即使策略跑输 CSI300，只要绝对回报为正，Sharpe 就可以很高
- 不同市场环境（牛市/熊市）下的 Sharpe 没有可比性
- 策略文件 `strategy_candidates.yaml` 中 Sharpe=1.758 是绝对 Sharpe，非超额 Sharpe

此外，`backtest_daily` 调用时传入 `benchmark=None`，qlib 不会计算超额收益序列。

**影响范围：** 所有策略评估指标，误导参数选择和目标设定  
**修复建议：**  
1. `compute_metrics` 增加 `benchmark_rets` 可选参数，计算信息比率（IR）= `mean(daily_alpha) / std(daily_alpha)`  
2. `BacktestEngine.run()` 传入正确 `benchmark`（SH000300）  
3. 在报表中同时展示绝对 Sharpe 和超额 Sharpe

---

### BUG-05 🔴 SectorFactorEngine 内存复杂度过高（逐板块逐股票双循环）

**位置：** `features/sector_factors.py:216-238`，函数 `_map_sector_stat`

**问题描述：**  
当前实现：先按板块计算聚合统计，再逐股票赋值，时间复杂度为 O(板块数 × 股票数)。对于 CSI1000（~250 行业 × 1000 只股票 × 1500 天 = ~3.75 亿次操作），内存和耗时极高。

```python
for sec in sectors:  # ~250 次
    members = [c for c in sector_s[sector_s == sec].index if c in metric.columns]
    ...
for inst in metric.columns:  # ~1000 次
    sec = sector_s.get(inst, "Unknown")
    result[inst] = sector_agg[sec]
```

更高效的实现：
```python
sector_mean = metric.T.groupby(sector_s).mean().T  # (dates × sectors)
result = sector_mean[sector_s]  # (dates × instruments) via broadcast
result.columns = metric.columns
```

**影响范围：** 训练速度（使用 sector 因子时慢 5-10x）  
**修复建议：** 用 groupby + reindex 替换双循环。

---

### BUG-06 ⚠️ `sector_reversal` 因子实际等于 `-sector_momentum`（无新信息）

**位置：** `features/sector_factors.py:204-207`

**问题代码：**
```python
def _sector_reversal(self, rets, sector_s, window):
    return -self._sector_momentum(rets, sector_s, window)
```

这只是 momentum 的负数，不是真正的反转因子。真正的反转信号应区分**短期过度反应**（短窗口均值回归）和**长期动量**（长窗口趋势延续），通常形如 `short_ret - long_ret` 或 `1w_ret - 4w_ret`。

**影响范围：** 因子信息含量重复，浪费模型容量  
**修复建议：** 将反转因子改为 `_sector_momentum(w_short) - _sector_momentum(w_long)` 形式。

---

### BUG-07 ⚠️ 停牌股票在信号生成时没有被过滤（价格缓存问题）

**位置：** `signals/generator.py:251-276`，函数 `_target_positions`

**问题描述：**  
`_target_positions` 以 `prices.get(inst, 0)` 跳过价格为 0 的股票，但停牌股票可能有来自上个交易日的缓存价格（非零），会被误纳入持仓目标。`UniverseFilter.exclude_suspended` 只过滤了 pred，但 `_target_positions` 直接使用 `top_stocks` 中可能残留的停牌股票。

**影响范围：** 日常信号生成，可能生成无法成交的买入指令  
**修复建议：** 在 `_target_positions` 中额外检查成交量（price_data 中 $volume=0 的过滤），或确保 universe_filter 在 generate() 入口处完整过滤。

---

### BUG-08 ⚠️ `factor_mining.py` 的 `_compute` 调用全局 qlib `D.features`

**位置：** `features/factor_mining.py:203-217`

**问题描述：**  
`FactorMiner._compute` 用 `D.features()` 获取因子值，但因子评估用到的时间范围来自 `price_data` 的索引，而 `D.features` 会调用 qlib 全局初始化的数据集。如果不同训练折叠使用了不同的 qlib 配置，或者 qlib 尚未初始化，会静默失败（try/except 吃掉了异常）。

**影响范围：** 因子挖掘结果不可复现  
**修复建议：** 明确传入 `instruments` 和日期范围，并增加 qlib 初始化状态检查；或将因子计算统一到 `FactorPipeline.compute()` 接口。

---

### BUG-09 ⚠️ `min_price` 过滤中 `price_data` 对齐逻辑复杂且有 NaN 潜在失效

**位置：** `data/universe.py:82-98`

**问题描述：**  
`min_price` 过滤对多时间步的预测序列（如整个 test period）使用了 "最后已知价格" 的 fallback，但 `fillna` 之前的 `aligned_prices.isna().any()` 会被整个 Series 的任何一个 NaN 触发，即使只有 1 只股票缺失价格，也会执行 per-instrument latest price 查找，引入额外开销且逻辑复杂。

**影响范围：** 回测时价格过滤可能与信号生成期间行为不一致  
**修复建议：** 简化为：对每个预测日期，只取该日期的最新可用价格；避免跨时间步的价格对齐。

---

### BUG-10 ⚠️ `LGBMAlphaModel._merge_extra` 方法未在文件内定义

**位置：** `models/lgbm_model.py`（`fit` L111，`predict` L170 都调用了 `self._merge_extra`）

**问题描述：**  
`lgbm_model.py` 中找不到 `_merge_extra` 的定义，它应该在 `BaseAlphaModel` 或某个 mixin 中。如果父类中不存在此方法（如由于重构被意外删除），会在运行时抛出 `AttributeError`，且当前测试没有覆盖到完整的 fit/predict 路径。

**影响范围：** 训练、推理全路径  
**修复建议：** 检查 `models/base.py`，确认 `_merge_extra` 方法存在且有正确实现；若不存在，在 `lgbm_model.py` 中补全。

---

## 2. 现有功能的优化机会

### OPT-01 消除重复的 `_load_stock_names` / `_code_to_qlib` 实现

**现状：** `data/universe.py`、`signals/generator.py`、`run_scheduled_rebalance.py` 中各有一份几乎相同的解析 `sector_stocks.json` 的代码，且每次调用都重新读文件。

**优化方案：**
```python
# data/utils.py
from functools import lru_cache

@lru_cache(maxsize=1)
def load_stock_names() -> dict[str, str]:
    """Cached {qlib_code: stock_name} from sector_stocks.json."""
    ...

@lru_cache(maxsize=1)
def code_to_qlib_instrument(code: str) -> str:
    ...
```
预期效果：消除重复代码 ~150 行，减少文件 I/O 次数，`exclude_st` 过滤速度提升 >3x。

---

### OPT-02 akshare 行业数据获取串行改并发

**位置：** `data/sector.py:138-158`，`_fetch_akshare()`

**现状：** 逐一请求每个行业板块（~200 次串行 HTTP），总耗时约 2-5 分钟。

**优化方案：**
```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def _fetch_akshare(self):
    industry_list = ak.stock_board_industry_name_em()
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(ak.stock_board_industry_cons_em, row["板块名称"]): row["板块名称"]
                   for _, row in industry_list.iterrows()}
        for fut in as_completed(futures):
            ...
```
预期效果：耗时从 3-5min 降至 30-60s。

---

### OPT-03 Walk-forward 折叠 CSV 输出路径隔离

**位置：** `run_walk_forward_validation.py:277`

**现状：** 所有折叠都争用 `backtest_results/grid_*.csv` 中时间最新的文件。

**优化方案：** 给 `run_backtest.py` 增加 `--output-dir` 参数，在 `_run_one_fold_universe` 中传入 `out_dir/fold_results/{tag}/`，直接指定输出路径而非靠时间戳猜测。

---

### OPT-04 FactorPipeline 并行计算

**位置：** `features/base.py`，`FactorPipeline.compute()`

**现状：** 各因子顺序执行（technical → sector → mined），sector 因子尤其慢。

**优化方案：**
```python
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor() as pool:
    futures = [pool.submit(factor.compute, price_data) for factor in self.factors]
    results = [f.result() for f in futures]
```
注意：需确认各因子无状态副作用（当前实现看起来符合）。预期效果：3 个因子并行时 ~2-3x 加速。

---

### OPT-05 模型特征重要性持久化与时间趋势分析

**现状：** `feature_importance()` 只在日志中打印，无结构化存储。

**优化方案：**  
在 `ModelTrainer` 训练完成后将特征重要性保存到 `models/{tag}_feature_importance.json`，并提供 `scripts/plot_feature_importance_trend.py` 对比不同时间折叠的重要性变化，检测因子退化迹象。

---

### OPT-06 集成学习使用 Bootstrap 抽样而非固定种子复制

**位置：** `models/lgbm_model.py:129-155`

**现状：** 多个 `ensemble_seeds` 只改变随机种子，训练集完全相同，集成效果接近"多次独立运行求均值"而非真正的 Bagging。

**优化方案：** 对每个 seed，用 bootstrap 抽样 80% 训练数据：
```python
if bagging_fraction < 1.0:
    idx = rng.choice(len(X_tr), int(len(X_tr) * bagging_fraction), replace=False)
    tr_ds = lgb.Dataset(X_tr.iloc[idx], label=y_tr.iloc[idx], ...)
```
预期效果：集成多样性提升，减少过拟合。

---

### OPT-07 `run_daily.py` 数据重复加载

**位置：** `run_daily.py`，`signals/generator.py:105-116`

**现状：** `build_dataset()` 内部加载了一次 qlib feature 数据；`universe_filter.requires_price_data()` 为 True 时又通过 `load_price_data()` 再加载一次价格数据，且 `_fetch_prices` 在 generate() 结束时可能第三次加载。

**优化方案：** 在 `SignalGenerator.generate()` 中统一预加载 price_data，作为所有下游调用的单一数据源。

---

### OPT-08 Walk-forward 折叠定义外化为配置（YAML）

**位置：** `run_walk_forward_validation.py:41-48`，`DEFAULT_FOLDS` 硬编码

**现状：** 折叠边界完全硬编码在 Python 中，无法不修改源码地增加新折叠（如 2027）。

**优化方案：** 将折叠定义移至 `config/walk_forward_folds.yaml`，支持 CLI 传入自定义折叠文件。

---

### OPT-09 SectorFactorEngine `_map_sector_stat` 向量化重构

**位置：** `features/sector_factors.py:216-238`（见 BUG-05）

详见 BUG-05。性能影响约 5-10x，应优先实施。

---

### OPT-10 Robust Score 公式参数可配置化

**位置：** `run_walk_forward_validation.py:130-135`

**现状：** `robust_score = mean_sharpe - 0.5*sharpe_std + 0.2*min_sharpe + 0.05*positive_sharpe_folds` 的系数完全硬编码，且系数本身（尤其是 `0.05 × positive_folds` 权重偏低）可能需要根据研究需求调整。

**优化方案：** 将系数提取到 config 或 CLI 参数，并文档化其含义与选择依据。

---

## 3. 缺失的量化能力规划

### CAP-01 🔴 基准超额收益体系（Alpha / 信息比率）

**重要性：** 高  
**描述：**  
目前所有回测指标均为绝对收益（相对于现金）。量化策略的核心价值在于 **超越基准**（CSI300/CSI500），而非绝对盈利。缺乏超额收益指标会造成：
- 牛市中所有策略都看起来"表现好"
- 无法识别策略的真实 Alpha 来源
- 对模型退化不敏感

**实施方案：**
1. 在 `compute_metrics()` 中增加 `benchmark_rets` 参数，计算以下指标：
   - `excess_annual_return` = 策略年化 - 基准年化
   - `information_ratio` = `mean(daily_alpha) / std(daily_alpha) * sqrt(252)`
   - `tracking_error` = `std(daily_alpha) * sqrt(252)`
   - `beta` = `cov(strategy, benchmark) / var(benchmark)`
2. 在 `BacktestEngine.run()` 中传入 `benchmark="SH000300"`
3. 在所有报告中同时显示绝对和超额指标

---

### CAP-02 🔴 基本面因子库（价值/质量/成长因子）

**重要性：** 高  
**描述：**  
框架目前完全基于价格-成交量数据（Alpha158 + 自定义技术因子）。A 股市场大量 Alpha 来自基本面，包括：

| 因子类别 | 代表因子 | 数据来源 |
|---------|---------|---------|
| 价值 | PE, PB, PS, PCF | 财报 |
| 质量 | ROE, ROA, 毛利率, 负债率 | 财报 |
| 成长 | 营收同比, 净利润同比, 预期修订 | 财报 + 分析师 |
| 分红 | 股息率, 分红增长 | 公告 |
| 分析师 | 一致预期上调/下调, 覆盖数变化 | Wind/聚源 |

**实施方案：**
1. 创建 `features/fundamental_factors.py`，注册为 `"fundamental"`
2. 数据源优先用 akshare 的财报接口（`ak.stock_financial_report_sina`）
3. 因子计算需处理财报滞后发布（T+45 天）避免前视偏差
4. 加入 `config/model.yaml → features.factors` 列表

---

### CAP-03 🔴 因子 IC 衰减分析（因子半衰期）

**重要性：** 高  
**描述：**  
当前 `signal_diagnostics.py` 只计算单一时间点的 IC/ICIR，无法回答：
- 模型在 1 天/3 天/5 天/10 天后预测能力如何衰减？
- 当前使用的持仓周期（hold_thresh）是否与信号半衰期匹配？
- 不同因子的最优持仓周期是否一致？

**实施方案：**
```python
# backtest/ic_decay.py
def compute_ic_decay(pred: pd.Series, price_data: pd.DataFrame, 
                     horizons: list[int] = [1,2,3,5,10,15,20]) -> pd.DataFrame:
    """计算不同预测期的 RankIC，返回 IC 衰减曲线"""
    ...
```
集成到 `run_backtest.py --ic-decay` 选项。

---

### CAP-04 🟡 多因子相关性与拥挤度监控

**重要性：** 中  
**描述：**  
技术因子库（MA cross、RSI、OBV、VWAP 等）之间可能存在高度相关性，导致：
- 有效因子数量少于表面上的数量
- LightGBM 可能将相关因子分配权重但不提升预测力
- 拥挤交易（同类型信号同时触发大量卖出）

**实施方案：**
1. 在 `features/library/screener.py` 的 `FactorScreener` 增加相关系数去重（已有框架）
2. 将 IC 相关矩阵集成到 `run_factor_mining.py` 输出
3. 每次训练后生成因子相关热图

---

### CAP-05 🟡 持仓换手率跟踪与成本敏感性分析

**重要性：** 中  
**描述：**  
框架回测使用固定的 `open_cost=0.0005, close_cost=0.0015`，但没有分析：
- 实际年化换手率是多少？
- 如果滑点增加 2bp，年化收益下降多少？
- 最优参数在高换手率时是否仍然稳健？

**实施方案：**
1. `compute_metrics()` 增加 `turnover` 指标（持仓变动天数 / 总天数）
2. 增加 `--slippage-sensitivity` 选项，在不同交易成本假设下跑同一组参数
3. 在报告中标注每个参数组合的理论换手率

---

### CAP-06 🟡 市场状态感知（熊/牛/震荡）

**重要性：** 中  
**描述：**  
A 股有显著的市场状态周期性（参考走势：2015 暴涨/暴跌、2018 熊市、2020 疫情、2021-2023 震荡下跌）。单一参数集在不同市场状态下表现差异巨大：
- 高换手（小 `hold_thresh`）在趋势市场中跑赢
- 低换手（大 `hold_thresh`）在震荡市场中避免来回打脸

**实施方案：**
1. `features/regime_features.py`：基于 CSI300 的移动均线、波动率区间、ADX 等判断当前市场状态
2. 在信号生成阶段根据市场状态动态切换策略参数（从 `config/strategy_candidates.yaml` 中选择）
3. Walk-forward 结果按市场状态分层分析

---

### CAP-07 🟡 持仓风险控制（止损/最大集中度/波动率目标）

**重要性：** 中  
**描述：**  
`csi800_aggressive_return` 策略在某些年份有 50-60% 的单股集中度（已在 `strategy_candidates.yaml` 注明），这在实盘中完全不可接受。框架缺乏：

- 单股最大权重上限（当前 `max_position_pct` 只在信号层生效，回测不受限）
- 组合波动率目标（如目标年化波动 15%，动态调整仓位）
- 动态止损（如最大回撤超过 15% 触发清仓/减仓）

**实施方案：**
1. 在 `BacktestEngine` 中增加 `position_limit` 参数传给 qlib 策略
2. 实现 `VolatilityTargetSizer`：根据历史已实现波动率调整每期总仓位
3. 在 `run_daily.py` 中增加最大回撤触发器

---

### CAP-08 🟡 归因分析（因子/板块/个股贡献分解）

**重要性：** 中  
**描述：**  
无法回答"策略收益来自哪里"是策略改进的最大障碍。需要：
- 每日/每月的板块贡献
- 各类因子（动量/价值/技术）的归因
- 单股 P&L 分解

**实施方案：**
1. `backtest/attribution.py`：基于 Brinson-Hood-Beebower 模型的简化版归因
2. 对每个持仓日计算：α = 个股超额 × 权重，按板块/因子聚合
3. 输出月度归因 CSV 和可视化热图

---

### CAP-09 🟡 模型退化监控与自动告警

**重要性：** 中  
**描述：**  
框架使用静态训练模型（最后一次完整训练后不更新）。模型在以下情况下会静默退化：
- 市场结构变化（政策因素、注册制改革等）
- 训练数据与当前市场的分布漂移
- 因子拥挤（大量资金跟随同类因子）

**实施方案：**
1. 在 `run_scheduled_rebalance.py` 中每周计算过去 20 天的滚动 RankIC，与历史均值比较
2. 如果滚动 IC 显著下降（如低于历史均值 - 2σ），触发告警通知
3. 将监控数据存储到 `signals/ic_monitor.csv`

---

### CAP-10 🟡 在线增量学习 / 定期重训计划

**重要性：** 中  
**描述：**  
目前模型是静态的，只有手动执行 `run_train.py` 才会更新。建议建立定期重训机制：
- 每季度（或每月）用扩展窗口重训一次
- 每次重训后自动运行最近 1 年的 hold-out 回测
- 通过 IC 对比决定是否用新模型替换旧模型

**实施方案：**
1. 增加 `run_scheduled_retrain.py`，参数化扩展窗口
2. 集成到 `launchd` 任务（每月第一个交易日）
3. 增加新老模型 IC 对比报告

---

### CAP-11 🟢 另类数据接入（舆情/分析师/资金流向）

**重要性：** 低（但长期重要）  
**描述：**  
A 股 Alpha 中有相当一部分来自非价格数据：

| 数据类型 | 来源 | 信号类型 |
|---------|------|---------|
| 北向资金流向 | akshare `ak.stock_hsgt_north_net_flow_in_em` | 趋势跟随 |
| 股东人数变化 | akshare `ak.stock_hold_num_cninfo` | 散户挤兑 |
| 融资融券 | akshare `ak.stock_margin_detail_szse` | 杠杆信号 |
| 业绩预告/快报 | akshare `ak.stock_notice_report` | 事件驱动 |
| 新闻情绪 | 东方财富新闻 API（crawler 可扩展） | 短期动量 |

**实施方案：** 通过 `CsvFactor` 接口加载预计算的另类数据因子（低侵入性方案）。

---

### CAP-12 🟢 多标签模型（收益/波动率/尾部风险联合预测）

**重要性：** 低  
**描述：**  
当前模型只预测单一标签（5 日收益率）。可以增加：
- **波动率预测**：训练第二个模型预测未来 realized vol，用于 Kelly 头寸调整
- **尾部风险预测**：预测极端负收益（左尾）概率，用于组合风险控制
- **收益/波动比**：用 Sharpe ratio 作为标签，而非原始收益

---

### CAP-13 🟢 策略回测压力测试

**重要性：** 低  
**描述：**  
框架缺乏对极端事件的模拟：
- 2015 年股灾（连续跌停无法出场）
- 2020 年 COVID（流动性骤降）
- 涨跌停板规则下的执行延迟

**实施方案：**  
在 `BacktestEngine` 中增加 `circuit_breaker_dates` 参数，模拟跌停无法出场的情景。

---

## 4. 与量化盈利目标的差距分析

### GAP-01 🔴 单股集中度风险未被管控

**问题：**  
`strategy_candidates.yaml` 中明确记录 `csi800_aggressive_return`（Sharpe 最高的策略）在部分年份单股权重高达 50-60%。回测中这是"合法"的，但实盘中：
- A 股单日成交额有限，大仓位进出会显著影响市价
- 持有 50% 单股等同于高度集中押注，一旦该股出现黑天鹅，组合受损极大
- 实盘净值将与"研究样本"偏差极大

**建议：**  
所有实盘使用的策略参数必须加入单股最大权重约束（建议 ≤ 20%），推荐使用 `csi1000_balanced`（topk=15）或 `csi800_stable_all_positive` 而非 `csi800_aggressive_return`。

---

### GAP-02 🔴 执行假设过于理想化（收盘价成交）

**问题：**  
回测假设以 **收盘价** 成交（`deal_price: close`），这意味着：
- 每天收盘后拿到信号，却用当天收盘价作为成交价（实际不可能）
- A 股 T+1 规则：今天买的股票明天才能卖，但回测中可能当天买卖
- 没有模拟集合竞价滑点（尤其是中小盘，开盘滑点可高达 0.5-1%）

**定量影响估算：**  
若每笔交易引入 0.2% 额外滑点，年化换手率 300%（topk=5 的情况），则滑点年化成本约 0.6%。对于 Sharpe=1.758 的策略，这相当于降低约 10-20% 的实际 Sharpe。

**建议：**  
1. 将 `deal_price` 改为次日开盘价（`open`），更符合实际  
2. 增加成交量约束（每次买入不超过当日成交量的 N%）

---

### GAP-03 🔴 训练/测试划分存在隐性数据泄露风险

**问题：**  
`sector_stocks.json` 是爬取的**当前**板块成分股数据，用于所有历史回测中的 ST 过滤和板块因子计算。这引入了**幸存者偏差**和**前视偏差**：
- 已退市/被 ST 的历史股票可能不在 `sector_stocks.json` 中（幸存者偏差）
- 当前的板块归属关系用于 2015-2020 年的历史数据（前视偏差，公司可能后来才被划入该板块）

**建议：**
1. 为回测构建历史快照式的板块成分股数据（按时间索引）
2. 使用 qlib 内置的历史成分股列表（`InstrumentProvider` 已有快照能力）

---

### GAP-04 🟡 走验折叠数量不足，统计检验力弱

**问题：**  
7 个折叠（2020-2026），每个折叠 1 年。统计上：
- 夏普比率的 t 检验：H0：真实 Sharpe=0，7 个样本的 t 值约为 `mean_sharpe / std_sharpe * sqrt(7)` ≈ 1.758 / 1.349 * 2.65 ≈ 3.45，p≈0.01
- 但这 7 个年份并非独立（有自相关），实际显著性更低
- 策略结论可能受少数特殊年份（2020/2021 A股牛市）主导

**建议：**  
1. 使用月度/季度折叠（更多折叠数，更强统计检验力）  
2. 在报告中显示 t 检验 p-value  
3. 分析特定折叠（如 2022 年）表现不佳的原因，确认这是系统性原因还是偶然

---

### GAP-05 🟡 Robust Score 公式存在自选择偏差

**问题：**  
`robust_score = mean_sharpe - 0.5*sharpe_std + 0.2*min_sharpe + 0.05*positive_folds` 中的系数（0.5, 0.2, 0.05）是在看到所有折叠结果后"设计"出来的，而非预先确定的。这本质上是又一轮隐式参数拟合，会导致对排名靠前策略的过度乐观估计。

**建议：**  
1. 将 robust_score 的系数预先在协议中固定，不随结果调整  
2. 或者使用无需系数的排名方法（如 Pareto 前沿：在 mean_sharpe 和 min_sharpe 双目标上取 Pareto 最优集）

---

### GAP-06 🟡 模型标签（5日收益）与持仓周期不匹配

**问题：**  
模型预测的是 **5 日收益率**（Alpha158 默认标签 `Ref($close, -5)/$close - 1`），但 `hold_thresh` 最优值在 8-10 天。这意味着：
- 模型被优化来预测 5 天后的表现
- 但实际持仓 8-10 天
- 持仓期超过信号预测期，信号在 6-10 天的预测能力已显著衰减

**建议：**  
1. IC 衰减分析（见 CAP-03）来确认最优持仓期  
2. 训练多个标签模型（1d, 5d, 10d, 20d），选择与 hold_thresh 最匹配的  
3. 或改用 `hold_thresh <= 5` 的参数组合

---

### GAP-07 🟡 缺乏真实可交易性验证

**问题：**  
topk=5 的策略选股后，每只股票占账户 20%（若账户 50 万则每只约 10 万）。对于 CSI800 中的中小盘股票，日均成交额可能只有 2000-5000 万，10 万资金买入对市价影响约 0.2-0.5%，实际成本高于回测假设。

**建议：**  
1. 在候选股票池中过滤日均成交额低于账户单笔金额 100 倍以下的股票  
2. 或限制策略只在 CSI300 成分股中操作（流动性充裕）

---

### GAP-08 🟢 Alpha 可能来源于市值暴露

**问题：**  
A 股的量化策略普遍存在小市值因子暴露（small-cap premium），但：
- CSI1000 包含大量小盘股，策略可能实际上是在收割小市值溢价而非真正的 Alpha
- 近年来小市值因子（微盘股）在 A 股大幅波动（2024 年微盘股大跌 35%）
- 如果策略的 Alpha 来源是市值暴露，则它在大盘跑赢时会显著跑输

**建议：**  
1. 增加 `size_neutralize` 选项（类似 `industry_neutralize`），在因子后处理中控制市值暴露  
2. 归因分析中分解出市值因子的贡献（见 CAP-08）

---

## 优先级汇总

| 序号 | 类别 | 标题 | 紧迫性 | 影响范围 |
|-----|------|------|--------|---------|
| BUG-01 | 缺陷 | `_code_to_qlib` 不一致 | 🔴 高 | 板块因子 |
| BUG-04 | 缺陷 | 无基准超额收益指标 | 🔴 高 | 所有评估 |
| BUG-05 | 缺陷 | SectorFactor 双循环性能 | 🔴 高 | 训练速度 |
| BUG-03 | 缺陷 | WFV 并行 CSV 竞争 | ⚠️ 中 | 并行模式 |
| OPT-01 | 优化 | 消除重复代码 | ⚠️ 中 | 可维护性 |
| OPT-02 | 优化 | akshare 并发获取 | ⚠️ 中 | 运行速度 |
| OPT-09 | 优化 | SectorFactor 向量化 | 🔴 高 | 训练速度 |
| CAP-01 | 能力 | 超额收益/信息比率 | 🔴 高 | 策略评估 |
| CAP-02 | 能力 | 基本面因子库 | 🔴 高 | Alpha 来源 |
| CAP-03 | 能力 | IC 衰减分析 | 🔴 高 | 参数优化 |
| CAP-07 | 能力 | 持仓风险控制 | 🔴 高 | 实盘安全 |
| CAP-09 | 能力 | 模型退化监控 | 🟡 中 | 实盘稳健 |
| GAP-01 | 差距 | 单股集中度风险 | 🔴 高 | 实盘安全 |
| GAP-02 | 差距 | 执行假设理想化 | 🔴 高 | 真实收益 |
| GAP-03 | 差距 | 前视偏差/幸存者偏差 | 🔴 高 | 策略可信度 |
| GAP-06 | 差距 | 标签与持仓期不匹配 | 🟡 中 | 模型优化 |

---

*本报告由全代码审计自动生成，覆盖 `models/`、`features/`、`data/`、`backtest/`、`signals/`、`agent/`、`crawler/`、`run_*.py`、`config/` 全部模块。*
