# quant_ex

`quant_ex` 是一个基于 **qlib + Alpha158** 的 A 股低频量化选股研究框架。当前目标不是自动实盘交易，而是帮助个人投资者持续做策略研究、参数验证、每日候选股筛选，并为后续小资金人工模拟交易做准备。

核心能力：

- qlib Alpha158 数据集构建与模型训练
- LightGBM / XGBoost / Ridge / Lasso / MLP 多模型训练，支持 bootstrap ensemble
- TopkDropout 策略回测、网格搜索、多 seed 稳健性评估
- Walk-forward 时间交叉验证（支持自定义折叠 YAML、t 检验显著性）
- 训练股票池与回测股票池交叉验证
- 因子流水线：技术因子、行业/概念因子、挖掘因子、市场状态感知因子（regime）
- FactorScreener：IC/ICIR 阈值 + 相关性去重，自动过滤低质量因子
- 信号后处理：行业中性化、市值中性化、rank/zscore 变换
- Brinson-Hood-Beebower 绩效归因（板块级 allocation / selection / interaction 分解）
- 流动性过滤：均量/均额下限
- 集中度风险检查：Herfindahl 指数 + 硬上限告警
- 每日信号、目标持仓、买卖差分
- IC 衰减分析、滚动 IC 监控
- qlib bin 数据自动更新
- Bark / PushPlus 等通知渠道
- 东方财富板块与成分股数据缓存
- Claude API 辅助回测参数优化

> 本项目仅用于研究和辅助决策，不构成投资建议，也不会自动下单。

---

## 目录

- [当前研究结论](#当前研究结论)
- [快速开始](#快速开始)
- [数据更新](#数据更新)
- [训练模型](#训练模型)
- [回测与网格搜索](#回测与网格搜索)
- [Walk-forward 验证](#walk-forward-验证)
- [每日信号](#每日信号)
- [定时调仓任务](#定时调仓任务)
- [策略候选配置](#策略候选配置)
- [因子流水线](#因子流水线)
- [信号后处理](#信号后处理)
- [绩效归因](#绩效归因)
- [通知渠道](#通知渠道)
- [配置说明](#配置说明)
- [模块结构](#模块结构)
- [东方财富数据](#东方财富数据)
- [常见问题](#常见问题)

---

## 当前研究结论

最新一轮完整 walk-forward 验证结果已经固化到：

```text
config/strategy_candidates.yaml
```

它记录了这轮实验的时间切分、候选训练股票池、策略参数网格和可复用候选策略。`optimization_results/` 是运行产物，默认不入库；真正需要长期保存的结论放在 `config/strategy_candidates.yaml`。

当前主要候选：

| 候选 | 训练股票池 | 回测股票池 | topk | n_drop | hold_thresh | 定位 |
|---|---|---|---:|---:|---:|---|
| `csi800_aggressive_return` | csi800 | csi300 | 5 | 3 | 8 | 收益最高，但单票集中度高 |
| `csi1000_balanced` | csi1000 | csi300 | 15 | 3 | 5 | 更均衡，适合作为人工选股优先候选 |
| `csi800_stable_all_positive` | csi800 | csi300 | 15 | 3 | 5 | 7 个 fold 均为正，但波动仍需观察 |

阶段性判断：

- `csi800/topk=5/n_drop=3/hold=8` 的 2026 表现很强，但收益高度集中在少数股票，最大单票权重一度超过 50%，更适合作为研究样本，不宜直接照搬到人工实盘。
- `csi1000/topk=15/n_drop=3/hold=5` 的信号诊断和风险表现更均衡，更适合继续做"个人低频辅助选股"的候选。
- 下一步重点不是继续盲目提高收益，而是加入单票仓位上限、行业暴露约束、流动性约束，再重新 walk-forward。

---

## 快速开始

### 1. 环境

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

本机轻量检查也可以使用已有环境：

```bash
/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -m pytest test/test_universe_filter.py test/test_trainer.py
```

项目自己的 `.venv` 当前用于 qlib 训练和回测。

### 2. 配置 qlib 数据路径

编辑 `config/base.yaml`：

```yaml
qlib:
  provider_uri: "./qlib_data/qlib_bin"
  region: "cn"
```

### 3. 检查注册表

```bash
./.venv/bin/python run_train.py --list-registry
```

预期模型注册包含：`lgbm, xgb, ridge, lasso, mlp`  
预期因子注册包含：`sector, technical, mined, regime`

---

## 数据更新

数据更新入口：

```bash
./.venv/bin/python run_update_qlib_data.py
```

该流程负责：Dolt clone/pull → Dolt SQL server → 导出 qlib source CSV → normalize → dump 成 qlib bin。

常用参数：

```bash
# 指定目录
./.venv/bin/python run_update_qlib_data.py \
  --workspace-dir ./qlib_data \
  --qlib-dir ./qlib_data/qlib_bin

# 已有 Dolt SQL server 正在跑时复用它
./.venv/bin/python run_update_qlib_data.py --reuse-dolt-server

# 跳过 dolt pull，只基于已有数据导出
./.venv/bin/python run_update_qlib_data.py --skip-dolt-pull

# 用 akshare 补充 Dolt 数据缺口（近几日 Dolt 未更新时）
./.venv/bin/python run_update_qlib_data.py --supplement-source akshare
```

注意：`qlib_data/`、`backtest_results/`、`optimization_results/` 都是运行产物，默认被 `.gitignore` 忽略。

---

## 训练模型

### 自定义模型

```bash
./.venv/bin/python run_train.py --model lgbm --tag baseline
./.venv/bin/python run_train.py --model xgb --tag xgb_baseline
./.venv/bin/python run_train.py --model ridge --tag ridge_baseline
```

只使用 Alpha158 原始特征：

```bash
./.venv/bin/python run_train.py --model lgbm --no-extra-factors --tag alpha158_only
```

加入行业因子：

```bash
./.venv/bin/python run_train.py --model lgbm --with-sector --tag sector_full
```

### Bootstrap Ensemble

在 `config/model.yaml` 中启用：

```yaml
model:
  ensemble:
    enabled: true
    seeds: [42, 123, 2024]
    bagging_fraction: 0.8   # 每个成员有放回采样 80% 训练数据（可选）
```

### qlib-native 模式

```bash
./.venv/bin/python run_train.py --qlib-native
```

训练完成后把 Recorder ID 写入 `config/base.yaml`：

```yaml
experiment:
  latest_recorder_id: "<recorder_id>"
```

自定义模型保存为 `models/*.pkl`（含 `_meta.json` 和 `_feature_importance.json` sidecar）。

---

## 回测与网格搜索

基础回测：

```bash
./.venv/bin/python run_backtest.py --model-path models/lgbm_xxx.pkl
```

指定参数网格：

```bash
./.venv/bin/python run_backtest.py \
  --model-path models/lgbm_xxx.pkl \
  --market csi300 \
  --topk 5,15,20 \
  --n-drop 1,3 \
  --hold-thresh 5,8,10
```

多 seed 稳健性：

```bash
./.venv/bin/python run_backtest.py --model-path models/lgbm_xxx.pkl --seeds
```

输出 CSV 到指定路径（用于 walk-forward 路径隔离）：

```bash
./.venv/bin/python run_backtest.py --model-path models/lgbm_xxx.pkl --output-csv results/my_run.csv
```

回测结果列包含：`annual_return`, `sharpe`, `max_drawdown`, `calmar`, `win_rate`, `ic`, `icir`, `rank_ic`, `rank_icir`

---

## Walk-forward 验证

完整时间交叉验证：

```bash
./.venv/bin/python run_walk_forward_validation.py \
  --train-universes csi300,csi800,csi1000 \
  --eval-market csi300 \
  --topk 5,15,20 \
  --n-drop 1,3 \
  --hold-thresh 5,8,10
```

并行运行（M3 建议 2-3 workers）：

```bash
./.venv/bin/python run_walk_forward_validation.py \
  --workers 3 --grid-workers 1 \
  --train-universes csi300,csi800,csi1000
```

自定义折叠定义（替换内置 7 折 2020-2026 方案）：

```bash
./.venv/bin/python run_walk_forward_validation.py \
  --folds-config config/walk_forward_folds.yaml \
  --train-universes csi300
```

示例折叠 YAML 格式见 `config/walk_forward_folds.yaml.example`。

调整稳健得分权重：

```bash
./.venv/bin/python run_walk_forward_validation.py \
  --robust-weights '{"mean_sharpe": 1.0, "sharpe_std": -0.3, "min_sharpe": 0.5, "positive_sharpe_folds": 0.05}'
```

汇总表包含 `sharpe_ttest_pvalue` 和 `return_ttest_pvalue` 统计显著性列。

输出目录结构：

```text
optimization_results/walk_forward_<run_id>/
├── configs/            # 每折训练配置
├── logs/               # 每折训练/回测日志
├── fold_results/       # 每折隔离 CSV（无并行竞争）
├── metadata.json
├── walk_forward_all_results.csv
├── walk_forward_summary.csv
└── walk_forward_report.md
```

---

## 每日信号

生成每日候选股：

```bash
./.venv/bin/python run_daily.py --model-path models/lgbm_xxx.pkl --dry-run
```

使用策略覆盖配置（训练池与评估池分离时推荐）：

```bash
./.venv/bin/python run_daily.py \
  --config config/daily_csi1000.yaml \
  --model-path models/lgbm_xxx.pkl \
  --dry-run
```

指定账户金额和当前持仓：

```bash
./.venv/bin/python run_daily.py \
  --model-path models/lgbm_xxx.pkl \
  --account 500000 \
  --positions SH600000:500,SZ000001:300
```

信号生成后会自动执行**集中度风险检查**，在 log 中输出：
- 单股权重超过 `max_position_pct` 时 WARNING
- 超过 `concentration_hard_limit` 时 ERROR
- Herfindahl 有效分散数报告

---

## 定时调仓任务

`run_scheduled_rebalance.py` 用于收盘后自动更新数据、回测、缓存调仓信号，并通过 Bark 推送次日调仓动作。

手动 mock 测试：

```bash
./.venv/bin/python run_scheduled_rebalance.py --mock --dry-run
```

安装 macOS launchd 定时任务：

```bash
scripts/install_daily_rebalance_launchd.sh
```

注册三个定时任务：

| 任务 | 时间 | 功能 |
|---|---:|---|
| `com.quant_ex.daily_rebalance` | 20:00 | 更新数据 → 回测 → 缓存信号 → 推送 Bark |
| `com.quant_ex.daily_rebalance.open_reminder` | 09:00 | 读缓存，开盘前再次提醒 |
| `com.quant_ex.daily_rebalance.close_reminder` | 14:00 | 读缓存，收盘前再次提醒 |

> **重要**：`start_date` 必须早于今天至少几个交易日。`TopkDropoutStrategy` 在回测首日不开仓，若 `start_date` 等于今天，报告将显示"无目标持仓"。

> **首日操作**：不要直接照搬"目标持仓摘要"，只执行"次交易日调仓动作"中的**买入**操作。后续日再正常执行买卖。

---

## 策略候选配置

```text
config/strategy_candidates.yaml
```

该文件不被 `load_config()` 自动加载，是人工归档研究结论的地方。`optimization_results/` 是瞬态，长期结论应整理到这里。

---

## 因子流水线

在 `config/model.yaml` 的 `features.factors` 列表中配置：

```yaml
model:
  features:
    factors:
      - name: technical            # 技术因子（动量、波动、换手等）
      - name: sector               # 行业/概念轮动因子（需 --with-sector）
        include_sector_momentum: true
        include_stock_vs_sector: true
        include_sector_reversal: true   # 真正的反转因子：短期动量 - 长期动量
        include_concept: true
      - name: mined                # 挖掘因子（需先运行 run_factor_mining.py）
        path: "./cache/mined_factors.json"
      - name: regime               # 市场状态感知因子（新增）
        windows: [20, 60]
        dd_window: 120
```

`regime` 因子产出：`regime_trend_{w}d`, `regime_vol_{w}d`, `regime_breadth_{w}d`, `regime_corr_{w}d`, `regime_drawdown`, `regime_label`（0=calm_bull / 1=calm_bear / 2=volatile_bull / 3=volatile_bear）。

### FactorScreener

自动过滤低 IC / 高相关因子：

```yaml
model:
  features:
    screener:                    # 新增，可选
      min_ic: 0.02
      min_icir: 0.3
      max_corr: 0.7
```

或在代码中：

```python
pipeline = FactorPipeline.from_config(factor_configs, screener_config={"min_ic": 0.02, "min_icir": 0.3})
kept = pipeline.compute_with_screening(price_data, forward_returns=label)
```

---

## 信号后处理

在 `config/base.yaml` 的 `signal.postprocess` 小节配置（均默认禁用，向后兼容）：

```yaml
signal:
  postprocess:
    enabled: true
    daily_transform: "rank"         # rank | zscore | none
    rank_pct: true
    industry_neutralize: false       # 行业中性化（减去同日同板块均值）
    size_neutralize: false           # 市值中性化（截面 OLS 去除线性市值暴露，新增）
```

---

## 流动性过滤

在 `strategy.universe_filter` 小节配置（新增，默认禁用）：

```yaml
strategy:
  universe_filter:
    exclude_kcb: true
    exclude_st: true
    exclude_suspended: true
    min_price: 3
    min_avg_volume: 1000000        # N 日均成交量下限（股数，新增）
    avg_volume_window: 20
    min_avg_amount: 50000000       # N 日均成交额下限（元，新增，需 $amount 列）
    avg_amount_window: 20
```

---

## 绩效归因

Brinson-Hood-Beebower 板块级分解（新增模块 `backtest/attribution.py`）：

```python
from quant_ex.backtest.attribution import brinson_attribution, format_attribution

result = brinson_attribution(
    portfolio_weights,   # (instrument, datetime) MultiIndex Series，每日持仓权重
    benchmark_weights,   # 基准权重，同结构
    returns,             # 日收益率，同结构
    sector_map,          # {instrument: sector_name}
)
print(format_attribution(result))
# 输出：Allocation / Selection / Interaction 按板块分解
```

---

## IC 诊断

```python
from quant_ex.backtest.signal_diagnostics import compute_ic_decay, compute_rolling_ic

# IC 衰减分析：确定最优 hold_thresh
decay = compute_ic_decay(pred, price_data)

# 滚动 IC 监控：检测模型退化
monitor = compute_rolling_ic(pred, price_data, horizon=5, window=20)
# 输出: datetime | daily_rank_ic | rolling_rank_ic | rolling_icir
```

---

## 通知渠道

```bash
cp config/notify.yaml.example config/notify.yaml
```

支持：Bark（iOS）、PushPlus（微信）、DingTalk（钉钉）、Server 酱、微信公众号模板消息。

测试：

```bash
./.venv/bin/python run_notify_test.py --channel bark
```

---

## 配置说明

配置加载顺序：

```text
config/base.yaml → config/model.yaml → config/notify.yaml → --config 覆盖文件
```

集中度风险配置（新增）：

```yaml
strategy:
  portfolio:
    max_position_pct: 0.25
    concentration_hard_limit: 0.35   # 可选，超出时 log ERROR
```

---

## 模块结构

```text
quant_ex/
├── config/
│   ├── base.yaml
│   ├── model.yaml
│   ├── notify.yaml.example
│   ├── strategy_candidates.yaml
│   ├── walk_forward_folds.yaml.example   # 自定义折叠示例（新增）
│   └── daily_csi1000.yaml               # per-strategy 覆盖配置示例
├── data/
│   ├── loader.py
│   ├── utils.py                  # 统一代码转换 + 股票名称缓存
│   ├── sector.py                 # 行业数据（并发 akshare 抓取）
│   ├── universe.py               # 股票池过滤（含流动性过滤）
│   ├── sources/                  # GapFiller（akshare/eastmoney 补充数据）
│   └── qlib_update/
├── features/
│   ├── base.py                   # BaseFactor + FactorRegistry + FactorPipeline
│   ├── sector_factors.py         # 向量化行业因子
│   ├── technical_factors.py      # 技术因子
│   ├── factor_mining.py          # 挖掘因子（含 qlib init 保护）
│   ├── regime_features.py        # 市场状态感知因子（新增）
│   └── library/
│       ├── screener.py           # FactorEvaluator + FactorScreener
│       └── cleaner.py
├── models/
│   ├── base.py
│   ├── lgbm_model.py             # 支持 bootstrap bagging
│   ├── nn_model.py
│   ├── linear_model.py
│   ├── xgb_model.py
│   └── trainer.py
├── backtest/
│   ├── engine.py
│   ├── grid_search.py
│   ├── metrics.py                # 含基准超额、IR、换手率指标
│   ├── signal_diagnostics.py     # IC 衰减、滚动 IC 监控
│   └── attribution.py            # Brinson 绩效归因（新增）
├── signals/
│   ├── generator.py              # 含停牌过滤、price_data 复用
│   └── postprocess.py            # 含市值中性化
├── notify/
│   └── pusher.py
├── crawler/
│   ├── eastmoney/
│   └── scripts/
├── agent/
│   └── auto_optimizer.py
├── run_train.py
├── run_backtest.py
├── run_walk_forward_validation.py
├── run_daily.py                  # 含集中度风险检查
├── run_update_qlib_data.py
├── run_factor_mining.py
├── run_scheduled_rebalance.py
├── run_notify_test.py
└── run_wechat_openids.py
```

---

## 东方财富数据

`crawler/eastmoney/` 是独立 SDK，不依赖 qlib。**不要使用代理**（直连可用，代理返回空结果）。

```bash
./.venv/bin/python crawler/scripts/fetch_sector_enums.py       # 刷新 sector_codes.json
./.venv/bin/python crawler/scripts/fetch_sector_stocks.py --resume  # 可断点续传
```

---

## Claude AI 优化器

```bash
export ANTHROPIC_API_KEY="..."
./.venv/bin/python run_backtest.py --optimize --n-iters 3
```

适合辅助研究，不能替代 walk-forward 验证。

---

## 常见问题

**Q: qlib 数据路径找不到？**  
A: 检查 `config/base.yaml → qlib.provider_uri`，目录下应有 `calendars/`、`features/`、`instruments/`。

**Q: Dolt 更新时提示 locked？**  
A: 确认无 dolt 进程在跑；stale LOCK 会自动清理。已有 SQL server 时用 `--reuse-dolt-server`。

**Q: `run_backtest.py` 并行报 semaphore 错误？**  
A: 用 `--grid-workers 1` 串行跑或降低并行数量。

**Q: Walk-forward 报告 sharpe_p 列是什么？**  
A: 一样本 t 检验 p 值，H₀: mean(sharpe) = 0。越小说明结果统计显著，通常期望 < 0.05。

**Q: 为什么 2026 某些参数收益特别高？**  
A: `csi800/topk=5/n_drop=3/hold=8` 收益集中在少数重仓股票，需加仓位上限后再验证。

**Q: 模型文件和实验结果为什么没有入库？**  
A: `models/*.pkl`、`backtest_results/`、`optimization_results/` 是运行产物，体积大且可再生。长期结论写入 `config/strategy_candidates.yaml`。

---

## License

MIT License. 本项目仅供学习和研究使用，不构成投资建议。市场有风险，投资需谨慎。


`quant_ex` 是一个基于 **qlib + Alpha158** 的 A 股低频量化选股研究框架。当前目标不是自动实盘交易，而是帮助个人投资者持续做策略研究、参数验证、每日候选股筛选，并为后续小资金人工模拟交易做准备。

核心能力：

- qlib Alpha158 数据集构建与模型训练
- LightGBM / XGBoost / Ridge / Lasso / MLP 多模型训练
- TopkDropout 策略回测、网格搜索、多 seed 稳健性评估
- walk-forward 时间交叉验证
- 训练股票池与回测股票池交叉验证
- 每日信号、目标持仓、买卖差分
- qlib bin 数据自动更新
- Bark / PushPlus 等通知渠道
- 东方财富板块与成分股数据缓存
- Claude API 辅助回测参数优化

> 本项目仅用于研究和辅助决策，不构成投资建议，也不会自动下单。

---

## 目录

- [当前研究结论](#当前研究结论)
- [快速开始](#快速开始)
- [数据更新](#数据更新)
- [训练模型](#训练模型)
- [回测与网格搜索](#回测与网格搜索)
- [Walk-forward 验证](#walk-forward-验证)
- [每日信号](#每日信号)
- [定时调仓任务](#定时调仓任务)
- [策略候选配置](#策略候选配置)
- [通知渠道](#通知渠道)
- [配置说明](#配置说明)
- [模块结构](#模块结构)
- [东方财富数据](#东方财富数据)
- [常见问题](#常见问题)

---

## 当前研究结论

最新一轮完整 walk-forward 验证结果已经固化到：

```text
config/strategy_candidates.yaml
```

它记录了这轮实验的时间切分、候选训练股票池、策略参数网格和可复用候选策略。`optimization_results/` 是运行产物，默认不入库；真正需要长期保存的结论放在 `config/strategy_candidates.yaml`。

当前主要候选：

| 候选 | 训练股票池 | 回测股票池 | topk | n_drop | hold_thresh | 定位 |
|---|---|---|---:|---:|---:|---|
| `csi800_aggressive_return` | csi800 | csi300 | 5 | 3 | 8 | 收益最高，但单票集中度高 |
| `csi1000_balanced` | csi1000 | csi300 | 15 | 3 | 5 | 更均衡，适合作为人工选股优先候选 |
| `csi800_stable_all_positive` | csi800 | csi300 | 15 | 3 | 5 | 7 个 fold 均为正，但波动仍需观察 |

阶段性判断：

- `csi800/topk=5/n_drop=3/hold=8` 的 2026 表现很强，但收益高度集中在少数股票，最大单票权重一度超过 50%，更适合作为研究样本，不宜直接照搬到人工实盘。
- `csi1000/topk=15/n_drop=3/hold=5` 的信号诊断和风险表现更均衡，更适合继续做“个人低频辅助选股”的候选。
- 下一步重点不是继续盲目提高收益，而是加入单票仓位上限、行业暴露约束、流动性约束，再重新 walk-forward。

---

## 快速开始

### 1. 环境

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

本机轻量检查也可以使用已有环境：

```bash
/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -m pytest test/test_universe_filter.py test/test_trainer.py
```

项目自己的 `.venv` 当前用于 qlib 训练和回测。

### 2. 配置 qlib 数据路径

编辑 `config/base.yaml`：

```yaml
qlib:
  provider_uri: "./qlib_data/qlib_bin"
  region: "cn"
```

### 3. 检查注册表

```bash
./.venv/bin/python run_train.py --list-registry
```

预期模型注册包含：

```text
lgbm, xgb, ridge, lasso, mlp
```

---

## 数据更新

数据更新入口：

```bash
./.venv/bin/python run_update_qlib_data.py
```

该流程已经从旧的外部 `dump_qlib_bin.sh` 迁移到项目内，主要逻辑在：

```text
data/qlib_update/
```

它负责：

- clone / 维护 Dolt 原始数据库
- 启动 Dolt SQL server
- 导出 qlib source CSV
- normalize 数据
- dump 成 qlib bin
- 生成可选 tarball

常用参数：

```bash
# 使用默认 workspace_dir 和 qlib.provider_uri
./.venv/bin/python run_update_qlib_data.py

# 指定目录
./.venv/bin/python run_update_qlib_data.py \
  --workspace-dir ./qlib_data \
  --qlib-dir ./qlib_data/qlib_bin

# 已有 Dolt SQL server 正在跑时复用它
./.venv/bin/python run_update_qlib_data.py --reuse-dolt-server

# 跳过 dolt pull，只基于已有数据导出
./.venv/bin/python run_update_qlib_data.py --skip-dolt-pull

# 浅克隆 Dolt 仓库
./.venv/bin/python run_update_qlib_data.py --shallow-dolt-clone --clone-depth 1
```

注意：

- `qlib_data/`、`backtest_results/`、`optimization_results/` 都是运行产物，默认被 `.gitignore` 忽略。
- Dolt 仓库被其他进程锁住时，脚本会给出明确提示。`dolt status` 遗留的 stale LOCK 文件现在会自动清理。
- 如果已有 `dolt sql-server` 在运行，可复用：`--reuse-dolt-server`
- qlib repo 和 scripts 路径会自动加入 `PYTHONPATH`，用于 normalize / dump 阶段。

---

## 训练模型

### 自定义模型

```bash
./.venv/bin/python run_train.py --model lgbm --tag baseline
./.venv/bin/python run_train.py --model xgb --tag xgb_baseline
./.venv/bin/python run_train.py --model ridge --tag ridge_baseline
./.venv/bin/python run_train.py --model lasso --tag lasso_baseline
```

只使用 Alpha158 原始特征：

```bash
./.venv/bin/python run_train.py --model lgbm --no-extra-factors --tag alpha158_only
```

加入行业因子：

```bash
./.venv/bin/python run_train.py --model lgbm --with-sector --tag sector_full
```

MLP 模型：

```bash
./.venv/bin/python run_train.py --model mlp --tag mlp_baseline
```

`mlp` 依赖 PyTorch，未安装时会给出明确错误。Apple Silicon 会优先使用 MPS。

### qlib-native 模式

```bash
./.venv/bin/python run_train.py --qlib-native
```

训练完成后会输出 Recorder ID，可填入 `config/base.yaml`：

```yaml
experiment:
  latest_recorder_id: "<recorder_id>"
```

自定义模型则保存为 `models/*.pkl`，通过 `--model-path` 使用。模型文件本身不入库。

---

## 回测与网格搜索

基础回测：

```bash
./.venv/bin/python run_backtest.py --model-path models/lgbm_xxx.pkl
```

指定参数网格：

```bash
./.venv/bin/python run_backtest.py \
  --model-path models/lgbm_xxx.pkl \
  --market csi300 \
  --topk 5,15,20 \
  --n-drop 1,3 \
  --hold-thresh 5,8,10
```

指定时间：

```bash
./.venv/bin/python run_backtest.py \
  --model-path models/lgbm_xxx.pkl \
  --start 2024-01-01 \
  --end 2025-12-31
```

探索多个候选股票池：

```bash
./.venv/bin/python run_backtest.py \
  --model-path models/lgbm_xxx.pkl \
  --markets csi300,csi500,csi800,csi1000
```

多 seed 稳健性：

```bash
./.venv/bin/python run_backtest.py --model-path models/lgbm_xxx.pkl --seeds
```

控制网格搜索并行：

```bash
# 默认使用全部 CPU worker
./.venv/bin/python run_backtest.py --model-path models/lgbm_xxx.pkl --grid-workers -1

# 受限环境或排查问题时串行
./.venv/bin/python run_backtest.py --model-path models/lgbm_xxx.pkl --grid-workers 1
```

回测结果会写入：

```text
backtest_results/
```

结果列包含：

- `annual_return`
- `sharpe`
- `max_drawdown`
- `calmar`
- `win_rate`
- `ic`
- `icir`
- `rank_ic`
- `rank_icir`

---

## Walk-forward 验证

完整时间交叉验证入口：

```bash
./.venv/bin/python run_walk_forward_validation.py \
  --train-universes csi300,csi800,csi1000 \
  --eval-market csi300 \
  --topk 5,15,20 \
  --n-drop 1,3 \
  --hold-thresh 5,8,10
```

固定 run id，支持断点续跑：

```bash
./.venv/bin/python run_walk_forward_validation.py \
  --run-id 20260428_full_wf \
  --train-universes csi300,csi800,csi1000 \
  --eval-market csi300 \
  --topk 5,15,20 \
  --n-drop 1,3 \
  --hold-thresh 5,8,10
```

并行运行 fold × train_universe：

```bash
./.venv/bin/python run_walk_forward_validation.py \
  --workers 3 \
  --grid-workers 1 \
  --train-universes csi300,csi800,csi1000
```

输出目录：

```text
optimization_results/walk_forward_<run_id>/
├── configs/
├── logs/
├── fold_results/
├── metadata.json
├── walk_forward_all_results.csv
├── walk_forward_summary.csv
└── walk_forward_report.md
```

`optimization_results/` 默认不入库。要长期保留的结论应整理到：

```text
config/strategy_candidates.yaml
```

---

## 每日信号

生成每日候选股：

```bash
./.venv/bin/python run_daily.py --model-path models/lgbm_xxx.pkl --dry-run
```

使用策略覆盖配置（推荐：训练股票池与评估股票池分离时）：

```bash
./.venv/bin/python run_daily.py \
  --config config/daily_csi1000.yaml \
  --model-path models/lgbm_xxx.pkl \
  --dry-run
```

指定账户金额和当前持仓：

```bash
./.venv/bin/python run_daily.py \
  --model-path models/lgbm_xxx.pkl \
  --account 500000 \
  --positions SH600000:500,SZ000001:300
```

没有接入真实通知渠道时，建议先用：

```bash
./.venv/bin/python run_daily.py --dry-run
```

---

## 定时调仓任务

`run_scheduled_rebalance.py` 用于收盘后自动更新数据、重放固定起点回测、缓存调仓信号，并通过 Bark 推送下一交易日需要执行的调仓动作。默认策略配置在 `config/base.yaml`：

```yaml
daily_rebalance:
  start_date: "2024-01-01"
  market: "csi1000"
  topk: 15
  n_drop: 3
  hold_thresh: 5
  account: 1000000
  model_path: ""           # 可填 models/*.pkl；为空时使用 experiment.latest_recorder_id
  notify_channel: "bark"
  reminder_rebuild_on_miss: true
```

真实运行前需要二选一配置模型来源：

```yaml
daily_rebalance:
  model_path: "models/lgbm_xxx.pkl"
```

或填写 `experiment.latest_recorder_id`。

手动 mock 测试，不更新数据、不回测、不推送：

```bash
./.venv/bin/python run_scheduled_rebalance.py --mock --dry-run
```

输出现在包含股票中文名称和板块信息：

```text
买入 SH600216 浙江医药: +500股 @ 12.40 约6,200元
目标持仓: SH600216 浙江医药 [原料药]: 500股 约6,200元
```

手动发送一条 mock Bark 测试：

```bash
./.venv/bin/python run_scheduled_rebalance.py --mock
```

安装 macOS launchd 定时任务：

```bash
scripts/install_daily_rebalance_launchd.sh
```

安装后会注册三个当前用户任务：

| 任务 | 时间 | 功能 |
|---|---:|---|
| `com.quant_ex.daily_rebalance` | 20:00 | 更新 qlib 数据，确认交易日，生成并缓存调仓信号，推送 Bark |
| `com.quant_ex.daily_rebalance.open_reminder` | 09:00 | 读取前一交易日缓存，开盘前再次提醒 |
| `com.quant_ex.daily_rebalance.close_reminder` | 14:00 | 读取前一交易日缓存，收盘前再次提醒 |

如果 09:00 或 14:00 没有读到当天要执行的缓存，且 `daily_rebalance.reminder_rebuild_on_miss: true`，脚本会尝试重新更新 qlib 数据，并从固定 `start_date` 回测到上一交易日，再缓存和推送提醒。这用于覆盖前一晚数据源延迟或 20:00 任务失败的情况。

查看 launchd 中的任务：

```bash
launchctl print gui/$(id -u) | grep quant_ex
launchctl print gui/$(id -u)/com.quant_ex.daily_rebalance
launchctl print gui/$(id -u)/com.quant_ex.daily_rebalance.open_reminder
launchctl print gui/$(id -u)/com.quant_ex.daily_rebalance.close_reminder
```

重点看 `runs`、`last exit code` 和 `event triggers`。`last exit code = 0` 通常表示上次执行成功。

日志文件：

```text
logs/daily_rebalance.out.log
logs/daily_rebalance.err.log
logs/daily_rebalance_open_reminder.out.log
logs/daily_rebalance_open_reminder.err.log
logs/daily_rebalance_close_reminder.out.log
logs/daily_rebalance_close_reminder.err.log
```

手动触发某个任务并看日志：

```bash
launchctl kickstart -k gui/$(id -u)/com.quant_ex.daily_rebalance.open_reminder
tail -n 100 logs/daily_rebalance_open_reminder.out.log
tail -n 100 logs/daily_rebalance_open_reminder.err.log
```

调仓缓存写入 `signals/daily_rebalance_cache/`，默认不入库。

---

## 策略候选配置

本项目现在有一个专门保存研究结论的配置文件：

```text
config/strategy_candidates.yaml
```

它不会被 `utils.config.load_config()` 自动加载，目的是避免把研究候选误当成默认实盘参数。使用方式是人工选择 candidate，然后按里面记录的训练股票池和策略参数运行训练/回测/每日信号。

查看候选：

```bash
./.venv/bin/python - <<'PY'
import yaml
from pathlib import Path

data = yaml.safe_load(Path("config/strategy_candidates.yaml").read_text())
print(data["selected"])
for name, item in data["candidates"].items():
    print(name, item["train_universe"], item["strategy"])
PY
```

示例：使用 `csi1000_balanced` 思路重新训练：

```bash
cat > /tmp/csi1000_balanced.yaml <<'YAML'
market:
  name: csi1000
YAML

./.venv/bin/python run_train.py \
  --config /tmp/csi1000_balanced.yaml \
  --model lgbm \
  --no-extra-factors \
  --tag csi1000_balanced
```

再回测：

```bash
./.venv/bin/python run_backtest.py \
  --model-path models/lgbm_csi1000_balanced_xxx.pkl \
  --market csi300 \
  --topk 15 \
  --n-drop 3 \
  --hold-thresh 5
```

---

## 通知渠道

复制模板：

```bash
cp config/notify.yaml.example config/notify.yaml
```

支持渠道：

| 渠道 | 用途 |
|---|---|
| Bark | iOS 推送 |
| PushPlus | 微信推送 |
| DingTalk | 钉钉机器人 |
| Server 酱 | 微信推送 |
| WeChat MP | 微信公众号模板消息 |

测试通知：

```bash
./.venv/bin/python run_notify_test.py --channel bark
./.venv/bin/python run_notify_test.py --channel pushplus
```

微信公众号 OpenID 查询脚本：

```bash
./.venv/bin/python run_wechat_openids.py
```

`config/notify.yaml` 不入库。

---

## 配置说明

配置加载顺序：

```text
config/base.yaml
  -> config/model.yaml
  -> config/strategy.yaml  # 兼容旧文件，通常不存在
  -> config/notify.yaml
  -> 用户通过 --config 指定的覆盖文件
```

常用配置：

```yaml
qlib:
  provider_uri: "./qlib_data/qlib_bin"
  region: "cn"

market:
  name: "csi300"
  candidates: ["csi300", "csi500", "csi800", "csi1000", "csiall", "all"]
  benchmark: "SH000300"

strategy:
  topk_dropout:
    topk: 10
    n_drop: 3
    hold_thresh: 5
  universe_filter:
    exclude_kcb: true
    exclude_list: []
    min_price: 3
    exclude_st: true           # 基于名称排除 ST 股
    exclude_suspended: true    # 排除最新交易日成交量为 0 的停牌股

signal:
  postprocess:
    enabled: true
    daily_transform: "rank"   # rank | zscore | none
    rank_pct: true
    industry_neutralize: false
```

LightGBM 配置：

```yaml
model:
  type: "lgbm"
  lightgbm:
    n_estimators: 1000
    learning_rate: 0.05
    num_threads: -1
    device_type: "cpu"
```

板块因子支持细粒度开关，方便做消融实验：

```yaml
model:
  features:
    factors:
      - name: "sector"
        include_sector_momentum: true
        include_sector_relative: true
        include_stock_vs_sector: true
        include_sector_reversal: true
        include_sector_volatility: true
        include_sector_id: true
        include_concept: true
        include_concept_id: true
```

多 seed ensemble：

```yaml
model:
  ensemble:
    enabled: true
    seeds: [42, 123, 2024]
```

---

## 模块结构

```text
quant_ex/
├── config/
│   ├── base.yaml
│   ├── model.yaml
│   ├── notify.yaml.example
│   ├── strategy_candidates.yaml
│   └── daily_csi1000.yaml    # per-strategy signal override (example)
├── data/
│   ├── loader.py
│   ├── qlib_update/
│   ├── sector.py
│   └── universe.py
├── features/
│   ├── factor_mining.py
│   ├── sector_factors.py
│   └── technical_factors.py
├── models/
│   ├── lgbm_model.py
│   ├── nn_model.py
│   ├── linear_model.py
│   ├── xgb_model.py
│   └── trainer.py
├── backtest/
│   ├── engine.py
│   ├── grid_search.py
│   ├── metrics.py
│   └── signal_diagnostics.py
├── signals/
│   ├── generator.py
│   └── postprocess.py
├── notify/
│   └── pusher.py
├── crawler/
│   ├── eastmoney/
│   └── scripts/
├── agent/
│   └── auto_optimizer.py
├── run_train.py
├── run_backtest.py
├── run_walk_forward_validation.py
├── run_daily.py
├── run_update_qlib_data.py
├── run_factor_mining.py
├── run_notify_test.py
└── run_wechat_openids.py
```

---

## 东方财富数据

`crawler/eastmoney/` 是独立 SDK，不强依赖 qlib。

常用脚本：

```bash
./.venv/bin/python crawler/scripts/fetch_sector_enums.py
./.venv/bin/python crawler/scripts/fetch_sector_stocks.py --resume
```

使用示例：

```python
from crawler.eastmoney import SectorAPI, SectorType

df = SectorAPI().get_sector_list(SectorType.INDUSTRY)
```

建议主训练/回测路径不要强依赖实时网络，优先使用缓存。

---

## Claude AI 优化器

`agent/auto_optimizer.py` 可读取网格搜索结果，让 Claude 给出下一轮参数建议。

```bash
export ANTHROPIC_API_KEY="..."
./.venv/bin/python run_backtest.py --optimize --n-iters 3
```

这个功能适合辅助研究，不应该替代 walk-forward 验证。

---

## 常见问题

**Q: qlib 数据路径找不到？**  
A: 检查 `config/base.yaml -> qlib.provider_uri`，目录下应有 `calendars/`、`features/`、`instruments/`。

**Q: Dolt 更新时提示 locked？**  
A: 说明另一个 Dolt 进程正在占用仓库。若确实有更新在跑，等待它完成；若只是已有 SQL server，可用 `--reuse-dolt-server`。

**Q: `run_backtest.py` 并行报系统权限或 semaphore 错误？**  
A: 用 `--grid-workers 1` 串行跑，或者降低并行数量。

**Q: 为什么 2026 某些参数收益特别高？**  
A: 已检查过 `csi800/topk=5/n_drop=3/hold=8`，收益主要来自少数重仓股票，单票集中度很高。需要加仓位上限后再验证。

**Q: 应该直接用 `config/strategy_candidates.yaml` 的 active candidate 吗？**  
A: 不建议直接实盘。优先用 `csi1000_balanced` 或 `csi800_stable_all_positive` 做人工辅助选股，并继续验证仓位约束。

**Q: 模型文件和实验结果为什么没有入库？**  
A: `models/*.pkl`、`models/*_meta.json`、`backtest_results/`、`optimization_results/` 默认是运行产物，体积大且可再生。长期结论写入 `config/strategy_candidates.yaml`。

---

## License

MIT License. 本项目仅供学习和研究使用，不构成投资建议。市场有风险，投资需谨慎。
