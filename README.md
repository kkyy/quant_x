# quant_ex

`quant_ex` 是一个基于 **qlib + Alpha158** 的 A 股低频量化选股研究框架，用于训练选股模型、做 walk-forward 验证、生成每日候选股和调仓动作，并沉淀可复用的研究配置。

项目定位偏向 **研究与辅助决策**，不是自动实盘交易系统。

当前核心能力：

- qlib Alpha158 数据集构建与模型训练
- LightGBM / XGBoost / Ridge / Lasso / MLP 多模型训练
- LightGBM bootstrap ensemble / bagging
- TopkDropout 策略回测、参数网格搜索、多 seed 稳健性评估
- Walk-forward 时间交叉验证，支持自定义折叠 YAML
- 因子流水线：technical / sector / mined / regime / northbound / fundamental
- FactorScreener：基于 IC / ICIR / 相关性去重的因子筛选
- 信号后处理：rank / zscore、行业中性化、市值中性化
- 市场状态识别与策略参数切换（regime switch）
- 流动性过滤、停牌过滤、集中度风险检查
- 每日信号生成、目标持仓与买卖差分
- IC 衰减分析、滚动 IC 监控、Brinson 绩效归因
- qlib bin 数据更新与缺口补数
- Bark / PushPlus / 钉钉 / Server 酱 / 微信模板消息通知
- 东方财富行业与概念数据缓存
- Claude API 辅助参数优化
- Web Dashboard：本地可视化面板（数据管理、模型训练/浏览、回测、信号生成、因子分析、配置编辑）

> 本项目仅用于研究和辅助决策，不构成投资建议。

---

## 目录

- [环境说明](#环境说明)
- [快速开始](#快速开始)
- [数据更新](#数据更新)
- [训练模型](#训练模型)
- [回测与网格搜索](#回测与网格搜索)
- [Walk-forward 验证](#walk-forward-验证)
- [每日信号与调仓](#每日信号与调仓)
- [定时任务](#定时任务)
- [因子与信号处理](#因子与信号处理)
- [Web Dashboard](#web-dashboard)
- [配置说明](#配置说明)
- [模块结构](#模块结构)
- [通知与外部依赖](#通知与外部依赖)
- [常见问题](#常见问题)

---

## 环境说明

### Python

- 目标 Python 版本：`>=3.9`
- 推荐做语法检查、导入检查和轻量测试的解释器：`/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9`
- 项目自己的 `.venv` 可用于本仓库训练/回测，但不应默认假设依赖完整

### qlib 数据目录

默认配置在 `config/base.yaml`：

```yaml
qlib:
  provider_uri: "./qlib_data/qlib_bin"
  region: "cn"
```

如使用本机已有 qlib 数据，也可以改成绝对路径。

---

## 快速开始

### 1. 安装依赖

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. 检查注册表

```bash
./.venv/bin/python run_train.py --list-registry
```

预期模型注册至少包含：

```text
lgbm, xgb, ridge, lasso, mlp
```

常用因子注册至少包含：

```text
technical, sector, mined, regime
```

### 3. 运行轻量测试

```bash
/Users/weidian/code/algorithms/Qbot/.venv/bin/python3.9 -m pytest test/test_universe_filter.py test/test_trainer.py test/test_data_sources.py
```

---

## 外部数据获取

`run_fetch_data.py` 独立于 qlib 训练流水线，用于拉取 akshare 数据并缓存到 `cache/<type>/`。

```bash
# 按类型获取
./.venv/bin/python run_fetch_data.py --type financial       # A 股基本面（利润表/现金流）
./.venv/bin/python run_fetch_data.py --type northbound      # 北向资金
./.venv/bin/python run_fetch_data.py --type analyst         # 分析师评级与 EPS 预测
./.venv/bin/python run_fetch_data.py --type balance_sheet   # 资产负债表
./.venv/bin/python run_fetch_data.py --type dividend        # 分红历史
./.venv/bin/python run_fetch_data.py --type earnings_guidance  # 业绩预告
./.venv/bin/python run_fetch_data.py --type insider         # 高管增减持
./.venv/bin/python run_fetch_data.py --type institutional   # 机构持仓（基金/QFII/社保）
./.venv/bin/python run_fetch_data.py --type margin          # 融资融券
./.venv/bin/python run_fetch_data.py --type pledge          # 股权质押
./.venv/bin/python run_fetch_data.py --type repurchase      # 回购进度
./.venv/bin/python run_fetch_data.py --type shareholder     # 股东户数
./.venv/bin/python run_fetch_data.py --type valuation       # 估值（PE/PB/市值）
./.venv/bin/python run_fetch_data.py --type visit           # 机构调研
./.venv/bin/python run_fetch_data.py --type all             # 全量获取

# 限定范围
./.venv/bin/python run_fetch_data.py --type financial --universe csi300
./.venv/bin/python run_fetch_data.py --type financial --symbols SH600519,SZ000001

# 强制刷新（忽略 TTL 缓存）
./.venv/bin/python run_fetch_data.py --type analyst --force
```

各类型数据缓存 TTL：1 天（margin/pledge/insider/repurchase），3 天（analyst），7 天（financial/visit），30 天（balance_sheet/dividend/earnings_guidance/institutional/shareholder）。

---

## 数据更新

数据更新入口：

```bash
./.venv/bin/python run_update_qlib_data.py
```

该流程负责：Dolt clone/pull → SQL server → 导出 source CSV → normalize → dump 成 qlib bin。

常用参数：

```bash
./.venv/bin/python run_update_qlib_data.py --skip-dolt-pull
./.venv/bin/python run_update_qlib_data.py --reuse-dolt-server
./.venv/bin/python run_update_qlib_data.py --supplement-source akshare
./.venv/bin/python run_update_qlib_data.py --workspace-dir ./qlib_data --qlib-dir ./qlib_data/qlib_bin
```

说明：

- `data/qlib_update/` 是数据更新主逻辑目录
- `data/sources/` 中的 GapFiller 可用 akshare 或 eastmoney 补足缺失交易日
- `qlib_data/`、`backtest_results/`、`optimization_results/` 都是运行产物，默认不应提交

---

## 训练模型

### 自定义模型模式

```bash
./.venv/bin/python run_train.py --model lgbm --tag baseline
./.venv/bin/python run_train.py --model xgb --tag xgb_baseline
./.venv/bin/python run_train.py --model ridge --tag ridge_baseline
./.venv/bin/python run_train.py --model lasso --tag lasso_baseline
./.venv/bin/python run_train.py --model mlp --tag mlp_baseline
```

仅使用 Alpha158：

```bash
./.venv/bin/python run_train.py --model lgbm --no-extra-factors --tag alpha158_only
```

启用板块因子：

```bash
./.venv/bin/python run_train.py --model lgbm --with-sector --tag sector_full
```

### qlib-native 模式

```bash
./.venv/bin/python run_train.py --qlib-native
```

该模式训练完成后，会生成 MLflow Recorder。后续需要把 Recorder ID 写入 `config/base.yaml` 的 `experiment.latest_recorder_id`。

### Ensemble

在 `config/model.yaml` 中启用：

```yaml
model:
  ensemble:
    enabled: true
    seeds: [42, 123, 2024]
    bagging_fraction: 0.8
```

自定义模型会输出到 `models/*.pkl`，并附带 `_meta.json` 与 `_feature_importance.json` sidecar 文件。

---

## 回测与网格搜索

基础回测：

```bash
./.venv/bin/python run_backtest.py --model-path models/lgbm_xxx.pkl
```

参数网格搜索：

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

输出独立 CSV：

```bash
./.venv/bin/python run_backtest.py --model-path models/lgbm_xxx.pkl --output-csv results/my_run.csv
```

Claude 参数优化：

```bash
export ANTHROPIC_API_KEY="..."
./.venv/bin/python run_backtest.py --optimize --n-iters 3
```

常见输出指标包括：`annual_return`、`sharpe`、`max_drawdown`、`calmar`、`win_rate`、`ic`、`icir`、`rank_ic`、`rank_icir`。

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

并行运行：

```bash
./.venv/bin/python run_walk_forward_validation.py \
  --workers 3 \
  --grid-workers 1 \
  --train-universes csi300,csi800,csi1000
```

自定义折叠：

```bash
./.venv/bin/python run_walk_forward_validation.py --folds-config config/walk_forward_folds.yaml
```

自定义稳健得分权重：

```bash
./.venv/bin/python run_walk_forward_validation.py \
  --robust-weights '{"mean_sharpe": 1.0, "sharpe_std": -0.3, "min_sharpe": 0.5, "positive_sharpe_folds": 0.05}'
```

输出目录位于：

```text
optimization_results/walk_forward_<run_id>/
```

汇总表会包含 `sharpe_ttest_pvalue` 和 `return_ttest_pvalue`，用于衡量结果显著性。

长期结论建议整理到 `config/strategy_candidates.yaml`，不要只保留在运行产物中。

---

## 每日信号与调仓

### 每日信号

```bash
./.venv/bin/python run_daily.py --model-path models/lgbm_xxx.pkl --dry-run
```

使用策略覆盖配置：

```bash
./.venv/bin/python run_daily.py \
  --config config/daily_csi1000.yaml \
  --model-path models/lgbm_xxx.pkl \
  --dry-run
```

带账户规模和当前持仓：

```bash
./.venv/bin/python run_daily.py \
  --model-path models/lgbm_xxx.pkl \
  --account 500000 \
  --positions SH600000:500,SZ000001:300
```

### 调仓提醒与计划生成

```bash
./.venv/bin/python run_scheduled_rebalance.py --mock --dry-run
./.venv/bin/python run_scheduled_rebalance.py --dry-run
```

该脚本会在收盘后执行：更新数据 → 重放回测 → 生成目标持仓和次交易日调仓动作 → 缓存结果 → 推送通知。

关键约束：

- `daily_rebalance.start_date` 必须早于今天至少几个交易日
- `TopkDropoutStrategy` 在回测首日不开仓，所以起点不能设成当天
- 首次跟踪策略时，应优先执行“次交易日调仓动作”中的买入项，而不是机械照搬“目标持仓摘要”

---

## 定时任务

安装 macOS launchd 任务：

```bash
scripts/install_daily_rebalance_launchd.sh
```

默认注册三个任务：

| 任务 | 时间 | 功能 |
|---|---:|---|
| `com.quant_ex.daily_rebalance` | 20:00 | 更新数据、回测、缓存信号、推送调仓动作 |
| `com.quant_ex.daily_rebalance.open_reminder` | 09:00 | 开盘前提醒 |
| `com.quant_ex.daily_rebalance.close_reminder` | 14:00 | 收盘前提醒 |

---

## 因子与信号处理

### 因子配置

在 `config/model.yaml` 的 `model.features.factors` 中声明因子：

```yaml
model:
  features:
    factors:
      - name: technical
      - name: sector
        include_sector_momentum: true
        include_stock_vs_sector: true
        include_sector_reversal: true
        include_concept: true
      - name: mined
        path: "./cache/mined_factors.json"
      - name: regime
        windows: [20, 60]
        dd_window: 120
      - name: northbound        # 北向资金
      - name: fundamental       # 财务因子（利润表/现金流）
      - name: pledge            # 股权质押率
      - name: margin            # 融资融券（余额/增减）
      - name: insider           # 高管增减持（5/20/60d 滚动）
      - name: analyst           # 分析师评级与 EPS 增速
      - name: shareholder       # 股东户数变化
      - name: dividend          # 股息率、分红连续性
      - name: valuation         # PE/PB/市值
      - name: balance_sheet     # 杠杆率、流动比率
      - name: earnings_guidance # 业绩预告类型与惊喜度
      - name: institutional     # 机构持仓（基金/QFII/社保）
      - name: repurchase        # 回购完成率
      - name: visit             # 机构调研频次
```

`regime` 因子会产出：`regime_trend_{w}d`、`regime_vol_{w}d`、`regime_breadth_{w}d`、`regime_corr_{w}d`、`regime_drawdown`、`regime_label`。

上述 akshare 数据驱动因子需提前运行 `run_fetch_data.py` 填充缓存，否则因子返回空 DataFrame。

### FactorScreener

```yaml
model:
  features:
    screener:
      min_ic: 0.02
      min_icir: 0.3
      max_corr: 0.7
```

### 信号后处理

在 `config/base.yaml` 中配置：

```yaml
signal:
  postprocess:
    enabled: true
    daily_transform: "rank"
    rank_pct: true
    industry_neutralize: false
    size_neutralize: false
```

### Regime 策略切换

```yaml
strategy:
  regime_switch:
    enabled: true
    rules:
      0:
        topk: 15
        n_drop: 3
        hold_thresh: 5
      1:
        topk: 10
        n_drop: 1
        hold_thresh: 8
      2:
        topk: 12
        n_drop: 2
        hold_thresh: 5
      3:
        topk: 8
        n_drop: 1
        hold_thresh: 10
```

`run_daily.py` 和 `run_scheduled_rebalance.py` 都会尝试自动检测并应用该切换。

### 流动性过滤与集中度

```yaml
strategy:
  universe_filter:
    exclude_kcb: true
    exclude_st: true
    exclude_suspended: true
    min_price: 3
    min_avg_volume: 1000000
    avg_volume_window: 20
    min_avg_amount: 50000000
    avg_amount_window: 20
  portfolio:
    max_position_pct: 0.25
    concentration_hard_limit: 0.35
```

### 诊断与归因

IC 诊断：

```python
from quant_ex.backtest.signal_diagnostics import compute_ic_decay, compute_rolling_ic

decay = compute_ic_decay(pred, price_data)
monitor = compute_rolling_ic(pred, price_data, horizon=5, window=20)
```

Brinson 归因：

```python
from quant_ex.backtest.attribution import brinson_attribution, format_attribution

result = brinson_attribution(portfolio_weights, benchmark_weights, returns, sector_map)
print(format_attribution(result))
```

---

## Web Dashboard

基于 React + FastAPI 的本地可视化面板，提供所有 quant_ex 功能的交互式访问。

### 启动

```bash
# 生产模式（单一进程，同时提供 API 和前端）
./.venv/bin/python web/run_web.py    # http://localhost:8000

# 开发模式（热重载）
# 终端 1：后端
./.venv/bin/python web/run_web.py

# 终端 2：前端
cd web/frontend
npm install    # 首次运行
npm run dev    # http://localhost:5173（自动代理 /api → :8000）
```

### 功能页面

| 页面 | 功能 |
|---|---|
| Dashboard | 系统状态总览、模型计数、缓存状态表、regime 状态 |
| Data Management | 数据获取（15 种类型）、缓存状态、股票查询 |
| Models | 模型训练表单、已保存模型浏览（含 meta + feature importance）、注册表 |
| Backtest | 网格搜索、Walk-forward 验证、结果浏览 |
| Signals | 信号生成、历史记录、调仓模拟、通知测试 |
| Factors | 因子库（19 个注册因子）、因子评估、因子挖掘 |
| Config | YAML 配置编辑器、策略候选、Regime 规则编辑 |
| System | 日志查看、缓存管理、运行时信息 |

### API

共 33 个 API 端点，分为 7 组路由：

- `/api/system/`：健康检查、运行时信息、日志、任务管理、SSE 流
- `/api/data/`：缓存状态、数据获取、股票查询
- `/api/models/`：模型列表/元数据/特征重要性、训练、注册表
- `/api/backtest/`：网格搜索、结果、Walk-forward、图表
- `/api/signals/`：信号生成、历史、regime、调仓、通知测试
- `/api/factors/`：因子列表、库、评估、挖掘
- `/api/config/`：YAML 读写、预设列表

---

## 配置说明

配置加载顺序：

```text
config/base.yaml → config/model.yaml → config/notify.yaml → --config 覆盖文件
```

常见配置职责：

- `config/base.yaml`：市场、策略、回测、daily_rebalance、信号处理
- `config/model.yaml`：模型参数、额外因子、ensemble
- `config/notify.yaml`：通知渠道配置，建议从 `config/notify.yaml.example` 复制
- `config/strategy_candidates.yaml`：长期保留的研究结论，不会被自动加载
- `config/walk_forward_folds.yaml.example`：自定义时间折模板

---

## 模块结构

```text
quant_ex/
├── config/
├── data/
│   ├── loader.py
│   ├── sector.py
│   ├── universe.py
│   ├── fetchers/          # 14 个 akshare 数据 fetcher（financial/northbound/pledge/margin/
│   │                      #   insider/analyst/shareholder/dividend/valuation/balance_sheet/
│   │                      #   earnings_guidance/institutional/repurchase/visit）
│   ├── qlib_update/
│   └── sources/
├── features/
│   ├── technical_factors.py
│   ├── sector_factors.py
│   ├── factor_mining.py
│   ├── regime_features.py
│   ├── northbound_factor.py
│   ├── fundamental_factor.py
│   ├── pledge_factor.py        # 股权质押
│   ├── margin_factor.py        # 融资融券
│   ├── insider_factor.py       # 高管增减持
│   ├── analyst_factor.py       # 分析师评级/EPS预期
│   ├── shareholder_factor.py   # 股东户数
│   ├── dividend_factor.py      # 分红
│   ├── valuation_factor.py     # 估值（PE/PB/市值）
│   ├── balance_sheet_factor.py # 资产负债表比率
│   ├── earnings_guidance_factor.py  # 业绩预告
│   ├── institutional_factor.py # 机构持仓
│   ├── repurchase_factor.py    # 回购
│   ├── visit_factor.py         # 机构调研
│   └── library/
├── models/
├── backtest/
├── signals/
├── strategy/
│   └── regime_switch.py
├── notify/
├── crawler/
│   └── eastmoney/
├── web/
│   ├── api/                    # FastAPI 后端
│   │   ├── app.py              # 应用工厂 + CORS + 静态文件挂载
│   │   ├── deps.py             # 共享依赖（配置加载、路径常量）
│   │   ├── routers/            # 7 个 API 路由（system/data/models/backtest/signals/factors/config）
│   │   └── services/           # TaskManager（后台任务 + SSE）、日志捕获
│   ├── frontend/               # React 前端（Vite + TypeScript + Tailwind）
│   │   ├── src/pages/          # 8 个页面组件
│   │   ├── src/api/client.ts   # API 客户端
│   │   ├── src/hooks/useSSE.ts # SSE 流 hook
│   │   └── src/components/     # Sidebar、Layout、共享组件
│   └── run_web.py              # 入口：uvicorn web.api.app:app
├── agent/
└── test/
```

---

## 通知与外部依赖

通知配置：

```bash
cp config/notify.yaml.example config/notify.yaml
./.venv/bin/python run_notify_test.py --channel bark
```

支持的通知渠道包括：Bark、PushPlus、DingTalk、Server 酱、微信公众号模板消息。

东方财富 SDK 位于 `crawler/eastmoney/`，独立于 qlib 主链路。直连可用，代理环境下可能出现空响应。

行业/概念数据抓取：

```bash
./.venv/bin/python crawler/scripts/fetch_sector_enums.py
./.venv/bin/python crawler/scripts/fetch_sector_stocks.py --resume
```

---

## 常见问题

**Q: qlib 数据路径找不到？**  
A: 检查 `config/base.yaml` 中的 `qlib.provider_uri`，目录下应包含 `calendars/`、`features/`、`instruments/`。

**Q: 数据更新时报 Dolt lock？**  
A: 先确认没有实际运行中的 `dolt sql-server`。如只是 stale lock，可直接重试；如已有 SQL server，则用 `--reuse-dolt-server`。

**Q: 并行回测报 semaphore 或资源竞争错误？**  
A: 降低 `--workers` / `--grid-workers`，或在 `config/model.yaml` 中下调 LightGBM 的 `num_threads`。

**Q: 为什么每日调仓没有目标持仓？**  
A: 通常是 `daily_rebalance.start_date` 设得太晚。回测首日不会开仓，起点应早于信号日几个交易日。

**Q: 为什么旧模型文件加载后报缺属性？**  
A: 旧 `pkl` 会依赖运行时兼容补丁。当前代码已通过 `__setstate__` 和默认值补齐大部分历史属性，但跨版本模型仍建议重新验证一次推理链路。

---

## License

MIT License. 本项目仅供学习和研究使用，不构成投资建议。市场有风险，投资需谨慎。