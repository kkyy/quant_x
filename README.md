# quant_ex

基于 **qlib + Alpha158** 的 A 股量化选股框架，支持多模型训练、因子挖掘、策略回测、每日信号生成与多渠道推送，并集成 Claude AI 进行参数自动优化。

---

## 目录

- [特性](#特性)
- [架构概览](#架构概览)
- [快速开始](#快速开始)
- [配置说明](#配置说明)
- [入口脚本](#入口脚本)
- [模块说明](#模块说明)
- [东方财富数据爬取](#东方财富数据爬取)
- [通知渠道](#通知渠道)
- [AI 优化器](#ai-优化器)
- [依赖安装](#依赖安装)
- [目录结构](#目录结构)
- [常见问题](#常见问题)

---

## 特性

- **Alpha158 因子体系**：直接复用 qlib 原生 158 个技术因子
- **行业因子**：19 个行业轮动特征（动量 / 相对强弱 / 超额收益 / 反转 / 波动率 / 行业 ID）
- **自动因子挖掘**：10 类因子表达式模板，枚举 + Rank-IC/ICIR 过滤，自动发现有效 alpha
- **灵活模型**：
  - qlib 原生 `LGBModel`（带 MLflow 实验管理）
  - 自定义 `lgbm` / `xgb` / `ridge` / `lasso` 模型（保存为 `.pkl`）
- **TopkDropout 策略**：可配置 topk / n_drop / hold_thresh，支持参数网格搜索
- **AI 自动优化**：基于 Claude API 迭代分析回测结果，自动缩小搜索空间
- **每日信号**：一键生成目标持仓 + 买卖信号，格式化报告
- **多渠道推送**：Bark（iOS）、PushPlus、钉钉、Server 酱、微信公众号模板消息
- **东方财富数据爬取**：独立 crawler 模块，支持板块/个股实时行情、资金流向、K线、成分股查询，内置 Python SDK 与完整板块代码枚举

---

## 架构概览

```
配置层        config/base.yaml + model.yaml + strategy.yaml + notify.yaml
                    ↓ deep-merge
数据层        DataLoader → qlib D.features() / DatasetH
              UniverseFilter → 排除科创板 / 黑名单 / 最低价格过滤
              SectorDataProvider → akshare (7 天本地缓存)
                    ↓
特征层        Alpha158 (158 个技术因子)
              SectorFactorEngine (19 个行业因子)
              FactorMiner (自动挖掘因子)
                    ↓
模型层        ModelTrainer
              ├── qlib_native → qlib.LGBModel + MLflow 记录
              └── custom     → lgbm / xgb / ridge / lasso (.pkl)
                    ↓
回测层        BacktestEngine → qlib backtest_daily / TopkDropoutStrategy
              GridSearchBacktest → 多参数组合并行评估
              AutoOptimizer → Claude API 迭代建议
                    ↓
信号层        SignalGenerator → TopK 持仓 + 买卖信号差分
              NotificationPusher → Bark / PushPlus / DingTalk / Server 酱 / WeChat MP
```

---

## 快速开始

### 1. 环境准备

```bash
# 克隆项目
git clone https://github.com/<your-username>/quant_ex.git
cd quant_ex

# 创建虚拟环境（推荐 Python 3.9+）
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# 安装依赖
pip install -r requirements.txt

# 可选：按包方式安装，启用 qx-train / qx-daily / qx-backtest 命令
pip install -e .
```

### 2. 准备数据

需要本地 qlib 数据（A 股日频数据），推荐使用 qlib 官方工具下载：

```bash
python -m qlib.run.get_data qlib_data --target_dir ~/qlib_data/cn_data --region cn
```

### 3. 编辑配置

```bash
# 修改 qlib 数据路径
vi config/base.yaml
# 将 qlib.provider_uri 设置为实际路径

# 如需启用通知，先复制模板
cp config/notify.yaml.example config/notify.yaml
```

### 4. 训练模型

```bash
# 方式 1：qlib 原生 LGBModel（会输出 Recorder ID）
python run_train.py --qlib-native

# 方式 2：自定义模型（保存到 models/*.pkl）
python run_train.py --model lgbm --tag baseline
python run_train.py --model lgbm --with-sector --tag sector_full
```

### 5. 生成每日信号

```bash
# 收盘后运行（17:00 之后）
python run_daily.py

# 直接加载自定义模型文件
python run_daily.py --model-path models/lgbm_sector_full_20260308_143021.pkl

# 指定当前持仓（股票代码:股数）
python run_daily.py --positions SH600000:500,SZ000001:300

# 演习模式（不发送通知）
python run_daily.py --dry-run
```

---

## 配置说明

所有配置采用 **YAML 深度合并**：`base.yaml → model.yaml → strategy.yaml → notify.yaml`，越后越优先。

说明：`notify.yaml` 默认不入库，通常从 `config/notify.yaml.example` 复制生成。

### config/base.yaml

```yaml
qlib:
  provider_uri: ~/qlib_data/qlib_bin   # qlib 数据路径
  region: cn

market:
  name: csi300        # 股票池：csi300 / csi500 / all
  benchmark: SH000300

training:
  fit_start: "2015-01-01"
  fit_end: "2023-12-31"
  valid_start: "2024-01-01"
  valid_end: "2024-06-30"
  test_start: "2024-07-01"

experiment:
  name: tutorial_exp
  latest_recorder_id: ""   # qlib-native 训练后填入；若使用 .pkl 可改走 --model-path
```

### config/model.yaml

```yaml
model:
  type: lgbm

  lightgbm:
    n_estimators: 1000
    learning_rate: 0.05
    max_depth: 8

  features:
    factors:
      - name: technical
      # - name: sector   # 需配合 run_train.py --with-sector
```

### config/strategy.yaml

```yaml
strategy:
  topk_dropout:
    topk: 10
    n_drop: 3
    hold_thresh: 5

  universe_filter:
    exclude_kcb: true
    exclude_list: ["SZ300442"]
    min_price: 2.0

backtest:
  account: 1000000
  open_cost: 0.0005
```

### config/notify.yaml

```yaml
bark:
  enabled: false
  device_key: ""    # 填入 Bark App 设备 Key

pushplus:
  enabled: false
  token: ""         # 填入 PushPlus Token

dingtalk:
  enabled: false
  webhook_url: ""
  secret: ""        # 可选，用于签名验证

serverchan:
  enabled: false
  send_key: ""

wechat_mp:
  enabled: false
  appid: ""
  appsecret: ""
  template_id: ""
  openids: [""]
```

---

## 入口脚本

### run_train.py — 模型训练

```bash
# 使用 qlib 原生训练（结果记录到 MLflow）
python run_train.py --qlib-native

# 使用自定义模型（保存到 models/*.pkl）
python run_train.py --model lgbm --tag baseline
python run_train.py --model xgb --tag xgb_baseline
python run_train.py --model lgbm --with-sector --tag sector_full

# 指定配置文件
python run_train.py --config my_config.yaml
```

`--qlib-native` 会输出 `Recorder ID`，需要回填到 `config/base.yaml` 的 `experiment.latest_recorder_id`。

自定义模型会保存到 `models/` 目录，可在后续通过 `--model-path` 直接加载，无需配置 Recorder。

---

### run_daily.py — 每日信号

```bash
python run_daily.py
python run_daily.py --dry-run                                # 不发推送
python run_daily.py --account 500000                         # 指定账户金额（默认 1,000,000）
python run_daily.py --positions SH600000:500,SZ000001:300   # 指定当前持仓
python run_daily.py --model-path models/lgbm_baseline_xxx.pkl
```

输出示例：

```
📅 交易日期：2026-03-04
🎯 目标持仓（Top10）：
  1. SH600036  招商银行   预测分: 0.0312
  2. SZ000858  五粮液     预测分: 0.0289
  ...

📊 操作信号：
  🟢 买入  SH600036  ¥35.20  ×100 手 → +3,520 元
  🔴 卖出  SH601318  ¥82.50  ×200 手 → -16,500 元
```

---

### run_backtest.py — 回测与网格搜索

```bash
# 使用默认参数网格
python run_backtest.py

# 直接回测自定义模型文件
python run_backtest.py --model-path models/lgbm_baseline_xxx.pkl

# 自定义参数范围
python run_backtest.py --topk 5,10,15,20 --n-drop 1,3,5 --hold-thresh 3,5,10

# 指定回测区间
python run_backtest.py --start 2024-01-01 --end 2025-12-31

# 启用 Claude AI 自动优化（需配置 ANTHROPIC_API_KEY）
python run_backtest.py --optimize --n-iters 3
```

结果保存到 `backtest_results/` 目录（CSV + JSON）。

---

### run_factor_mining.py — 因子自动挖掘

```bash
python run_factor_mining.py
python run_factor_mining.py --min-ic 0.03 --min-icir 0.4 --top-n 30
```

挖掘 10 类因子表达式模板（动量、均值回复、量价相关、换手、价格位置、波动率等），按 Rank-ICIR 排序，保存到 `cache/mined_factors.json`。

---

## 模块说明

| 模块 | 文件 | 功能 |
|------|------|------|
| 数据加载 | `data/loader.py` | 封装 qlib D.features，构建 DatasetH |
| 股票池过滤 | `data/universe.py` | 排除科创板 / 黑名单 / 最低价格 |
| 行业数据 | `data/sector.py` | akshare 行业映射，7 天 TTL 缓存 |
| 行业因子 | `features/sector_factors.py` | 19 个行业轮动因子 |
| 因子挖掘 | `features/factor_mining.py` | 基于模板的 alpha 因子自动发现 |
| 模型封装 | `models/lgbm_model.py` / `models/xgb_model.py` / `models/linear_model.py` | LightGBM / XGBoost / Ridge / Lasso 模型实现 |
| 训练管理 | `models/trainer.py` | qlib_native（MLflow）/ custom（pkl）双模式 |
| 回测引擎 | `backtest/engine.py` | 封装 qlib backtest_daily |
| 网格搜索 | `backtest/grid_search.py` | 多参数组合回测，Sharpe 排序 |
| 性能指标 | `backtest/metrics.py` | 年化收益、Sharpe、最大回撤、Calmar 等 |
| 信号生成 | `signals/generator.py` | TopK 持仓计算 + 差分交易信号 |
| 推送通知 | `notify/pusher.py` | Bark / PushPlus / 钉钉 / Server 酱 / 微信公众号模板消息 |
| AI 优化器 | `agent/auto_optimizer.py` | Claude API 迭代参数优化 |
| 配置管理 | `utils/config.py` | YAML 深度合并加载 |
| 日志 | `utils/logger.py` | 文件 + 控制台日志 |
| qlib 工具 | `utils/qlib_utils.py` | 持仓转换、信号计算、Recorder 加载 |

---

## 东方财富数据爬取

`crawler/` 是独立的东方财富接口封装模块，提供板块行情、个股行情、资金流向、K 线历史数据的完整 Python SDK，不依赖 qlib，可单独使用。

> 接口完整说明见 [`crawler/eastmoney_api.md`](crawler/eastmoney_api.md)

### 模块结构

```
crawler/
├── eastmoney/
│   ├── client.py       # EastMoneyClient（HTTP 基础客户端，自动重试 + 指数退避）
│   ├── enums.py        # 枚举定义：SectorType / KlineInterval / AdjustType /
│   │                   # QuoteField / FundField / SectorCode
│   │                   # SectorCodeRegistry / SectorStocksRegistry / to_secid
│   ├── sector.py       # SectorAPI（板块列表 / 成分股 / 资金流）
│   ├── stock.py        # StockAPI（个股行情 / 个股资金流）
│   └── kline.py        # KlineAPI（板块 & 个股 K 线，支持日/周/月/分钟）
├── scripts/
│   ├── fetch_sector_enums.py   # 拉取全量板块代码 → data/sector_codes.json
│   └── fetch_sector_stocks.py  # 拉取全量成分股   → data/sector_stocks.json
├── data/
│   ├── sector_codes.json       # 行业(~497) + 概念(~468) + 地域(31) 板块代码表
│   └── sector_stocks.json      # 各板块成分股列表（按需运行脚本生成）
├── api_demo.py         # 完整调用示例（含离线演示）
└── eastmoney_api.md    # 接口文档（URL / 参数 / 字段 / SDK 用法）
```

### 快速使用

```python
from eastmoney import SectorAPI, StockAPI, KlineAPI
from eastmoney import SectorType, KlineInterval, AdjustType, SectorCode

# 行业板块实时行情（按涨跌幅降序）
df = SectorAPI().get_sector_list(SectorType.INDUSTRY)

# 板块成分股（新能源车）
df = SectorAPI().get_sector_stocks(SectorCode.NEW_ENERGY_VEHICLE.value)

# 板块资金流向（概念板块，按主力净流入降序）
df = SectorAPI().get_sector_fund_flow(SectorType.CONCEPT)

# 个股实时行情（DeepSeek 板块成分股）
df = StockAPI().get_stock_quote(bk_code=SectorCode.DEEPSEEK.value)

# 个股资金流向（全市场）
df = StockAPI().get_stock_fund_flow()

# K 线（板块或个股，支持日/周/月/分钟周期）
df = KlineAPI().get_kline("BK0475", interval=KlineInterval.DAY)
df = KlineAPI().get_kline("600519", adjust=AdjustType.FORWARD, start_date="20230101")
```

### 板块代码查询

```python
from eastmoney import SectorCodeRegistry, SectorStocksRegistry

# 按关键词搜索板块
SectorCodeRegistry.find_by_name("芯片")
# [{'code': 'BK0565', 'name': '芯片国产替代', 'type': 'concept'}, ...]

# 查询股票所属全部板块（需先运行 fetch_sector_stocks.py）
SectorStocksRegistry.find_sectors_by_stock("600519")

# 生成股票→行业板块映射（用于因子构建）
mapping = SectorStocksRegistry.stock_sector_map("industry")
```

### 数据维护脚本

```bash
# 1. 刷新板块代码枚举（约 1000 个板块）
python crawler/scripts/fetch_sector_enums.py --proxy http://127.0.0.1:7890

# 2. 拉取成分股（支持断点续传，每 50 个板块自动保存一次）
python crawler/scripts/fetch_sector_stocks.py --proxy http://127.0.0.1:7890 --type industry
python crawler/scripts/fetch_sector_stocks.py --proxy http://127.0.0.1:7890 --type concept --resume
```

---

## 通知渠道

先执行 `cp config/notify.yaml.example config/notify.yaml`，再在 `config/notify.yaml` 中启用对应渠道并填入凭证：

| 渠道 | 说明 | 获取方式 |
|------|------|----------|
| **Bark** | iOS 推送 | 从 [Bark App](https://bark.day.app/) 获取设备 Key |
| **PushPlus** | 微信推送 | 注册 [PushPlus](http://www.pushplus.plus/) 获取 Token |
| **钉钉** | 钉钉群机器人 | 钉钉群 → 机器人管理 → Webhook + 可选安全密钥 |
| **Server 酱** | 微信推送 | 注册 [Server酱](https://sct.ftqq.com/) 获取 SendKey |
| **微信公众号** | 模板消息推送 | 微信公众平台服务号 → AppID / AppSecret / 模板 ID / OpenID |

---

## AI 优化器

使用 Claude API（`claude-opus-4-6`）对回测结果进行智能分析，自动建议下一轮参数网格。

**配置方式：**

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
python run_backtest.py --optimize --n-iters 3
```

**工作流：**

1. 以初始参数网格运行网格搜索
2. Claude 分析结果（年化收益、Sharpe、回撤等），输出最优参数建议
3. 以新网格再次搜索，循环 `n-iters` 次
4. 所有迭代结果保存到 `optimization_results/` 目录

---

## 依赖安装

```bash
pip install -r requirements.txt
```

主要依赖：

| 包 | 用途 |
|----|------|
| `pyqlib` | 量化研究框架（数据、回测、MLflow 记录） |
| `lightgbm` | LightGBM 梯度提升模型 |
| `akshare` | A 股行业分类数据 |
| `anthropic` | Claude AI API 客户端 |
| `pandas` / `numpy` | 数据处理 |
| `pyyaml` | 配置文件解析 |
| `requests` | 推送通知 HTTP 请求 |
| `mlflow` | 模型实验管理（qlib 依赖） |

## 开发与测试

```bash
# 安装开发依赖
pip install -e .[dev]

# 运行当前仓库已有测试
python -m pytest test/test_universe_filter.py test/test_trainer.py
```

---

## 目录结构

```
quant_ex/
├── config/
│   ├── base.yaml          # 基础配置（qlib 路径、市场、训练区间）
│   ├── model.yaml         # 模型超参数
│   ├── strategy.yaml      # 策略参数 & 回测设置
│   ├── notify.yaml.example# 推送通知模板
│   └── notify.yaml        # 本地复制生成的通知凭证（可选，不入库）
├── data/
│   ├── loader.py          # qlib 数据加载封装
│   ├── universe.py        # 股票池过滤
│   └── sector.py          # 行业映射（akshare + 缓存）
├── features/
│   ├── sector_factors.py  # 19 个行业因子
│   └── factor_mining.py   # 自动因子挖掘
├── models/
│   ├── lgbm_model.py      # LightGBM 模型封装
│   └── trainer.py         # 训练流程管理
├── backtest/
│   ├── engine.py          # qlib 回测引擎封装
│   ├── grid_search.py     # 参数网格搜索
│   └── metrics.py         # 绩效指标计算
├── signals/
│   └── generator.py       # 每日信号生成
├── notify/
│   └── pusher.py          # 多渠道推送
├── agent/
│   └── auto_optimizer.py  # Claude AI 自动优化
├── utils/
│   ├── config.py          # 配置加载（YAML 合并）
│   ├── logger.py          # 日志设置
│   └── qlib_utils.py      # qlib 工具函数
├── crawler/
│   ├── eastmoney/         # 东方财富 SDK（client / enums / sector / stock / kline）
│   ├── scripts/           # 数据维护脚本（fetch_sector_enums / fetch_sector_stocks）
│   ├── data/              # 板块代码表 & 成分股列表（JSON）
│   ├── api_demo.py        # 调用示例
│   └── eastmoney_api.md   # 接口完整文档
├── run_train.py           # 入口：模型训练
├── run_daily.py           # 入口：每日信号
├── run_backtest.py        # 入口：回测 & 网格搜索
├── run_factor_mining.py   # 入口：因子挖掘
├── requirements.txt
└── README.md
```

---

## 常见问题

**Q: 运行时提示 `qlib not initialized`？**
A: 确认 `config/base.yaml` 中 `qlib.provider_uri` 指向正确的 qlib 数据目录，且目录内有 `calendars/`、`instruments/` 等子目录。

**Q: 训练后 `run_daily.py` / `run_backtest.py` 报错找不到模型？**
A: 有两种方式：
1. `--qlib-native` 训练后，把 `Recorder ID` 填入 `config/base.yaml` → `experiment.latest_recorder_id`。
2. 自定义模型训练后，直接使用 `--model-path models/xxx.pkl`。

**Q: `strategy.universe_filter.exclude_st` 和 `min_avg_volume_m` 为什么没明显生效？**
A: 这两个字段目前仍是预留配置，当前代码实际接入的是 `exclude_kcb`、`exclude_list` 和 `min_price`。如需 ST 或流动性过滤，需要额外行情/证券状态数据源再接入。

**Q: 行业数据加载失败？**
A: 检查网络连接（akshare 需要访问东方财富数据接口）。也可删除 `cache/sector_map.json` 强制刷新。

**Q: AI 优化不工作？**
A: 确认环境变量 `ANTHROPIC_API_KEY` 已正确设置，且账户有 claude-opus-4-6 访问权限。

**Q: 如何添加自定义通知渠道？**
A: 在 `notify/pusher.py` 的 `NotificationPusher` 类中参照现有渠道添加新方法，并在 `send()` 中调用即可。

---

## License

MIT License. 本项目仅供学习和研究使用，不构成投资建议。市场有风险，投资需谨慎。
