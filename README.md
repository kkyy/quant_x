# quant_ex

基于 **qlib + LightGBM (Alpha158)** 的 A 股量化选股框架，支持因子挖掘、策略回测、自动信号生成与多渠道推送，并集成 Claude AI 进行参数自动优化。

---

## 目录

- [特性](#特性)
- [架构概览](#架构概览)
- [快速开始](#快速开始)
- [配置说明](#配置说明)
- [入口脚本](#入口脚本)
- [模块说明](#模块说明)
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
  - qlib 原生 LGBModel（带 MLflow 实验管理）
  - 自定义 `LGBMAlphaModel`（支持注入行业 + 挖掘因子）
- **TopkDropout 策略**：可配置 topk / n_drop / hold_thresh，支持参数网格搜索
- **AI 自动优化**：基于 Claude API 迭代分析回测结果，自动缩小搜索空间
- **每日信号**：一键生成目标持仓 + 买卖信号，格式化报告
- **多渠道推送**：Bark（iOS）、PushPlus（微信）、钉钉、Server 酱

---

## 架构概览

```
配置层        config/base.yaml + model.yaml + strategy.yaml + notify.yaml
                    ↓ deep-merge
数据层        DataLoader → qlib D.features() / DatasetH
              UniverseFilter → 排除科创板 / ST / 黑名单 / 价格 & 流动性过滤
              SectorDataProvider → akshare (7 天本地缓存)
                    ↓
特征层        Alpha158 (158 个技术因子)
              SectorFactorEngine (19 个行业因子)
              FactorMiner (自动挖掘因子)
                    ↓
模型层        ModelTrainer
              ├── qlib_native → qlib.LGBModel + MLflow 记录
              └── custom     → LGBMAlphaModel (.pkl)
                    ↓
回测层        BacktestEngine → qlib backtest_daily / TopkDropoutStrategy
              GridSearchBacktest → 多参数组合并行评估
              AutoOptimizer → Claude API 迭代建议
                    ↓
信号层        SignalGenerator → TopK 持仓 + 买卖信号差分
              NotificationPusher → Bark / PushPlus / DingTalk / Server 酱
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
```

### 4. 训练模型

```bash
python run_train.py
# 训练完成后，复制输出的 Recorder ID 填入 config/base.yaml 的 latest_recorder_id
```

### 5. 生成每日信号

```bash
# 收盘后运行（17:00 之后）
python run_daily.py

# 指定当前持仓（股票代码:股数）
python run_daily.py --positions SH600000:500,SZ000001:300

# 演习模式（不发送通知）
python run_daily.py --dry-run
```

---

## 配置说明

所有配置采用 **YAML 深度合并**：`base.yaml → model.yaml → strategy.yaml → notify.yaml`，越后越优先。

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
  name: quant_ex_alpha158
  latest_recorder_id: ""   # 训练后填入
```

### config/model.yaml

```yaml
lightgbm:
  n_estimators: 1000
  learning_rate: 0.05
  max_depth: 8

features:
  use_sector_factors: true
  sector_momentum_windows: [5, 10, 20, 60]
```

### config/strategy.yaml

```yaml
topk_dropout:
  topk: 10
  n_drop: 3
  hold_thresh: 5

universe_filter:
  exclude_kcb: true   # 排除科创板
  exclude_st: true
  exclude_list: ["SZ300442"]
  min_price: 2.0
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
```

---

## 入口脚本

### run_train.py — 模型训练

```bash
# 使用 qlib 原生训练（推荐，结果记录到 MLflow）
python run_train.py

# 使用自定义模型（支持行业因子注入）
python run_train.py --no-qlib --with-sector

# 指定配置文件
python run_train.py --config my_config.yaml
```

训练完成后，将输出的 `Recorder ID` 填入 `config/base.yaml` 的 `latest_recorder_id` 字段。

---

### run_daily.py — 每日信号

```bash
python run_daily.py
python run_daily.py --dry-run                                # 不发推送
python run_daily.py --account 500000                         # 指定账户金额（默认 1,000,000）
python run_daily.py --positions SH600000:500,SZ000001:300   # 指定当前持仓
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
| 股票池过滤 | `data/universe.py` | 排除科创板 / ST / 黑名单 / 最低价格 |
| 行业数据 | `data/sector.py` | akshare 行业映射，7 天 TTL 缓存 |
| 行业因子 | `features/sector_factors.py` | 19 个行业轮动因子 |
| 因子挖掘 | `features/factor_mining.py` | 基于模板的 alpha 因子自动发现 |
| 模型封装 | `models/lgbm_model.py` | LightGBM + 行业/挖掘因子注入 |
| 训练管理 | `models/trainer.py` | qlib_native（MLflow）/ custom（pkl）双模式 |
| 回测引擎 | `backtest/engine.py` | 封装 qlib backtest_daily |
| 网格搜索 | `backtest/grid_search.py` | 多参数组合回测，Sharpe 排序 |
| 性能指标 | `backtest/metrics.py` | 年化收益、Sharpe、最大回撤、Calmar 等 |
| 信号生成 | `signals/generator.py` | TopK 持仓计算 + 差分交易信号 |
| 推送通知 | `notify/pusher.py` | Bark / PushPlus / 钉钉 / Server 酱 |
| AI 优化器 | `agent/auto_optimizer.py` | Claude API 迭代参数优化 |
| 配置管理 | `utils/config.py` | YAML 深度合并加载 |
| 日志 | `utils/logger.py` | 文件 + 控制台日志 |
| qlib 工具 | `utils/qlib_utils.py` | 持仓转换、信号计算、Recorder 加载 |

---

## 通知渠道

在 `config/notify.yaml` 中启用对应渠道并填入凭证：

| 渠道 | 说明 | 获取方式 |
|------|------|----------|
| **Bark** | iOS 推送 | 从 [Bark App](https://bark.day.app/) 获取设备 Key |
| **PushPlus** | 微信推送 | 注册 [PushPlus](http://www.pushplus.plus/) 获取 Token |
| **钉钉** | 钉钉群机器人 | 钉钉群 → 机器人管理 → Webhook + 可选安全密钥 |
| **Server 酱** | 微信推送 | 注册 [Server酱](https://sct.ftqq.com/) 获取 SendKey |

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

---

## 目录结构

```
quant_ex/
├── config/
│   ├── base.yaml          # 基础配置（qlib 路径、市场、训练区间）
│   ├── model.yaml         # 模型超参数
│   ├── strategy.yaml      # 策略参数 & 回测设置
│   └── notify.yaml        # 推送通知凭证
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

**Q: 训练后 `run_daily.py` 报错找不到 Recorder？**
A: 将 `run_train.py` 输出的 Recorder ID 填入 `config/base.yaml` → `experiment.latest_recorder_id`。

**Q: 行业数据加载失败？**
A: 检查网络连接（akshare 需要访问东方财富数据接口）。也可删除 `cache/sector_map.json` 强制刷新。

**Q: AI 优化不工作？**
A: 确认环境变量 `ANTHROPIC_API_KEY` 已正确设置，且账户有 claude-opus-4-6 访问权限。

**Q: 如何添加自定义通知渠道？**
A: 在 `notify/pusher.py` 的 `NotificationPusher` 类中参照现有渠道添加新方法，并在 `send()` 中调用即可。

---

## License

MIT License. 本项目仅供学习和研究使用，不构成投资建议。市场有风险，投资需谨慎。
