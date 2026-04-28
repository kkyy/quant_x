# quant_ex

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
- Dolt 仓库被其他进程锁住时，脚本会给出明确提示，避免误删或破坏正在下载的数据。
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
│   └── strategy_candidates.yaml
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
