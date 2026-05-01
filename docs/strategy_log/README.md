# Strategy Iteration Log

本目录用于长期保存**策略配置迭代表格日志**，服务两个目标：

1. 方便人工按时间回看每一轮策略迭代、参数变化、模型来源和结果变化。
2. 方便代码代理或模型在下一轮做 ablation / overlay / 稳健性决策时，直接读取结构化历史，而不是只翻散落的 markdown 和 csv 产物。

## 主文件

- `strategy_iteration_log.csv`：主表，按 `iteration_date` 升序维护。

## 维护规则

- 每新增一个长期保留的策略配置，或对某个候选做出明确迭代结论，都应追加一行。
- 不记录一次性的临时调试参数；只记录“值得后续比较或复用”的策略版本。
- `config_path` 填相对路径；如果该策略没有独立配置文件，可填 `-`，并在 `notes` 中说明。
- `result_source` 填支撑该条记录的文件，如 `optimization_results/...csv`、`...md`、`config/strategy_candidates.yaml`。
- `next_ablation` 只写下一步最重要的一条，不要堆太多待办。

## 推荐字段解释

- `strategy_id`：稳定的策略标识，便于后续引用。
- `parent_strategy_id`：本次迭代基于哪个父策略演化而来。
- `iteration_date`：做出本轮结论的日期，而不是训练开始日期。
- `stage`：如 `baseline`、`overlay`、`candidate`、`retired`、`promoted`。
- `decision`：如 `keep`、`compare_next`、`fallback`、`do_not_promote`。
- `notes`：一句话总结本轮读数。
- `next_ablation`：下一轮最关键的对照实验方向。

## 使用建议

- 先读 `strategy_iteration_log.csv` 再决定跑哪些回测。
- 做新策略时，优先和 `decision=compare_next` 或 `decision=keep` 的条目比较。
- 若某条策略已经失效或被更优版本替代，不删除旧记录，只新增一条更新状态的记录。