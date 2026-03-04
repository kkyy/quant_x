"""
AI-powered strategy parameter optimizer.

Uses Claude API to analyse grid-search results, explain the findings,
and propose the next parameter grid to search — iteratively converging
toward a better strategy configuration.

Usage:
    optimizer = AutoOptimizer()           # reads ANTHROPIC_API_KEY from env
    results = optimizer.run_loop(
        backtest_engine, pred,
        initial_grid={"topk": [5,10,15], "n_drop": [1,3], "hold_thresh": [3,5]},
        n_iterations=3,
    )
"""
from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

_SYSTEM = """你是一位量化投资策略优化专家，专注于A股市场。
你的任务是分析回测参数搜索结果，给出下一轮的优化方向。

TopkDropout 策略参数说明：
- topk:        最大持仓股票数（越多分散风险但信号稀释）
- n_drop:      每次调仓时替换的股票数（越大换手越高）
- hold_thresh: 持股最少天数（越大越稳定但反应慢）

回测指标说明：
- annual_return: 年化收益率（越高越好）
- sharpe:        夏普比率（>1 优秀，>2 极好，要规避过拟合）
- max_drawdown:  最大回撤（负数，绝对值越小越好）
- calmar:        年化收益/最大回撤（越高越好）
- win_rate:      胜率

输出 **严格合法的 JSON**，包含以下字段：
{
  "analysis":      "对结果的关键分析（2-4句话）",
  "best_params":   {"topk": 10, "n_drop": 3, "hold_thresh": 5},
  "next_grid":     {"topk": [8,10,12], "n_drop": [2,3,4], "hold_thresh": [3,5,7]},
  "reasoning":     "为什么这样调整（逻辑推理）",
  "risk_warnings": ["潜在风险1", "潜在风险2"]
}"""


class AutoOptimizer:
    """LLM-powered iterative strategy optimizer."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "claude-opus-4-6",
    ):
        self.model = model
        self._api_key = api_key
        self._client = None

    # ── public API ────────────────────────────────────────────────────────────

    def analyse(
        self,
        grid_results: pd.DataFrame,
        iteration: int = 1,
        history: Optional[List[Dict]] = None,
    ) -> Dict[str, Any]:
        """
        Analyse one round of grid-search results.

        Returns:
            dict with analysis, best_params, next_grid, reasoning, risk_warnings
        """
        client = self._client_or_init()
        prompt = self._build_prompt(grid_results, iteration, history or [])

        response = client.messages.create(
            model=self.model,
            max_tokens=1500,
            system=_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text
        result = self._parse_json(text)
        result.setdefault("raw_response", text)
        return result

    def run_loop(
        self,
        backtest_engine,
        pred: pd.Series,
        initial_grid: Optional[Dict[str, List[Any]]] = None,
        n_iterations: int = 3,
        save_dir: str = "./optimization_results",
        universe_filter=None,
    ) -> List[Dict]:
        """
        Run the full optimisation loop.

        Each iteration:
        1. Grid-search backtest over current_grid
        2. Claude analyses results → suggests next_grid
        3. Repeat

        Returns:
            List of per-iteration records
        """
        from ..backtest.grid_search import GridSearchBacktest

        Path(save_dir).mkdir(parents=True, exist_ok=True)

        searcher = GridSearchBacktest(backtest_engine, pred, {})
        current_grid = initial_grid or GridSearchBacktest.DEFAULT_GRID
        history: List[Dict] = []
        all_records: List[Dict] = []

        for it in range(1, n_iterations + 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"  优化第 {it}/{n_iterations} 轮 | 搜索网格: {current_grid}")
            logger.info(f"{'='*60}")

            results_df = searcher.run(
                param_grid=current_grid,
                universe_filter=universe_filter,
            )
            results_df.to_csv(f"{save_dir}/iter_{it:02d}_results.csv", index=False)

            try:
                suggestion = self.analyse(results_df, it, history)
                next_grid = suggestion.get("next_grid") or GridSearchBacktest.DEFAULT_GRID
                best = suggestion.get("best_params", {})

                logger.info(f"  LLM 分析: {suggestion.get('analysis', '')}")
                logger.info(f"  最优参数: {best}")
                logger.info(f"  下轮网格: {next_grid}")
                for w in suggestion.get("risk_warnings", []):
                    logger.warning(f"  ⚠️  {w}")

                Path(f"{save_dir}/iter_{it:02d}_suggestion.json").write_text(
                    json.dumps(suggestion, ensure_ascii=False, indent=2), encoding="utf-8"
                )

                record = {"iteration": it, "grid": current_grid,
                          "best_params": best, "suggestion": suggestion}
                history.append(record)
                all_records.append(record)
                current_grid = next_grid

            except Exception as e:
                logger.error(f"LLM 分析失败: {e}  — 使用统计最优参数继续")
                best = GridSearchBacktest.best_params(results_df)
                all_records.append({"iteration": it, "grid": current_grid,
                                     "best_params": best, "error": str(e)})
                current_grid = {k: [v] for k, v in best.items()} if best else current_grid

        Path(f"{save_dir}/summary.json").write_text(
            json.dumps(all_records, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
        )
        logger.info(f"\n优化完成。结果已保存到 {save_dir}/")
        return all_records

    # ── private ───────────────────────────────────────────────────────────────

    def _client_or_init(self):
        if self._client is None:
            import os
            import anthropic
            key = self._api_key or os.environ.get("ANTHROPIC_API_KEY")
            if not key:
                raise EnvironmentError(
                    "请设置环境变量 ANTHROPIC_API_KEY 或在 AutoOptimizer(api_key=...) 中传入"
                )
            self._client = anthropic.Anthropic(api_key=key)
        return self._client

    def _build_prompt(
        self,
        df: pd.DataFrame,
        iteration: int,
        history: List[Dict],
    ) -> str:
        lines = [f"## 第 {iteration} 轮参数搜索结果\n"]

        if not df.empty:
            top = df.head(10)
            lines.append(f"共测试 {len(df)} 组参数，Top-10 如下（按夏普降序）:\n")
            for _, row in top.iterrows():
                lines.append(
                    f"topk={int(row.get('topk',0))}  n_drop={int(row.get('n_drop',0))}"
                    f"  hold={int(row.get('hold_thresh',0))}"
                    f" | Sharpe={row.get('sharpe',0):.3f}"
                    f"  年化={row.get('annual_return',0):.2%}"
                    f"  回撤={row.get('max_drawdown',0):.2%}"
                    f"  Calmar={row.get('calmar',0):.3f}"
                )
        else:
            lines.append("本轮无有效结果。\n")

        if history:
            lines.append("\n## 历史迭代摘要\n")
            for h in history:
                lines.append(
                    f"第{h['iteration']}轮最优: {h.get('best_params', {})} | "
                    f"分析: {h.get('suggestion', {}).get('analysis', '')[:80]}"
                )

        lines.append("\n请输出 JSON 格式的分析和建议。")
        return "\n".join(lines)

    @staticmethod
    def _parse_json(text: str) -> Dict:
        import re
        m = re.search(r'\{[\s\S]*\}', text)
        if m:
            try:
                return json.loads(m.group())
            except json.JSONDecodeError:
                pass
        return {"analysis": text, "best_params": {}, "next_grid": {}, "reasoning": text}
