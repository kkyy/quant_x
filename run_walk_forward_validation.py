#!/usr/bin/env python3
"""Run walk-forward model training and strategy validation.

This script retrains models for each chronological fold, backtests only the
fold's future test window, and writes fold CSVs plus an aggregate report under
optimization_results/walk_forward_*.
"""
from __future__ import annotations

import argparse
import concurrent.futures as _cf
import json
import shutil
import subprocess
import sys
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
import yaml


REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_PYTHON = REPO_ROOT / ".venv" / "bin" / "python"


@dataclass(frozen=True)
class Fold:
    name: str
    fit_start: str
    fit_end: str
    valid_start: str
    valid_end: str
    test_start: str
    test_end: str


DEFAULT_FOLDS = [
    Fold("test_2020", "2015-01-01", "2018-12-31", "2019-01-01", "2019-12-31", "2020-01-01", "2020-12-31"),
    Fold("test_2021", "2015-01-01", "2019-12-31", "2020-01-01", "2020-12-31", "2021-01-01", "2021-12-31"),
    Fold("test_2022", "2015-01-01", "2020-12-31", "2021-01-01", "2021-12-31", "2022-01-01", "2022-12-31"),
    Fold("test_2023", "2015-01-01", "2021-12-31", "2022-01-01", "2022-12-31", "2023-01-01", "2023-12-31"),
    Fold("test_2024", "2015-01-01", "2021-12-31", "2022-01-01", "2023-12-31", "2024-01-01", "2024-12-31"),
    Fold("test_2025", "2015-01-01", "2022-12-31", "2023-01-01", "2024-12-31", "2025-01-01", "2025-12-31"),
    Fold("test_2026", "2015-01-01", "2023-12-31", "2024-01-01", "2025-12-31", "2026-01-01", datetime.now().strftime("%Y-%m-%d")),
]


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_int_csv(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def run_command(command: list[str], log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log_file:
        log_file.write("$ " + " ".join(command) + "\n\n")
        log_file.flush()
        subprocess.run(
            command,
            cwd=REPO_ROOT,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            check=True,
        )


def write_fold_config(path: Path, fold: Fold, train_universe: str) -> None:
    config = {
        "market": {
            "name": train_universe,
        },
        "training": {
            "fit_start": fold.fit_start,
            "fit_end": fold.fit_end,
            "valid_start": fold.valid_start,
            "valid_end": fold.valid_end,
            "test_start": fold.test_start,
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(config, sort_keys=False, allow_unicode=True), encoding="utf-8")


def newest_model_for_tag(tag: str, after: float) -> Path:
    candidates = [
        path for path in (REPO_ROOT / "models").glob(f"lgbm_{tag}_*.pkl")
        if path.stat().st_mtime >= after
    ]
    if not candidates:
        candidates = list((REPO_ROOT / "models").glob(f"lgbm_{tag}_*.pkl"))
    if not candidates:
        raise FileNotFoundError(f"No trained model found for tag={tag}")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def latest_grid_csv() -> Path:
    candidates = list((REPO_ROOT / "backtest_results").glob("grid_*.csv"))
    if not candidates:
        raise FileNotFoundError("No backtest grid CSV found")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def summarize(results: pd.DataFrame) -> pd.DataFrame:
    group_cols = ["train_universe", "eval_market", "topk", "n_drop", "hold_thresh"]
    rows = []
    for keys, group in results.groupby(group_cols, dropna=False):
        row = dict(zip(group_cols, keys))
        row.update(
            folds=int(group["fold"].nunique()),
            mean_annual_return=group["annual_return"].mean(),
            median_annual_return=group["annual_return"].median(),
            mean_sharpe=group["sharpe"].mean(),
            median_sharpe=group["sharpe"].median(),
            min_sharpe=group["sharpe"].min(),
            sharpe_std=group["sharpe"].std(ddof=0),
            mean_max_drawdown=group["max_drawdown"].mean(),
            worst_max_drawdown=group["max_drawdown"].min(),
            positive_return_folds=int((group["annual_return"] > 0).sum()),
            positive_sharpe_folds=int((group["sharpe"] > 0).sum()),
            mean_rank_ic=group["rank_ic"].mean() if "rank_ic" in group else float("nan"),
            mean_rank_icir=group["rank_icir"].mean() if "rank_icir" in group else float("nan"),
        )
        row["robust_score"] = (
            row["mean_sharpe"]
            - 0.5 * row["sharpe_std"]
            + 0.2 * row["min_sharpe"]
            + 0.05 * row["positive_sharpe_folds"]
        )
        rows.append(row)
    return pd.DataFrame(rows).sort_values(
        ["robust_score", "mean_sharpe", "worst_max_drawdown"],
        ascending=[False, False, False],
    )


def pct(value: float) -> str:
    if pd.isna(value):
        return "nan"
    return f"{value:.2%}"


def write_report(path: Path, summary: pd.DataFrame, results: pd.DataFrame, args: argparse.Namespace) -> None:
    top = summary.head(12).copy()
    lines = [
        "# Walk-forward Validation Report",
        "",
        f"- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"- Train universes: `{args.train_universes}`",
        f"- Eval market: `{args.eval_market}`",
        f"- Strategy grid: topk=`{args.topk}`, n_drop=`{args.n_drop}`, hold_thresh=`{args.hold_thresh}`",
        f"- Folds: {results['fold'].nunique()}",
        "",
        "## Best Robust Configurations",
        "",
        "| rank | train_universe | topk | n_drop | hold | mean annual | mean sharpe | min sharpe | sharpe std | worst drawdown | positive folds | rank_ic | rank_icir |",
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for idx, (_, row) in enumerate(top.iterrows(), start=1):
        lines.append(
            "| {rank} | {universe} | {topk} | {n_drop} | {hold} | {annual} | {sharpe:.3f} | "
            "{min_sharpe:.3f} | {sharpe_std:.3f} | {dd} | {pos}/{folds} | {rank_ic:.4f} | {rank_icir:.4f} |".format(
                rank=idx,
                universe=row["train_universe"],
                topk=int(row["topk"]),
                n_drop=int(row["n_drop"]),
                hold=int(row["hold_thresh"]),
                annual=pct(row["mean_annual_return"]),
                sharpe=row["mean_sharpe"],
                min_sharpe=row["min_sharpe"],
                sharpe_std=row["sharpe_std"],
                dd=pct(row["worst_max_drawdown"]),
                pos=int(row["positive_sharpe_folds"]),
                folds=int(row["folds"]),
                rank_ic=row["mean_rank_ic"],
                rank_icir=row["mean_rank_icir"],
            )
        )

    best = summary.iloc[0]
    lines.extend(
        [
            "",
            "## Current Read",
            "",
            (
                f"The current robust winner is `{best['train_universe']}` training with "
                f"`topk={int(best['topk'])}, n_drop={int(best['n_drop'])}, "
                f"hold_thresh={int(best['hold_thresh'])}`. "
                f"It has mean Sharpe `{best['mean_sharpe']:.3f}`, min Sharpe `{best['min_sharpe']:.3f}`, "
                f"and worst drawdown `{pct(best['worst_max_drawdown'])}` across folds."
            ),
            "",
            "Treat this as a research candidate, not proof of live profitability. The decisive checks are fold stability, drawdown tolerance, and whether the same parameters remain good without re-optimizing each year.",
            "",
            "## Artifacts",
            "",
            f"- All fold rows: `{path.parent / 'walk_forward_all_results.csv'}`",
            f"- Aggregated summary: `{path.parent / 'walk_forward_summary.csv'}`",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _run_one_fold_universe(
    fold: "Fold",
    train_universe: str,
    args: "argparse.Namespace",
    out_dir: Path,
    run_id: str,
) -> "pd.DataFrame":
    """Train + backtest one (fold, train_universe) combination.

    Returns a DataFrame with result rows annotated with fold metadata.
    """
    python = Path(args.python)
    configs_dir = out_dir / "configs"
    logs_dir = out_dir / "logs"
    fold_results_dir = out_dir / "fold_results"

    tag = f"wf_{train_universe}_{fold.name}_{run_id}"
    cfg_path = configs_dir / f"{tag}.yaml"
    write_fold_config(cfg_path, fold, train_universe)

    print(f"\n=== Train {tag} ===", flush=True)
    before_train = datetime.now().timestamp()
    train_cmd = [
        str(python),
        "run_train.py",
        "--config",
        str(cfg_path),
        "--model",
        "lgbm",
        "--no-extra-factors",
        "--tag",
        tag,
    ]
    run_command(train_cmd, logs_dir / f"{tag}_train.log")
    model_path = newest_model_for_tag(tag, before_train)

    print(
        f"=== Backtest {tag} on {args.eval_market} {fold.test_start}..{fold.test_end} ===",
        flush=True,
    )
    backtest_cmd = [
        str(python),
        "run_backtest.py",
        "--config",
        str(cfg_path),
        "--model-path",
        str(model_path),
        "--market",
        args.eval_market,
        "--topk",
        args.topk,
        "--n-drop",
        args.n_drop,
        "--hold-thresh",
        args.hold_thresh,
        "--start",
        fold.test_start,
        "--end",
        fold.test_end,
        "--grid-workers",
        str(args.grid_workers),
    ]
    if args.seeds:
        backtest_cmd.append("--seeds")
    run_command(backtest_cmd, logs_dir / f"{tag}_backtest.log")

    grid_path = latest_grid_csv()
    fold_results_dir.mkdir(parents=True, exist_ok=True)
    dest = fold_results_dir / f"{tag}_on_{args.eval_market}.csv"
    shutil.copy2(grid_path, dest)

    frame = pd.read_csv(dest)
    frame.insert(0, "model_path", str(model_path.relative_to(REPO_ROOT)))
    frame.insert(0, "eval_market", args.eval_market)
    frame.insert(0, "train_universe", train_universe)
    frame.insert(0, "fold", fold.name)
    frame.insert(0, "test_end", fold.test_end)
    frame.insert(0, "test_start", fold.test_start)
    return frame


def run_validation(args: argparse.Namespace) -> Path:
    # Keep the venv launcher path intact. Resolving it follows the symlink to
    # the base interpreter and can drop the virtualenv's site-packages.
    run_id = args.run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = REPO_ROOT / "optimization_results" / f"walk_forward_{run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "run_id": run_id,
        "args": vars(args),
        "folds": [fold.__dict__ for fold in DEFAULT_FOLDS],
    }
    (out_dir / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    all_results_path = out_dir / "walk_forward_all_results.csv"
    all_frames = []
    done_keys = set()
    if all_results_path.exists():
        existing = pd.read_csv(all_results_path)
        if not existing.empty:
            all_frames.append(existing)
            done_keys = {
                (str(row.fold), str(row.train_universe))
                for row in existing[["fold", "train_universe"]].drop_duplicates().itertuples(index=False)
            }
            print(f"Resuming from {all_results_path}: {len(done_keys)} fold×universe pairs already done", flush=True)
    train_universes = parse_csv(args.train_universes)
    combos = [
        (fold, universe)
        for fold in DEFAULT_FOLDS
        for universe in train_universes
        if (fold.name, universe) not in done_keys
    ]

    workers = max(1, args.workers)
    _write_lock = threading.Lock()

    def _save_partial(frames: list) -> None:
        """Persist partial results (called under lock)."""
        if not frames:
            return
        combined = pd.concat(frames, ignore_index=True)
        combined.to_csv(out_dir / "walk_forward_all_results.csv", index=False)
        summarize(combined).to_csv(out_dir / "walk_forward_summary.csv", index=False)

    if not combos:
        print("All requested fold×universe pairs are already complete; regenerating summary/report.", flush=True)
    elif workers == 1:
        for fold, train_universe in combos:
            frame = _run_one_fold_universe(fold, train_universe, args, out_dir, run_id)
            all_frames.append(frame)
            with _write_lock:
                _save_partial(all_frames)
    else:
        print(
            f"\nRunning {len(combos)} fold×universe combinations with {workers} parallel workers",
            flush=True,
        )
        with _cf.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_combo = {
                executor.submit(
                    _run_one_fold_universe, fold, universe, args, out_dir, run_id
                ): (fold, universe)
                for fold, universe in combos
            }
            for future in _cf.as_completed(future_to_combo):
                fold, universe = future_to_combo[future]
                try:
                    frame = future.result()
                    with _write_lock:
                        all_frames.append(frame)
                        _save_partial(all_frames)
                except Exception as exc:
                    print(
                        f"WARNING: fold={fold.name} universe={universe} FAILED: {exc}",
                        flush=True,
                    )

    results = pd.concat(all_frames, ignore_index=True)
    summary = summarize(results)
    all_path = out_dir / "walk_forward_all_results.csv"
    summary_path = out_dir / "walk_forward_summary.csv"
    report_path = out_dir / "walk_forward_report.md"
    results.to_csv(all_path, index=False)
    summary.to_csv(summary_path, index=False)
    write_report(report_path, summary, results, args)
    print(f"\nReport saved: {report_path}", flush=True)
    return report_path


def main(argv: Iterable[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Walk-forward validation for quant_ex")
    parser.add_argument("--python", default=str(DEFAULT_PYTHON if DEFAULT_PYTHON.exists() else sys.executable))
    parser.add_argument("--train-universes", default="csi300,csi800,csi1000")
    parser.add_argument("--eval-market", default="csi300")
    parser.add_argument("--topk", default="5,15,20")
    parser.add_argument("--n-drop", default="1,3")
    parser.add_argument("--hold-thresh", default="5,8,10")
    parser.add_argument("--seeds", action="store_true", help="Run multi-seed backtests for every strategy row")
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of fold×universe pairs to run in parallel (default: 1 serial). "
             "Set to 2-3 on M3; each worker runs a full train+backtest subprocess chain.",
    )
    parser.add_argument(
        "--grid-workers",
        type=int,
        default=-1,
        help="Parallel workers for the backtest grid search inside each fold "
             "(-1 = all CPU cores, 1 = serial). Passed through to run_backtest.py.",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    # Validate early so typos fail before the first long train.
    parse_csv(args.train_universes)
    parse_int_csv(args.topk)
    parse_int_csv(args.n_drop)
    parse_int_csv(args.hold_thresh)

    run_validation(args)


if __name__ == "__main__":
    main()
