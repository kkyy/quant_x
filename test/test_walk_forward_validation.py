from argparse import Namespace
from pathlib import Path

import pandas as pd

import run_walk_forward_validation as wfv


def _args(with_extra_factors: bool) -> Namespace:
    return Namespace(
        python=str(wfv.DEFAULT_PYTHON),
        eval_market="csi300",
        topk="15",
        n_drop="3",
        hold_thresh="8",
        seeds=False,
        grid_workers=1,
        train_config=None,
        _train_config_dict=None,
        with_extra_factors=with_extra_factors,
    )


def _run_single_fold(monkeypatch, tmp_path: Path, with_extra_factors: bool) -> list[list[str]]:
    commands: list[list[str]] = []

    def fake_run_command(command: list[str], log_path: Path) -> None:
        commands.append(command)
        if "--output-csv" in command:
            dest = Path(command[command.index("--output-csv") + 1])
            dest.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                [
                    {
                        "topk": 15,
                        "n_drop": 3,
                        "hold_thresh": 8,
                        "annual_return": 0.1,
                        "sharpe": 1.0,
                        "max_drawdown": -0.1,
                        "rank_ic": 0.02,
                    }
                ]
            ).to_csv(dest, index=False)

    monkeypatch.setattr(wfv, "run_command", fake_run_command)
    monkeypatch.setattr(
        wfv,
        "newest_model_for_tag",
        lambda tag, before_ts: wfv.REPO_ROOT / "models" / f"lgbm_{tag}_fake.pkl",
    )

    fold = wfv.Fold(
        "test_fold",
        "2015-01-01",
        "2018-12-31",
        "2019-01-01",
        "2019-12-31",
        "2020-01-01",
        "2020-12-31",
    )
    wfv._run_one_fold_universe(fold, "csi1000", _args(with_extra_factors), tmp_path, "unit")
    return commands


def test_walk_forward_defaults_to_no_extra_factors(monkeypatch, tmp_path):
    commands = _run_single_fold(monkeypatch, tmp_path, with_extra_factors=False)
    train_cmd = commands[0]
    assert "--no-extra-factors" in train_cmd


def test_walk_forward_can_enable_extra_factors(monkeypatch, tmp_path):
    commands = _run_single_fold(monkeypatch, tmp_path, with_extra_factors=True)
    train_cmd = commands[0]
    assert "--no-extra-factors" not in train_cmd
