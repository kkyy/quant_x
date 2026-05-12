import json

import pandas as pd
from fastapi.testclient import TestClient

from web.api.app import create_app
from web.api.routers import backtest as backtest_router
from web.api.routers import data as data_router
from web.api.routers import signals as signals_router
from web.api.services import chart_service
from web.api.services.data_service import _json_safe_quote_records


def test_spa_deep_link_falls_back_to_index():
    client = TestClient(create_app())

    response = client.get("/models")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert '<div id="root">' in response.text


def test_api_routes_are_not_intercepted_by_spa_fallback():
    client = TestClient(create_app())

    response = client.get("/api/system/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_sector_list_uses_sector_map_cache(monkeypatch, tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    (cache_dir / "sector_map.json").write_text(
        '{"SH600000": "Banks", "SZ000001": "Banks", "SH600519": "Liquor"}',
        encoding="utf-8",
    )
    monkeypatch.setattr(data_router, "CACHE_DIR", cache_dir)

    client = TestClient(create_app())
    response = client.get("/api/data/sectors")

    assert response.status_code == 200
    assert response.json() == [
        {"sector_id": "Banks", "sector_name": "Banks", "stock_count": 2},
        {"sector_id": "Liquor", "sector_name": "Liquor", "stock_count": 1},
    ]


def test_stock_quote_records_are_strict_json_safe():
    df = pd.DataFrame(
        {
            "open": [1.0, float("nan")],
            "close": [float("inf"), float("-inf")],
            "volume": [100, pd.NA],
        },
        index=pd.to_datetime(["2026-05-11", "2026-05-12"]),
    )

    records = _json_safe_quote_records(df)

    assert records == [
        {"date": "2026-05-11", "open": 1.0, "close": None, "volume": 100},
        {"date": "2026-05-12", "open": None, "close": None, "volume": None},
    ]
    json.dumps(records, allow_nan=False)


def test_equity_curve_accepts_qlib_bench_column(monkeypatch, tmp_path):
    result_dir = tmp_path / "backtest_results"
    result_dir.mkdir()
    (result_dir / "daily.csv").write_text(
        "date,return,bench\n2026-01-01,0.1,0.05\n2026-01-02,-0.1,0.0\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(chart_service, "BACKTEST_RESULTS_DIR", result_dir)

    curve = chart_service.parse_equity_curve("daily.csv")

    assert curve["portfolio"] == [1.1, 0.99]
    assert curve["benchmark"] == [1.05, 1.05]
    assert curve["excess"] == [0.05, -0.06]


def test_notify_test_defaults_to_dry_run_without_sending():
    client = TestClient(create_app())

    response = client.post(
        "/api/signals/notify-test",
        json={"title": "Test", "content": "Preview only"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["dry_run"] is True
    assert payload["sent"] is False


def test_notify_test_requires_confirmation_for_real_send():
    client = TestClient(create_app())

    response = client.post(
        "/api/signals/notify-test",
        json={
            "title": "Test",
            "content": "Should not send",
            "dry_run": False,
            "confirm_send": False,
        },
    )

    assert response.status_code == 400
    assert "confirm_send=true" in response.json()["detail"]


def test_rebalance_command_includes_web_supported_safety_flags():
    req = signals_router.RebalanceRequest(
        mock=False,
        dry_run=True,
        config="config/daily_csi1000.yaml",
        positions="SH600489:900",
        position_date="2026-05-08",
        min_action_value=1000,
        skip_update=True,
        force=True,
        notify_channel="bark",
    )

    cmd = signals_router._build_rebalance_cmd(req)

    assert "--dry-run" in cmd
    assert "--skip-update" in cmd
    assert "--force" in cmd
    assert cmd[cmd.index("--config") + 1] == "config/daily_csi1000.yaml"
    assert cmd[cmd.index("--positions") + 1] == "SH600489:900"
    assert cmd[cmd.index("--position-date") + 1] == "2026-05-08"
    assert cmd[cmd.index("--min-action-value") + 1] == "1000.0"
    assert cmd[cmd.index("--notify-channel") + 1] == "bark"


def test_grid_command_includes_advanced_web_params():
    req = backtest_router.GridSearchRequest(
        model_path="models/demo.pkl",
        topk=[5, 15],
        n_drop=[1, 3],
        hold_thresh=[5, 8],
        start="2024-01-01",
        end="2026-05-11",
        market="csi300",
        multi_seed=True,
        optimize=True,
        n_iters=5,
        grid_workers=4,
        output_csv="backtest_results/demo.csv",
        slippage_sensitivity=True,
        slippage_multipliers=[0.0, 1.0, 2.0],
        markets=["csi300", "csi1000"],
        explore_markets=True,
    )

    cmd = backtest_router._build_grid_cmd(req)

    assert "--seeds" in cmd
    assert "--optimize" in cmd
    assert "--slippage-sensitivity" in cmd
    assert "--explore-markets" in cmd
    assert cmd[cmd.index("--model-path") + 1] == "models/demo.pkl"
    assert cmd[cmd.index("--topk") + 1] == "5,15"
    assert cmd[cmd.index("--n-drop") + 1] == "1,3"
    assert cmd[cmd.index("--hold-thresh") + 1] == "5,8"
    assert cmd[cmd.index("--n-iters") + 1] == "5"
    assert cmd[cmd.index("--grid-workers") + 1] == "4"
    assert cmd[cmd.index("--output-csv") + 1] == "backtest_results/demo.csv"
    assert cmd[cmd.index("--slippage-multipliers") + 1] == "0.0,1.0,2.0"
    assert cmd[cmd.index("--markets") + 1] == "csi300,csi1000"


def test_wfv_command_includes_advanced_web_params():
    req = backtest_router.WFVRequest(
        train_universes=["csi300", "csi800"],
        eval_market="csi1000",
        topk=[5, 20],
        n_drop=[1, 5],
        hold_thresh=[5, 10],
        workers=2,
        seeds=True,
        run_id="wfv_demo",
        grid_workers=3,
        robust_weights={"mean_sharpe": 1.0, "sharpe_std": -0.3},
        folds_config="config/walk_forward_folds.yaml",
        train_config="config/model.yaml",
    )

    cmd = backtest_router._build_wfv_cmd(req)

    assert "--seeds" in cmd
    assert cmd[cmd.index("--train-universes") + 1] == "csi300,csi800"
    assert cmd[cmd.index("--eval-market") + 1] == "csi1000"
    assert cmd[cmd.index("--topk") + 1] == "5,20"
    assert cmd[cmd.index("--n-drop") + 1] == "1,5"
    assert cmd[cmd.index("--hold-thresh") + 1] == "5,10"
    assert cmd[cmd.index("--workers") + 1] == "2"
    assert cmd[cmd.index("--run-id") + 1] == "wfv_demo"
    assert cmd[cmd.index("--grid-workers") + 1] == "3"
    assert cmd[cmd.index("--folds-config") + 1] == "config/walk_forward_folds.yaml"
    assert cmd[cmd.index("--train-config") + 1] == "config/model.yaml"
    assert '"sharpe_std": -0.3' in cmd[cmd.index("--robust-weights") + 1]
