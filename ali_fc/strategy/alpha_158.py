import os



def run_qlib():

    MARKET = "csi300"
    BENCHMARK = "SH000300"

    EXP_NAME = "tutorial_exp"

    data_provide_uri = "/tmp/qlib_data/cn_data"

    import qlib

    qlib.init(provider_uri=data_provide_uri, region="cn")

    handler_kwargs = {
        "start_time": "2015-01-01",
        "end_time": "2026-12-31",
        "fit_start_time": "2015-01-01",
        "fit_end_time": "2021-12-31",
        "instruments": MARKET,
        #"label" : ["Ref(Mean(Ref($close, -1) / $close - 1, 5), -4)"], # 5日收益均值
        #"label": ["Ref(Mean(Log(Ref($close,-1)/$close), 5), -4)"], # 对数收益的均值，gpt教我的
    }
    handler_conf = {
        "class": "Alpha158",
        "module_path": "qlib.contrib.data.handler",
        "kwargs": handler_kwargs,
    }

    from qlib.utils import init_instance_by_config
    hd = init_instance_by_config(handler_conf)

    from qlib.data.dataset import TSDatasetH

    dataset_conf = {
        "class": "DatasetH",
        "module_path": "qlib.data.dataset",
        "kwargs": {
            "handler": hd,
            "segments": {
                "train": ("2015-01-01", "2021-12-31"),
                "valid": ("2022-01-01", "2023-12-31"),
                "test": ("2024-01-01", "2026-09-30"),
            },
        },
    }
    dataset = init_instance_by_config(dataset_conf)

    from qlib.workflow import R
    from qlib.workflow.record_temp import SignalRecord, PortAnaRecord, SigAnaRecord

    model = init_instance_by_config(
        {
            "class": "LGBModel",
            "module_path": "qlib.contrib.model.gbdt",
            "kwargs": {
                "loss": "mse",
                "colsample_bytree": 0.8879,
                "learning_rate": 0.0421,
                "subsample": 0.8789,
                "lambda_l1": 205.6999,
                "lambda_l2": 580.9768,
                "max_depth": 8,
                "num_leaves": 210,
                "num_threads": 20,
            },
        }
    )

    # start exp to train model
    with R.start(experiment_name=EXP_NAME):
        model.fit(dataset)
        R.save_objects(trained_model=model)

        rec = R.get_recorder()
        rid = rec.id  # save the record id

        # Inference and saving signal
        sr = SignalRecord(model, dataset, rec)
        sr.generate()

    ###################################
    # prediction, backtest & analysis
    ###################################
    port_analysis_config = {
        "executor": {
            "class": "SimulatorExecutor",
            "module_path": "qlib.backtest.executor",
            "kwargs": {
                "time_per_step": "day",
                "generate_portfolio_metrics": True,
            },
        },
        "strategy": {
            "class": "TopkDropoutStrategy",
            "module_path": "qlib.contrib.strategy.signal_strategy",
            "kwargs": {
                "signal": "<PRED>",
                "topk": 5,
                "n_drop": 3,
                "hold_thresh":5
            },
        },
        "backtest": {
            "start_time": "2024-01-01",
            "end_time": "2025-08-31",
            "account": 1000000,
            "benchmark": BENCHMARK,
            "exchange_kwargs": {
                "freq": "day",
                "limit_threshold": 0.095,
                "deal_price": "close",
                "open_cost": 0.0005,
                "close_cost": 0.0015,
                "min_cost": 5,
            },
        },
    }

    # backtest and analysis
    with R.start(experiment_name=EXP_NAME, recorder_id=rid, resume=True):
        # signal-based analysis
        rec = R.get_recorder()
        sar = SigAnaRecord(rec)
        sar.generate()

        #  portfolio-based analysis: backtest
        par = PortAnaRecord(rec, port_analysis_config, "day")
        par.generate()

    # load recorder
    recorder = R.get_recorder(recorder_id=rid, experiment_name=EXP_NAME)

    # load previous results
    pred_df = recorder.load_object("pred.pkl")
    report_normal_df = recorder.load_object("portfolio_analysis/report_normal_1day.pkl")
    positions = recorder.load_object("portfolio_analysis/positions_normal_1day.pkl")
    analysis_df = recorder.load_object("portfolio_analysis/port_analysis_1day.pkl")

    # Previous Model can be loaded. but it is not used.
    loaded_model = recorder.load_object("trained_model")

    import pandas as pd
    from datetime import datetime

    def positions_to_weight_df(positions_dict):
        """
        将 Qlib 回测输出的 positions 字典转换为权重 DataFrame
        
        参数:
            positions_dict: 原始的嵌套字典，key 是 Timestamp，value 包含 'position' 字段
        
        返回:
            pd.DataFrame: 行为日期，列为股票代码，值为持仓权重（weight）
        """
        records = []

        for date, data in positions_dict.items():
            pos = data.position
            # 排除 cash 和 now_account_value 等非股票字段
            stock_weights = {
                symbol: info['weight'] 
                for symbol, info in pos.items() 
                if isinstance(info, dict) and 'weight' in info
            }
            # 添加日期作为索引
            stock_weights['date'] = pd.to_datetime(date)
            records.append(stock_weights)

        # 转为 DataFrame
        df = pd.DataFrame(records)
        if not df.empty:
            df = df.set_index('date').sort_index()
        return df

    # 转换为权重 DataFrame
    weight_df = positions_to_weight_df(positions)
    weight_df.fillna(0, inplace=True)  # 缺失值填0，表示未持仓
    weight_df.to_csv('tml.csv')

if __name__ == "__main__":
    run_qlib()