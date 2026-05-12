./.venv/bin/python run_scheduled_rebalance.py \
    --config config/csi1000_adaptive_overlay_20.yaml \
    --model-path models/lgbm_universe-csi1000_20260428_155547.pkl \
    --start-date previous_trade_date \
    --min-action-value 1000 \
    --dry-run
