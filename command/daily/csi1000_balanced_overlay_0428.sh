./.venv/bin/python run_scheduled_rebalance.py \
    --config config/csi1000_balanced_overlay.yaml \
    --model-path models/lgbm_sector_csi1000_balanced_20260428_235851.pkl \
    --today 2026-04-30 \
    --positions SH600489:900,SH600900:900,SH601021:500,SH603259:100,SH603993:1300 \
    --min-action-value 1000