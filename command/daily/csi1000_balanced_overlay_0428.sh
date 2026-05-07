./.venv/bin/python run_scheduled_rebalance.py \
    --config config/csi1000_balanced_overlay.yaml \
    --model-path models/lgbm_sector_csi1000_balanced_20260428_235851.pkl \
    --today 2026-05-06 \
    --positions SH600489:900:2026-04-29,SH600900:900:2026-04-29,SH601021:500:2026-04-29,SH603259:100:2026-04-29,SH603993:1300:2026-04-29 \
    --min-action-value 1000