#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/Users/weidian/code/quant_ex"
TARGET_DIR="$HOME/Library/LaunchAgents"
LABELS=(
  "com.quant_ex.daily_rebalance"
  "com.quant_ex.daily_rebalance.open_reminder"
  "com.quant_ex.daily_rebalance.close_reminder"
)

mkdir -p "$TARGET_DIR" "$ROOT_DIR/logs"

for LABEL in "${LABELS[@]}"; do
  SOURCE_PLIST="$ROOT_DIR/scripts/$LABEL.plist"
  TARGET_PLIST="$TARGET_DIR/$LABEL.plist"

  cp "$SOURCE_PLIST" "$TARGET_PLIST"
  launchctl bootout "gui/$(id -u)" "$TARGET_PLIST" >/dev/null 2>&1 || true
  launchctl bootstrap "gui/$(id -u)" "$TARGET_PLIST"
  launchctl enable "gui/$(id -u)/$LABEL"
  echo "Installed $LABEL"
done

echo "Schedules:"
echo "  com.quant_ex.daily_rebalance                20:00 generate/cache signal"
echo "  com.quant_ex.daily_rebalance.open_reminder  09:00 send cached reminder"
echo "  com.quant_ex.daily_rebalance.close_reminder 14:00 send cached reminder"
echo "Check status: launchctl print gui/$(id -u)/<label>"
