#!/usr/bin/env bash
# Install launchd plist agents for daily rebalancing.
# Paths are resolved dynamically from the script's location.
set -euo pipefail

PROJ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="$PROJ/.venv/bin/python"
TARGET_DIR="$HOME/Library/LaunchAgents"
LOG_DIR="$PROJ/logs"
mkdir -p "$TARGET_DIR" "$LOG_DIR"

AGENTS=(
  "com.quant_ex.daily_rebalance:20:0"
  "com.quant_ex.daily_rebalance.open_reminder:9:0"
  "com.quant_ex.daily_rebalance.close_reminder:14:0"
)

for ENTRY in "${AGENTS[@]}"; do
  IFS=':' read -r LABEL HOUR MINUTE <<< "$ENTRY"
  PLIST="$TARGET_DIR/$LABEL.plist"

  # Build extra args based on label
  EXTRA_ARGS=""
  if [[ "$LABEL" == *open_reminder ]]; then
    EXTRA_ARGS="<string>--remind</string><string>--reminder-label</string><string>open</string>"
  elif [[ "$LABEL" == *close_reminder ]]; then
    EXTRA_ARGS="<string>--remind</string><string>--reminder-label</string><string>close</string>"
  fi

  cat > "$PLIST" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$LABEL</string>

  <key>WorkingDirectory</key>
  <string>$PROJ</string>

  <key>ProgramArguments</key>
  <array>
    <string>$PY</string>
    <string>$PROJ/run_scheduled_rebalance.py</string>
    <string>--config</string>
    <string>$PROJ/config/csi1000_balanced_overlay.yaml</string>
    $EXTRA_ARGS
  </array>

  <key>EnvironmentVariables</key>
  <dict>
    <key>MPLCONFIGDIR</key>
    <string>/private/tmp/quant_ex_matplotlib</string>
  </dict>

  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key>
    <integer>$HOUR</integer>
    <key>Minute</key>
    <integer>$MINUTE</integer>
  </dict>

  <key>StandardOutPath</key>
  <string>$LOG_DIR/${LABEL##*.}.out.log</string>
  <key>StandardErrorPath</key>
  <string>$LOG_DIR/${LABEL##*.}.err.log</string>

  <key>RunAtLoad</key>
  <false/>
</dict>
</plist>
EOF

  launchctl bootout "gui/$(id -u)" "$PLIST" >/dev/null 2>&1 || true
  launchctl bootstrap "gui/$(id -u)" "$PLIST"
  launchctl enable "gui/$(id -u)/$LABEL"
  echo "Installed $LABEL"
done

echo ""
echo "Schedules:"
echo "  com.quant_ex.daily_rebalance                20:00 generate/cache signal"
echo "  com.quant_ex.daily_rebalance.open_reminder  09:00 send cached reminder"
echo "  com.quant_ex.daily_rebalance.close_reminder 14:00 send cached reminder"
echo "Check status: launchctl print gui/$(id -u)/<label>"
