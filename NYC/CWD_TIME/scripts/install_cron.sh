#!/usr/bin/env bash
set -euo pipefail

PROJ_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CRON_SRC="$PROJ_ROOT/copwatchdog-scheduler/cron/copwatchdog.cron"

if [ ! -f "$CRON_SRC" ]; then
  echo "[!] Cron source not found: $CRON_SRC" >&2
  exit 2
fi

echo "[*] Installing cron jobs from $CRON_SRC for user: $(whoami)"
crontab -l 2>/dev/null | sed '/copwatchdog-scheduler/d' > /tmp/current_cron.$$
cat "$CRON_SRC" >> /tmp/current_cron.$$ 
crontab /tmp/current_cron.$$ 
rm -f /tmp/current_cron.$$

echo "[*] Cron installed. To view: crontab -l"
#!/usr/bin/env bash
set -euo pipefail

# === Variables ===
SCRIPT_PATH="/path/to/copwatchdog-scheduler/scripts/run_copwatchdog.sh"
RETRY_SCRIPT_PATH="/path/to/copwatchdog-scheduler/scripts/retry_until_published.sh"

# === Install Cron Job ===
echo "[*] Setting up cron job to run on the 1st of every month..."

# Create a new cron job
(crontab -l 2>/dev/null; echo "0 0 1 * * $SCRIPT_PATH || $RETRY_SCRIPT_PATH") | crontab -

echo "[*] Cron job installed successfully."