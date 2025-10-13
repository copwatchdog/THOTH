#!/bin/bash
# run_copwatchdog.sh
# Script to run Thoth (copwatchdog) scraper and log output

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

LOG_FILE="$REPO_DIR/copwatchdog.log"

echo "[$(date +'%Y-%m-%d %H:%M:%S')] Starting copwatchdog run" | tee -a "$LOG_FILE"

cd "$REPO_DIR/NYC/BRAIN" || { echo "Could not cd to $REPO_DIR/NYC/BRAIN" | tee -a "$LOG_FILE"; exit 2; }

if [ -f "$REPO_DIR/.env" ]; then
  echo "Found project .env file; ensure dependencies are available." | tee -a "$LOG_FILE"
fi

python3 main.py >> "$LOG_FILE" 2>&1
PY_EXIT=$?
if [ $PY_EXIT -ne 0 ]; then
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] Scraper exited with status $PY_EXIT" | tee -a "$LOG_FILE"
else
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] Scraper completed successfully (exit 0)" | tee -a "$LOG_FILE"
fi

echo "[$(date +'%Y-%m-%d %H:%M:%S')] Finished copwatchdog run" | tee -a "$LOG_FILE"

if [ -x "$REPO_DIR/scripts/import_to_db.sh" ]; then
  echo "Triggering import_to_db.sh" | tee -a "$LOG_FILE"
  "$REPO_DIR/scripts/import_to_db.sh" >> "$LOG_FILE" 2>&1 || echo "Import script failed" | tee -a "$LOG_FILE"
fi

exit 0
