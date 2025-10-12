#!/usr/bin/env bash
set -euo pipefail

# Retry wrapper: run on the 1st, check whether the target CSV contains entries
# for the target month; if not, retry daily until the 7th. Useful when the
# publisher posts the monthly trials later in the first week.

# Usage: retry_until_published.sh [YYYY-MM] [max_day]
# Example: retry_until_published.sh 2025-10 7

BASE_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
CSV_PATH="$BASE_DIR/CSV/copwatchdog.csv"
WRAPPER="$BASE_DIR/copwatchdog-scheduler/scripts/run_copwatchdog.sh"
MAX_DAY=${2:-7}

if [ $# -lt 1 ]; then
  echo "Usage: $0 YYYY-MM [max_day]" >&2
  exit 2
fi

TARGET_MONTH="$1"  # format YYYY-MM

today=$(date +%Y-%m-%d)
day_of_month=$(date +%d | sed 's/^0//')

echo "[*] Retry wrapper start: target_month=$TARGET_MONTH today=$today (day $day_of_month)"

# Check function: returns 0 if CSV contains rows that appear to be for TARGET_MONTH.
check_csv_for_month() {
  if [ ! -f "$CSV_PATH" ]; then
    return 1
  fi
  # Simple heuristic: check if any date column contains TARGET_MONTH
  # Accepts dates like 2025-10-xx or 10/2025 or Oct 2025
  if grep -E "${TARGET_MONTH}" -q "$CSV_PATH"; then
    return 0
  fi
  if grep -E "${TARGET_MONTH#*-}/${TARGET_MONTH%*-}" -q "$CSV_PATH"; then
    return 0
  fi
  # Month name check
  month_name=$(date -d "${TARGET_MONTH}-01" +"%b" 2>/dev/null || date -j -f "%Y-%m-%d" "${TARGET_MONTH}-01" +"%b" 2>/dev/null || true)
  if [ -n "$month_name" ] && grep -i -q "$month_name[[:space:]]*${TARGET_MONTH%*-}" "$CSV_PATH"; then
    return 0
  fi
  return 1
}

# If already published, run once and exit
if check_csv_for_month; then
  echo "[*] Target month $TARGET_MONTH already present in CSV. Running once and exiting."
  /bin/bash "$WRAPPER"
  exit 0
fi

# If we're past MAX_DAY, still run once (best effort) then exit
if [ "$day_of_month" -gt "$MAX_DAY" ]; then
  echo "[*] Today is day $day_of_month > max_day $MAX_DAY. Doing one run and exiting."
  /bin/bash "$WRAPPER"
  exit 0
fi

# Otherwise, we're within retry window. Run, then check; if not present, exit with 1
# so cron/systemd timer can reschedule daily until success or max_day.
echo "[*] Within retry window (<= $MAX_DAY). Running wrapper now."
/bin/bash "$WRAPPER"

if check_csv_for_month; then
  echo "[*] After run: target month $TARGET_MONTH present in CSV. Success."
  exit 0
else
  echo "[!] After run: target month $TARGET_MONTH NOT found. Exiting non-zero to allow retry."
  exit 1
fi
#!/usr/bin/env bash
set -euo pipefail

# === Paths ===
BASE_DIR="/Users/vicstizzi/ART/YUMYODA/TECH/COPWATCHDOG"
SCRAPE_SCRIPT="$BASE_DIR/scripts/run_copwatchdog.sh"
MAX_RETRIES=5
RETRY_INTERVAL=86400  # 24 hours in seconds

# === Function to check for publication ===
check_publication() {
  # This function should implement the logic to check if NYPD Trials are published.
  # For demonstration, we'll assume a placeholder command that checks for the publication.
  # Replace this with the actual check logic.
  if python3 "$BASE_DIR/scrape_bot.py" --check-publication; then
    return 0  # Trials published
  else
    return 1  # Trials not published
  fi
}

# === Main logic ===
echo "[*] Checking for NYPD Trials publication..."
for ((i=1; i<=MAX_RETRIES; i++)); do
  if check_publication; then
    echo "[*] Trials published. Running the scraper..."
    bash "$SCRAPE_SCRIPT"
    exit 0
  else
    echo "[!] Trials not published. Attempt $i of $MAX_RETRIES. Retrying in 24 hours..."
    sleep "$RETRY_INTERVAL"
  fi
done

echo "[!] Failed to find NYPD Trials publication after $MAX_RETRIES attempts."
exit 1