#!/usr/bin/env bash
set -euo pipefail

# Wrapper that runs the project's existing run_copwatchdog.sh and logs output.
# Resolves BASE_DIR relative to this script so cron can call the wrapper from anywhere.

BASE_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
LOG_DIR="$BASE_DIR/logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG="$LOG_DIR/run_copwatchdog_$TIMESTAMP.log"

echo "[*] Running copwatchdog (wrapper). Log: $LOG"

if [ -x "$BASE_DIR/SQL/run_copwatchdog.sh" ]; then
  /bin/bash "$BASE_DIR/SQL/run_copwatchdog.sh" > "$LOG" 2>&1 || {
    echo "[!] run_copwatchdog.sh failed; see $LOG" >&2
    exit 1
  }
else
  echo "[!] Could not find $BASE_DIR/SQL/run_copwatchdog.sh" | tee "$LOG" >&2
  exit 2
fi

echo "[*] Completed copwatchdog run. Log: $LOG"
#!/usr/bin/env bash
set -euo pipefail

# === Paths ===
BASE_DIR="/Users/vicstizzi/ART/YUMYODA/TECH/COPWATCHDOG"
CSV_PATH="$BASE_DIR/CSV/copwatchdog.csv"
SQL_PATH="$BASE_DIR/SQL/cwd_doberman_001.sql"

# === DB Info ===
DB_HOST="localhost"
DB_PORT="5432"
DB_USER="copwatchdog"
DB_NAME="copwatchdog"
DB_PASS="${DB_PASS:-}"

# === 1. Run scrape ===
echo "[*] Running scrape bot..."
mkdir -p "$BASE_DIR/logs"
SCRAPE_LOG="$BASE_DIR/logs/scrape_$(date +%Y%m%d_%H%M%S).log"
if ! python3 "$BASE_DIR/scrape_bot.py" > "$SCRAPE_LOG" 2>&1; then
  echo "[!] Scraper failed. Log: $SCRAPE_LOG"
  tail -n +1 "$SCRAPE_LOG" | sed -n '1,200p'
  exit 1
else
  echo "[*] Scraper succeeded. Log: $SCRAPE_LOG"
fi

# === 2. Ensure tunnel ===
echo "[*] Checking DB connectivity..."
if ! PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c '\q' > /dev/null 2>&1; then
  echo "[*] DB unreachable locally; attempting to establish SSH tunnel..."
  if command -v autossh > /dev/null 2>&1; then
    echo "[*] Using autossh to create a persistent tunnel..."
    autossh -f -M 0 -N -L ${DB_PORT}:psql002.mayfirst.cx:5432 ${DB_USER}@shell.mayfirst.org
  else
    echo "[*] autossh not found; using ssh (one-shot tunnel)..."
    ssh -f -N -L ${DB_PORT}:psql002.mayfirst.cx:5432 ${DB_USER}@shell.mayfirst.org
  fi
  sleep 1
  if ! PGPASSWORD="$DB_PASS" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c '\q' > /dev/null 2>&1; then
    echo "[!] Could not connect to DB even after creating tunnel."
    exit 1
  fi
fi

# === 3. Import into PostgreSQL ===
echo "[*] Importing CSV into database..."
if [ -n "$DB_PASS" ]; then
  PGPASSWORD="$DB_PASS" psql \
    -v ON_ERROR_STOP=1 \
    -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
    -v csv="$CSV_PATH" -f "$SQL_PATH"
else
  psql -v ON_ERROR_STOP=1 -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
    -v csv="$CSV_PATH" -f "$SQL_PATH"
fi

echo "[*] Done."