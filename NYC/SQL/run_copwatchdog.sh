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
# Prefer DB_PASS from environment for security. If empty, script will rely on ~/.pgpass.
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
# Use psql connection test which exercises auth, not just TCP
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
    echo "[!] Could not connect to DB even after creating tunnel.";
    exit 1
  fi
fi

# === 3. Import into PostgreSQL ===
echo "[*] Importing CSV into database..."
# If DB_PASS was provided use it for this invocation, otherwise rely on ~/.pgpass
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
