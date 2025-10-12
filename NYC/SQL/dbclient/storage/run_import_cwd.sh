#!/bin/bash
# Wrapper to reliably run the psql import script from the machine that has the CSV.
# Usage: ./run_import_cwd.sh [path-to-csv] [psql_host] [psql_port] [psql_user] [psql_db]
# Example:
# ./run_import_cwd.sh /Users/vicstizzi/ART/YUMYODA/TECH/COPWATCHDOG/CSV/copwatchdog.csv psql002.mayfirst.cx 5432 myuser copwatchdog

set -euo pipefail

CSV_PATH=${1:-/Users/vicstizzi/ART/YUMYODA/TECH/COPWATCHDOG/CSV/copwatchdog.csv}
PSQL_HOST=${2:-psql002.mayfirst.cx}
PSQL_PORT=${3:-5432}
PSQL_USER=${4:-$USER}
PSQL_DB=${5:-copwatchdog}
SQL_FILE="/Users/vicstizzi/.dbclient/storage/1759113315508@@psql002.mayfirst.cx@5432@copwatchdog/cwd_doberman_001.sql"

if [ ! -f "$CSV_PATH" ]; then
  echo "ERROR: CSV file not found at: $CSV_PATH"
  exit 2
fi

echo "Running import: CSV=$CSV_PATH -> ${PSQL_HOST}:${PSQL_PORT}/${PSQL_DB}"

psql -h "$PSQL_HOST" -p "$PSQL_PORT" -U "$PSQL_USER" -d "$PSQL_DB" \
  -v ON_ERROR_STOP=1 \
  -v csv="$CSV_PATH" \
  -f "$SQL_FILE"

# After import, display the staging table contents (first 200 rows) in the terminal.
echo "\nShowing up to 200 rows from bronze.doberman_fetch:\n"
psql -h "$PSQL_HOST" -p "$PSQL_PORT" -U "$PSQL_USER" -d "$PSQL_DB" -x -P pager=off \
  -c "SELECT * FROM bronze.doberman_fetch LIMIT 200;"
