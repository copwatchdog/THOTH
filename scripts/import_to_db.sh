
#!/bin/bash
# import_to_db.sh
# Import CSVs from NYC/CSV/FILES into the database table cwd_raw.officers_raw
# Uses credentials from a .env file (top-level or NYC/BRAIN/.env). Sends
# simple email notifications on success/failure to the configured recipient.

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_FILE="$REPO_DIR/copwatchdog.log"
CSV_DIR="$REPO_DIR/NYC/CSV/FILES"
ARCHIVE_DIR="$REPO_DIR/NYC/CSV/ARCHIVE"
NOTIFY_ADDR="stizzi@yumyoda.com"

echo "[$(date +'%Y-%m-%d %H:%M:%S')] Starting import_to_db.sh" | tee -a "$LOG_FILE"

# Load environment variables from .env (top-level first, then NYC/BRAIN)
if [ -f "$REPO_DIR/.env" ]; then
	# shellcheck disable=SC1090
	set -a; source "$REPO_DIR/.env"; set +a
	echo "Sourced $REPO_DIR/.env" | tee -a "$LOG_FILE"
elif [ -f "$REPO_DIR/NYC/BRAIN/.env" ]; then
	set -a; source "$REPO_DIR/NYC/BRAIN/.env"; set +a
	echo "Sourced $REPO_DIR/NYC/BRAIN/.env" | tee -a "$LOG_FILE"
else
	echo "No .env file found; cannot obtain DB credentials. Aborting." | tee -a "$LOG_FILE"
	exit 2
fi

# Helper: send email notification if a mail program exists
send_mail() {
	local subject="$1"
	local body="$2"
	if command -v mail >/dev/null 2>&1; then
		echo "$body" | mail -s "$subject" "$NOTIFY_ADDR" || true
	elif command -v sendmail >/dev/null 2>&1; then
		printf "Subject: %s\n\n%s" "$subject" "$body" | sendmail "$NOTIFY_ADDR" || true
	else
		echo "No mail/sendmail available; cannot send notification" | tee -a "$LOG_FILE"
	fi
}

# Ensure ARCHIVE_DIR exists
mkdir -p "$ARCHIVE_DIR"

processed=0
failed=0

# Find CSV files (glob may expand to literal pattern if none; handle that)
shopt -s nullglob
csv_files=("$CSV_DIR"/*.csv)
shopt -u nullglob

if [ ${#csv_files[@]} -eq 0 ]; then
	echo "No CSV files found in $CSV_DIR; nothing to import." | tee -a "$LOG_FILE"
	echo "[$(date +'%Y-%m-%d %H:%M:%S')] Finished import_to_db.sh (no files)" | tee -a "$LOG_FILE"
	exit 0
fi

for csv in "${csv_files[@]}"; do
	echo "[$(date +'%Y-%m-%d %H:%M:%S')] Importing $csv" | tee -a "$LOG_FILE"

	# Use psql. Prefer DATABASE_URL if set; otherwise rely on PG* env vars.
	if [ -n "${DATABASE_URL-}" ]; then
		PSQL_CMD=(psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c)
	else
		PSQL_CMD=(psql -v ON_ERROR_STOP=1 -c)
	fi

		# If a Hermes/db helper exists, prefer that (it knows the project's conventions)
		if [ -x "$REPO_DIR/scripts/db/import_into_cwd_raw.sh" ]; then
			echo "Found helper scripts/db/import_into_cwd_raw.sh; using it to import $csv" | tee -a "$LOG_FILE"
			if "$REPO_DIR/scripts/db/import_into_cwd_raw.sh" "$csv" >>"$LOG_FILE" 2>&1; then
				echo "[$(date +'%Y-%m-%d %H:%M:%S')] Import succeeded for $csv (via helper)" | tee -a "$LOG_FILE"
				processed=$((processed + 1))
			else
				echo "[$(date +'%Y-%m-%d %H:%M:%S')] Import FAILED for $csv (via helper)" | tee -a "$LOG_FILE"
				failed=$((failed + 1))
				tail -n 200 "$LOG_FILE" | sed -n '1,200p' > "$REPO_DIR/last_import_failure.log" || true
				send_mail "Thoth import FAILED" "Import failed for $csv using helper. See last_import_failure.log on the server.\n\nRecent log:\n$(tail -n 50 "$LOG_FILE")"
			fi
		else
			if "${PSQL_CMD[@]}" "\copy cwd_raw.officers_raw FROM '$csv' WITH (FORMAT csv, HEADER true)" >>"$LOG_FILE" 2>&1; then
				echo "[$(date +'%Y-%m-%d %H:%M:%S')] Import succeeded for $csv (direct)" | tee -a "$LOG_FILE"
				processed=$((processed + 1))
			else
				echo "[$(date +'%Y-%m-%d %H:%M:%S')] Import FAILED for $csv (direct)" | tee -a "$LOG_FILE"
				failed=$((failed + 1))
				tail -n 200 "$LOG_FILE" | sed -n '1,200p' > "$REPO_DIR/last_import_failure.log" || true
				send_mail "Thoth import FAILED" "Import failed for $csv using direct psql. See last_import_failure.log on the server.\n\nRecent log:\n$(tail -n 50 "$LOG_FILE")"
			fi
		fi
done

echo "[$(date +'%Y-%m-%d %H:%M:%S')] Import run complete: processed=$processed failed=$failed" | tee -a "$LOG_FILE"

if [ $failed -eq 0 ]; then
	send_mail "Thoth import SUCCEEDED" "Import completed successfully. Files processed: $processed.\n\nRecent log:\n$(tail -n 50 "$LOG_FILE")"
	exit 0
else
	send_mail "Thoth import PARTIAL FAILURE" "Import completed with failures. processed=$processed failed=$failed.\n\nRecent log:\n$(tail -n 50 "$LOG_FILE")"
	exit 3
fi

