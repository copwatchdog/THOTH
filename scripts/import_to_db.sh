
#!/bin/bash
# import_to_db.sh
# Import CSVs from NYC/CSV into the database table cwd_raw.officers_raw
# Uses credentials from a .env file and sends notifications on success/failure

set -euo pipefail

# Set up paths
THOTH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HERMES="$(cd "$THOTH/.." && pwd)"
LOGS_DIR="$THOTH/LOGS"
CSV_DIR="$THOTH/NYC/CSV"
NOTIFY_ADDR="stizzi@yumyoda.com"

# Set up log files
mkdir -p "$LOGS_DIR"
IMPORT_LOG="$LOGS_DIR/import.log"
SQL_LOG="$LOGS_DIR/sql.log"
ERROR_LOG="$LOGS_DIR/error.log"

# Clear existing log files
for log in "$IMPORT_LOG" "$SQL_LOG" "$ERROR_LOG"; do
    > "$log"  # Clear file contents while preserving file
done

# Logging function
log() {
    local level="$1"
    local message="$2"
    local type="${3:-}"  # Optional third parameter with empty default
    local timestamp="$(date +'%Y-%m-%d %H:%M:%S')"
    local log_message="[$timestamp] [$level] $message"
    
    # Log to import log
    echo "$log_message" | tee -a "$IMPORT_LOG"
    
    # Log errors to error log
    if [ "$level" = "ERROR" ]; then
        echo "$log_message" >> "$ERROR_LOG"
    fi
    
    # Log SQL-related messages to SQL log
    if [ "$type" = "SQL" ]; then
        echo "$log_message" >> "$SQL_LOG"
    fi
}

log "INFO" "Starting import_to_db.sh"

# Load environment variables from .env files
load_env() {
    if [ -f "$THOTH/.env" ]; then
        set -a; source "$THOTH/.env"; set +a
        log "INFO" "Loaded environment from $THOTH/.env"
        return 0
    elif [ -f "$THOTH/NYC/BRAIN/.env" ]; then
        set -a; source "$THOTH/NYC/BRAIN/.env"; set +a
        log "INFO" "Loaded environment from $THOTH/NYC/BRAIN/.env"
        return 0
    else
        log "ERROR" "No .env file found for database credentials"
        return 1
    fi
}

# Enhanced notification system
send_notification() {
    local level="$1"
    local subject="$2"
    local body="$3"
    local log_file="$4"
    
    # Prepare email body with log excerpt
    local email_body="$body\n\nRecent log entries:\n"
    if [ -f "$log_file" ]; then
        email_body+="$(tail -n 20 "$log_file")"
    fi
    
    # Try available mail programs
    if command -v mail >/dev/null 2>&1; then
        echo -e "$email_body" | mail -s "[$level] $subject" "$NOTIFY_ADDR"
        log "INFO" "Notification sent via mail to $NOTIFY_ADDR"
    elif command -v sendmail >/dev/null 2>&1; then
        printf "Subject: [%s] %s\n\n%s" "$level" "$subject" "$email_body" | sendmail "$NOTIFY_ADDR"
        log "INFO" "Notification sent via sendmail to $NOTIFY_ADDR"
    else
        log "WARN" "No mail program available - notification not sent"
        return 1
    fi
}

# Initialize counters
processed=0
failed=0
total_records=0

# Start import process
log "INFO" "Starting database import process"

# Find CSV files (glob may expand to literal pattern if none; handle that)
shopt -s nullglob
csv_files=("$CSV_DIR"/*.csv)
shopt -u nullglob

log "INFO" "Found ${#csv_files[@]} CSV files to process:" "SQL"
for f in "${csv_files[@]}"; do
    log "INFO" "  - $(basename "$f")" "SQL"
done

if [ ${#csv_files[@]} -eq 0 ]; then
	log "INFO" "No CSV files found in $CSV_DIR; nothing to import."
	log "INFO" "Finished import_to_db.sh (no files)"
	exit 0
fi

for csv in "${csv_files[@]}"; do
	log "INFO" "Importing $csv" "SQL"

	# Use psql. Prefer DATABASE_URL if set; otherwise try service configuration, then PG* env vars.
	if [ -n "${DATABASE_URL-}" ]; then
		PSQL_CMD=(psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c)
	else
		# Always try to use the service configuration first
		log "INFO" "Using service=copwatchdog configuration" "SQL"
		PSQL_CMD=(psql "service=copwatchdog" -v ON_ERROR_STOP=1 -c)
	fi

		# Always use the helper script for proper column mapping
		if [ ! -x "$HERMES/scripts/db/import_into_cwd_raw.sh" ]; then
			log "ERROR" "Required helper script not found: $HERMES/scripts/db/import_into_cwd_raw.sh" "SQL"
			failed=$((failed + 1))
			send_mail "Thoth import FAILED" "Required helper script not found: $HERMES/scripts/db/import_into_cwd_raw.sh"
			continue
		fi

		log "INFO" "Using helper script to import $csv" "SQL"
		if "$HERMES/scripts/db/import_into_cwd_raw.sh" "$csv" 2>&1 | tee -a "$IMPORT_LOG"; then
			log "INFO" "Import succeeded for $csv" "SQL"
			processed=$((processed + 1))
		else
			log "ERROR" "Import FAILED for $csv" "SQL"
			failed=$((failed + 1))
			error_log="$LOGS_DIR/import_failure.log"
			> "$error_log"  # Clear the file first
			tail -n 200 "$IMPORT_LOG" | sed -n '1,200p' > "$error_log" || true
			send_mail "Thoth import FAILED" "Import failed for $csv. See import.log and error.log for details."
		fi
done

log "INFO" "Import run complete: processed=$processed failed=$failed" "SQL"

# If any imports succeeded, trigger HERMES ETL to transform raw data to clean
if [ $processed -gt 0 ]; then
	log "INFO" "Triggering HERMES ETL transformation"
	
	if [ -x "$HERMES/scripts/hermes/run_hermes_etl.sh" ]; then
		if "$HERMES/scripts/hermes/run_hermes_etl.sh" >> "$IMPORT_LOG" 2>&1; then
			log "INFO" "HERMES ETL completed successfully"
		else
			log "ERROR" "HERMES ETL failed - check logs"
			send_mail "HERMES ETL FAILED" "HERMES transformation failed after successful import. processed=$processed failed=$failed.\n\nRecent log:\n$(tail -n 50 "$LOG_FILE")"
		fi
	else
		echo "[$(date +'%Y-%m-%d %H:%M:%S')] HERMES ETL script not found at $HERMES/scripts/hermes/run_hermes_etl.sh" | tee -a "$LOG_FILE"
	fi
fi

if [ $failed -eq 0 ]; then
	send_mail "Thoth import SUCCEEDED" "Import completed successfully. Files processed: $processed.\n\nRecent log:\n$(tail -n 50 "$LOG_FILE")"
	exit 0
else
	send_mail "Thoth import PARTIAL FAILURE" "Import completed with failures. processed=$processed failed=$failed.\n\nRecent log:\n$(tail -n 50 "$LOG_FILE")"
	exit 3
fi

