
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
HERMES_LOG="$LOGS_DIR/hermes.log"
ERROR_LOG="$LOGS_DIR/errors.log"

# Don't clear the logs - they're managed by the main script

# Logging function
log() {
    local level="$1"
    local message="$2"
    local component="${3:-THOTH_IMPORT}"
    local script_name="$(basename "${BASH_SOURCE[0]}")"
    local timestamp="$(date +'%Y-%m-%d %H:%M:%S')"
    local log_message="[$timestamp] [$level] [$component] [$script_name] $message"
    
    # Log to import log
    echo "$log_message" | tee -a "$HERMES_LOG"
    
    # Log errors to error log
    if [ "$level" = "ERROR" ] || [ "$level" = "WARNING" ]; then
        echo "$log_message" >> "$ERROR_LOG"
    fi
}

log "INFO" "Starting THOTH import script" "THOTH_IMPORT"

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
        log "INFO" "Notification sent via mail to $NOTIFY_ADDR" "NOTIFY"
    elif command -v sendmail >/dev/null 2>&1; then
        printf "Subject: [%s] %s\n\n%s" "$level" "$subject" "$email_body" | sendmail "$NOTIFY_ADDR"
        log "INFO" "Notification sent via sendmail to $NOTIFY_ADDR" "NOTIFY"
    else
        log "WARNING" "No mail program available - notification not sent" "NOTIFY"
        return 1
    fi
}

# Initialize counters
processed=0
failed=0
total_records=0

# Set up SSH tunnel for database connection
setup_db_tunnel() {
    # First, verify if existing connection works (regardless of how it was set up)
    if conn_test=$(PGPASSWORD="${DB_PASS}" psql -h localhost -p "${SSH_LOCAL_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -c "SELECT 1" 2>&1); then
        log "INFO" "Database connection already working properly" "DB_TUNNEL"
        return 0
    fi
    
    # If we're here, connection failed or doesn't exist - kill any stale tunnels
    if pgrep -f "ssh.*${SSH_HOST}.*${SSH_LOCAL_PORT}.*${DB_HOST}.*${DB_PORT}" > /dev/null; then
        log "WARNING" "Found stale SSH tunnel - recreating" "DB_TUNNEL"
        pkill -f "ssh.*${SSH_HOST}.*${SSH_LOCAL_PORT}.*${DB_HOST}.*${DB_PORT}"
        sleep 1
    else
        log "INFO" "Setting up new database tunnel..." "DB_TUNNEL"
    fi
    
    # Check for tunnel script
    if [ ! -x "$HERMES/scripts/setup_db_tunnel.sh" ]; then
        log "ERROR" "setup_db_tunnel.sh not found or not executable at: $HERMES/scripts/setup_db_tunnel.sh" "DB_TUNNEL"
        return 1
    fi
    
    # Try to setup tunnel
    if tunnel_output=$("$HERMES/scripts/setup_db_tunnel.sh" 2>&1); then
        log "INFO" "Tunnel setup output: $tunnel_output" "DB_TUNNEL"
        
        # Wait a moment for the tunnel to stabilize
        sleep 3
        
        # Verify connection after tunnel setup
        if conn_test=$(PGPASSWORD="${DB_PASS}" psql -h localhost -p "${SSH_LOCAL_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -c "SELECT 1" 2>&1); then
            log "INFO" "Database tunnel established successfully" "DB_TUNNEL"
            return 0
        else
            log "ERROR" "Database connection failed after tunnel setup" "DB_TUNNEL"
            log "ERROR" "Connection test output: $conn_test" "DB_TUNNEL"
            return 1
        fi
    else
        log "ERROR" "Failed to setup database tunnel" "DB_TUNNEL"
        log "ERROR" "Tunnel setup error: $tunnel_output" "DB_TUNNEL"
        return 1
    fi
}

# Start import process
log "INFO" "Starting database import process" "THOTH_IMPORT"

# Setup database tunnel - ESSENTIAL for database imports
if ! load_env || ! setup_db_tunnel; then
    log "ERROR" "Failed to establish database connection - cannot proceed with import"
    exit 1
fi

# Apply schema fixes FIRST to ensure officer_id constraint is fixed
SCHEMA_SQL="$HERMES/SQL/create_cwd_schemas.sql"
if [[ -f "$SCHEMA_SQL" ]]; then
    log "INFO" "Applying schema fixes before import" "THOTH_IMPORT"
    if psql "service=copwatchdog" -f "$SCHEMA_SQL" >> "$HERMES_LOG" 2>&1; then
        log "INFO" "Schema fixes applied successfully" "THOTH_IMPORT"
    else
        log "ERROR" "Failed to apply schema fixes - import may fail" "THOTH_IMPORT"
        # Continue anyway as the schema might already be correct
    fi
else
    log "WARNING" "Schema SQL file not found at $SCHEMA_SQL" "THOTH_IMPORT"
fi

# Find CSV files (glob may expand to literal pattern if none; handle that)
shopt -s nullglob
csv_files=("$CSV_DIR"/*.csv)
shopt -u nullglob

log "INFO" "Found ${#csv_files[@]} CSV files to process" "THOTH_IMPORT"
for f in "${csv_files[@]}"; do
    log "INFO" "  - $(basename "$f")" "THOTH_IMPORT"
done

if [ ${#csv_files[@]} -eq 0 ]; then
	log "INFO" "No CSV files found in $CSV_DIR; nothing to import." "THOTH_IMPORT"
	log "INFO" "Finished import_to_db.sh (no files)" "THOTH_IMPORT"
	exit 0
fi

for csv in "${csv_files[@]}"; do
	log "INFO" "Importing $csv" "THOTH_IMPORT"

	# Use psql. Prefer DATABASE_URL if set; otherwise try service configuration, then PG* env vars.
	if [ -n "${DATABASE_URL-}" ]; then
		PSQL_CMD=(psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c)
	else
		# Always try to use the service configuration first
		log "INFO" "Using service=copwatchdog configuration" "THOTH_IMPORT"
		PSQL_CMD=(psql "service=copwatchdog" -v ON_ERROR_STOP=1 -c)
	fi

		# Always use the helper script for proper column mapping
		if [ ! -x "$HERMES/scripts/db/import_into_cwd_raw.sh" ]; then
			log "ERROR" "Required helper script not found: $HERMES/scripts/db/import_into_cwd_raw.sh" "THOTH_IMPORT"
			failed=$((failed + 1))
			send_notification "ERROR" "Thoth import FAILED" "Required helper script not found: $HERMES/scripts/db/import_into_cwd_raw.sh" "$ERROR_LOG" || true
			continue
		fi

		log "INFO" "Using helper script to import $csv" "THOTH_IMPORT"
		if "$HERMES/scripts/db/import_into_cwd_raw.sh" "$csv" 2>&1 | tee -a "$HERMES_LOG"; then
			log "INFO" "Import succeeded for $csv" "THOTH_IMPORT"
			processed=$((processed + 1))
		else
			log "ERROR" "Import FAILED for $csv" "THOTH_IMPORT"
			failed=$((failed + 1))
			send_notification "ERROR" "Thoth import FAILED" "Import failed for $csv" "$ERROR_LOG" || true
		fi
done

log "INFO" "Import run complete: processed=$processed failed=$failed" "THOTH_IMPORT"

# If any imports succeeded, trigger HERMES ETL to transform raw data to clean
if [ $processed -gt 0 ]; then
	log "INFO" "Triggering HERMES ETL transformation" "THOTH_IMPORT"
	
	if [ -x "$HERMES/scripts/hermes/run_hermes_etl.sh" ]; then
		# Always pass --apply-schema to make sure any schema changes are applied
		if "$HERMES/scripts/hermes/run_hermes_etl.sh" --apply-schema 2>&1 | tee -a "$HERMES_LOG"; then
			log "INFO" "HERMES ETL completed successfully" "THOTH_IMPORT"
		else
			log "ERROR" "HERMES ETL failed - check logs" "THOTH_IMPORT"
			send_notification "ERROR" "HERMES ETL FAILED" "HERMES transformation failed after successful import. processed=$processed failed=$failed." "$ERROR_LOG" || true
		fi
	else
		log "ERROR" "HERMES ETL script not found at $HERMES/scripts/hermes/run_hermes_etl.sh" "THOTH_IMPORT"
	fi
fi

exit 0

