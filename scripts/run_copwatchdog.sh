#!/bin/bash
# run_copwatchdog.sh
# Script to run Thoth (copwatchdog) scraper and log output

set -euo pipefail

# Set up logging
THOTH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HERMES="$(cd "$THOTH/.." && pwd)"
LOGS_DIR="$THOTH/LOGS"
mkdir -p "$LOGS_DIR"

# Set up standardized log files
THOTH_LOG="$LOGS_DIR/thoth.log"
HERMES_LOG="$LOGS_DIR/hermes.log"
ERROR_LOG="$LOGS_DIR/errors.log"

# Initialize/rotate log files if needed
for log in "$THOTH_LOG" "$HERMES_LOG" "$ERROR_LOG"; do
    if [ -f "$log" ]; then
        # Rotate if file exists and is not empty
        if [ -s "$log" ]; then
            mv "$log" "${log}.$(date +%Y%m%d-%H%M%S).bak"
        fi
    fi
    touch "$log"
done

# Logging function
log() {
    local level="$1"
    local message="$2"
    local component="${3:-THOTH}"
    local script_name="$(basename "${BASH_SOURCE[0]}")"
    local timestamp="$(date +'%Y-%m-%d %H:%M:%S')"
    local log_message="[$timestamp] [$level] [$component] [$script_name] $message"
    
    # Log to main log and hermes log too
    echo "$log_message" | tee -a "$THOTH_LOG" "$HERMES_LOG"
    
    # Log errors to error log
    if [ "$level" = "ERROR" ] || [ "$level" = "WARNING" ]; then
        echo "$log_message" >> "$ERROR_LOG"
    fi
}

# Database connection functions are now moved to HERMES scripts
# THOTH does not require database connection to run

log "INFO" "Release the hounds!"

# No database check needed for THOTH
# Database connections are handled separately by HERMES scripts

# Navigate to BRAIN directory
if ! cd "$THOTH/NYC/BRAIN"; then
    log "ERROR" "Failed to access BRAIN directory at: $THOTH/NYC/BRAIN"
    exit 2
fi

# Check environment setup
if [ -f ".env" ]; then
    log "INFO" "Found .env file in BRAIN directory"
else
    log "WARN" "No .env file found in BRAIN directory"
fi

# Run the scraper
log "INFO" "Starting copwatchdog scraper..."
if python3 main.py 2>&1 | tee -a "$THOTH_LOG"; then
    log "INFO" "Scraper completed successfully"
    SCRAPER_SUCCESS=true
else
    PY_EXIT=$?
    log "ERROR" "Scraper failed with exit code: $PY_EXIT"
    # Check if this was a catastrophic failure
    if [ $PY_EXIT -gt 1 ]; then
        log "ERROR" "Critical scraper failure detected. Halting execution."
        exit $PY_EXIT
    fi
    SCRAPER_SUCCESS=false
fi
# Run database import if scraper succeeded or we want to process existing files
if [ -x "$THOTH/scripts/import_to_db.sh" ]; then
    log "INFO" "Starting database import process..."
    if "$THOTH/scripts/import_to_db.sh" 2>&1 | tee -a "$HERMES_LOG"; then
        log "INFO" "Database import completed successfully"
        
        # Run HERMES ETL
        log "INFO" "Starting HERMES ETL transformation..."
        if [ -x "$HERMES/scripts/hermes/run_hermes_etl.sh" ]; then
            if "$HERMES/scripts/hermes/run_hermes_etl.sh" 2>&1 | tee -a "$HERMES_LOG"; then
                log "INFO" "HERMES ETL transformation completed successfully"
            else
                log "ERROR" "HERMES ETL transformation failed"
                exit 4
            fi
        else
            log "ERROR" "HERMES ETL script not found or not executable"
            exit 3
        fi
    else
        log "ERROR" "Database import process failed"
        exit 3
    fi
else
    log "ERROR" "Import script not found or not executable"
    exit 3
fi

log "INFO" "Copwatchdog run completed successfully"
exit 0
