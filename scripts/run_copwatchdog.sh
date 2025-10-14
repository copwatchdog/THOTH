#!/bin/bash
# run_copwatchdog.sh
# Script to run Thoth (copwatchdog) scraper and log output

set -euo pipefail

# Set up logging
THOTH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HERMES="$(cd "$THOTH/.." && pwd)"
LOGS_DIR="$THOTH/LOGS"
mkdir -p "$LOGS_DIR"

# Set up log files for different components
MAIN_LOG="$LOGS_DIR/thoth.log"
SCRAPER_LOG="$LOGS_DIR/scraper.log"
DB_LOG="$LOGS_DIR/database.log"
ETL_LOG="$LOGS_DIR/etl.log"
ERROR_LOG="$LOGS_DIR/error.log"

# Clear existing log files
for log in "$MAIN_LOG" "$SCRAPER_LOG" "$DB_LOG" "$ETL_LOG" "$ERROR_LOG"; do
    > "$log"  # Clear file contents while preserving file
done

# Logging function
log() {
    local level="$1"
    local message="$2"
    local timestamp="$(date +'%Y-%m-%d %H:%M:%S')"
    local log_message="[$timestamp] [$level] $message"
    
    # Log to main log and specific component log if specified
    echo "$log_message" | tee -a "$MAIN_LOG"
    
    # Log errors to error log
    if [ "$level" = "ERROR" ]; then
        echo "$log_message" >> "$ERROR_LOG"
    fi
}

# Function to check and setup database connection
check_db_connection() {
    log "INFO" "Checking database connection..."
    
    # Test connection details
    local db_test_output
    db_test_output=$(psql -h localhost -p 5433 -U copwatchdog -d copwatchdog -c "SELECT version();" 2>&1)
    local db_status=$?
    
    if [ $db_status -eq 0 ]; then
        log "INFO" "Database connection successful"
        echo "$db_test_output" >> "$DB_LOG"
        return 0
    fi
    
    log "WARN" "Database connection failed. Attempting to setup tunnel..."
    echo "Connection error details: $db_test_output" >> "$DB_LOG"
    
    # Check for tunnel script
    if [ ! -x "$HERMES/scripts/setup_db_tunnel.sh" ]; then
        log "ERROR" "setup_db_tunnel.sh not found or not executable at: $HERMES/scripts/setup_db_tunnel.sh"
        return 1
    fi
    
    # Try to setup tunnel
    log "INFO" "Running database tunnel setup..."
    if "$HERMES/scripts/setup_db_tunnel.sh" >> "$DB_LOG" 2>&1; then
        # Verify connection after tunnel setup
        if psql -h localhost -p 5433 -U copwatchdog -d copwatchdog -c "SELECT 1" >> "$DB_LOG" 2>&1; then
            log "INFO" "Database tunnel established successfully"
            return 0
        else
            log "ERROR" "Database connection failed after tunnel setup"
            return 1
        fi
    else
        log "ERROR" "Failed to setup database tunnel"
        return 1
    fi
}

log "INFO" "Release the hounds!"

# Check database connection before proceeding
if ! check_db_connection; then
    log "ERROR" "Aborting: Database connection required"
    exit 1
fi

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
if python3 main.py 2>&1 | tee -a "$SCRAPER_LOG"; then
    log "INFO" "Scraper completed successfully"
else
    PY_EXIT=$?
    log "ERROR" "Scraper failed with exit code: $PY_EXIT"
    # Continue execution - we might still want to process existing files
fi
# Run database import if scraper succeeded or we want to process existing files
if [ -x "$THOTH/scripts/import_to_db.sh" ]; then
    log "INFO" "Starting database import process..."
    if "$THOTH/scripts/import_to_db.sh" 2>&1 | tee -a "$DB_LOG"; then
        log "INFO" "Database import completed successfully"
        
        # Run HERMES ETL
        log "INFO" "Starting HERMES ETL transformation..."
        if [ -x "$HERMES/scripts/hermes/run_hermes_etl.sh" ]; then
            if "$HERMES/scripts/hermes/run_hermes_etl.sh" 2>&1 | tee -a "$ETL_LOG"; then
                log "INFO" "HERMES ETL transformation completed successfully"
            else
                log "ERROR" "HERMES ETL transformation failed"
                echo "Check $ETL_LOG for detailed error information"
                exit 4
            fi
        else
            log "ERROR" "HERMES ETL script not found or not executable"
            exit 3
        fi
    else
        log "ERROR" "Database import process failed"
        echo "Check $DB_LOG for detailed error information"
        exit 3
    fi
else
    log "ERROR" "Import script not found or not executable"
    exit 3
fi

log "INFO" "Copwatchdog run completed successfully"
exit 0
