-- psql-friendly import + transform for copwatchdog CSV
-- IMPORTANT: This script uses psql client meta-commands (lines starting with a backslash)
-- (for example: \echo, \if, \set, \copy). You MUST run it with the psql program from the
-- machine that has the CSV file. Do NOT run this SQL file from a GUI or a client that
-- forwards statements directly to the server (those will produce syntax errors).
--
-- Example (from zsh) — runs the import with the CSV path that your scraper writes to:
-- psql -h psql002.mayfirst.cx -p 5432 -U your_user -d copwatchdog \
--   -v ON_ERROR_STOP=1 \
--   -v csv='/Users/vicstizzi/ART/YUMYODA/TECH/COPWATCHDOG/CSV/copwatchdog.csv' \
--   -f /Users/vicstizzi/ART/YUMYODA/TECH/COPWATCHDOG/SQL/cwd_doberman_001.sql
\echo 'Starting import script: cwd_doberman_001.sql'

-- ==============================================================
-- Block 1: Create table
-- Create a permissive staging table (all text) so messy CSVs import reliably
-- ==============================================================
CREATE SCHEMA IF NOT EXISTS bronze;

-- staging: all-text table
-- create staging table if not exists; preserve table permissions, TRUNCATE each run
CREATE TABLE IF NOT EXISTS bronze.doberman_fetch_raw (
    date text,
    time text,
    rank text,
    first text,
    last text,
    room text,
    case_type text,
    badge text,
    pct text,
    pct_url text,
    started text,
    last_earned text,
    disciplined text,
    articles text,
    complaints text,
    allegations text,
    substantiated text,
    charges text,
    unsubstantiated text,
    guidelined text,
    lawsuits text,
    total_settlements text,
    status text,
    base_salary text,
    pay_basis text,
    regular_hours text,
    regular_gross_paid text,
    ot_hours text,
    total_ot_paid text,
    total_other_pay text
);
CREATE TABLE IF NOT EXISTS bronze.doberman_fetch (
    date_report date,
    time_report time,
    rank text,
    first text,
    last text,
    room text,
    case_type text,
    badge text,
    pct text,
    pct_url text,
    started date,
    last_earned_text text,
    disciplined boolean,
    articles integer,
    complaints integer,
    allegations integer,
    substantiated integer,
    charges integer,
    unsubstantiated integer,
    guidelined integer,
    lawsuits integer,
    total_settlements numeric,
    status text,
    base_salary numeric,
    pay_basis text,
    regular_hours numeric,
    regular_gross_paid numeric,
    ot_hours numeric,
    total_ot_paid numeric,
    total_other_pay numeric
);

-- ==============================================================
-- Block 2: Create heading (psql-visible)
-- ==============================================================
\echo '=== Import: bronze.doberman_fetch ==='

-- If the caller didn't set the csv variable, default to your local path
\if :{?csv}
\else
    \set csv '/Users/vicstizzi/ART/YUMYODA/TECH/COPWATCHDOG/CSV/copwatchdog.csv'
\endif

-- ==============================================================
-- Block 3: Import CSV file
-- Use psql client-side copy so the server doesn't need access to the local filesystem
-- ==============================================================
\echo 'Importing CSV from:' :csv
-- empty staging table, then import
TRUNCATE TABLE bronze.doberman_fetch_raw;
\copy bronze.doberman_fetch_raw FROM :'csv' CSV HEADER DELIMITER ','

\echo 'Staging import complete.'
SELECT 'staging_rows=' || count(*) FROM bronze.doberman_fetch;
-- Run the server-side transform script which safely populates the final typed table
\ir cwd_doberman_transform.sql

-- Final counts
SELECT 'final_rows=' || count(*) FROM bronze.doberman_fetch;

\echo 'Import script finished.'

-- end of script
