DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
        CREATE SCHEMA bronze;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'bronze' AND tablename = 'doberman_fetch') THEN
        EXECUTE 'DROP TABLE bronze.doberman_fetch';
    END IF;
END $$;

\echo 'Starting import script: cwd_doberman_001.sql'

-- Ensure schema exists
CREATE SCHEMA IF NOT EXISTS bronze;

-- Create a permissive staging table (all text) so messy CSVs import reliably
DROP TABLE IF EXISTS bronze.doberman_fetch;
CREATE TABLE bronze.doberman_fetch (
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

-- if the caller didn't set the csv variable, default to your local path
\if :csv
\else
    \set csv '/Users/vicstizzi/ART/YUMYODA/TECH/COPWATCHDOG/CSV/copwatchdog.csv'
\endif

\echo 'Staging table created. Importing CSV from:' :csv

-- Use psql client-side copy so the server doesn't need access to the local filesystem
\copy bronze.doberman_fetch FROM :'csv' CSV HEADER DELIMITER ','

\echo 'Staging import complete.'
SELECT 'staging_rows=' || count(*) FROM bronze.doberman_fetch;