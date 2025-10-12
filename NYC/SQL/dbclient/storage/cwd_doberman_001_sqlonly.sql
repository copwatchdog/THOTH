-- SQL-only variant of cwd_doberman_001.sql
-- This version removes psql client meta-commands and uses server-side COPY. Use only
-- if the CSV file is accessible on the database server filesystem and owned/readable
-- by the postgres server process.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
        CREATE SCHEMA bronze;
    END IF;
END $$;

-- Drop/create staging table (all text)
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

-- Replace the path below with the absolute path where the CSV is located on the DB server.
COPY bronze.doberman_fetch FROM '/path/on/dbserver/copwatchdog.csv' CSV HEADER DELIMITER ',';
