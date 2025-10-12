/* 2025-09-28 23:31:42 [87 ms] */ 
CREATE SCHEMA bronze;
/* 2025-09-28 23:31:42 [87 ms] */ 
CREATE SCHEMA bronze;
/* 2025-09-28 23:32:00 [110 ms] */ 
CREATE TABLE bronze.doberman_fetch (
    Date DATE,
    Time TIME,
    Rank INTEGER,
    First VARCHAR(100),
    Last VARCHAR(100),
    Room VARCHAR(100),
    Case_Type VARCHAR(100),
    Badge VARCHAR(100),
    PCT VARCHAR(100),
    PCT_URL VARCHAR(255),
    Started DATE,
    Last_Earned DATE,
    Disciplined BOOLEAN,
    Articles INTEGER,
    Complaints INTEGER,
    Allegations INTEGER,
    Substantiated INTEGER,
    Charges INTEGER,
    Unsubstantiated INTEGER,
    Guidelined INTEGER,
    Lawsuits INTEGER,
    Total_Settlements INTEGER,
    Status VARCHAR(100),
    Base_Salary NUMERIC(10, 2),
    Pay_Basis VARCHAR(100),
    Regular_Hours NUMERIC(10, 2),
    Regular_Gross_Paid NUMERIC(10, 2),
    OT_Hours NUMERIC(10, 2),
    Total_OT_Paid NUMERIC(10, 2),
    Total_Other_Pay NUMERIC(10, 2)
);
/* 2025-09-28 23:32:00 [110 ms] */ 
CREATE TABLE bronze.doberman_fetch (
    Date DATE,
    Time TIME,
    Rank INTEGER,
    First VARCHAR(100),
    Last VARCHAR(100),
    Room VARCHAR(100),
    Case_Type VARCHAR(100),
    Badge VARCHAR(100),
    PCT VARCHAR(100),
    PCT_URL VARCHAR(255),
    Started DATE,
    Last_Earned DATE,
    Disciplined BOOLEAN,
    Articles INTEGER,
    Complaints INTEGER,
    Allegations INTEGER,
    Substantiated INTEGER,
    Charges INTEGER,
    Unsubstantiated INTEGER,
    Guidelined INTEGER,
    Lawsuits INTEGER,
    Total_Settlements INTEGER,
    Status VARCHAR(100),
    Base_Salary NUMERIC(10, 2),
    Pay_Basis VARCHAR(100),
    Regular_Hours NUMERIC(10, 2),
    Regular_Gross_Paid NUMERIC(10, 2),
    OT_Hours NUMERIC(10, 2),
    Total_OT_Paid NUMERIC(10, 2),
    Total_Other_Pay NUMERIC(10, 2)
);
/* 2025-09-29 07:07:46 [95 ms] */ 
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'bronze' AND tablename = 'doberman_fetch') THEN
        EXECUTE 'DROP TABLE bronze.doberman_fetch';
    END IF;
END $$;
/* 2025-09-29 07:07:46 [95 ms] */ 
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'bronze' AND tablename = 'doberman_fetch') THEN
        EXECUTE 'DROP TABLE bronze.doberman_fetch';
    END IF;
END $$;
/* 2025-09-29 12:39:41 [82 ms] */ 
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
        CREATE SCHEMA bronze;
    END IF;
END $$;
/* 2025-09-29 12:39:41 [82 ms] */ 
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
        CREATE SCHEMA bronze;
    END IF;
END $$;
/* 2025-09-29 12:39:44 [85 ms] */ 
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'bronze' AND tablename = 'doberman_fetch') THEN
        EXECUTE 'DROP TABLE bronze.doberman_fetch';
    END IF;
END $$;
/* 2025-09-29 12:39:44 [85 ms] */ 
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'bronze' AND tablename = 'doberman_fetch') THEN
        EXECUTE 'DROP TABLE bronze.doberman_fetch';
    END IF;
END $$;
/* 2025-09-29 12:39:46 [109 ms] */ 
CREATE TABLE bronze.doberman_fetch (
    Date DATE,
    Time TIME,
    Rank INTEGER,
    First VARCHAR(100),
    Last VARCHAR(100),
    Room VARCHAR(100),
    Case_Type VARCHAR(100),
    Badge VARCHAR(100),
    PCT VARCHAR(100),
    PCT_URL VARCHAR(255),
    Started DATE,
    Last_Earned DATE,
    Disciplined BOOLEAN,
    Articles INTEGER,
    Complaints INTEGER,
    Allegations INTEGER,
    Substantiated INTEGER,
    Charges INTEGER,
    Unsubstantiated INTEGER,
    Guidelined INTEGER,
    Lawsuits INTEGER,
    Total_Settlements INTEGER,
    Status VARCHAR(100),
    Base_Salary NUMERIC(10, 2),
    Pay_Basis VARCHAR(100),
    Regular_Hours NUMERIC(10, 2),
    Regular_Gross_Paid NUMERIC(10, 2),
    OT_Hours NUMERIC(10, 2),
    Total_OT_Paid NUMERIC(10, 2),
    Total_Other_Pay NUMERIC(10, 2)
);
/* 2025-09-29 12:39:46 [109 ms] */ 
CREATE TABLE bronze.doberman_fetch (
    Date DATE,
    Time TIME,
    Rank INTEGER,
    First VARCHAR(100),
    Last VARCHAR(100),
    Room VARCHAR(100),
    Case_Type VARCHAR(100),
    Badge VARCHAR(100),
    PCT VARCHAR(100),
    PCT_URL VARCHAR(255),
    Started DATE,
    Last_Earned DATE,
    Disciplined BOOLEAN,
    Articles INTEGER,
    Complaints INTEGER,
    Allegations INTEGER,
    Substantiated INTEGER,
    Charges INTEGER,
    Unsubstantiated INTEGER,
    Guidelined INTEGER,
    Lawsuits INTEGER,
    Total_Settlements INTEGER,
    Status VARCHAR(100),
    Base_Salary NUMERIC(10, 2),
    Pay_Basis VARCHAR(100),
    Regular_Hours NUMERIC(10, 2),
    Regular_Gross_Paid NUMERIC(10, 2),
    OT_Hours NUMERIC(10, 2),
    Total_OT_Paid NUMERIC(10, 2),
    Total_Other_Pay NUMERIC(10, 2)
);
/* 2025-09-29 13:12:35 [92 ms] */ 
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'bronze' AND tablename = 'doberman_fetch') THEN
        EXECUTE 'DROP TABLE bronze.doberman_fetch';
    END IF;
END $$;
/* 2025-09-29 13:12:35 [92 ms] */ 
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'bronze' AND tablename = 'doberman_fetch') THEN
        EXECUTE 'DROP TABLE bronze.doberman_fetch';
    END IF;
END $$;
/* 2025-09-29 13:12:38 [86 ms] */ 
CREATE TABLE bronze.doberman_fetch (
    Date VARCHAR(50),
    Time VARCHAR(50),
    Rank VARCHAR(200),
    First VARCHAR(200),
    Last VARCHAR(200),
    Room VARCHAR(100),
    Case_Type VARCHAR(300),
    Badge VARCHAR(100),
    PCT VARCHAR(100),
    PCT_URL VARCHAR(500),
    Started VARCHAR(50),
    Last_Earned VARCHAR(200),
    Disciplined VARCHAR(10),
    Articles VARCHAR(50),
    Complaints VARCHAR(50),
    Allegations VARCHAR(50),
    Substantiated VARCHAR(50),
    Charges VARCHAR(50),
    Unsubstantiated VARCHAR(50),
    Guidelined VARCHAR(50),
    Lawsuits VARCHAR(50),
    Total_Settlements VARCHAR(50),
    Status VARCHAR(100),
    Base_Salary VARCHAR(100),
    Pay_Basis VARCHAR(100),
    Regular_Hours VARCHAR(100),
    Regular_Gross_Paid VARCHAR(100),
    OT_Hours VARCHAR(100),
    Total_OT_Paid VARCHAR(100),
    Total_Other_Pay VARCHAR(100)
);
/* 2025-09-29 13:12:38 [86 ms] */ 
CREATE TABLE bronze.doberman_fetch (
    Date VARCHAR(50),
    Time VARCHAR(50),
    Rank VARCHAR(200),
    First VARCHAR(200),
    Last VARCHAR(200),
    Room VARCHAR(100),
    Case_Type VARCHAR(300),
    Badge VARCHAR(100),
    PCT VARCHAR(100),
    PCT_URL VARCHAR(500),
    Started VARCHAR(50),
    Last_Earned VARCHAR(200),
    Disciplined VARCHAR(10),
    Articles VARCHAR(50),
    Complaints VARCHAR(50),
    Allegations VARCHAR(50),
    Substantiated VARCHAR(50),
    Charges VARCHAR(50),
    Unsubstantiated VARCHAR(50),
    Guidelined VARCHAR(50),
    Lawsuits VARCHAR(50),
    Total_Settlements VARCHAR(50),
    Status VARCHAR(100),
    Base_Salary VARCHAR(100),
    Pay_Basis VARCHAR(100),
    Regular_Hours VARCHAR(100),
    Regular_Gross_Paid VARCHAR(100),
    OT_Hours VARCHAR(100),
    Total_OT_Paid VARCHAR(100),
    Total_Other_Pay VARCHAR(100)
);
/* 2025-09-29 13:15:34 [92 ms] */ 
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
        CREATE SCHEMA bronze;
    END IF;
END $$;
/* 2025-09-29 13:15:34 [92 ms] */ 
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
        CREATE SCHEMA bronze;
    END IF;
END $$;
/* 2025-09-29 13:15:36 [77 ms] */ 
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'bronze' AND tablename = 'doberman_fetch') THEN
        EXECUTE 'DROP TABLE bronze.doberman_fetch';
    END IF;
END $$;
/* 2025-09-29 13:15:36 [77 ms] */ 
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'bronze' AND tablename = 'doberman_fetch') THEN
        EXECUTE 'DROP TABLE bronze.doberman_fetch';
    END IF;
END $$;
/* 2025-09-29 13:16:35 [100 ms] */ 
CREATE SCHEMA IF NOT EXISTS bronze;
/* 2025-09-29 13:16:35 [100 ms] */ 
CREATE SCHEMA IF NOT EXISTS bronze;
/* 2025-09-29 13:16:46 [79 ms] */ 
DROP TABLE IF EXISTS bronze.doberman_fetch;
/* 2025-09-29 13:16:46 [79 ms] */ 
DROP TABLE IF EXISTS bronze.doberman_fetch;
/* 2025-09-29 13:17:07 [96 ms] */ 
CREATE TABLE IF NOT EXISTS bronze.doberman_fetch_typed (
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
/* 2025-09-29 13:17:07 [96 ms] */ 
CREATE TABLE IF NOT EXISTS bronze.doberman_fetch_typed (
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
/* 2025-09-29 13:17:15 [199 ms] */ 
TRUNCATE TABLE bronze.doberman_fetch_typed;
/* 2025-09-29 13:17:15 [199 ms] */ 
TRUNCATE TABLE bronze.doberman_fetch_typed;
/* 2025-09-29 13:40:55 [541 ms] */ 
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
        CREATE SCHEMA bronze;
    END IF;
END $$;
/* 2025-09-29 13:40:55 [541 ms] */ 
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
        CREATE SCHEMA bronze;
    END IF;
END $$;
/* 2025-09-29 13:40:57 [94 ms] */ 
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'bronze' AND tablename = 'doberman_fetch') THEN
        EXECUTE 'DROP TABLE bronze.doberman_fetch';
    END IF;
END $$;
/* 2025-09-29 13:40:57 [94 ms] */ 
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'bronze' AND tablename = 'doberman_fetch') THEN
        EXECUTE 'DROP TABLE bronze.doberman_fetch';
    END IF;
END $$;
/* 2025-09-29 13:41:19 [122 ms] */ 
CREATE SCHEMA IF NOT EXISTS bronze;
/* 2025-09-29 13:41:19 [122 ms] */ 
CREATE SCHEMA IF NOT EXISTS bronze;
/* 2025-09-29 13:41:30 [65 ms] */ 
DROP TABLE IF EXISTS bronze.doberman_fetch;
/* 2025-09-29 13:41:30 [65 ms] */ 
DROP TABLE IF EXISTS bronze.doberman_fetch;
/* 2025-09-29 13:50:34 [90 ms] */ 
CREATE TABLE IF NOT EXISTS bronze.doberman_fetch (
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
/* 2025-09-29 13:50:34 [90 ms] */ 
CREATE TABLE IF NOT EXISTS bronze.doberman_fetch (
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
/* 2025-09-29 14:02:49 [541 ms] */ 
SELECT * FROM bronze.doberman_fetch LIMIT 100;
/* 2025-09-29 14:02:49 [541 ms] */ 
SELECT * FROM bronze.doberman_fetch LIMIT 100;
/* 2025-09-29 14:03:28 [93 ms] */ 
CREATE TABLE IF NOT EXISTS bronze.doberman_fetch (
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
/* 2025-09-29 14:03:28 [93 ms] */ 
CREATE TABLE IF NOT EXISTS bronze.doberman_fetch (
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
/* 2025-09-29 14:03:35 [75 ms] */ 
SELECT * FROM bronze.doberman_fetch LIMIT 100;
/* 2025-09-29 14:03:35 [75 ms] */ 
SELECT * FROM bronze.doberman_fetch LIMIT 100;
/* 2025-09-29 14:04:04 [84 ms] */ 
SELECT * FROM bronze.doberman_fetch LIMIT 16;
/* 2025-09-29 14:04:04 [84 ms] */ 
SELECT * FROM bronze.doberman_fetch LIMIT 16;
/* 2025-09-29 14:05:36 [118 ms] */ 
CREATE SCHEMA IF NOT EXISTS bronze;
/* 2025-09-29 14:05:36 [118 ms] */ 
CREATE SCHEMA IF NOT EXISTS bronze;
/* 2025-09-29 14:05:39 [76 ms] */ 
CREATE TABLE IF NOT EXISTS bronze.doberman_fetch (
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
/* 2025-09-29 14:05:39 [76 ms] */ 
CREATE TABLE IF NOT EXISTS bronze.doberman_fetch (
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
/* 2025-09-29 14:05:50 [76 ms] */ 
SELECT * FROM bronze.doberman_fetch LIMIT 16;
/* 2025-09-29 14:05:50 [76 ms] */ 
SELECT * FROM bronze.doberman_fetch LIMIT 16;
/* 2025-09-29 14:06:10 [91 ms] */ 
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
        CREATE SCHEMA bronze;
    END IF;
END $$;
/* 2025-09-29 14:06:10 [91 ms] */ 
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
        CREATE SCHEMA bronze;
    END IF;
END $$;
/* 2025-09-29 14:06:13 [80 ms] */ 
DO $$
BEGIN
    -- Previously we dropped the staging table here; dropping can cause race conditions
    -- and surprises. Keep the table and create it idempotently below instead.
    -- IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'bronze' AND tablename = 'doberman_fetch') THEN
    --     EXECUTE 'DROP TABLE bronze.doberman_fetch';
    -- END IF;
END $$;
/* 2025-09-29 14:06:13 [80 ms] */ 
DO $$
BEGIN
    -- Previously we dropped the staging table here; dropping can cause race conditions
    -- and surprises. Keep the table and create it idempotently below instead.
    -- IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'bronze' AND tablename = 'doberman_fetch') THEN
    --     EXECUTE 'DROP TABLE bronze.doberman_fetch';
    -- END IF;
END $$;
/* 2025-09-29 14:06:17 [81 ms] */ 
CREATE SCHEMA IF NOT EXISTS bronze;
/* 2025-09-29 14:06:17 [81 ms] */ 
CREATE SCHEMA IF NOT EXISTS bronze;
/* 2025-09-29 14:06:20 [94 ms] */ 
CREATE TABLE IF NOT EXISTS bronze.doberman_fetch (
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
/* 2025-09-29 14:06:20 [94 ms] */ 
CREATE TABLE IF NOT EXISTS bronze.doberman_fetch (
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
/* 2025-09-29 14:06:24 [78 ms] */ 
SELECT * FROM bronze.doberman_fetch LIMIT 16;
/* 2025-09-29 14:06:24 [78 ms] */ 
SELECT * FROM bronze.doberman_fetch LIMIT 16;
/* 2025-09-29 14:14:27 [37 ms] */ 
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
        CREATE SCHEMA bronze;
    END IF;
END $$;
/* 2025-09-29 14:14:27 [37 ms] */ 
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
        CREATE SCHEMA bronze;
    END IF;
END $$;
/* 2025-09-29 14:14:30 [80 ms] */ 
DROP TABLE IF EXISTS bronze.doberman_fetch;
/* 2025-09-29 14:14:30 [80 ms] */ 
DROP TABLE IF EXISTS bronze.doberman_fetch;
/* 2025-09-29 14:14:39 [79 ms] */ 
CREATE TABLE IF NOT EXISTS bronze.doberman_fetch_typed (
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
/* 2025-09-29 14:14:39 [79 ms] */ 
CREATE TABLE IF NOT EXISTS bronze.doberman_fetch_typed (
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
/* 2025-09-29 14:14:43 [85 ms] */ 
TRUNCATE TABLE bronze.doberman_fetch_typed;
/* 2025-09-29 14:14:43 [85 ms] */ 
TRUNCATE TABLE bronze.doberman_fetch_typed;
/* 2025-09-29 14:14:50 [80 ms] */ 
SELECT 'done_typed=' || count(*) FROM bronze.doberman_fetch_typed LIMIT 100;
/* 2025-09-29 14:14:50 [80 ms] */ 
SELECT 'done_typed=' || count(*) FROM bronze.doberman_fetch_typed LIMIT 100;
/* 2025-09-29 14:24:40 [317 ms] */ 
DROP TABLE IF EXISTS bronze.doberman_fetch;
/* 2025-09-29 14:24:40 [317 ms] */ 
DROP TABLE IF EXISTS bronze.doberman_fetch;
/* 2025-09-29 14:24:43 [91 ms] */ 
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
/* 2025-09-29 14:24:43 [91 ms] */ 
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
/* 2025-09-29 14:24:46 [75 ms] */ 
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
        CREATE SCHEMA bronze;
    END IF;
END $$;
/* 2025-09-29 14:24:46 [75 ms] */ 
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
        CREATE SCHEMA bronze;
    END IF;
END $$;
/* 2025-09-29 14:24:54 [81 ms] */ 
DROP TABLE IF EXISTS bronze.doberman_fetch;
/* 2025-09-29 14:24:54 [81 ms] */ 
DROP TABLE IF EXISTS bronze.doberman_fetch;
/* 2025-09-29 14:25:01 [46 ms] */ 
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
/* 2025-09-29 14:25:01 [46 ms] */ 
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
/* 2025-09-29 14:27:33 [81 ms] */ 
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
        CREATE SCHEMA bronze;
    END IF;
END $$;
/* 2025-09-29 14:27:33 [81 ms] */ 
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
        CREATE SCHEMA bronze;
    END IF;
END $$;
/* 2025-09-29 14:27:37 [79 ms] */ 
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'bronze' AND tablename = 'doberman_fetch') THEN
        EXECUTE 'DROP TABLE bronze.doberman_fetch';
    END IF;
END $$;
/* 2025-09-29 14:27:37 [79 ms] */ 
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'bronze' AND tablename = 'doberman_fetch') THEN
        EXECUTE 'DROP TABLE bronze.doberman_fetch';
    END IF;
END $$;
/* 2025-09-29 14:28:32 [125 ms] */ 
CREATE SCHEMA IF NOT EXISTS bronze;
/* 2025-09-29 14:28:32 [125 ms] */ 
CREATE SCHEMA IF NOT EXISTS bronze;
/* 2025-09-29 14:28:52 [81 ms] */ 
DROP TABLE IF EXISTS bronze.doberman_fetch;
/* 2025-09-29 14:28:52 [81 ms] */ 
DROP TABLE IF EXISTS bronze.doberman_fetch;
/* 2025-09-29 14:29:01 [51 ms] */ 
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
/* 2025-09-29 14:29:01 [51 ms] */ 
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
/* 2025-09-29 14:32:55 [79 ms] */ 
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
        CREATE SCHEMA bronze;
    END IF;
END $$;
/* 2025-09-29 14:32:55 [79 ms] */ 
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'bronze') THEN
        CREATE SCHEMA bronze;
    END IF;
END $$;
/* 2025-09-29 14:33:03 [89 ms] */ 
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'bronze' AND tablename = 'doberman_fetch') THEN
        EXECUTE 'DROP TABLE bronze.doberman_fetch';
    END IF;
END $$;
/* 2025-09-29 14:33:03 [89 ms] */ 
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'bronze' AND tablename = 'doberman_fetch') THEN
        EXECUTE 'DROP TABLE bronze.doberman_fetch';
    END IF;
END $$;
/* 2025-09-29 14:33:14 [167 ms] */ 
CREATE SCHEMA IF NOT EXISTS bronze;
/* 2025-09-29 14:33:14 [167 ms] */ 
CREATE SCHEMA IF NOT EXISTS bronze;
/* 2025-09-29 14:33:25 [105 ms] */ 
DROP TABLE IF EXISTS bronze.doberman_fetch;
/* 2025-09-29 14:33:25 [105 ms] */ 
DROP TABLE IF EXISTS bronze.doberman_fetch;
