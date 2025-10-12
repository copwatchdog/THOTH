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