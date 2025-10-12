-- Transform script: populate bronze.doberman_fetch from bronze.doberman_fetch_raw
-- This runs as a server-side DO block (plpgsql). It will only truncate/insert
-- when the staging table has rows, providing transactional safety and notices.

DO $$
DECLARE
    staging_count integer;
    inserted_count integer;
BEGIN
    SELECT count(*) INTO staging_count FROM bronze.doberman_fetch_raw;
    IF staging_count IS NULL OR staging_count = 0 THEN
        RAISE NOTICE 'No rows in staging (bronze.doberman_fetch_raw); skipping population.';
        RETURN;
    END IF;

    -- Perform populate inside this DO block (single transaction for these operations)
    TRUNCATE TABLE bronze.doberman_fetch;

    INSERT INTO bronze.doberman_fetch (
        date_report, time_report, rank, first, last, room, case_type, badge, pct, pct_url,
        started, last_earned_text, disciplined,
        articles, complaints, allegations, substantiated, charges, unsubstantiated, guidelined, lawsuits,
        total_settlements, status, base_salary, pay_basis,
        regular_hours, regular_gross_paid, ot_hours, total_ot_paid, total_other_pay
    )
    SELECT
        CASE WHEN trim(NULLIF(date,''))<>'' THEN to_date(date,'MM/DD/YYYY') ELSE NULL END,
        CASE WHEN trim(NULLIF(time,''))<>'' AND time ~ '^[0-9]{3,4}$' THEN to_timestamp(time,'HH24MI')::time
                 WHEN trim(NULLIF(time,''))<>'' AND time ~ '^[0-9]{1,2}:[0-9]{2}$' THEN NULLIF(time,'')::time
                 ELSE NULL END,
        rank, first, last, room, case_type, badge, pct, pct_url,
        CASE WHEN trim(NULLIF(started,''))<>'' THEN to_date(started,'MM/DD/YYYY') ELSE NULL END,
        last_earned,
        CASE WHEN upper(trim(NULLIF(disciplined,''))) IN ('Y','TRUE','T') THEN true
                 WHEN upper(trim(NULLIF(disciplined,''))) IN ('N','FALSE','F') THEN false
                 ELSE NULL END,
        NULLIF(regexp_replace(articles,'[^0-9]','','g'),'')::int,
        NULLIF(regexp_replace(complaints,'[^0-9]','','g'),'')::int,
        NULLIF(regexp_replace(allegations,'[^0-9]','','g'),'')::int,
        NULLIF(regexp_replace(substantiated,'[^0-9]','','g'),'')::int,
        NULLIF(regexp_replace(charges,'[^0-9]','','g'),'')::int,
        NULLIF(regexp_replace(unsubstantiated,'[^0-9]','','g'),'')::int,
        NULLIF(regexp_replace(guidelined,'[^0-9]','','g'),'')::int,
        NULLIF(regexp_replace(lawsuits,'[^0-9]','','g'),'')::int,
        NULLIF(regexp_replace(total_settlements,'[^0-9.]','','g'),'')::numeric,
        status,
        NULLIF(regexp_replace(base_salary,'[^0-9.]','','g'),'')::numeric,
        pay_basis,
        NULLIF(regexp_replace(regular_hours,'[^0-9.]','','g'),'')::numeric,
        NULLIF(regexp_replace(regular_gross_paid,'[^0-9.]','','g'),'')::numeric,
        NULLIF(regexp_replace(ot_hours,'[^0-9.]','','g'),'')::numeric,
        NULLIF(regexp_replace(total_ot_paid,'[^0-9.]','','g'),'')::numeric,
        NULLIF(regexp_replace(total_other_pay,'[^0-9.]','','g'),'')::numeric
    FROM bronze.doberman_fetch_raw;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RAISE NOTICE 'Population complete: % rows inserted into bronze.doberman_fetch.', inserted_count;
END
$$ LANGUAGE plpgsql;
