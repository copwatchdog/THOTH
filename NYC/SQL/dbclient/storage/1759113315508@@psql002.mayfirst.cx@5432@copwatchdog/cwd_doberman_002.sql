\COPY bronze.doberman_fetch
FROM '/Users/vicstizzi/ART/YUMYODA/TECH/COPWATCHDOG/CSV/copwatchdog.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ',',
    VERBOSE
);