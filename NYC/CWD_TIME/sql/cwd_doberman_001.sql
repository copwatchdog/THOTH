-- SQL commands for importing data from the CSV file into the PostgreSQL database

-- Ensure the target table exists
CREATE TABLE IF NOT EXISTS nypd_trials (
    id SERIAL PRIMARY KEY,
    trial_date DATE NOT NULL,
    case_number VARCHAR(255),
    description TEXT,
    location VARCHAR(255)
);

-- Import data from the CSV file
COPY nypd_trials(trial_date, case_number, description, location)
FROM :'csv'
WITH (FORMAT csv, HEADER true);