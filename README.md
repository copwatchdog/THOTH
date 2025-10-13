# CopWatchdog

## About

CopWatchdog is an abolitionist data pipeline and media platform tracking NYPD officers' profiles, trials, and public records. We build transparent, community-driven tools for accountability and organizing, grounded in anti-surveillance and anti-capitalist values.

## Project Structure

```
copwatchdog/
├── NYC/
│   ├── BRAIN/
│   │   ├── copwatchdog.csv   # Latest extracted officer data
│   │   ├── main.py           # Main extraction script
│   │   └── VERSIONS/         # Version history of extraction scripts
│   └── CSV/
│       └── FILES/            # CSV files for importing to database
├── scripts/
│   ├── import_to_db.sh       # Database import script
│   └── run_copwatchdog.sh    # Script to run the extraction process
├── copwatchdog.csv           # Master copy of extracted data
├── LICENSE                   # License file
└── README.md                 # This file
```

## Extraction Process

CopWatchdog works by:

1. **Data Collection**: The Python-based scraper (`NYC/BRAIN/main.py`) automatically visits the NYPD Trials public calendar website using a headless Chrome browser.

2. **Table Identification**: The scraper intelligently identifies relevant tables containing officer information by scoring them based on keywords.

3. **Data Extraction**: Once the relevant tables are found, the system extracts officer data including:
   - Trial dates, times, and locations
   - Officer names and ranks
   - Case types and details

4. **Data Enrichment**: Additional officer information is gathered from 50-a.org and other public sources, including:
   - Badge numbers and precinct information
   - Complaint histories and settlement amounts
   - Salary and overtime data

5. **Storage & Import**: The extracted data is saved to CSV files (`copwatchdog.csv` and `NYC/BRAIN/copwatchdog.csv`) and can be imported to a database using the `scripts/import_to_db.sh` script.

## CopWatchDog Community License 1.0 (Simple)

You're free to use, copy, change, and share this software only if:

- You don't use it to make money or run a business that profits from it.
- You don't use it for any police, military, prison, or surveillance work.

If you share or change the software, you have to:

- Keep these rules in place.
- Share your changes with the same rules.

We provide this software as-is, with no promises it works.

If you break these rules, your right to use the software ends.
