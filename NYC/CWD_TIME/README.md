copwatchdog-scheduler
=====================

This small scheduler contains helper scripts and cron configuration to run
the CopWatchdog scraper on the 1st of every month and retry daily through
the 7th if the data hasn't been published yet.

Files added:
- scripts/run_copwatchdog.sh - wrapper that calls the existing run_copwatchdog.sh and logs
- scripts/retry_until_published.sh - runs wrapper, checks CSV for target month, exits non-zero to allow retries
- scripts/install_cron.sh - installs cron from cron/copwatchdog.cron
- cron/copwatchdog.cron - cron entries for monthly run and daily retries

Install (cron):
1. Inspect/adjust paths in cron/copwatchdog.cron if necessary.
2. Run:

```bash
/path/to/copwatchdog/copwatchdog-scheduler/scripts/install_cron.sh
```

Test a one-off run locally:

```bash
/path/to/copwatchdog/copwatchdog-scheduler/scripts/retry_until_published.sh 2025-10 7
```
# copwatchdog-scheduler

## Overview
The Copwatchdog project is designed to scrape NYPD Trials data, check database connectivity, and import the data into a PostgreSQL database. The project includes scripts for running the scraping process, handling retries if data is not published on expected dates, and setting up scheduled tasks using cron and systemd.

## Project Structure
```
copwatchdog-scheduler
├── scripts
│   ├── run_copwatchdog.sh        # Main script to run the scraping and data import process.
│   ├── retry_until_published.sh   # Script to retry scraping if trials are not found on the first of the month.
│   └── install_cron.sh           # Script to set up cron jobs for scheduled execution.
├── service
│   ├── copwatchdog.service        # Systemd service file for managing the Copwatchdog application.
│   └── copwatchdog.timer          # Systemd timer file to trigger the service on the 1st of every month.
├── cron
│   └── copwatchdog.cron           # Cron job configuration for running the main script and retries.
├── sql
│   └── cwd_doberman_001.sql       # SQL commands for importing data from CSV into PostgreSQL.
├── csv
│   └── copwatchdog.csv            # Data source containing information for the application.
├── scrape
│   └── scrape_bot.py              # Python script responsible for scraping NYPD Trials data.
├── logs
│   └── .gitkeep                   # Keeps the logs directory tracked by Git.
├── README.md                       # Documentation for the project.
└── LICENSE                         # Licensing information for the project.
```

## Setup Instructions
1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd copwatchdog-scheduler
   ```

2. **Install dependencies**:
   Ensure you have Python 3 and the necessary libraries installed for the scraping bot.

3. **Configure database settings**:
   Update the database connection settings in `run_copwatchdog.sh` as needed.

4. **Set up cron jobs**:
   Run the `install_cron.sh` script to set up the cron job that will execute the scraping process on the 1st of every month.

5. **Run the service**:
   Use the systemd service and timer files to manage the application. Enable and start the service using:
   ```bash
   sudo systemctl enable copwatchdog.timer
   sudo systemctl start copwatchdog.timer
   ```

## Usage
- The main script `run_copwatchdog.sh` will be executed automatically based on the cron job or systemd timer.
- If the NYPD Trials data is not published on the expected date, the `retry_until_published.sh` script will handle retries on specified fallback dates.

## License
This project is licensed under the MIT License. See the LICENSE file for more details.