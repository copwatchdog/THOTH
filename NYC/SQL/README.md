README — copwatchdog SQL folder

This folder contains psql-ready query/import scripts for the Copwatchdog project.

Important: the main script `cwd_doberman_001.sql` uses psql client meta-commands (backslash commands such as `\copy`, `\set`, `\echo`, `\if`). You must run it with the `psql` program (not via a GUI that forwards SQL directly to the server).

Quick commands (run from a zsh terminal)

1) Run the import directly with psql (recommended)

Replace `your_user` as appropriate. This reads the CSV from your workstation and uses psql's client-side `\copy`:

```bash
psql -h psql002.mayfirst.cx -p 5432 -U your_user -d copwatchdog \
  -v ON_ERROR_STOP=1 \
  -v csv='/Users/vicstizzi/ART/YUMYODA/TECH/COPWATCHDOG/CSV/copwatchdog.csv' \
  -f /Users/vicstizzi/ART/YUMYODA/TECH/COPWATCHDOG/SQL/cwd_doberman_001.sql
```

If you prefer non-interactive password passing (less secure):

```bash
PGPASSWORD='your_password' psql -h psql002.mayfirst.cx -p 5432 -U your_user -d copwatchdog \
  -v ON_ERROR_STOP=1 \
  -v csv='/Users/vicstizzi/ART/YUMYODA/TECH/COPWATCHDOG/CSV/copwatchdog.csv' \
  -f /Users/vicstizzi/ART/YUMYODA/TECH/COPWATCHDOG/SQL/cwd_doberman_001.sql
```

2) Use the provided wrapper (checks CSV exists and prints table)

The wrapper `run_import_cwd.sh` was created under `/Users/vicstizzi/.dbclient/storage/`. Make it executable and run it (it will run the import then display up to 200 rows from the staging table):

```bash
cd /Users/vicstizzi/.dbclient/storage
chmod +x run_import_cwd.sh
./run_import_cwd.sh /Users/vicstizzi/ART/YUMYODA/TECH/COPWATCHDOG/CSV/copwatchdog.csv psql002.mayfirst.cx 5432 your_user copwatchdog
```

3) Just view the staging table contents after import

```bash
psql -h psql002.mayfirst.cx -p 5432 -U your_user -d copwatchdog -x -P pager=off -c "SELECT * FROM bronze.doberman_fetch LIMIT 200;"
```

Notes / troubleshooting

- If you get an error about `must be superuser or have privileges of the pg_read_server_files role to COPY from a file`, you are using server-side `COPY`. This README and `cwd_doberman_001.sql` use client-side `\copy` which avoids that problem — run with psql from the machine that has the CSV.

- The Database Client extension (`cweijan.vscode-postgresql-client2`) is not the `psql` program. It will forward SQL to the server and does NOT support psql backslash meta-commands. To use the extension GUI you must either:
  - Import the CSV by other means (extension import / upload) and then run the typed-table population statements, or
  - Convert the script to use server-side `COPY` and place the CSV on the DB host and ensure the DB role has `pg_read_server_files` (requires superuser to grant).

- If you want, I can create a GUI-friendly SQL-only version that only runs the typed-table population (assumes staging table already populated), and add it to this folder.

Contact

If you run any of the commands and paste the terminal output here, I will debug the next step (permissions, CSV format issues, or psql errors) and update scripts accordingly.
