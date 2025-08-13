CopWatchdog
About

CopWatchdog is an abolitionist data pipeline and media platform tracking NYPD officers’ profiles, trials, and public records. We build transparent, community-driven tools for accountability and organizing, grounded in anti-surveillance and anti-capitalist values.
License

CopWatchdog is licensed under the CopWatchDog Community License 1.0, which strictly prohibits use by law enforcement or commercial entities. It protects against commodification and enforces copyleft to keep the software in community hands only. See LICENSE.md for details.
Project Directory Layout (Medallion Architecture)

copwatchdog/
├── data/
│   ├── bronze/              # Raw, immutable scraped/downloaded files (append-only)
│   ├── silver/              # Cleaned, normalized, reproducible tables
│   └── gold/                # Curated, integrated datasets for consumption (append + updates)
├── etl/
│   ├── extract/             # Scrapers & data fetchers
│   ├── transform/           # Cleaning & normalization scripts
│   └── load/                # Integration and load scripts for gold layer
├── db/
│   └── copwatchdog.db       # SQLite database (optional)
├── cli/
│   └── copwatchdog.py       # CLI entrypoint for data operations
├── docs/
│   ├── schema.md            # Data dictionary and schema documentation
│   └── usage.md             # Pipeline usage and CLI guide
├── notebooks/               # Jupyter notebooks for exploration
├── requirements.txt
└── README.md

Medallion Architecture Breakdown
Attribute	Bronze	Silver	Gold
Folder	data/bronze/	data/silver/	data/gold/
Definition	Immutable raw ingests	Cleaned, normalized datasets	Curated, consumption-ready datasets
Objective	Preserve source audit trail	Normalize for QA & joins	Build canonical profiles & events
Object Type	Raw files (PDF, HTML, JSON)	CSV/SQL tables	CSV/SQL tables/views
Load Method	Append-only, timestamped	Full-refresh (overwrite safe)	Incremental append + targeted updates
Data Transformation	None (store as-is)	Cleaning, normalization	Integration, deduplication, enrichment
Data Modeling	None	Flat tables	Flat/simple star schema with keys
Retention	Keep all files, checksum-named	Overwrite silver files	Append-only; maintain change-log table
Access	Developers only (protected)	Developers & analysts	Public, read-only downstream consumers
Audience	Devs, data engineers	Devs, analysts	Organizers, researchers, journalists
Naming Conventions

    Use snake_case lowercase with underscores.

    Avoid SQL reserved words.

    Use source prefix for Bronze and Silver: <source>_<entity> (e.g., nypd_trials, 50a_profiles).

    Use category prefix for Gold tables with domain terms:

        dim_ for dimensions (e.g., dim_officers)

        fact_ for facts/events (e.g., fact_trials)

        agg_ for aggregations (e.g., agg_salaries)

Primary Data Sources

    NYPD Trial Calendar
    Raw monthly trial schedules scraped from the official NYC site.

    50-a.org
    Public officer profiles with misconduct and salary data.

    NYC Open Data - Payroll
    Official structured payroll data filtered for NYPD.

Short procedural rules

    Bronze: Append raw files with checksum metadata; no deletions.

    Silver: Idempotent full-refresh cleaning from Bronze.

    Gold: Append new records; update only on corrections; never truncate. Maintain change-log for provenance.
